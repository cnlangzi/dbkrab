package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/alert"
	"github.com/cnlangzi/dbkrab/internal/cdc"
	"github.com/cnlangzi/dbkrab/internal/cdcadmin"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/offset"
	"github.com/cnlangzi/dbkrab/internal/retry"
)

// PollMetrics holds per-poll performance metrics
type PollMetrics struct {
	FetchedChanges    int           // total CDC changes fetched this poll
	ProcessedTx       int           // number of transactions processed
	SyncDurationMs    int64         // time spent in sync (handler + store) in milliseconds
	StoreDurationMs   int64         // time spent in store write in milliseconds
	DLQCount          int           // number of transactions sent to DLQ
	LastPollTime      time.Time     // when the poll cycle started (fetch time)
	LastFetchTime     time.Time     // when changes were fetched (same as LastPollTime)
	LastSyncTime      time.Time     // when sync completed
	LastLSN           string        // LSN after this poll
	SyncTPS           float64       // computed TPS (fetched_changes / sync_duration_seconds)
	EndToEndLatencyMs int64         // end-to-end latency from fetch to sync complete
}

// pollMetricsWindow maintains a sliding window of recent poll metrics
type pollMetricsWindow struct {
	samples    []PollMetrics
	maxSamples int
	mu         sync.Mutex
}

func newPollMetricsWindow(maxSamples int) *pollMetricsWindow {
	return &pollMetricsWindow{
		samples:    make([]PollMetrics, 0, maxSamples),
		maxSamples: maxSamples,
	}
}

func (w *pollMetricsWindow) add(m PollMetrics) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.samples = append(w.samples, m)
	if len(w.samples) > w.maxSamples {
		w.samples = w.samples[1:]
	}
}

func (w *pollMetricsWindow) avgTPS() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.samples) == 0 {
		return 0
	}
	var totalTPS float64
	for _, s := range w.samples {
		totalTPS += s.SyncTPS
	}
	return totalTPS / float64(len(w.samples))
}

func (w *pollMetricsWindow) avgLatencyMs() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.samples) == 0 {
		return 0
	}
	var totalLatency int64
	for _, s := range w.samples {
		totalLatency += s.EndToEndLatencyMs
	}
	return totalLatency / int64(len(w.samples))
}

// Poller polls MSSQL CDC tables for changes
type Poller struct {
	cfg           *config.Config
	db            *sql.DB
	querier       CDCQuerier
	cdcAdmin      *cdcadmin.Admin
	gapDetector   *cdc.GapDetector
	alertManager  *alert.AlertManager
	offsets       offset.StoreInterface
	store        Store
	handler       Handler
	dlq           *dlq.DLQ
	stopCh        chan struct{}
	stopOnce      sync.Once
	paused        bool
	pausedMu      sync.RWMutex
	lastGapCheck  time.Time
	gapCheckMu    sync.RWMutex
	txBuffer      *TransactionBuffer // DEPRECATED: no longer used, transactions handled via processDirect
	
	// Graceful degradation fields
	disconnectStart time.Time
	disconnectMu    sync.RWMutex
	
	// Hot reload fields
	reloadCh      <-chan *config.Config  // Channel for config reload signals
	pendingCfg    *config.Config         // Pending config to apply
	
	// Metrics fields
	metricsMu        sync.RWMutex        // protects metrics
	metrics          PollMetrics          // last poll metrics
	metricsWindow    *pollMetricsWindow   // 1-minute sliding window (~60 samples at 1s interval)
}

// Store interface for storing changes
type Store interface {
	Write(tx *Transaction) error
	Close() error
}

// Handler interface for custom processing
// Handle processes a transaction with the given context for cancellation/timeout.
type Handler interface {
	Handle(ctx context.Context, tx *Transaction) error
}

// CDCQuerier interface for CDC database operations
type CDCQuerier interface {
	GetMinLSN(ctx context.Context, captureInstance string) ([]byte, error)
	GetMaxLSN(ctx context.Context) ([]byte, error)
	IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error)
	GetChanges(ctx context.Context, captureInstance string, tableName string, fromLSN []byte, toLSN []byte) ([]cdc.Change, error)
}

// PluginHandler is a function type for plugin-based handling
type PluginHandler func(ctx context.Context, tx *Transaction) error

// Handle implements Handler interface
func (h PluginHandler) Handle(ctx context.Context, tx *Transaction) error {
	return h(ctx, tx)
}

type tablePollResult struct {
	table       string
	changes     []Change
	lastLSN     LSN
	err         error
}

var ErrNoNewData = errors.New("no new data available for table")

// GetFromLSN returns the starting LSN for CDC queries and whether new data is available.
// It uses the stored offset to determine if there are new changes to fetch.
func (p *Poller) GetFromLSN(ctx context.Context, table string, stored offset.Offset) (fromLSN []byte, hasData bool, err error) {
	// Case 1: Fresh start - no stored offset
	if stored.LSN == "" {
		schema, tableName := cdc.ParseTableName(table)
		captureInstance := cdc.CaptureInstanceName(schema, tableName)
		minLSN, err := p.querier.GetMinLSN(ctx, captureInstance)
		if err != nil {
			return nil, false, fmt.Errorf("get min LSN: %w", err)
		}
		return minLSN, true, nil
	}

	// Case 2: Resume - have stored offset, check if there is new data
	storedLSN, parseErr := ParseLSN(stored.LSN)
	if parseErr != nil || len(storedLSN) == 0 {
		// Invalid stored LSN, treat as fresh start
		schema, tableName := cdc.ParseTableName(table)
		captureInstance := cdc.CaptureInstanceName(schema, tableName)
		minLSN, err := p.querier.GetMinLSN(ctx, captureInstance)
		if err != nil {
			return nil, false, fmt.Errorf("get min LSN: %w", err)
		}
		return minLSN, true, nil
	}

	// Compare incrementLSN(stored) with GetMaxLSN()
	// If incrementLSN(stored) > GetMaxLSN(): no new data
	// If incrementLSN(stored) <= GetMaxLSN(): has new data
	nextLSN, err := p.querier.IncrementLSN(ctx, storedLSN)
	if err != nil {
		return nil, false, fmt.Errorf("increment LSN: %w", err)
	}

	maxLSN, err := p.querier.GetMaxLSN(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("get max LSN: %w", err)
	}

	// Compare: if nextLSN > maxLSN, there is no new data
	if LSN(nextLSN).Compare(LSN(maxLSN)) > 0 {
		// No new data - return error to indicate skip
		return nil, false, nil
	}

	// Has new data - return stored LSN as fromLSN
	return storedLSN, true, nil
}

// NewPoller creates a new poller
func NewPoller(cfg *config.Config, db *sql.DB, store Store, offsetStore offset.StoreInterface, dlqStore *dlq.DLQ) *Poller {
	// Parse SQL Server timezone from config
	mssqlTimezone := config.ParseTimezone(cfg.MSSQL.Timezone)

	poller := &Poller{
		cfg:      cfg,
		db:       db,
		querier:  cdc.NewQuerier(db, mssqlTimezone),
		cdcAdmin: cdcadmin.NewAdmin(&cfg.MSSQL),
		offsets:  offsetStore,
		store:    store,
		dlq:      dlqStore,
		stopCh:   make(chan struct{}),
		metricsWindow: newPollMetricsWindow(60), // ~60 samples for 1-minute window
	}

	// TransactionBuffer is deprecated - not needed with simplified polling
	// Keeping config for backward compatibility but not using it
	if cfg.CDC.TransactionBuffer.Enabled {
		slog.Warn("transaction_buffer is deprecated, not used in simplified polling")
	}

	// Initialize gap detector if CDC protection is enabled
	if cfg.CDC.Gap.Enabled {
		poller.gapDetector = cdc.NewGapDetector(db)
		poller.alertManager = alert.NewAlertManager(cfg.CDC.Gap.Alert)
		slog.Info("CDC gap protection enabled")
	}

	return poller
}

// SetHandler sets a custom handler
func (p *Poller) SetHandler(h Handler) {
	p.handler = h
}

// SetReloadChan sets the config reload channel for hot reload
func (p *Poller) SetReloadChan(ch <-chan *config.Config) {
	p.reloadCh = ch
}

// Start begins polling
func (p *Poller) Start(ctx context.Context) error {
	// Load existing offsets
	if err := p.offsets.Load(); err != nil {
		return fmt.Errorf("load offsets: %w", err)
	}

	// Check and enable CDC for configured tables
	slog.Info("checking CDC status for configured tables", "tables", p.cfg.Tables)
	statuses, err := p.cdcAdmin.CheckAndEnableCDC(p.cfg.Tables)
	if err != nil {
		// Log the error but continue - some tables might still work
		slog.Warn("CDC auto-enable failed (requires DBO privileges)", "error", err)
		slog.Warn("Please ask DBA to enable CDC for the tables, or run with a user that has db_owner role")
	}
	for _, status := range statuses {
		if status.CDCEnabled {
			if status.EnableError != "" {
				slog.Warn("CDC enable attempted but failed (requires DBO privileges)", 
					"table", status.Schema+"."+status.Table, 
					"capture_instance", status.CaptureInstance,
					"error", status.EnableError)
			} else if status.NeedsEnable {
				slog.Info("CDC enabled for table", "table", status.Schema+"."+status.Table, "capture_instance", status.CaptureInstance)
			} else {
				slog.Info("CDC already enabled for table", "table", status.Schema+"."+status.Table, "capture_instance", status.CaptureInstance)
			}
		} else {
			slog.Warn("CDC not enabled for table", "table", status.Schema+"."+status.Table, "capture_instance", status.CaptureInstance)
		}
	}

	interval, err := p.cfg.Interval()
	if err != nil {
		return fmt.Errorf("parse interval: %w", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("starting CDC poller", "interval", interval, "tables", len(p.cfg.Tables))

	// Graceful degradation: track reconnection state
	var reconnectDelay time.Duration
	if p.cfg.GracefulDegradation.Enabled {
		reconnectDelay = p.cfg.ReconnectBaseDelay()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stopCh:
			return nil
		case newCfg := <-p.reloadCh:
			// Config reload signal received
			p.pendingCfg = newCfg
			slog.Info("config reload pending, will apply at next poll cycle")
		case <-ticker.C:
			if p.pendingCfg != nil {
				if err := p.checkAndApplyConfig(ticker); err != nil {
					slog.Error("failed to apply config", "error", err)
				}
			}

			if err := p.poll(ctx); err != nil {
				slog.Error("poll error", "error", err)

				// Graceful degradation: handle MSSQL disconnection
				if p.cfg.GracefulDegradation.Enabled && p.isMSSQLDisconnectError(err) {
					if handled := p.handleDisconnection(ctx, err, reconnectDelay); handled {
						// Reset reconnect delay on success
						reconnectDelay = p.cfg.ReconnectBaseDelay()
						continue
					}
					// Exponential backoff for next retry
					reconnectDelay = min(reconnectDelay*2, p.cfg.ReconnectMaxDelay())
				}

				// Continue polling despite errors
			}

			// Check for pending config after successful poll
			if p.pendingCfg != nil {
				if err := p.checkAndApplyConfig(ticker); err != nil {
					slog.Error("failed to apply config", "error", err)
				}
			}
		}
	}
}

// Stop stops the poller
func (p *Poller) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	// Close transaction buffer
	if p.txBuffer != nil {
		p.txBuffer.Close()
	}
}

// poll performs one polling cycle
// P0 fix: offset is only updated after store successfully writes
// P0 fix: multi-table sync via min LSN checkpoint
// P0-6 fix: CDC queries have timeout to prevent blocking
func (p *Poller) poll(ctx context.Context) error {
	// Check if paused due to gap detection
	if p.isPaused() {
		slog.Warn("poller is paused due to CDC gap detection")
		return nil
	}

	// Metrics: track fetch time
	fetchTime := time.Now()

	// P0-6: Use timeout context for CDC queries to prevent blocking
	const queryTimeout = 10 * time.Second
	const changesTimeout = 10 * time.Second // Separate timeout for GetChanges
	
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// Get max LSN from MSSQL
	_, err := p.querier.GetMaxLSN(queryCtx)
	if err != nil {
		return fmt.Errorf("get max LSN: %w", err)
	}

	// CDC gap detection (before polling)
	// Respect check_interval configuration
	if p.gapDetector != nil && p.shouldCheckGaps() {
		gapCtx, gapCancel := context.WithTimeout(ctx, 5*time.Second)
		defer gapCancel()
		if err := p.checkGaps(gapCtx); err != nil {
			slog.Error("gap detection error", "error", err)
			// Gap detection errors don't block polling, but are logged
		}
		p.recordGapCheck()
	}

	// Poll all tables and collect results (without updating offsets)
	// Uses new GetFromLSN approach: incrementLSN vs GetMaxLSN to determine if new data exists
	results := make([]tablePollResult, 0, len(p.cfg.Tables))

	for _, table := range p.cfg.Tables {
		schema, tableName := cdc.ParseTableName(table)
		captureInstance := cdc.CaptureInstanceName(schema, tableName)

		// Get stored offset
		stored, err := p.offsets.Get(table)
		if err != nil {
			results = append(results, tablePollResult{table: table, err: fmt.Errorf("get offset: %w", err)})
			continue
		}

		// Use GetFromLSN to determine fromLSN and whether new data is available
		// This uses incrementLSN(stored) vs GetMaxLSN() comparison
		fromLSN, hasData, err := p.GetFromLSN(ctx, table, stored)
		if err != nil {
			results = append(results, tablePollResult{table: table, err: err})
			continue
		}

		// No new data - skip this table
		if !hasData || fromLSN == nil {
			slog.Debug("no new data for table, skipping", "table", table)
			continue
		}

		// Get changes since last poll (separate timeout for observability)
		changesCtx, changesCancel := context.WithTimeout(ctx, changesTimeout)
		cdcChanges, err := p.querier.GetChanges(changesCtx, captureInstance, tableName, fromLSN, nil)
		changesCancel()
		if err != nil {
			results = append(results, tablePollResult{table: table, err: err})
			continue
		}

		// Convert CDC changes to internal Change format
		// No need to filter LSN == fromLSN since GetFromLSN guarantees fromLSN is correct
		// No need for hash-based deduplication within batch since INSERT OR REPLACE handles it in store
		var changes []Change
		for _, c := range cdcChanges {
			hashID := ComputeChangeID(c.TransactionID, c.Table, c.Data, c.LSN, Operation(c.Operation))
			changes = append(changes, Change{
				Table:         c.Table,
				TransactionID: c.TransactionID,
				LSN:           c.LSN,
				Operation:     Operation(c.Operation),
				Data:          c.Data,
				CommitTime:    c.CommitTime,
				ID:            hashID,
			})
		}

		// Get last LSN from the last change
		var lastLSN LSN
		if len(changes) > 0 {
			lastLSN = changes[len(changes)-1].LSN
		} else {
			// No changes but hasData=true means there might be changes at this LSN
			// Use fromLSN's increment as the next position
			lastLSN = LSN(fromLSN)
		}

		//nolint:staticcheck // SA4010 false positive - results is used after the loop
		results = append(results, tablePollResult{
			table:   table,
			changes: changes,
			lastLSN: lastLSN,
			err:     nil,
		})
	}

	// Collect all changes from successful polls
	// Collect all changes from successful polls
	var allChanges []Change
	for _, r := range results {
		if r.err == nil && len(r.changes) > 0 {
			allChanges = append(allChanges, r.changes...)
		}
	}

	// Always use processDirect - no transaction buffer needed
	// All changes from the same poll cycle arrive together, simplifying cross-table handling
	if len(allChanges) > 0 {
		return p.processDirect(ctx, allChanges, results, fetchTime)
	}
	// No changes but still update offsets for observability
	return p.updateOffsets(ctx, results, allChanges, fetchTime, time.Duration(0), time.Duration(0), 0, 0)
}

// processDirect processes changes without transaction buffer (legacy behavior)
func (p *Poller) processDirect(ctx context.Context, allChanges []Change, results []tablePollResult, fetchTime time.Time) error {
	syncStartTime := time.Now()

	// Group by transaction ID and deliver
	txs := p.groupByTransaction(allChanges)

	// Track DLQ count and store duration
	dlqCount := 0
	var totalStoreDuration time.Duration

	// Process all transactions
	var processErrors []error
	for _, tx := range txs {
		// Handler processing with retry (non-blocking: continue even if handler fails)
		// Plugin writes to its own sink
		if p.handler != nil {
			var handlerErr error
			err := retry.DoWithName(ctx, func() error {
				handlerErr = p.handler.Handle(ctx, &tx)
				return handlerErr
			}, retry.DefaultRetryConfig(), fmt.Sprintf("handler_tx_%s", tx.ID))
			if err != nil {
				slog.Error("handler error",
					"tx_id", tx.ID,
					"error", err)
				p.writeToDLQ(&tx, err, "handler")
				dlqCount++
			}
		}

		// Store processing with retry (blocking: must succeed for offset advancement)
		if p.store != nil {
			storeStart := time.Now()
			err := retry.DoWithName(ctx, func() error {
				return p.store.Write(&tx)
			}, retry.DefaultRetryConfig(), fmt.Sprintf("store_tx_%s", tx.ID))
			storeDuration := time.Since(storeStart)
			totalStoreDuration += storeDuration
			if err != nil {
				slog.Error("store error",
					"tx_id", tx.ID,
					"error", err)
				p.writeToDLQ(&tx, err, "store")
				processErrors = append(processErrors, fmt.Errorf("store tx %s: %w", tx.ID, err))
				dlqCount++
			}
		}
	}

	// P0 fix: Only update offsets after successful store write
	if len(processErrors) > 0 {
		return fmt.Errorf("store errors: %v", processErrors)
	}

	syncEndTime := time.Now()
	syncDuration := syncEndTime.Sub(syncStartTime)

	// All transactions successfully written - now update offsets
	return p.updateOffsets(ctx, results, allChanges, fetchTime, syncDuration, totalStoreDuration, dlqCount, len(txs))
}

// updateOffsets updates offset checkpoints for successfully polled tables
// Each table maintains its own independent LSN offset.
// Only tables with changes have their offsets advanced.
// Tables with no changes keep their existing offset.
func (p *Poller) updateOffsets(ctx context.Context, results []tablePollResult, allChanges []Change, fetchTime time.Time, syncDuration time.Duration, storeDuration time.Duration, dlqCount int, txCount int) error {
	// Track the maximum LSN across all tables for observability/metrics
	var maxLSN LSN
	tablesUpdated := 0

	// Update each table's offset independently based on its own lastLSN
	for _, r := range results {
		if r.err != nil {
			// Table had an error - don't update its offset
			slog.Warn("skipping offset update for table with error",
				"table", r.table,
				"error", r.err)
			continue
		}

		if len(r.changes) == 0 {
			// No changes for this table - keep existing offset
			slog.Debug("no changes for table, keeping existing offset", "table", r.table)
			continue
		}

		// Table has changes - advance its offset to the LSN after the last change.
		// MSSQL CDC functions return rows where __$start_lsn >= from_lsn,
		// so we must store incrementLSN(lastLSN) to avoid re-fetching the same row.
		nextLSN, err := p.querier.IncrementLSN(ctx, []byte(r.lastLSN))
		if err != nil {
			slog.Error("failed to increment LSN for offset", "table", r.table, "lastLSN", r.lastLSN.String(), "error", err)
			continue
		}
		if err := p.offsets.Set(r.table, LSN(nextLSN).String()); err != nil {
			slog.Error("failed to save offset", "table", r.table, "error", err)
			continue
		}
		tablesUpdated++

		// Track max LSN for observability
		if maxLSN.IsZero() || r.lastLSN.Compare(maxLSN) > 0 {
			maxLSN = r.lastLSN
		}

		slog.Debug("advanced table offset",
			"table", r.table,
			"lsn", r.lastLSN.String(),
			"changes", len(r.changes))
	}

	if tablesUpdated > 0 {
		slog.Info("advanced offsets for tables",
			"tables_updated", tablesUpdated,
			"max_lsn", maxLSN.String())
	}

	// Use max LSN for observability state (not for checkpointing)
	lastLSN := ""
	if !maxLSN.IsZero() {
		lastLSN = maxLSN.String()
	}

	// Always update poller state for observability
	if sqliteStore, ok := p.store.(interface{ UpdatePollerState(string, int) error }); ok {
		slog.Debug("updating poller state", "lastLSN", lastLSN, "changes", len(allChanges))
		if err := sqliteStore.UpdatePollerState(lastLSN, len(allChanges)); err != nil {
			slog.Warn("failed to update poller state", "error", err)
		}
	} else {
		slog.Debug("store does not support UpdatePollerState")
	}

	// Record metrics and emit summary log if changes were fetched
	if len(allChanges) > 0 {
		syncEndTime := time.Now()
		endToEndLatency := syncEndTime.Sub(fetchTime)
		
		// Compute TPS: fetched_changes / sync_duration_seconds
		var syncTPS float64
		if syncDuration.Seconds() > 0 {
			syncTPS = float64(len(allChanges)) / syncDuration.Seconds()
		}

		// Update metrics window
		pm := PollMetrics{
			FetchedChanges:    len(allChanges),
			ProcessedTx:       txCount,
			SyncDurationMs:    syncDuration.Milliseconds(),
			StoreDurationMs:   storeDuration.Milliseconds(),
			DLQCount:          dlqCount,
			LastPollTime:      fetchTime,
			LastFetchTime:     fetchTime,
			LastSyncTime:      syncEndTime,
			LastLSN:           lastLSN,
			SyncTPS:           syncTPS,
			EndToEndLatencyMs: endToEndLatency.Milliseconds(),
		}
		p.metricsWindow.add(pm)
		p.metricsMu.Lock()
		p.metrics = pm
		p.metricsMu.Unlock()

		// Emit structured INFO summary log for non-empty polls
		slog.Info("[poll cycle]",
			"cdc_fetched", len(allChanges),
			"tx_count", txCount,
			"sync_tps", fmt.Sprintf("%.1f", syncTPS),
			"sync_ms", syncDuration.Milliseconds(),
			"store_ms", storeDuration.Milliseconds(),
			"dlq", dlqCount,
			"lsn", lastLSN,
		)
	} else {
		// Empty poll - Debug level only
		slog.Debug("[poll cycle] no changes fetched")
	}

	return nil
}

// isMSSQLDisconnectError checks if the error is related to MSSQL disconnection
func (p *Poller) isMSSQLDisconnectError(err error) bool {
	if err == nil {
		return false
	}

	// Normalize error string for case-insensitive matching
	errStr := strings.ToLower(err.Error())

	// Common MSSQL disconnection errors
	disconnectPatterns := []string{
		"connection reset",
		"connection closed",
		"broken pipe",
		"eof",
		"i/o timeout",
		"context deadline exceeded",
		"no connection",
		"network related",
	}

	for _, pattern := range disconnectPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// handleDisconnection handles MSSQL disconnection with graceful degradation
func (p *Poller) handleDisconnection(ctx context.Context, err error, reconnectDelay time.Duration) bool {
	// Record Disconnect start time
	p.disconnectMu.Lock()
	if p.disconnectStart.IsZero() {
		p.disconnectStart = time.Now()
		slog.Warn("MSSQL disconnection detected, entering reconnection mode", 
			"error", err)
	}
	p.disconnectMu.Unlock()

	// Check if exceeded max disconnect duration
	maxDuration := p.cfg.MaxDisconnectDuration()

	p.disconnectMu.RLock()
	disconnectDuration := time.Since(p.disconnectStart)
	p.disconnectMu.RUnlock()

	if disconnectDuration > maxDuration {
		slog.Error("MSSQL disconnection exceeded maximum duration, alerting",
			"duration", disconnectDuration,
			"max_duration", maxDuration)
		// Alert if alert manager is available
		if p.alertManager != nil {
			p.alertManager.SendEmergency(
				"MSSQL Disconnection Timeout",
				cdc.GapInfo{
					Table:     "ALL",
					CheckedAt: time.Now(),
				},
			)
		}
		return false
	}

	// Wait for reconnect delay
	slog.Info("waiting before reconnection attempt",
		"delay", reconnectDelay,
		"disconnect_duration", disconnectDuration)

	select {
	case <-ctx.Done():
		return false
	case <-p.stopCh:
		return false
	case <-time.After(reconnectDelay):
		// Try to reconnect by testing the connection
		if p.testConnection(ctx) {
			slog.Info("MSSQL reconnection successful",
				"disconnect_duration", time.Since(p.disconnectStart))
			// Reset disconnect start
			p.disconnectMu.Lock()
			p.disconnectStart = time.Time{}
			p.disconnectMu.Unlock()
			return true
		}
		return false
	}
}

// testConnection tests if MSSQL connection is restored
func (p *Poller) testConnection(ctx context.Context) bool {
	_, err := p.querier.GetMaxLSN(ctx)
	return err == nil
}

// min returns the minimum of two durations
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// groupByTransaction groups changes by transaction ID
// It also defensively filters out OpUpdateBefore changes which should not reach
// the sink writer or internal store. UPDATE_BEFORE rows are silently dropped
// to prevent "unknown operation type: UPDATE_BEFORE" errors.
func (p *Poller) groupByTransaction(changes []Change) []Transaction {
	txMap := make(map[string]*Transaction)

	for _, c := range changes {
		// Defensively filter out UPDATE_BEFORE changes - they should not be
		// captured when using net_changes mode, but we filter as a safeguard
		// to prevent DLQ errors from reaching the sink writer.
		if c.Operation == OpUpdateBefore {
			slog.Debug("groupByTransaction: silently dropping UPDATE_BEFORE change",
				"table", c.Table,
				"tx_id", c.TransactionID,
				"lsn", fmt.Sprintf("%x", c.LSN))
			continue
		}

		tx, exists := txMap[c.TransactionID]
		if !exists {
			tx = NewTransaction(c.TransactionID)
			txMap[c.TransactionID] = tx
		}
		tx.AddChange(c)
	}

	// Convert map to slice
	result := make([]Transaction, 0, len(txMap))
	for _, tx := range txMap {
		result = append(result, *tx)
	}

	return result
}

// checkGaps checks for CDC gaps across all monitored tables
func (p *Poller) checkGaps(ctx context.Context) error {
	warningLagBytes := p.cfg.CDC.Gap.WarningLagBytes
	criticalLagBytes := p.cfg.CDC.Gap.CriticalLagBytes

	// Parse duration thresholds with explicit error handling
	warningLagDuration, err := p.cfg.WarningLagDuration()
	if err != nil {
		slog.Warn("invalid warning_lag_duration config, using default 1h", "error", err)
		warningLagDuration = 1 * time.Hour
	}
	criticalLagDuration, err := p.cfg.CriticalLagDuration()
	if err != nil {
		slog.Warn("invalid critical_lag_duration config, using default 6h", "error", err)
		criticalLagDuration = 6 * time.Hour
	}

	for _, table := range p.cfg.Tables {
		schema, tableName := cdc.ParseTableName(table)
		captureInstance := cdc.CaptureInstanceName(schema, tableName)

		// Get current LSN from offset
		currentLSN := []byte{}
		stored, err := p.offsets.Get(table)
		if err == nil && stored.LSN != "" {
			parsed, parseErr := ParseLSN(stored.LSN)
			if parseErr == nil && len(parsed) > 0 {
				currentLSN = []byte(parsed)
			}
		}

		// Check for gaps
		gap, err := p.gapDetector.CheckGap(ctx, tableName, captureInstance, currentLSN)
		if err != nil {
			slog.Error("gap check error", "table", table, "error", err)
			continue
		}

		// Handle gap based on severity
		if gap.HasGap {
			// Data loss detected - emergency
			slog.Error("CDC data loss detected",
				"table", table,
				"missing_lsn_start", fmt.Sprintf("%X", gap.MissingLSNRange.Start),
				"missing_lsn_end", fmt.Sprintf("%X", gap.MissingLSNRange.End))
			p.alertManager.SendEmergency("CDC Data Loss Detected", gap)
			p.pause()
			return fmt.Errorf("CDC data loss for %s", table)
		}

		if gap.IsGapCritical(criticalLagBytes, criticalLagDuration) {
			// Critical lag
			slog.Error("CDC lag critical",
				"table", table,
				"lag_bytes", gap.LagBytes,
				"lag_duration", gap.LagDuration)
			p.alertManager.SendCritical("CDC Lag Critical", gap)
			// Continue polling but alert
		}

		if gap.IsGapWarning(warningLagBytes, warningLagDuration) {
			// Warning lag
			slog.Warn("CDC lag warning",
				"table", table,
				"lag_bytes", gap.LagBytes,
				"lag_duration", gap.LagDuration)
			p.alertManager.SendWarning("CDC Lag Warning", gap)
		}
	}

	return nil
}

// pause pauses the poller
func (p *Poller) pause() {
	p.pausedMu.Lock()
	defer p.pausedMu.Unlock()
	p.paused = true
	slog.Warn("poller paused")
}

// isPaused returns true if the poller is paused
func (p *Poller) isPaused() bool {
	p.pausedMu.RLock()
	defer p.pausedMu.RUnlock()
	return p.paused
}

// shouldCheckGaps returns true if enough time has passed since last gap check
func (p *Poller) shouldCheckGaps() bool {
	interval, err := p.cfg.CDCCheckInterval()
	if err != nil {
		// Invalid config, check immediately
		return true
	}
	if interval <= 0 {
		// Disabled or invalid, check immediately
		return true
	}

	p.gapCheckMu.RLock()
	lastCheck := p.lastGapCheck
	p.gapCheckMu.RUnlock()

	return time.Since(lastCheck) >= interval
}

// recordGapCheck records the current time as the last gap check time
func (p *Poller) recordGapCheck() {
	p.gapCheckMu.Lock()
	p.lastGapCheck = time.Now()
	p.gapCheckMu.Unlock()
}

// writeToDLQ writes a failed transaction to the dead letter queue
func (p *Poller) writeToDLQ(tx *Transaction, err error, source string) {
	if p.dlq == nil {
		slog.Warn("cannot write to DLQ: not initialized",
			"trace_id", tx.TraceID,
			"tx_id", tx.ID)
		return
	}

	// Determine retry count from error if it's a RetryError
	retryCount := 0
	var retryErr *retry.RetryError
	if errors.As(err, &retryErr) {
		retryCount = retryErr.RetryCount
	}

	// Get the first change to extract table name and operation
	var tableName, operation string
	if len(tx.Changes) > 0 {
		tableName = tx.Changes[0].Table
		operation = tx.Changes[0].Operation.String()
	}

	// Encode transaction data as JSON
	txData := map[string]interface{}{
		"transaction_id": tx.ID,
		"changes":        tx.Changes,
	}
	changeJSON, encodeErr := json.Marshal(txData)
	if encodeErr != nil {
		slog.Error("failed to encode transaction data",
			"trace_id", tx.TraceID,
			"tx_id", tx.ID,
			"error", encodeErr)
		changeJSON = []byte("{}")
	}

	// Get LSN from first change
	var lsn string
	if len(tx.Changes) > 0 {
		lsn = fmt.Sprintf("%v", tx.Changes[0].LSN)
	}

	entry := &dlq.DLQEntry{
		TraceID:      tx.TraceID,
		Source:       source,
		LSN:          lsn,
		TableName:    tableName,
		Operation:    operation,
		ChangeData:   string(changeJSON),
		ErrorMessage: fmt.Sprintf("%s error: %v", source, err),
		RetryCount:   retryCount,
		Status:       dlq.StatusPending,
	}

	if writeErr := p.dlq.Write(entry); writeErr != nil {
		slog.Error("failed to write DLQ entry",
			"trace_id", tx.TraceID,
			"tx_id", tx.ID,
			"error", writeErr)
		return
	}

	// Send alert
	if p.alertManager != nil {
		gapInfo := cdc.GapInfo{
			Table:       tableName,
			LagBytes:    0,
			LagDuration: 0,
			CheckedAt:   time.Now(),
		}
		p.alertManager.SendWarning(
			fmt.Sprintf("Transaction %s (trace: %s) written to DLQ", tx.ID, tx.TraceID),
			gapInfo,
		)
	}

	slog.Warn("transaction written to DLQ",
		"trace_id", tx.TraceID,
		"tx_id", tx.ID,
		"table", tableName,
		"operation", operation,
		"retries", retryCount)
}

// checkAndApplyConfig checks if config can be applied and applies it at transaction boundary
func (p *Poller) checkAndApplyConfig(ticker *time.Ticker) error {
	if p.pendingCfg == nil {
		return nil
	}

	newCfg := p.pendingCfg

	// No need to check transaction_buffer changes - not used in simplified polling

	// Safe to apply config now
	slog.Info("applying config changes", "tables", len(newCfg.Tables), "interval", newCfg.CDC.Interval)

	// Apply polling_interval change
	if newCfg.CDC.Interval != p.cfg.CDC.Interval {
		newInterval, err := newCfg.Interval()
		if err != nil {
			slog.Warn("invalid polling_interval in new config", "error", err)
		} else {
			ticker.Reset(newInterval)
			slog.Info("polling interval updated", "new_interval", newInterval)
		}
	}

	// Apply tables change (add/remove from polling list)
	if !tablesEqual(p.cfg.Tables, newCfg.Tables) {
		slog.Info("tables list changed", "old_count", len(p.cfg.Tables), "new_count", len(newCfg.Tables))
		// Tables are read from p.cfg.Tables in poll(), so just update the reference
	}

	// Apply cdc_protection thresholds (safe to update immediately)
	p.cfg.CDC.Gap = newCfg.CDC.Gap
	slog.Debug("cdc_protection thresholds updated")

	// Apply graceful_degradation params (safe to update immediately)
	p.cfg.GracefulDegradation = newCfg.GracefulDegradation
	slog.Debug("graceful_degradation params updated")

	// No transaction buffer rebuild needed - deprecated

	// Update main config reference
	p.cfg = newCfg
	p.pendingCfg = nil

	slog.Info("config reload complete")
	return nil
}

// tablesEqual checks if two table lists are equal (order-independent)
func tablesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	// Use map as set for order-independent comparison
	setA := make(map[string]struct{}, len(a))
	for _, s := range a {
		setA[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := setA[s]; !ok {
			return false
		}
	}
	return true
}


// GetMetrics returns the current poll metrics and 1-minute window averages
func (p *Poller) GetMetrics() map[string]interface{} {
	p.metricsMu.RLock()
	m := p.metrics
	p.metricsMu.RUnlock()
	
	// Get window averages
	avgTPS := p.metricsWindow.avgTPS()
	avgLatency := p.metricsWindow.avgLatencyMs()
	
	return map[string]interface{}{
		"last_fetched":        m.FetchedChanges,
		"last_processed_tx":   m.ProcessedTx,
		"last_sync_tps":       m.SyncTPS,
		"last_sync_duration_ms": m.SyncDurationMs,
		"last_dlq_count":      m.DLQCount,
		"avg_tps_1m":          avgTPS,
		"avg_latency_1m_ms":   avgLatency,
		"last_poll_time":      m.LastPollTime,
		"last_lsn":            m.LastLSN,
	}
}
