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

// Poller polls MSSQL CDC tables for changes
type Poller struct {
	cfg           *config.Config
	db            *sql.DB
	querier       *cdc.Querier
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
	txBuffer      *TransactionBuffer // For cross-table transaction integrity
	
	// Graceful degradation fields
	disconnectStart time.Time
	disconnectMu    sync.RWMutex
	
	// Hot reload fields
	reloadCh      <-chan *config.Config  // Channel for config reload signals
	pendingCfg    *config.Config         // Pending config to apply
	needDrain     bool                   // Signal to drain txBuffer before rebuild
	needRebuildTx bool                   // Signal to rebuild txBuffer after drain
}

// Store interface for storing changes
type Store interface {
	Write(tx *Transaction) error
	WriteOps(ops []Sink) error
	Close() error
}

// Handler interface for custom processing
// Handle returns transformed operations for the caller to write via Store
type Handler interface {
	Handle(tx *Transaction) ([]Sink, error)
}

// PluginHandler is a function type for plugin-based handling
type PluginHandler func(tx *Transaction) ([]Sink, error)

// Handle implements Handler interface
func (h PluginHandler) Handle(tx *Transaction) ([]Sink, error) {
	return h(tx)
}

type tablePollResult struct {
	table    string
	changes  []Change
	lastLSN  LSN
	err      error
}

// NewPoller creates a new poller
func NewPoller(cfg *config.Config, db *sql.DB, store Store, offsetStore offset.StoreInterface, dlqStore *dlq.DLQ) *Poller {
	poller := &Poller{
		cfg:      cfg,
		db:       db,
		querier:  cdc.NewQuerier(db),
		cdcAdmin: cdcadmin.NewAdmin(&cfg.MSSQL),
		offsets:  offsetStore,
		store:    store,
		dlq:      dlqStore,
		stopCh:   make(chan struct{}),
	}

	// Initialize transaction buffer if enabled
	if cfg.TransactionBuffer.Enabled {
		maxWaitTime, err := time.ParseDuration(cfg.TransactionBuffer.MaxWaitTime)
		if err != nil {
			slog.Warn("invalid transaction_buffer.max_wait_time, using default 30s", "error", err)
			maxWaitTime = 30 * time.Second
		}
		poller.txBuffer = NewTransactionBuffer(
			maxWaitTime,
			cfg.TransactionBuffer.MaxTransactionsPerBatch,
			cfg.TransactionBuffer.MaxBatchBytes,
		)
		slog.Info("transaction buffer enabled",
			"max_wait_time", maxWaitTime,
			"max_transactions_per_batch", cfg.TransactionBuffer.MaxTransactionsPerBatch,
			"max_batch_bytes", cfg.TransactionBuffer.MaxBatchBytes)
	}

	// Initialize gap detector if CDC protection is enabled
	if cfg.CDCProtection.Enabled {
		poller.gapDetector = cdc.NewGapDetector(db)
		poller.alertManager = alert.NewAlertManager(cfg.CDCProtection.Alert)
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

	interval, err := p.cfg.PollingInterval()
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
			// Flush buffer before exit
			p.flushBuffer(ctx)
			return ctx.Err()
		case <-p.stopCh:
			// Flush buffer before stop
			p.flushBuffer(ctx)
			return nil
		case newCfg := <-p.reloadCh:
			// Config reload signal received
			p.pendingCfg = newCfg
			slog.Info("config reload pending, will apply at transaction boundary")
		case <-ticker.C:
			// Check if need to drain buffer before applying config
			if p.needDrain {
				if p.txBuffer != nil && !p.txBuffer.IsEmpty() {
					// Skip polling, wait for buffer to drain
					slog.Debug("waiting for txBuffer to drain before applying config")
					continue
				}
				// Buffer is empty, safe to apply config
				if err := p.checkAndApplyConfig(ticker); err != nil {
					slog.Error("failed to apply config", "error", err)
				}
				continue
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

			// Check for pending config after successful poll (at transaction boundary)
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

	// P0-6: Use timeout context for CDC queries to prevent blocking
	const queryTimeout = 10 * time.Second
	const minLSNTimeout = 5 * time.Second // Separate timeout for GetMinLSN
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
	results := make([]tablePollResult, 0, len(p.cfg.Tables))

	for _, table := range p.cfg.Tables {
		schema, tableName := cdc.ParseTableName(table)
		captureInstance := cdc.CaptureInstanceName(schema, tableName)

		// Get starting LSN from offset store (no DB query, no timeout needed)
		startLSN := LSN{}
		stored, err := p.offsets.Get(table)
		if err == nil && stored.LSN != "" {
			parsed, parseErr := ParseLSN(stored.LSN)
			if parseErr == nil && len(parsed) > 0 {
				startLSN = parsed
			}
		}

		// P0-6: Separate timeouts for GetMinLSN and GetChanges
		// If first time, get min LSN from CDC
		if len(startLSN) == 0 || startLSN.IsZero() {
			minLSNCtx, minLSNCancel := context.WithTimeout(ctx, minLSNTimeout)
			minLSN, err := p.querier.GetMinLSN(minLSNCtx, captureInstance)
			minLSNCancel()
			if err != nil {
				results = append(results, tablePollResult{table: table, err: err})
				continue
			}
			startLSN = minLSN
		}

		// Get changes since last poll (separate timeout for observability)
		changesCtx, changesCancel := context.WithTimeout(ctx, changesTimeout)
		cdcChanges, err := p.querier.GetChanges(changesCtx, captureInstance, startLSN, nil)
		changesCancel()
		if err != nil {
			results = append(results, tablePollResult{table: table, err: err})
			continue
		}

		// Convert cdc.Change to core.Change
		changes := make([]Change, len(cdcChanges))
		for i, c := range cdcChanges {
			changes[i] = Change{
				Table:         c.Table,
				TransactionID: c.TransactionID,
				LSN:           c.LSN,
				Operation:     Operation(c.Operation),
				Data:          c.Data,
				CommitTime:    c.CommitTime,
			}
		}

		// Get last LSN from the last change or use max LSN
		var lastLSN LSN
		if len(changes) > 0 {
			lastLSN = changes[len(changes)-1].LSN
		} else {
			lastLSN = startLSN
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
	var allChanges []Change
	var validResults []tablePollResult
	for _, r := range results {
		if r.err == nil && len(r.changes) > 0 {
			allChanges = append(allChanges, r.changes...)
			validResults = append(validResults, r)
		}
	}

	// If transaction buffer is enabled, use it for cross-table integrity
	if p.txBuffer != nil {
		if len(allChanges) > 0 {
			return p.processWithBuffer(ctx, allChanges, results)
		}
		// No changes but still update offsets for observability
		return p.updateOffsets(results, allChanges)
	}

	// Otherwise, use legacy direct processing
	if len(allChanges) > 0 {
		return p.processDirect(ctx, allChanges, results)
	}
	// No changes but still update offsets for observability
	return p.updateOffsets(results, allChanges)
}

// processWithBuffer processes changes using transaction buffer for cross-table integrity
func (p *Poller) processWithBuffer(ctx context.Context, allChanges []Change, results []tablePollResult) error {

	// Add all changes to buffer
	for _, change := range allChanges {
		p.txBuffer.Add(change)
	}

	// Get complete transactions (timed out)
	completeTxs := p.txBuffer.GetCompleteTransactions()
	if len(completeTxs) == 0 {
		return nil
	}

	// Process complete transactions
	var processErrors []error
	for _, tx := range completeTxs {
		var sinkOps []Sink

		// Handler processing with retry
		if p.handler != nil {
			var handlerErr error
			err := retry.DoWithName(ctx, func() error {
				sinkOps, handlerErr = p.handler.Handle(tx)
				return handlerErr
			}, retry.DefaultRetryConfig(), fmt.Sprintf("handler_tx_%s", tx.ID))
			if err != nil {
				slog.Error("handler error",
					"trace_id", tx.TraceID,
					"tx_id", tx.ID,
					"error", err)
				p.writeToDLQ(tx, err, "handler")
			}
		}

		// Store processing with retry
		if p.store != nil {
			err := retry.DoWithName(ctx, func() error {
				return p.store.Write(tx)
			}, retry.DefaultRetryConfig(), fmt.Sprintf("store_tx_%s", tx.ID))
			if err != nil {
				slog.Error("store error",
					"trace_id", tx.TraceID,
					"tx_id", tx.ID,
					"error", err)
				p.writeToDLQ(tx, err, "store")
				processErrors = append(processErrors, fmt.Errorf("store tx %s: %w", tx.ID, err))
			}

			// Write transformed DataSets from handler
			if len(sinkOps) > 0 {
				err := retry.DoWithName(ctx, func() error {
					return p.store.WriteOps(sinkOps)
				}, retry.DefaultRetryConfig(), fmt.Sprintf("store_ops_tx_%s", tx.ID))
				if err != nil {
					slog.Error("store ops error",
						"trace_id", tx.TraceID,
						"tx_id", tx.ID,
						"error", err)
					p.writeToDLQ(tx, err, "store_ops")
					processErrors = append(processErrors, fmt.Errorf("store ops tx %s: %w", tx.ID, err))
				}
			}
		}
	}

	// Update offsets only if all transactions succeeded
	if len(processErrors) > 0 {
		return fmt.Errorf("store errors: %v", processErrors)
	}

	// Update offsets (same logic as processDirect)
	return p.updateOffsets(results, allChanges)
}

// processDirect processes changes without transaction buffer (legacy behavior)
func (p *Poller) processDirect(ctx context.Context, allChanges []Change, results []tablePollResult) error {

	// Group by transaction ID and deliver
	txs := p.groupByTransaction(allChanges)

	// Process all transactions
	var processErrors []error
	for _, tx := range txs {
		var sinkOps []Sink

		// Handler processing with retry (non-blocking: continue even if handler fails)
		if p.handler != nil {
			var handlerErr error
			err := retry.DoWithName(ctx, func() error {
				sinkOps, handlerErr = p.handler.Handle(&tx)
				return handlerErr
			}, retry.DefaultRetryConfig(), fmt.Sprintf("handler_tx_%s", tx.ID))
			if err != nil {
				slog.Error("handler error",
					"tx_id", tx.ID,
					"error", err)
				p.writeToDLQ(&tx, err, "handler")
			}
		}

		// Store processing with retry (blocking: must succeed for offset advancement)
		if p.store != nil {
			err := retry.DoWithName(ctx, func() error {
				return p.store.Write(&tx)
			}, retry.DefaultRetryConfig(), fmt.Sprintf("store_tx_%s", tx.ID))
			if err != nil {
				slog.Error("store error",
					"tx_id", tx.ID,
					"error", err)
				p.writeToDLQ(&tx, err, "store")
				processErrors = append(processErrors, fmt.Errorf("store tx %s: %w", tx.ID, err))
			}

			// Write transformed DataSets from handler
			if len(sinkOps) > 0 {
				err := retry.DoWithName(ctx, func() error {
					return p.store.WriteOps(sinkOps)
				}, retry.DefaultRetryConfig(), fmt.Sprintf("store_ops_tx_%s", tx.ID))
				if err != nil {
					slog.Error("store ops error",
						"tx_id", tx.ID,
						"error", err)
					p.writeToDLQ(&tx, err, "store_ops")
					processErrors = append(processErrors, fmt.Errorf("store ops tx %s: %w", tx.ID, err))
				}
			}
		}
	}

	// P0 fix: Only update offsets after successful store write
	if len(processErrors) > 0 {
		return fmt.Errorf("store errors: %v", processErrors)
	}

	// All transactions successfully written - now update offsets
	return p.updateOffsets(results, allChanges)
}

// updateOffsets updates offset checkpoints for successfully polled tables
func (p *Poller) updateOffsets(results []tablePollResult, allChanges []Change) error {
	// Build set of successfully polled tables
	successfulTables := make(map[string]bool)
	for _, r := range results {
		if r.err == nil {
			successfulTables[r.table] = true
		}
	}

	// Find minimum lastLSN across all tables that had changes
	var validResults []tablePollResult
	for _, r := range results {
		if r.err == nil && len(r.changes) > 0 {
			validResults = append(validResults, r)
		}
	}

	// Update poller state in store for observability (even if no changes)
	var lastLSN string
	if len(validResults) > 0 {
		minLSN := validResults[0].lastLSN
		for _, r := range validResults[1:] {
			if r.lastLSN.Compare(minLSN) < 0 {
				minLSN = r.lastLSN
			}
		}

		// Only advance offsets for successfully polled tables
		for table := range successfulTables {
			if err := p.offsets.Set(table, minLSN.String()); err != nil {
				slog.Error("failed to save offset", "table", table, "error", err)
			}
		}

		slog.Info("advanced global checkpoint",
			"lsn", minLSN.String(),
			"tables", len(successfulTables))

		lastLSN = minLSN.String()
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
func (p *Poller) groupByTransaction(changes []Change) []Transaction {
	txMap := make(map[string]*Transaction)

	for _, c := range changes {
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
	warningLagBytes := p.cfg.CDCProtection.WarningLagBytes
	criticalLagBytes := p.cfg.CDCProtection.CriticalLagBytes

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
		TraceID:     tx.TraceID,
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

	// Check if transaction_buffer change requires drain
	if newCfg.TransactionBuffer.Enabled != p.cfg.TransactionBuffer.Enabled ||
		newCfg.TransactionBuffer.MaxWaitTime != p.cfg.TransactionBuffer.MaxWaitTime ||
		newCfg.TransactionBuffer.MaxTransactionsPerBatch != p.cfg.TransactionBuffer.MaxTransactionsPerBatch ||
		newCfg.TransactionBuffer.MaxBatchBytes != p.cfg.TransactionBuffer.MaxBatchBytes {
		// Need to drain buffer before applying
		if p.txBuffer != nil && !p.txBuffer.IsEmpty() {
			slog.Info("transaction_buffer changed, waiting for buffer to drain")
			p.needDrain = true
			p.needRebuildTx = true
			// Will apply on next cycle after drain
			return nil
		}
	}

	// Safe to apply config now
	slog.Info("applying config changes", "tables", len(newCfg.Tables), "interval", newCfg.Interval)

	// Apply polling_interval change
	if newCfg.Interval != p.cfg.Interval {
		newInterval, err := newCfg.PollingInterval()
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
	p.cfg.CDCProtection = newCfg.CDCProtection
	slog.Debug("cdc_protection thresholds updated")

	// Apply graceful_degradation params (safe to update immediately)
	p.cfg.GracefulDegradation = newCfg.GracefulDegradation
	slog.Debug("graceful_degradation params updated")

	// Handle transaction_buffer rebuild if needed
	if p.needRebuildTx {
		if err := p.rebuildTxBuffer(newCfg); err != nil {
			slog.Error("failed to rebuild txBuffer", "error", err)
			return err
		}
		p.needRebuildTx = false
		p.needDrain = false
	}

	// Update main config reference
	p.cfg = newCfg
	p.pendingCfg = nil

	slog.Info("config reload complete")
	return nil
}

// rebuildTxBuffer rebuilds the transaction buffer with new config
func (p *Poller) rebuildTxBuffer(newCfg *config.Config) error {
	if !newCfg.TransactionBuffer.Enabled {
		// Disable transaction buffer
		if p.txBuffer != nil {
			p.txBuffer.Close()
			p.txBuffer = nil
			slog.Info("transaction buffer disabled")
		}
		return nil
	}

	maxWaitTime, err := time.ParseDuration(newCfg.TransactionBuffer.MaxWaitTime)
	if err != nil {
		slog.Warn("invalid transaction_buffer.max_wait_time, using default 30s", "error", err)
		maxWaitTime = 30 * time.Second
	}

	// Close old buffer
	if p.txBuffer != nil {
		p.txBuffer.Close()
	}

	// Create new buffer with updated config
	p.txBuffer = NewTransactionBuffer(
		maxWaitTime,
		newCfg.TransactionBuffer.MaxTransactionsPerBatch,
		newCfg.TransactionBuffer.MaxBatchBytes,
	)

	slog.Info("transaction buffer rebuilt",
		"max_wait_time", maxWaitTime,
		"max_transactions_per_batch", newCfg.TransactionBuffer.MaxTransactionsPerBatch,
		"max_batch_bytes", newCfg.TransactionBuffer.MaxBatchBytes)

	return nil
}

// flushBuffer forces flush all pending transactions (for shutdown)
func (p *Poller) flushBuffer(ctx context.Context) {
	if p.txBuffer == nil {
		return
	}

	slog.Info("flushing transaction buffer", "pending_count", p.txBuffer.Size())

	// Get all pending transactions
	completeTxs := p.txBuffer.Flush()
	if len(completeTxs) == 0 {
		return
	}

	// Process each transaction
	for _, tx := range completeTxs {
		var sinkOps []Sink

		// Handler processing
		if p.handler != nil {
			var handlerErr error
			err := retry.DoWithName(ctx, func() error {
				sinkOps, handlerErr = p.handler.Handle(tx)
				return handlerErr
			}, retry.DefaultRetryConfig(), fmt.Sprintf("flush_handler_tx_%s", tx.ID))
			if err != nil {
				slog.Error("flush handler error",
					"trace_id", tx.TraceID,
					"tx_id", tx.ID,
					"error", err)
				p.writeToDLQ(tx, err, "flush_handler")
			}
		}

		// Store processing
		if p.store != nil {
			err := retry.DoWithName(ctx, func() error {
				return p.store.Write(tx)
			}, retry.DefaultRetryConfig(), fmt.Sprintf("flush_store_tx_%s", tx.ID))
			if err != nil {
				slog.Error("flush store error",
					"trace_id", tx.TraceID,
					"tx_id", tx.ID,
					"error", err)
				p.writeToDLQ(tx, err, "flush_store")
			}

			// Write transformed DataSets from handler
			if len(sinkOps) > 0 {
				err := retry.DoWithName(ctx, func() error {
					return p.store.WriteOps(sinkOps)
				}, retry.DefaultRetryConfig(), fmt.Sprintf("flush_store_ops_tx_%s", tx.ID))
				if err != nil {
					slog.Error("flush store ops error",
						"trace_id", tx.TraceID,
						"tx_id", tx.ID,
						"error", err)
					p.writeToDLQ(tx, err, "flush_store_ops")
				}
			}
		}
	}

	slog.Info("transaction buffer flush complete", "flushed_count", len(completeTxs))
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

