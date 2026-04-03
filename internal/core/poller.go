package core

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/alert"
	"github.com/cnlangzi/dbkrab/internal/cdc"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/offset"
	"github.com/cnlangzi/dbkrab/internal/metrics"
)

// Poller polls MSSQL CDC tables for changes
type Poller struct {
	cfg           *config.Config
	db            *sql.DB
	querier       *cdc.Querier
	gapDetector   *cdc.GapDetector
	alertManager  *alert.AlertManager
	offsets       offset.StoreInterface
	sink          Sink
	handler       Handler
	stopCh        chan struct{}
	stopOnce      sync.Once
	paused        bool
	pausedMu      sync.RWMutex
	lastGapCheck  time.Time
	gapCheckMu    sync.RWMutex
	txBuffer      *TransactionBuffer // For cross-table transaction integrity
	metrics       *metrics.Metrics   // Prometheus metrics
}

// Sink interface for storing changes
type Sink interface {
	Write(tx *Transaction) error
	Close() error
}

// Handler interface for custom processing
type Handler interface {
	Handle(tx *Transaction) error
}

// PluginHandler is a function type for plugin-based handling
type PluginHandler func(tx *Transaction) error

// Handle implements Handler interface
func (h PluginHandler) Handle(tx *Transaction) error {
	return h(tx)
}

type tablePollResult struct {
	table    string
	changes  []Change
	lastLSN  LSN
	err      error
}

// NewPoller creates a new poller
func NewPoller(cfg *config.Config, db *sql.DB, sink Sink, offsetStore offset.StoreInterface, m *metrics.Metrics) *Poller {
	poller := &Poller{
		cfg:      cfg,
		db:       db,
		querier:  cdc.NewQuerier(db),
		offsets:  offsetStore,
		sink:     sink,
		stopCh:   make(chan struct{}),
		metrics:  m,
	}

	// Initialize transaction buffer if enabled
	if cfg.TransactionBuffer.Enabled {
		maxWaitTime, err := time.ParseDuration(cfg.TransactionBuffer.MaxWaitTime)
		if err != nil {
			log.Printf("Invalid transaction_buffer.max_wait_time, using default 30s: %v", err)
			maxWaitTime = 30 * time.Second
		}
		poller.txBuffer = NewTransactionBuffer(maxWaitTime)
		log.Printf("Transaction buffer enabled (max_wait_time=%v)", maxWaitTime)
	}

	// Initialize gap detector if CDC protection is enabled
	if cfg.CDCProtection.Enabled {
		poller.gapDetector = cdc.NewGapDetector(db)
		poller.alertManager = alert.NewAlertManager(cfg.CDCProtection.Alert)
		log.Println("CDC gap protection enabled")
	}

	return poller
}

// SetHandler sets a custom handler
func (p *Poller) SetHandler(h Handler) {
	p.handler = h
}

// Start begins polling
func (p *Poller) Start(ctx context.Context) error {
	// Load existing offsets
	if err := p.offsets.Load(); err != nil {
		return fmt.Errorf("load offsets: %w", err)
	}

	interval, err := p.cfg.PollingInterval()
	if err != nil {
		return fmt.Errorf("parse interval: %w", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Starting CDC poller with %v interval for %d tables", interval, len(p.cfg.Tables))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stopCh:
			return nil
		case <-ticker.C:
			if err := p.poll(ctx); err != nil {
				log.Printf("Poll error: %v", err)
				// Continue polling despite errors
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
// P0 fix: offset is only updated after sink successfully writes
// P0 fix: multi-table sync via min LSN checkpoint
func (p *Poller) poll(ctx context.Context) error {
	// Check if paused due to gap detection
	if p.isPaused() {
		log.Println("Poller is paused due to CDC gap detection")
		return nil
	}

	// Get max LSN from MSSQL
	_, err := p.querier.GetMaxLSN(ctx)
	if err != nil {
		return fmt.Errorf("get max LSN: %w", err)
	}

	// CDC gap detection (before polling)
	// Respect check_interval configuration
	if p.gapDetector != nil && p.shouldCheckGaps() {
		if err := p.checkGaps(ctx); err != nil {
			log.Printf("Gap detection error: %v", err)
			// Gap detection errors don't block polling, but are logged
		}
		p.recordGapCheck()
	}

	// Poll all tables and collect results (without updating offsets)
	results := make([]tablePollResult, 0, len(p.cfg.Tables))

	for _, table := range p.cfg.Tables {
		schema, tableName := cdc.ParseTableName(table)
		captureInstance := cdc.CaptureInstanceName(schema, tableName)

		// Get starting LSN from offset store
		startLSN := LSN{}
		stored, err := p.offsets.Get(table)
		if err == nil && stored.LSN != "" {
			parsed, parseErr := ParseLSN(stored.LSN)
			if parseErr == nil && len(parsed) > 0 {
				startLSN = parsed
			}
		}

		// If first time, get min LSN from CDC
		if len(startLSN) == 0 || startLSN.IsZero() {
			minLSN, err := p.querier.GetMinLSN(ctx, captureInstance)
			if err != nil {
				results = append(results, tablePollResult{table: table, err: err})
				continue
			}
			startLSN = minLSN
		}

		// Get changes since last poll
		cdcChanges, err := p.querier.GetChanges(ctx, captureInstance, startLSN, nil)
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
			}
		}

		// Get last LSN from the last change or use max LSN
		var lastLSN LSN
		if len(changes) > 0 {
			lastLSN = changes[len(changes)-1].LSN
		} else {
			lastLSN = startLSN
		}

		results = append(results, tablePollResult{ //nolint:all // SA4010 false positive
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

	// Nothing to process
	if len(allChanges) == 0 {
		return nil
	}

	// If transaction buffer is enabled, use it for cross-table integrity
	if p.txBuffer != nil {
		return p.processWithBuffer(ctx, allChanges, results)
	}

	// Otherwise, use legacy direct processing
	return p.processDirect(ctx, allChanges, results)
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
		// Record transaction start
		if p.metrics != nil {
			p.metrics.RecordTransaction(false)
		}

		// Handler processing
		if p.handler != nil {
			if err := p.handler.Handle(tx); err != nil {
				log.Printf("Handler error for tx %s: %v", tx.ID, err)
				// Handler errors don't block offset advancement
			}
		}

		// Sink processing
		if p.sink != nil {
			if err := p.sink.Write(tx); err != nil {
				log.Printf("Sink error for tx %s: %v", tx.ID, err)
				processErrors = append(processErrors, fmt.Errorf("sink tx %s: %w", tx.ID, err))
				if p.metrics != nil {
					p.metrics.RecordTransaction(true)
				}
			}
		}
	}

	// Update offsets only if all transactions succeeded
	if len(processErrors) > 0 {
		return fmt.Errorf("sink errors: %v", processErrors)
	}

	// Update offsets (same logic as processDirect)
	return p.updateOffsets(results)
}

// processDirect processes changes without transaction buffer (legacy behavior)
func (p *Poller) processDirect(ctx context.Context, allChanges []Change, results []tablePollResult) error {

	// Group by transaction ID and deliver
	txs := p.groupByTransaction(allChanges)

	// Process all transactions
	var processErrors []error
	for _, tx := range txs {
		// Record transaction
		if p.metrics != nil {
			p.metrics.RecordTransaction(false)
		}

		// Handler processing (non-blocking: continue even if handler fails)
		if p.handler != nil {
			if err := p.handler.Handle(&tx); err != nil {
				log.Printf("Handler error for tx %s: %v", tx.ID, err)
			}
		}

		// Sink processing (blocking: must succeed for offset advancement)
		if p.sink != nil {
			if err := p.sink.Write(&tx); err != nil {
				log.Printf("Sink error for tx %s: %v", tx.ID, err)
				processErrors = append(processErrors, fmt.Errorf("sink tx %s: %w", tx.ID, err))
				if p.metrics != nil {
					p.metrics.RecordTransaction(true)
				}
			}
		}
	}

	// P0 fix: Only update offsets after successful sink write
	if len(processErrors) > 0 {
		return fmt.Errorf("sink errors: %v", processErrors)
	}

	// All transactions successfully written - now update offsets
	return p.updateOffsets(results)
}

// updateOffsets updates offset checkpoints for successfully polled tables
func (p *Poller) updateOffsets(results []tablePollResult) error {
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
				log.Printf("Failed to save offset for %s: %v", table, err)
			}
		}

		log.Printf("Advanced global checkpoint to LSN %s across %d successfully polled tables", minLSN.String(), len(successfulTables))
	}

	return nil
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
		log.Printf("Warning: invalid warning_lag_duration config, using default 1h: %v", err)
		warningLagDuration = 1 * time.Hour
	}
	criticalLagDuration, err := p.cfg.CriticalLagDuration()
	if err != nil {
		log.Printf("Warning: invalid critical_lag_duration config, using default 6h: %v", err)
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
			log.Printf("Gap check error for %s: %v", table, err)
			continue
		}

		// Handle gap based on severity
		if gap.HasGap {
			// Data loss detected - emergency
			log.Printf("[EMERGENCY] CDC data loss detected for %s: missing LSN %X to %X",
				table, gap.MissingLSNRange.Start, gap.MissingLSNRange.End)
			p.alertManager.SendEmergency("CDC Data Loss Detected", gap)
			p.pause()
			return fmt.Errorf("CDC data loss for %s", table)
		}

		if gap.IsGapCritical(criticalLagBytes, criticalLagDuration) {
			// Critical lag
			log.Printf("[CRITICAL] CDC lag critical for %s: %d bytes, %v",
				table, gap.LagBytes, gap.LagDuration)
			p.alertManager.SendCritical("CDC Lag Critical", gap)
			// Continue polling but alert
		}

		if gap.IsGapWarning(warningLagBytes, warningLagDuration) {
			// Warning lag
			log.Printf("[WARNING] CDC lag warning for %s: %d bytes, %v",
				table, gap.LagBytes, gap.LagDuration)
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
	log.Println("Poller paused")
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
