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
)

// Poller polls MSSQL CDC tables for changes
type Poller struct {
	cfg           *config.Config
	db            *sql.DB
	querier       *cdc.Querier
	gapDetector   *cdc.GapDetector
	alertManager  *alert.AlertManager
	offsets       *offset.Store
	sink          Sink
	handler       Handler
	stopCh        chan struct{}
	stopOnce      sync.Once
	paused        bool
	pausedMu      sync.RWMutex
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

// tablePollResult holds the result of polling a single table
type tablePollResult struct {
	table    string
	changes  []Change
	lastLSN  LSN
	err      error
}

// NewPoller creates a new poller
func NewPoller(cfg *config.Config, db *sql.DB, sink Sink) *Poller {
	poller := &Poller{
		cfg:      cfg,
		db:       db,
		querier:  cdc.NewQuerier(db),
		offsets:  offset.NewStore(cfg.Offset),
		sink:     sink,
		stopCh:   make(chan struct{}),
	}

	// Initialize gap detector if CDC protection is enabled
	if cfg.CDCProtection.Enabled {
		poller.gapDetector = cdc.NewGapDetector(db)
		
		// Convert config.AlertChannel to alert.AlertChannel
		alertChannels := make([]alert.AlertChannel, len(cfg.CDCProtection.Alert.Channels))
		for i, ch := range cfg.CDCProtection.Alert.Channels {
			alertChannels[i] = alert.AlertChannel{
				Type:       ch.Type,
				URL:        ch.URL,
				Recipients: ch.Recipients,
			}
		}
		
		poller.alertManager = alert.NewAlertManager(alert.AlertConfig{
			Enabled:    cfg.CDCProtection.Alert.Enabled,
			WebhookURL: cfg.CDCProtection.Alert.WebhookURL,
			Channels:   alertChannels,
		})
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
			}
		}
	}
}

// Stop stops the poller
func (p *Poller) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
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
	maxLSN, err := p.querier.GetMaxLSN(ctx)
	if err != nil {
		return fmt.Errorf("get max LSN: %w", err)
	}

	// CDC gap detection (before polling)
	if p.gapDetector != nil {
		if err := p.checkGaps(ctx); err != nil {
			log.Printf("Gap detection error: %v", err)
			// Gap detection errors don't block polling, but are logged
		}
	}

	// Poll all tables and collect results (without updating offsets)
	results := make([]tablePollResult, 0, len(p.cfg.Tables))
	for _, table := range p.cfg.Tables {
		changes, lastLSN, err := p.pollTable(ctx, table, maxLSN)
		results = append(results, tablePollResult{
			table:   table,
			changes: changes,
			lastLSN: lastLSN,
			err:     err,
		})
		if err != nil {
			log.Printf("Error polling table %s: %v", table, err)
		}
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

	// Group by transaction ID and deliver
	txs := p.groupByTransaction(allChanges)
	
	// Process all transactions
	var processErrors []error
	for _, tx := range txs {
		// Handler processing (non-blocking: continue even if handler fails)
		if p.handler != nil {
			if err := p.handler.Handle(&tx); err != nil {
				log.Printf("Handler error for tx %s: %v", tx.ID, err)
				// Handler errors don't block offset advancement
				// Plugins may have their own retry/recovery logic
			}
		}
		
		// Sink processing (blocking: must succeed for offset advancement)
		if p.sink != nil {
			if err := p.sink.Write(&tx); err != nil {
				log.Printf("Sink error for tx %s: %v", tx.ID, err)
				processErrors = append(processErrors, fmt.Errorf("sink tx %s: %w", tx.ID, err))
			}
		}
	}

	// P0 fix: Only update offsets after successful sink write
	if len(processErrors) > 0 {
		// Some transactions failed to write - don't advance offsets
		// This ensures we'll re-poll these changes on next cycle
		return fmt.Errorf("sink errors: %v", processErrors)
	}

	// All transactions successfully written - now update offsets
	// P0 fix: Multi-table sync - use min LSN across all tables as global checkpoint
	// P0 fix (review): Only advance offsets for tables that were successfully polled
	// Failed poll tables keep their previous checkpoint to prevent data loss
	
	// Build set of successfully polled tables (including those with no changes)
	// Tables with no changes still need their offset updated to avoid re-polling empty range
	successfulTables := make(map[string]bool)
	for _, r := range results {
		if r.err == nil {
			successfulTables[r.table] = true
		}
	}
	
	if len(validResults) > 0 {
		// Find minimum lastLSN across all tables that had changes
		minLSN := validResults[0].lastLSN
		for _, r := range validResults[1:] {
			if r.lastLSN.Compare(minLSN) < 0 {
				minLSN = r.lastLSN
			}
		}

		// Only advance offsets for successfully polled tables
		// Failed poll tables keep their previous checkpoint
		for table := range successfulTables {
			if err := p.offsets.Set(table, minLSN.String()); err != nil {
				log.Printf("Failed to save offset for %s: %v", table, err)
			}
		}
		
		log.Printf("Advanced global checkpoint to LSN %s across %d successfully polled tables", minLSN.String(), len(successfulTables))
	} else if len(successfulTables) > 0 {
		// No changes found, but some tables were successfully polled
		// Advance their offsets to maxLSN to avoid re-polling empty range
		maxLSNKey := LSN(maxLSN)
		for table := range successfulTables {
			if err := p.offsets.Set(table, maxLSNKey.String()); err != nil {
				log.Printf("Failed to save offset for %s: %v", table, err)
			}
		}
		log.Printf("Advanced offset to LSN %s for %d tables (no changes found)", maxLSNKey.String(), len(successfulTables))
	}

	return nil
}

// pollTable polls a single table for changes
// Returns (changes, lastLSN, error) - does NOT update offset
// P0 fix: offset update moved to poll() after successful sink write
func (p *Poller) pollTable(ctx context.Context, table string, maxLSN []byte) ([]Change, LSN, error) {
	schema, tableName := cdc.ParseTableName(table)
	captureInstance := cdc.CaptureInstanceName(schema, tableName)

	// Get starting LSN from offset store
	startLSN := LSN{}
	if stored, ok := p.offsets.Get(table); ok {
		parsed, err := ParseLSN(stored.LSN)
		if err == nil && len(parsed) > 0 {
			startLSN = parsed
		}
	}

	// If first time, get min LSN from CDC
	if len(startLSN) == 0 || startLSN.IsZero() {
		minLSN, err := p.querier.GetMinLSN(ctx, captureInstance)
		if err != nil {
			return nil, nil, fmt.Errorf("get min LSN: %w", err)
		}
		startLSN = minLSN
	}

	// Query changes from CDC
	rawChanges, err := p.querier.GetChanges(ctx, captureInstance, startLSN, maxLSN)
	if err != nil {
		return nil, nil, err
	}

	// Convert to core.Change
	changes := make([]Change, 0, len(rawChanges))
	var lastLSN LSN
	for _, rc := range rawChanges {
		changes = append(changes, Change{
			Table:         table,
			TransactionID: rc.TransactionID,
			LSN:           rc.LSN,
			Operation:     Operation(rc.Operation),
			Data:          rc.Data,
		})
		lastLSN = rc.LSN
	}

	// Return changes and lastLSN - offset update happens in poll() after sink success
	return changes, lastLSN, nil
}

// groupByTransaction groups changes by transaction ID
func (p *Poller) groupByTransaction(changes []Change) []Transaction {
	txs := make(map[string]*Transaction)

	for _, c := range changes {
		txID := c.TransactionID
		if txID == "" {
			txID = "unknown"
		}

		if _, ok := txs[txID]; !ok {
			txs[txID] = NewTransaction(txID)
		}
		txs[txID].AddChange(c)
	}

	result := make([]Transaction, 0, len(txs))
	for _, tx := range txs {
		result = append(result, *tx)
	}

	return result
}

// checkGaps checks for CDC gaps across all monitored tables
func (p *Poller) checkGaps(ctx context.Context) error {
	warningLagBytes := p.cfg.CDCProtection.WarningLagBytes
	criticalLagBytes := p.cfg.CDCProtection.CriticalLagBytes
	warningLagDuration, _ := p.cfg.WarningLagDuration()
	criticalLagDuration, _ := p.cfg.CriticalLagDuration()

	for _, table := range p.cfg.Tables {
		schema, tableName := cdc.ParseTableName(table)
		captureInstance := cdc.CaptureInstanceName(schema, tableName)

		// Get current LSN from offset
		currentLSN := []byte{}
		if stored, ok := p.offsets.Get(table); ok {
			parsed, err := ParseLSN(stored.LSN)
			if err == nil && len(parsed) > 0 {
				currentLSN = []byte(parsed)
			}
		}

		// Check for gaps
		gap, err := p.gapDetector.CheckGap(ctx, captureInstance, currentLSN)
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

// resume resumes the poller
func (p *Poller) resume() {
	p.pausedMu.Lock()
	defer p.pausedMu.Unlock()
	p.paused = false
	log.Println("Poller resumed")
}

// isPaused returns true if the poller is paused
func (p *Poller) isPaused() bool {
	p.pausedMu.RLock()
	defer p.pausedMu.RUnlock()
	return p.paused
}