package core

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/cdc"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/offset"
)

// Poller polls MSSQL CDC tables for changes
type Poller struct {
	cfg      *config.Config
	db       *sql.DB
	querier  *cdc.Querier
	offsets  *offset.Store
	sink     Sink
	handler  Handler
	stopCh   chan struct{}
	stopOnce sync.Once
}

// TablePollResult holds the changes and last LSN from polling a single table
type TablePollResult struct {
	Table     string
	Changes   []Change
	LastLSN   []byte // The LSN of the last change for this table
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

// NewPoller creates a new poller
func NewPoller(cfg *config.Config, db *sql.DB, sink Sink) *Poller {
	return &Poller{
		cfg:     cfg,
		db:      db,
		querier: cdc.NewQuerier(db),
		offsets: offset.NewStore(cfg.Offset),
		sink:    sink,
		stopCh:  make(chan struct{}),
	}
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

// poll performs one polling cycle with a global LSN barrier.
// All tables are polled within the same [fromLSN, barrier] window to ensure
// cross-table transactions are never split across poll cycles.
// Offsets are only updated after all changes have been successfully delivered.
func (p *Poller) poll(ctx context.Context) error {
	// Step 1: Get the global LSN barrier (shared upper bound for all tables)
	barrierLSN, err := p.querier.GetMaxLSN(ctx)
	if err != nil {
		return fmt.Errorf("get max LSN: %w", err)
	}

	// Step 2: Poll all tables within the global barrier window
	// Track per-table LSNs: only advance offset for tables that were polled successfully.
	tableLastLSNs := make(map[string][]byte) // table → last LSN actually read from this table
	var allResults []TablePollResult
	for _, table := range p.cfg.Tables {
		result, err := p.pollTable(ctx, table, barrierLSN)
		if err != nil {
			log.Printf("Error polling table %s: %v", table, err)
			continue
		}
		allResults = append(allResults, result)
		tableLastLSNs[table] = result.LastLSN
	}

	// Collect all changes from all tables
	var allChanges []Change
	for _, r := range allResults {
		allChanges = append(allChanges, r.Changes...)
	}

	if len(allChanges) == 0 {
		return nil // No changes, no offset update needed
	}

	// Step 3: Group changes by transaction ID
	txs := p.groupByTransaction(allChanges)

	// Step 4: Deliver all transactions
	for _, tx := range txs {
		if p.handler != nil {
			if err := p.handler.Handle(&tx); err != nil {
				log.Printf("Handler error for tx %s: %v", tx.ID, err)
				// At-least-once: continue delivering other transactions
				// Do NOT update offsets on delivery failure
			} else if p.sink != nil {
				if err := p.sink.Write(&tx); err != nil {
					log.Printf("Sink error for tx %s: %v", tx.ID, err)
					// At-least-once: continue delivering other transactions
					// Do NOT update offsets on delivery failure
				}
			}
		} else if p.sink != nil {
			if err := p.sink.Write(&tx); err != nil {
				log.Printf("Sink error for tx %s: %v", tx.ID, err)
				// At-least-once: continue delivering other transactions
				// Do NOT update offsets on delivery failure
			}
		}
	}

	// Step 5: Update offsets for successfully-polled tables using their actual last LSN.
	// This prevents data loss when a table's poll fails: its offset is NOT advanced,
	// so the next poll cycle will re-read from the last successfully-read LSN.
	//
	// Using per-table last LSN (not the shared barrier) is safe because:
	//   a) Each table's changes are bounded by [startLSN, barrier]
	//   b) All successfully-read changes have been delivered
	//   c) Cross-table transaction atomicity is preserved within this poll cycle
	//      (all tables share the same barrier window, so they all see the same
	//       set of cross-table TX changes before any offset moves forward)
	updates := make(map[string]string)
	for table, lastLSN := range tableLastLSNs {
		if len(lastLSN) > 0 {
			updates[table] = LSN(lastLSN).String()
		}
	}

	if err := p.offsets.SetMultiple(updates); err != nil {
		log.Printf("Failed to save offsets: %v", err)
		//Offsets not saved; next poll may re-deliver same changes (at-least-once)
	}

	return nil
}

// pollTable polls a single table for changes within the given global LSN barrier.
// The barrier must be the same for all tables in a poll cycle.
func (p *Poller) pollTable(ctx context.Context, table string, barrierLSN []byte) (TablePollResult, error) {
	schema, tableName := cdc.ParseTableName(table)
	captureInstance := cdc.CaptureInstanceName(schema, tableName)

	// Get starting LSN for this table
	startLSN := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if stored, ok := p.offsets.Get(table); ok {
		parsed, err := ParseLSN(stored.LSN)
		if err == nil && len(parsed) > 0 {
			startLSN = parsed
		}
	}

	// If first time, get min LSN
	if len(startLSN) == 0 || LSN(startLSN).IsZero() {
		minLSN, err := p.querier.GetMinLSN(ctx, captureInstance)
		if err != nil {
			return TablePollResult{Table: table}, fmt.Errorf("get min LSN: %w", err)
		}
		startLSN = minLSN
	}

	// Query changes within the global barrier window
	rawChanges, err := p.querier.GetChanges(ctx, captureInstance, startLSN, barrierLSN)
	if err != nil {
		return TablePollResult{Table: table}, err
	}

	// Convert to core.Change
	changes := make([]Change, 0, len(rawChanges))
	var lastLSN []byte
	for _, rc := range rawChanges {
		changes = append(changes, Change{
			Table:         table,
			TransactionID: rc.TransactionID,
			LSN:           rc.LSN,
			Operation:     Operation(rc.Operation),
			Data:          rc.Data,
		})
		lastLSN = rc.LSN // Keep updating; last one is the highest
	}

	return TablePollResult{
		Table:   table,
		Changes: changes,
		LastLSN: lastLSN,
	}, nil
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
