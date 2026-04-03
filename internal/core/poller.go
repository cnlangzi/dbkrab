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
	cfg     *config.Config
	db      *sql.DB
	querier *cdc.Querier
	offsets *offset.Store
	sink    Sink
	handler Handler
	stopCh  chan struct{}
	wg      sync.WaitGroup
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
	close(p.stopCh)
	p.wg.Wait()
}

// poll performs one polling cycle
func (p *Poller) poll(ctx context.Context) error {
	// Get max LSN from MSSQL
	maxLSN, err := p.querier.GetMaxLSN(ctx)
	if err != nil {
		return fmt.Errorf("get max LSN: %w", err)
	}

	// Poll each table
	var allChanges []Change
	for _, table := range p.cfg.Tables {
		changes, err := p.pollTable(ctx, table, maxLSN)
		if err != nil {
			log.Printf("Error polling table %s: %v", table, err)
			continue
		}
		allChanges = append(allChanges, changes...)
	}

	// Group by transaction ID and deliver
	if len(allChanges) > 0 {
		txs := p.groupByTransaction(allChanges)
		for _, tx := range txs {
			if p.handler != nil {
				if err := p.handler.Handle(&tx); err != nil {
					log.Printf("Handler error for tx %s: %v", tx.ID, err)
				}
			}
			if p.sink != nil {
				if err := p.sink.Write(&tx); err != nil {
					log.Printf("Sink error for tx %s: %v", tx.ID, err)
				}
			}
		}
	}

	return nil
}

// pollTable polls a single table for changes
func (p *Poller) pollTable(ctx context.Context, table string, maxLSN []byte) ([]Change, error) {
	schema, tableName := cdc.ParseTableName(table)
	captureInstance := cdc.CaptureInstanceName(schema, tableName)

	// Get starting LSN
	startLSN := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0} // Start from beginning by default
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
			return nil, fmt.Errorf("get min LSN: %w", err)
		}
		startLSN = minLSN
	}

	// Query changes from CDC
	rawChanges, err := p.querier.GetChanges(ctx, captureInstance, startLSN, maxLSN)
	if err != nil {
		return nil, err
	}

	// Convert to core.Change
	changes := make([]Change, 0, len(rawChanges))
	for _, rc := range rawChanges {
		changes = append(changes, Change{
			Table:         table,
			TransactionID: rc.TransactionID,
			LSN:           rc.LSN,
			Operation:     Operation(rc.Operation),
			Data:          rc.Data,
		})
	}

	// Update offset to max LSN
	if len(changes) > 0 {
		lastLSN := changes[len(changes)-1].LSN
		if err := p.offsets.Set(table, LSN(lastLSN).String()); err != nil {
			log.Printf("Failed to save offset for %s: %v", table, err)
		}
	}

	return changes, nil
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