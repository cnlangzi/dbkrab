package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/store"
)

// ReplayService replays CDC changes from the store
type ReplayService struct {
	store   store.Store
	handler Handler
}

// ReplayResult contains the replay statistics
type ReplayResult struct {
	TotalLSNs     int
	TotalChanges  int
	ProcessedLSNs int
	FailedLSNs    int
}

// NewReplayService creates a new ReplayService
func NewReplayService(store store.Store, handler Handler) *ReplayService {
	return &ReplayService{
		store:   store,
		handler: handler,
	}
}

// Execute replays all CDC changes from the store in LSN order
func (r *ReplayService) Execute(ctx context.Context) (*ReplayResult, error) {
	// Get all unique LSNs from store
	lsns, err := r.store.GetLSNs()
	if err != nil {
		return nil, fmt.Errorf("failed to get LSNs: %w", err)
	}

	if len(lsns) == 0 {
		slog.Info("no changes to replay")
		return &ReplayResult{}, nil
	}

	slog.Info("starting replay", "lsn_count", len(lsns))

	result := &ReplayResult{TotalLSNs: len(lsns)}

	// Process each LSN in order
	for i, lsn := range lsns {
		if err := r.replayLSN(ctx, lsn, result); err != nil {
			slog.Error("failed to replay LSN", "lsn", lsn, "index", i+1, "total", len(lsns), "error", err)
			result.FailedLSNs++
			continue
		}

		result.ProcessedLSNs++
		slog.Debug("replayed LSN", "lsn", lsn, "index", i+1, "total", len(lsns))
	}

	slog.Info("replay completed",
		"total_lsns", result.TotalLSNs,
		"processed", result.ProcessedLSNs,
		"failed", result.FailedLSNs,
		"total_changes", result.TotalChanges)

	return result, nil
}

// replayLSN replays all changes for a specific LSN
func (r *ReplayService) replayLSN(ctx context.Context, lsn string, result *ReplayResult) error {
	// Get all changes for this LSN
	changes, err := r.store.GetChangesWithLSN(lsn)
	if err != nil {
		return fmt.Errorf("get changes for LSN %s: %w", lsn, err)
	}

	if len(changes) == 0 {
		slog.Debug("no changes for LSN", "lsn", lsn)
		return nil
	}

	// Count total changes
	result.TotalChanges += len(changes)

	// Build transaction from changes
	tx := r.buildTransaction(changes)

	// Handle the transaction
	if err := r.handler.Handle(ctx, tx); err != nil {
		return fmt.Errorf("handle transaction %s: %w", tx.ID, err)
	}

	return nil
}

// buildTransaction builds a core.Transaction from core.Change slice
// It groups changes by transaction ID and filters out UPDATE_BEFORE operations
func (r *ReplayService) buildTransaction(changes []core.Change) *Transaction {
	// Group by transaction ID, filtering out UPDATE_BEFORE
	txMap := make(map[string]*Transaction)

	for _, c := range changes {
		// Filter out UPDATE_BEFORE operations (operation == 3)
		if c.Operation == core.OpUpdateBefore { // OpUpdateBefore
			slog.Debug("buildTransaction: silently dropping UPDATE_BEFORE change",
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

		// Already core.Change, add directly
		tx.AddChange(c)
	}

	// Convert map to slice - should only have one transaction per LSN
	if len(txMap) == 0 {
		return &Transaction{
			ID:      "",
			Changes: []Change{},
		}
	}

	// Get the first (and only) transaction
	for _, tx := range txMap {
		return tx
	}

	return &Transaction{
		ID:      "",
		Changes: []Change{},
	}
}