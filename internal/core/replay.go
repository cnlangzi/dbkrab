package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/monitor"
)

// ReplayStore defines the interface for replay operations
// This is a local interface to avoid import cycle with store package
type ReplayStore interface {
	GetLSNs() ([]string, error)
	GetChangesWithLSN(lsn string) ([]Change, error)
}

// ReplayService replays CDC changes from the store
type ReplayService struct {
	store     ReplayStore
	handler   Handler
	dlq       *dlq.DLQ    // DLQ for recording failures
	monitorDB *monitor.DB // For writing batch logs
}

// ReplayResult contains the replay statistics
type ReplayResult struct {
	TotalLSNs     int
	TotalChanges  int
	ProcessedLSNs int
	FailedLSNs    int
}

// NewReplayService creates a new ReplayService with DLQ support
func NewReplayService(store ReplayStore, handler Handler, dlq *dlq.DLQ, monitorDB *monitor.DB) *ReplayService {
	return &ReplayService{
		store:     store,
		handler:   handler,
		dlq:       dlq,
		monitorDB: monitorDB,
	}
}

// ProgressCallback is called after each LSN is processed
type ProgressCallback func(processed int, total int)

// Execute replays all CDC changes from the store in LSN order
// progressCb is optional - if provided, it's called after each LSN with current progress
func (r *ReplayService) Execute(ctx context.Context, progressCb ProgressCallback) (*ReplayResult, error) {
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
		// Honor caller cancellation between LSNs
		if ctxErr := ctx.Err(); ctxErr != nil {
			slog.Warn("replay cancelled",
				"error", ctxErr,
				"processed", result.ProcessedLSNs,
				"failed", result.FailedLSNs,
				"total", len(lsns),
			)
			return result, ctxErr
		}

		if err := r.replayLSN(ctx, lsn, result); err != nil {
			slog.Error("failed to replay LSN", "lsn", lsn, "index", i+1, "total", len(lsns), "error", err)
			result.FailedLSNs++
			continue
		}

		result.ProcessedLSNs++
		slog.Debug("replayed LSN", "lsn", lsn, "index", i+1, "total", len(lsns))

		// Call progress callback if provided
		if progressCb != nil {
			progressCb(result.ProcessedLSNs, len(lsns))
		}
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

	// Filter out UPDATE_BEFORE operations (same filtering as poller's groupByTransaction)
	filteredChanges := make([]Change, 0, len(changes))
	for _, c := range changes {
		if c.Operation == OpUpdateBefore {
			slog.Debug("replayLSN: silently dropping UPDATE_BEFORE change",
				"table", c.Table,
				"tx_id", c.TransactionID,
				"lsn", fmt.Sprintf("%x", c.LSN))
			continue
		}
		filteredChanges = append(filteredChanges, c)
	}

	if len(filteredChanges) == 0 {
		slog.Debug("replayLSN: skip LSN with no valid changes after filtering UPDATE_BEFORE", "lsn", lsn)
		return nil
	}

	// Handle the full LSN batch (per-LSN semantics, matching poller behavior)
	batchCtx := NewBatchContext()
	if err := r.handler.Handle(ctx, filteredChanges, batchCtx); err != nil {
		// Write to DLQ on failure (change-scoped, same as poller)
		r.writeChangesToDLQ(filteredChanges, err, lsn, "replay_handler")
		return fmt.Errorf("handle batch %s: %w", batchCtx.BatchID, err)
	}

	// Write batch_log for observability (like Poller does)
	if r.monitorDB != nil && batchCtx.BatchID != "" {
		batchLog := &monitor.BatchLog{
			BatchID:     batchCtx.BatchID,
			FetchedRows: len(filteredChanges),
			TxCount:     0, // No transaction grouping (per-LSN semantics)
			DLQCount:    0,
			DurationMs:  0,
			Status:      monitor.PullStatusSuccess,
		}
		if err := r.monitorDB.WriteBatchLog(batchLog); err != nil {
			slog.Warn("failed to write batch_log for replay", "batch_id", batchCtx.BatchID, "error", err)
		}
	}

	return nil
}

// buildTransaction builds a Transaction from Change slice
// It groups changes by transaction ID and filters out UPDATE_BEFORE operations
func (r *ReplayService) buildTransaction(changes []Change) *Transaction {
	// Group by transaction ID, filtering out UPDATE_BEFORE
	txMap := make(map[string]*Transaction)

	for _, c := range changes {
		// Filter out UPDATE_BEFORE operations
		if c.Operation == OpUpdateBefore {
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

		// Already Change, add directly
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

// writeChangesToDLQ writes failed changes to the dead letter queue (change-scoped).
// Each change is written as a separate DLQ entry for granular retry.
func (r *ReplayService) writeChangesToDLQ(changes []Change, err error, lsn string, source string) {
	if r.dlq == nil {
		slog.Warn("cannot write to DLQ: not initialized",
			"source", source,
			"lsn", lsn,
			"changes", len(changes))
		return
	}

	// Write each change as a separate DLQ entry (change-scoped)
	for i, c := range changes {
		// Encode change data as JSON
		changeJSON, encodeErr := json.Marshal(c)
		if encodeErr != nil {
			slog.Error("failed to encode change data",
				"table", c.Table,
				"lsn", fmt.Sprintf("%x", c.LSN),
				"error", encodeErr)
			changeJSON = []byte("{}")
		}

		// Use Change.ID for trace correlation (content-based, deterministic)
		// This allows tracing the same change across multiple DLQ entries if retried
		entry := &dlq.DLQEntry{
			TraceID:      c.ID,
			Source:       source,
			LSN:          fmt.Sprintf("%x", c.LSN),
			TableName:    c.Table,
			Operation:    c.Operation.String(),
			ChangeData:   string(changeJSON),
			ErrorMessage: fmt.Sprintf("%s error (batch[%d]): %v", source, i, err),
			RetryCount:   0,
			Status:       dlq.StatusPending,
		}

		if writeErr := r.dlq.Write(entry); writeErr != nil {
			slog.Error("failed to write DLQ entry",
				"table", c.Table,
				"lsn", fmt.Sprintf("%x", c.LSN),
				"error", writeErr)
		}
	}
}

// writeToDLQ writes a failed transaction to the dead letter queue (deprecated, change-scoped preferred).
func (r *ReplayService) writeToDLQ(tx *Transaction, handlerErr error, lsn string, source string) {
	if r.dlq == nil {
		slog.Warn("cannot write to DLQ: not initialized",
			"trace_id", tx.TraceID,
			"tx_id", tx.ID)
		return
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

	entry := &dlq.DLQEntry{
		TraceID:      tx.TraceID,
		Source:       source,
		LSN:          lsn,
		TableName:    tableName,
		Operation:    operation,
		ChangeData:   string(changeJSON),
		ErrorMessage: fmt.Sprintf("%s error: %v", source, handlerErr),
		RetryCount:   0,
		Status:       dlq.StatusPending,
	}

	if writeErr := r.dlq.Write(entry); writeErr != nil {
		slog.Error("failed to write DLQ entry",
			"trace_id", tx.TraceID,
			"tx_id", tx.ID,
			"error", writeErr)
		return
	}

	slog.Warn("transaction written to DLQ during replay",
		"trace_id", tx.TraceID,
		"tx_id", tx.ID,
		"table", tableName,
		"operation", operation,
		"lsn", lsn)
}
