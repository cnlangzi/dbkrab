package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

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
	dlq       *dlq.DLQ      // DLQ for recording failures
	monitorDB *monitor.DB  // For writing batch logs
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

	// Create a single BatchContext for the entire replay execution
	batchCtx := NewBatchContext()

	// Aggregate metrics across all LSNs
	var totalFetchedRows int
	var totalTxCount int
	var totalDLQCount int

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
			break
		}

		lsnResult, err := r.replayLSN(ctx, lsn, result, batchCtx)
		if err != nil {
			slog.Error("failed to replay LSN", "lsn", lsn, "index", i+1, "total", len(lsns), "error", err)
			result.FailedLSNs++
			if lsnResult != nil {
				totalDLQCount += lsnResult.DLQCount
			}
			continue
		}

		// Aggregate metrics from this LSN
		if lsnResult != nil {
			totalFetchedRows += lsnResult.FetchedRows
			totalTxCount += lsnResult.TxCount
			totalDLQCount += lsnResult.DLQCount
		}

		result.ProcessedLSNs++
		slog.Debug("replayed LSN", "lsn", lsn, "index", i+1, "total", len(lsns))

		// Call progress callback if provided
		if progressCb != nil {
			progressCb(result.ProcessedLSNs, len(lsns))
		}
	}

	// Write single batch_log with aggregated metrics
	if r.monitorDB != nil && batchCtx.BatchID != "" {
		status := monitor.PullStatusSuccess
		if result.FailedLSNs > 0 {
			status = monitor.PullStatusPartial
		}

		batchLog := &monitor.BatchLog{
			BatchID:      batchCtx.BatchID,
			FetchedRows:  totalFetchedRows,
			TxCount:      totalTxCount,
			DLQCount:     totalDLQCount,
			DurationMs:   time.Since(batchCtx.StartTime).Milliseconds(),
			Status:       status,
			CreatedAt:    batchCtx.StartTime,
		}
		if err := r.monitorDB.WriteBatchLog(batchLog); err != nil {
			slog.Warn("failed to write batch_log for replay", "batch_id", batchCtx.BatchID, "error", err)
		}
	}

	slog.Info("replay completed",
		"total_lsns", result.TotalLSNs,
		"processed", result.ProcessedLSNs,
		"failed", result.FailedLSNs,
		"total_changes", result.TotalChanges)

	return result, nil
}

// LSNReplayResult contains metrics from processing a single LSN
type LSNReplayResult struct {
	FetchedRows int
	TxCount     int
	DLQCount    int
}

// replayLSN replays all changes for a specific LSN
func (r *ReplayService) replayLSN(ctx context.Context, lsn string, result *ReplayResult, batchCtx *BatchContext) (*LSNReplayResult, error) {
	// Get all changes for this LSN
	changes, err := r.store.GetChangesWithLSN(lsn)
	if err != nil {
		return nil, fmt.Errorf("get changes for LSN %s: %w", lsn, err)
	}

	if len(changes) == 0 {
		slog.Debug("no changes for LSN", "lsn", lsn)
		return &LSNReplayResult{}, nil
	}

	// Count total changes
	result.TotalChanges += len(changes)

	// Build transaction from changes
	tx := r.buildTransaction(changes)

	// Skip if transaction has no changes (all were UPDATE_BEFORE)
	if len(tx.Changes) == 0 {
		slog.Debug("replayLSN: skip LSN with no valid changes after filtering UPDATE_BEFORE", "lsn", lsn)
		return &LSNReplayResult{}, nil
	}

	// Track DLQ count for this LSN
	dlqCount := 0

	// Handle the transaction using the shared batchCtx
	// Use tx.Changes to pass []Change (not *Transaction, matching Handler interface)
	if err := r.handler.Handle(ctx, tx.Changes, batchCtx); err != nil {
		// Write to DLQ on failure (same logic as Poller)
		r.writeToDLQ(tx, err, lsn, "replay_handler")
		dlqCount++
		return &LSNReplayResult{FetchedRows: len(changes), TxCount: 1, DLQCount: dlqCount}, fmt.Errorf("handle transaction %s: %w", tx.ID, err)
	}

	return &LSNReplayResult{
		FetchedRows: len(changes),
		TxCount:     1,
		DLQCount:    dlqCount,
	}, nil
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

// writeToDLQ writes a failed transaction to the dead letter queue
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
