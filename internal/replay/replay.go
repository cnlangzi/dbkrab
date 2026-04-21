package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/monitor"
)

// Store defines the interface for replay operations
type Store interface {
	GetLSNs() ([]string, error)
	GetChangesWithLSN(lsn string) ([]core.Change, error)
}

// ReplayService replays CDC changes from the store.
type ReplayService struct {
	store        Store
	handler      core.Handler
	dlq          *dlq.DLQ
	monitorDB    *monitor.DB
	stateManager *core.StateManager // State coordination with Poller/Snapshot
}

// NewReplayService creates a new ReplayService.
func NewReplayService(store Store, handler core.Handler, dlq *dlq.DLQ, monitorDB *monitor.DB, stateManager *core.StateManager) *ReplayService {
	return &ReplayService{
		store:        store,
		handler:      handler,
		dlq:          dlq,
		monitorDB:    monitorDB,
		stateManager: stateManager,
	}
}

// Execute replays all CDC changes from the store in LSN order.
// Returns error if StateManager indicates non-idle state or replay fails.
func (r *ReplayService) Execute(ctx context.Context, progressCb ProgressCallback) (*ReplayResult, error) {
	// Check state before starting
	if !r.stateManager.CanStart(core.StateReplay) {
		return nil, fmt.Errorf("cannot start replay: current state is %s", r.stateManager.Current())
	}

	// Get all unique LSNs from store
	lsns, err := r.store.GetLSNs()
	if err != nil {
		return nil, fmt.Errorf("failed to get LSNs: %w", err)
	}

	if len(lsns) == 0 {
		slog.Info("no changes to replay")
		return &ReplayResult{}, nil
	}

	// Create cancellable context and register with StateManager
	replayCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := r.stateManager.Start(core.StateReplay, cancel); err != nil {
		cancel()
		return nil, err
	}
	defer r.stateManager.Stop()

	slog.Info("starting replay", "lsn_count", len(lsns))

	result := &ReplayResult{TotalLSNs: len(lsns)}
	batchCtx := core.NewBatchContext()

	var totalFetchedRows, totalTxCount, totalDLQCount int

	// Process each LSN in order
	for i, lsn := range lsns {
		// Check for cancellation
		if ctxErr := replayCtx.Err(); ctxErr != nil {
			slog.Warn("replay cancelled",
				"error", ctxErr,
				"processed", result.ProcessedLSNs,
				"failed", result.FailedLSNs,
				"total", len(lsns))
			break
		}

		lsnResult, err := r.replayLSN(replayCtx, lsn, result, batchCtx)
		if err != nil {
			slog.Error("failed to replay LSN", "lsn", lsn, "index", i+1, "total", len(lsns), "error", err)
			result.FailedLSNs++
			if lsnResult != nil {
				totalDLQCount += lsnResult.DLQCount
			}
			continue
		}

		if lsnResult != nil {
			totalFetchedRows += lsnResult.FetchedRows
			totalTxCount += lsnResult.TxCount
			totalDLQCount += lsnResult.DLQCount
		}

		result.ProcessedLSNs++
		slog.Debug("replayed LSN", "lsn", lsn, "index", i+1, "total", len(lsns))

		// Update metadata for progress tracking
		r.stateManager.SetMetadata("processed", result.ProcessedLSNs)
		r.stateManager.SetMetadata("total", len(lsns))

		if progressCb != nil {
			progressCb(result.ProcessedLSNs, len(lsns))
		}
	}

	// Write batch_log
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

// replayLSN replays all changes for a specific LSN
func (r *ReplayService) replayLSN(ctx context.Context, lsn string, result *ReplayResult, batchCtx *core.BatchContext) (*LSNReplayResult, error) {
	changes, err := r.store.GetChangesWithLSN(lsn)
	if err != nil {
		return nil, fmt.Errorf("get changes for LSN %s: %w", lsn, err)
	}

	if len(changes) == 0 {
		slog.Debug("no changes for LSN", "lsn", lsn)
		return &LSNReplayResult{}, nil
	}

	result.TotalChanges += len(changes)
	tx := r.buildTransaction(changes)

	if len(tx.Changes) == 0 {
		slog.Debug("replayLSN: skip LSN with no valid changes", "lsn", lsn)
		return &LSNReplayResult{}, nil
	}

	dlqCount := 0
	if err := r.handler.Handle(ctx, tx.Changes, batchCtx); err != nil {
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
func (r *ReplayService) buildTransaction(changes []core.Change) *core.Transaction {
	txMap := make(map[string]*core.Transaction)

	for _, c := range changes {
		if c.Operation == core.OpUpdateBefore {
			slog.Debug("buildTransaction: dropping UPDATE_BEFORE",
				"table", c.Table,
				"tx_id", c.TransactionID,
				"lsn", fmt.Sprintf("%x", c.LSN))
			continue
		}

		tx, exists := txMap[c.TransactionID]
		if !exists {
			tx = core.NewTransaction(c.TransactionID)
			txMap[c.TransactionID] = tx
		}
		tx.AddChange(c)
	}

	if len(txMap) == 0 {
		return &core.Transaction{ID: "", Changes: []core.Change{}}
	}

	for _, tx := range txMap {
		return tx
	}

	return &core.Transaction{ID: "", Changes: []core.Change{}}
}

// writeToDLQ writes a failed transaction to DLQ
func (r *ReplayService) writeToDLQ(tx *core.Transaction, handlerErr error, lsn string, source string) {
	if r.dlq == nil {
		slog.Warn("cannot write to DLQ: not initialized", "trace_id", tx.TraceID, "tx_id", tx.ID)
		return
	}

	var tableName, operation string
	if len(tx.Changes) > 0 {
		tableName = tx.Changes[0].Table
		operation = tx.Changes[0].Operation.String()
	}

	txData := map[string]interface{}{
		"transaction_id": tx.ID,
		"changes":        tx.Changes,
	}
	changeJSON, _ := json.Marshal(txData)

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
		slog.Error("failed to write DLQ entry", "trace_id", tx.TraceID, "tx_id", tx.ID, "error", writeErr)
		return
	}

	slog.Warn("transaction written to DLQ during replay",
		"trace_id", tx.TraceID,
		"tx_id", tx.ID,
		"table", tableName,
		"operation", operation,
		"lsn", lsn)
}