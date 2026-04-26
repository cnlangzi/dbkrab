package replay

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/json"
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
	transformer  core.Transformer
	dlq          *dlq.DLQ
	monitorDB    *monitor.DB
	stateManager *core.StateManager // State coordination with Poller/Snapshot

	// runningCancel is the cancel func of the currently running replay (nil if not running)
	runningCancel context.CancelFunc
	runningMu     sync.Mutex
}

// NewReplayService creates a new ReplayService.
func NewReplayService(store Store, transformer core.Transformer, dlq *dlq.DLQ, monitorDB *monitor.DB, stateManager *core.StateManager) *ReplayService {
	return &ReplayService{
		store:        store,
		transformer:  transformer,
		dlq:          dlq,
		monitorDB:    monitorDB,
		stateManager: stateManager,
	}
}

// Execute starts replay in a goroutine and returns immediately.
// Returns (nil, nil) if replay is already running.
// This function manages StateReplay/StateIdle transitions.
// Replay runs to completion - there is no stop functionality.
func (r *ReplayService) Execute(ctx context.Context, progressCb ProgressCallback) (*ReplayResult, error) {
	// Check if replay is already running
	r.runningMu.Lock()
	if r.runningCancel != nil {
		r.runningMu.Unlock()
		return nil, nil // Already running
	}

	// Create a context that will never be cancelled (replay runs to completion)
	runningCtx, cancel := context.WithCancel(context.Background())
	r.runningCancel = cancel
	r.runningMu.Unlock()

	// Set state to replay - Poller will detect and skip processing
	r.stateManager.Set(core.StateReplay)

	// Start replay in goroutine
	go func() {
		defer func() {
			r.runningMu.Lock()
			r.runningCancel = nil
			r.runningMu.Unlock()
			r.stateManager.Set(core.StateIdle)
		}()

		r.runReplay(runningCtx, progressCb)
	}()

	return nil, nil // Started successfully
}

// runReplay does the actual replay work. Called from goroutine.
func (r *ReplayService) runReplay(ctx context.Context, progressCb ProgressCallback) {
	// Get all unique LSNs from store
	lsns, err := r.store.GetLSNs()
	if err != nil {
		slog.Error("replay failed to get LSNs", "error", err)
		return
	}

	if len(lsns) == 0 {
		slog.Info("no changes to replay")
		return
	}

	slog.Info("starting replay", "lsn_count", len(lsns))

	result := &ReplayResult{TotalLSNs: len(lsns)}
	batchCtx := core.NewBatchContext()

	var totalFetchedRows, totalTxCount, totalDLQCount int

	// Process each LSN in order (no cancellation - runs to completion)
	for i, lsn := range lsns {
		lsnResult, err := r.replayLSN(ctx, lsn, result, batchCtx)
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
		var status monitor.PullStatus
		switch {
		case result.ProcessedLSNs == 0 && result.FailedLSNs > 0:
			status = monitor.PullStatusFailed
		case result.ProcessedLSNs < result.TotalLSNs:
			status = monitor.PullStatusPartial
		default:
			status = monitor.PullStatusSuccess
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
	if err := r.transformer.Transform(ctx, tx.Changes, batchCtx); err != nil {
		r.writeToDLQ(tx, err, lsn, "transformer")
		dlqCount++
		return &LSNReplayResult{FetchedRows: len(changes), TxCount: 1, DLQCount: dlqCount}, fmt.Errorf("transform transaction %s: %w", tx.ID, err)
	}

	return &LSNReplayResult{
		FetchedRows: len(changes),
		TxCount:     1,
		DLQCount:    dlqCount,
	}, nil
}

// buildTransaction builds a Transaction from Change slice.
// All changes are aggregated into a single transaction (one LSN = one target-side transaction).
func (r *ReplayService) buildTransaction(changes []core.Change) *core.Transaction {
	tx := core.NewTransaction("")
	var txID string

	for _, c := range changes {
		if c.Operation == core.OpUpdateBefore {
			slog.Debug("buildTransaction: dropping UPDATE_BEFORE",
				"table", c.Table,
				"tx_id", c.TransactionID,
				"lsn", fmt.Sprintf("%x", c.LSN))
			continue
		}

		if txID == "" {
			txID = c.TransactionID
			tx.ID = txID
		}
		tx.AddChange(c)
	}

	return tx
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
	changeJSON, err := json.Marshal(txData)
	if err != nil {
		slog.Error("failed to marshal transaction for DLQ", "error", err, "tx_id", tx.ID)
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