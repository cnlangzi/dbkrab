package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/monitor"
	"github.com/cnlangzi/dbkrab/internal/retry"
)

// Transformer interface for ETL processing of CDC changes
type Transformer interface {
	Transform(ctx context.Context, changes []Change, batchCtx *BatchContext) error
}

// Runtime orchestrates data capture and ETL processing.
// It holds a Capturer for data ingestion and handles post-fetch logic
// (Transformer, DLQ, state management).
// Note: Store is now internal to ChangeCapturer; Runtime does not manage it.
type Runtime struct {
	transformer Transformer
	stateMgr    *StateManager
	dlq         *dlq.DLQ
	monitorDB   *monitor.DB

	currentCapturer Capturer
	mu              sync.Mutex
}

// NewRuntime creates a new Runtime.
func NewRuntime(
	transformer Transformer,
	stateMgr *StateManager,
	dlq *dlq.DLQ,
	monitorDB *monitor.DB,
) *Runtime {
	return &Runtime{
		transformer: transformer,
		stateMgr:    stateMgr,
		dlq:         dlq,
		monitorDB:   monitorDB,
	}
}

// Run starts the runtime with the given Capturer and runs until EOS or error.
func (r *Runtime) Run(ctx context.Context, capturer Capturer) error {
	r.mu.Lock()
	if r.currentCapturer != nil {
		r.currentCapturer.Stop()
	}
	r.currentCapturer = capturer
	r.mu.Unlock()

	// Use a ticker to drive polling cycles.
	// For ChangeCapturer: polls at configured interval.
	// For Snapshot/Replay (one-shot): returns EOS on first Fetch, loop exits immediately.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		// Wait for next poll cycle or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Poll when ticker fires
		}

		result := capturer.Fetch(ctx)
		if result.EOS {
			return nil
		}

		// Skip processing if context is already cancelled
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if len(result.Changes) == 0 {
			continue
		}

		batchCtx := &BatchContext{
			BatchID:   result.BatchID,
			StartTime: time.Now(),
		}

		// Convert capture.Change to core.Change
		changes := r.convertChanges(result.Changes)

		// Transformer processing with retry
		if r.transformer != nil {
			var transformerErr error
			err := retry.DoWithName(ctx, func() error {
				transformerErr = r.transformer.Transform(ctx, changes, batchCtx)
				return transformerErr
			}, retry.DefaultRetryConfig(), "transform")
			if err != nil {
				slog.Error("transformer error", "error", err, "batch_id", batchCtx.BatchID)
				r.writeToDLQ(changes, transformerErr, "transformer")
			}
		}

		// Write batch log (after transformer, regardless of capturer type)
		if r.monitorDB != nil {
			pullStatus := monitor.PullStatusSuccess
			durationMs := time.Since(batchCtx.StartTime).Milliseconds()
			batchLog := &monitor.BatchLog{
				BatchID:     batchCtx.BatchID,
				FetchedRows: len(changes),
				TxCount:     0,
				DLQCount:    0,
				DurationMs:  durationMs,
				Status:      pullStatus,
				CreatedAt:   batchCtx.StartTime,
			}
			if err := r.monitorDB.WriteBatchLog(batchLog); err != nil {
				slog.Warn("failed to write batch_log", "error", err)
			}
			if err := r.monitorDB.Flush(); err != nil {
				slog.Warn("failed to flush logs_db", "error", err)
			}
		}
	}
}

// SwitchCapturer stops the current Capturer and starts a new one.
func (r *Runtime) SwitchCapturer(ctx context.Context, capturer Capturer) error {
	return r.Run(ctx, capturer)
}

// convertChanges converts CaptureChange (map) to core.Change.
func (r *Runtime) convertChanges(captureChanges []CaptureChange) []Change {
	changes := make([]Change, 0, len(captureChanges))
	for _, cc := range captureChanges {
		// Extract required fields from map
		table, _ := cc["table"].(string)
		txID, _ := cc["transaction_id"].(string)
		lsn, _ := cc["lsn"].([]byte)
		opVal, _ := cc["operation"].(int)
		data, _ := cc["data"].(map[string]interface{})
		commitTime, _ := cc["commit_time"].(time.Time)
		id, _ := cc["id"].(string)

		changes = append(changes, Change{
			Table:         table,
			TransactionID: txID,
			LSN:           lsn,
			Operation:     Operation(opVal),
			Data:          data,
			CommitTime:    commitTime,
			ID:            id,
		})
	}
	return changes
}

// writeToDLQ writes failed changes to the dead letter queue.
func (r *Runtime) writeToDLQ(changes []Change, err error, source string) {
	if r.dlq == nil {
		slog.Warn("cannot write to DLQ: not initialized", "source", source)
		return
	}

	for _, c := range changes {
		changeJSON, _ := json.Marshal(c)
		entry := &dlq.DLQEntry{
			TraceID:      c.ID,
			Source:       source,
			LSN:          fmt.Sprintf("%x", c.LSN),
			TableName:    c.Table,
			Operation:    c.Operation.String(),
			ChangeData:   string(changeJSON),
			ErrorMessage: source + " error: " + err.Error(),
			RetryCount:   0,
			Status:       dlq.StatusPending,
		}
		if writeErr := r.dlq.Write(entry); writeErr != nil {
			slog.Error("failed to write DLQ entry", "error", writeErr)
		}
	}
}
