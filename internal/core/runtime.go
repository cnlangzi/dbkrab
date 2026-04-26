package core

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/json"
	"github.com/cnlangzi/dbkrab/internal/monitor"
	"github.com/cnlangzi/dbkrab/internal/retry"
)

// Transformer interface for ETL processing of CDC changes
type Transformer interface {
	Transform(ctx context.Context, changes []Change, batchCtx *BatchContext) error
}

// Runtime orchestrates data capture and ETL processing.
// It manages all three capturers (CDC, Snapshot, Replay) and handles
// post-fetch logic (Transformer, DLQ, state management).
// Note: Store is now internal to ChangeCapturer; Runtime does not manage it.
type Runtime struct {
	capturers   map[CapturerName]Capturer
	current     CapturerName
	transformer Transformer
	dlq         *dlq.DLQ
	monitorDB   *monitor.DB
	mu          sync.RWMutex
}

// NewRuntime creates a new Runtime with all capturers initialized.
func NewRuntime(
	capturers map[CapturerName]Capturer,
	transformer Transformer,
	dlq *dlq.DLQ,
	monitorDB *monitor.DB,
) *Runtime {
	return &Runtime{
		capturers:   capturers,
		current:     CapturerCDC,
		transformer: transformer,
		dlq:         dlq,
		monitorDB:   monitorDB,
	}
}

// CDC returns the CDC capturer instance.
func (r *Runtime) CDC() Capturer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.capturers[CapturerCDC]
}

// Snapshot returns the Snapshot capturer instance.
func (r *Runtime) Snapshot() Capturer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.capturers[CapturerSnapshot]
}

// Replay returns the Replay capturer instance.
func (r *Runtime) Replay() Capturer {
	return r.capturers[CapturerReplay]
}

// SwitchTo switches the current capturer to the specified one.
// Called by Dashboard to manually switch between capturers.
func (r *Runtime) SwitchTo(name CapturerName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = name
}

// Run starts the runtime and loops until context is cancelled.
// It polls the current capturer on each tick and switches capturers based on NextCapturer.
func (r *Runtime) Run(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	defer r.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.mu.RLock()
			current := r.current
			capturer := r.capturers[current]
			r.mu.RUnlock()

			result := capturer.Fetch(ctx)

			// Switch capturer if needed
			if result.NextCapturer != current {
				r.mu.Lock()
				r.current = result.NextCapturer
				r.mu.Unlock()
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
}

// Close stops all capturers. Called when runtime is shutting down.
func (r *Runtime) Close() {
	for _, capturer := range r.capturers {
		capturer.Stop()
	}
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
