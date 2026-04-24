package snapshot

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// SnapshotCapturer fetches full table snapshots.
// It reads all rows from configured tables using snapshot isolation.
// Snapshot is a one-shot operation - once all tables are read, Fetch returns EOS.
type SnapshotCapturer struct {
	querier *Querier
	tables  []CDCTable

	mu        sync.Mutex
	offset    int              // Current offset in changes slice
	changes   []core.CaptureChange // All captured changes
	started   bool
	completed bool
	stopped   bool
	stopCh    chan struct{}
}

// NewSnapshotCapturer creates a new SnapshotCapturer.
func NewSnapshotCapturer(querier *Querier, tables []CDCTable) *SnapshotCapturer {
	return &SnapshotCapturer{
		querier: querier,
		tables:  tables,
		stopCh:  make(chan struct{}),
	}
}

// Fetch runs the snapshot and returns changes in batches.
// The first call to Fetch starts the snapshot.
// After all tables are processed, returns NextCapturer: CapturerCDC to switch back.
// This is a blocking call that runs the full snapshot.
func (c *SnapshotCapturer) Fetch(ctx context.Context) *core.CaptureResult {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return &core.CaptureResult{NextCapturer: core.CapturerCDC}
	}
	if c.completed {
		c.mu.Unlock()
		return &core.CaptureResult{NextCapturer: core.CapturerCDC}
	}
	if !c.started {
		c.started = true
		c.mu.Unlock()

		batchCtx := core.NewBatchContext()
		fetchTime := time.Now()

		// Run snapshot for each table
		for _, table := range c.tables {
			slog.Info("SnapshotCapturer: processing table", "table", table.Schema+"."+table.Name)

			// Create a handler that collects changes
			handler := newCollectingHandler(table.Name)

			// Run snapshot for this table
			_, err := c.querier.Run(ctx, table.Schema, table.Name, handler)
			if err != nil {
				slog.Error("SnapshotCapturer: failed to run snapshot", "table", table.Schema+"."+table.Name, "error", err)
				// Continue with other tables
				continue
			}

			// Append handler changes to capturer changes
			c.changes = append(c.changes, handler.Changes...)

			slog.Info("SnapshotCapturer: table completed", "table", table.Schema+"."+table.Name, "rows", len(handler.Changes))
		}

		duration := time.Since(fetchTime)
		slog.Info("SnapshotCapturer: snapshot completed", "total_changes", len(c.changes), "duration_ms", duration.Milliseconds())

		c.mu.Lock()
		c.completed = true
		c.mu.Unlock()

		// Return all changes with instruction to switch to ChangeCapturer
		return &core.CaptureResult{
			Changes:      c.changes,
			BatchID:      batchCtx.BatchID,
			NextCapturer: core.CapturerCDC,
		}
	}
	c.mu.Unlock()

	// After started and not completed, but Fetch called again - shouldn't happen
	return &core.CaptureResult{NextCapturer: core.CapturerCDC}
}

// Stop signals the capturer to stop.
func (c *SnapshotCapturer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	close(c.stopCh)
}

// collectingHandler implements TableHandler to collect changes.
type collectingHandler struct {
	TableName string
	Changes   []core.CaptureChange
}

func newCollectingHandler(tableName string) *collectingHandler {
	return &collectingHandler{
		TableName: tableName,
		Changes:   make([]core.CaptureChange, 0),
	}
}

// HandleTable converts core.Change batch to CaptureChange and appends to buffer.
func (h *collectingHandler) HandleTable(ctx context.Context, changes []core.Change) error {
	for _, c := range changes {
		// Convert core.Change to CaptureChange (map)
		change := core.CaptureChange{
			"table":       c.Table,
			"data":        c.Data,
			"operation":    int(c.Operation),
			"commit_time": c.CommitTime,
		}
		h.Changes = append(h.Changes, change)
	}
	return nil
}

// Ensure we implement Capturer interface
var _ core.Capturer = (*SnapshotCapturer)(nil)
