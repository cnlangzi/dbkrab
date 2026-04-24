package snapshot

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/offset"
)

// CDCTable represents a CDC-enabled table.
type CDCTable struct {
	Schema string
	Name   string
}

// GetCDCTables returns CDC-enabled tables from config.
func GetCDCTables(cfg *config.Config) []CDCTable {
	tables := make([]CDCTable, 0, len(cfg.Tables))
	for i, t := range cfg.Tables {
		parts := strings.SplitN(t, ".", 2)
		if len(parts) != 2 {
			slog.Warn("snapshot: skipped malformed table entry", "index", i, "entry", t)
			continue
		}
		tables = append(tables, CDCTable{
			Schema: parts[0],
			Name:   parts[1],
		})
	}
	return tables
}

// Progress represents the current state of a snapshot operation.
type Progress struct {
	TotalTables     int
	ProcessedTables int
	CurrentTable    string
	Started         bool
	Completed       bool
	Stopped         bool
	Error           string
}

// SnapshotCapturer fetches full table snapshots via Runtime.
// It processes tables one by one, returning changes in batches of 1000.
// After all tables are processed it returns NextCapturer: CapturerCDC.
type SnapshotCapturer struct {
	querier     *Querier
	tables      []CDCTable
	offsetStore offset.StoreInterface

	mu           sync.Mutex
	pending      bool // set by Restart, cleared on first Fetch; reports as Started so status shows "running"
	started      bool
	completed    bool
	stopped      bool
	stopCh       chan struct{}
	tableIndex   int
	currentTable string
	changes      []core.CaptureChange
	changeIndex  int
	lastError    string
}

// NewSnapshotCapturer creates a new SnapshotCapturer.
// offsetStore is used to persist the start LSN for each table after it is snapshotted,
// so CDC can resume from the correct position afterwards.
func NewSnapshotCapturer(querier *Querier, tables []CDCTable, offsetStore offset.StoreInterface) *SnapshotCapturer {
	return &SnapshotCapturer{
		querier:     querier,
		tables:      tables,
		offsetStore: offsetStore,
		stopCh:      make(chan struct{}),
	}
}

// Restart resets all state so the capturer can run a fresh snapshot.
// It is safe to call even if the previous run completed or was stopped.
func (c *SnapshotCapturer) Restart() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = true // marks "triggered but Fetch not yet called"
	c.started = false
	c.completed = false
	c.stopped = false
	c.tableIndex = 0
	c.currentTable = ""
	c.changes = nil
	c.changeIndex = 0
	c.lastError = ""
	c.stopCh = make(chan struct{})
}

// Stop signals the capturer to stop. Idempotent.
func (c *SnapshotCapturer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	c.stopped = true
	close(c.stopCh)
}

// Progress returns the current snapshot progress.
func (c *SnapshotCapturer) Progress() Progress {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Progress{
		TotalTables:     len(c.tables),
		ProcessedTables: c.tableIndex,
		CurrentTable:    c.currentTable,
		Started:         c.started || c.pending, // pending counts as started for status reporting
		Completed:       c.completed,
		Stopped:         c.stopped,
		Error:           c.lastError,
	}
}

// Fetch processes tables one by one, returning changes in batches of 1000.
// Returns NextCapturer: CapturerCDC when all tables are done.
func (c *SnapshotCapturer) Fetch(ctx context.Context) *core.CaptureResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return &core.CaptureResult{NextCapturer: core.CapturerCDC}
	}
	if c.completed {
		return &core.CaptureResult{NextCapturer: core.CapturerCDC}
	}

	if !c.started {
		c.pending = false
		c.started = true
		slog.Info("SnapshotCapturer: starting", "table_count", len(c.tables))
	}

	// Return buffered changes from the current table
	if c.changeIndex < len(c.changes) {
		batchCtx := core.NewBatchContext()
		end := c.changeIndex + 1000
		if end > len(c.changes) {
			end = len(c.changes)
		}
		result := &core.CaptureResult{
			Changes:      c.changes[c.changeIndex:end],
			BatchID:      batchCtx.BatchID,
			NextCapturer: core.CapturerSnapshot,
		}
		c.changeIndex = end
		return result
	}

	// Load next table
	for c.tableIndex < len(c.tables) {
		table := c.tables[c.tableIndex]
		c.tableIndex++
		c.currentTable = fmt.Sprintf("%s.%s", table.Schema, table.Name)

		slog.Info("SnapshotCapturer: processing table", "table", c.currentTable,
			"progress", c.tableIndex, "/", len(c.tables))

		handler := newCollectingHandler(table.Name)

		// querier.Run is blocking but releases the mutex implicitly since Fetch holds it —
		// unlock while doing I/O so Stop/Progress calls can proceed.
		c.mu.Unlock()
		startLSN, err := c.querier.Run(ctx, table.Schema, table.Name, handler)
		c.mu.Lock()

		if c.stopped {
			return &core.CaptureResult{NextCapturer: core.CapturerCDC}
		}

		if err != nil {
			slog.Error("SnapshotCapturer: table failed", "table", c.currentTable, "error", err)
			c.lastError = fmt.Sprintf("table %s: %v", c.currentTable, err)
			continue
		}

		// Update offset so CDC resumes from the right LSN after snapshot
		if c.offsetStore != nil && len(startLSN) > 0 {
			c.mu.Unlock()
			nextLSN, lsnErr := c.querier.IncrementLSN(ctx, startLSN)
			c.mu.Lock()

			if lsnErr != nil {
				slog.Warn("SnapshotCapturer: failed to increment LSN", "table", c.currentTable, "error", lsnErr)
			} else {
				startStr := hex.EncodeToString(startLSN)
				nextStr := hex.EncodeToString(nextLSN)
				if setErr := c.offsetStore.Set(c.currentTable, startStr, nextStr); setErr != nil {
					slog.Warn("SnapshotCapturer: failed to set offset", "table", c.currentTable, "error", setErr)
				} else if flushErr := c.offsetStore.Flush(); flushErr != nil {
					slog.Warn("SnapshotCapturer: failed to flush offset", "table", c.currentTable, "error", flushErr)
				}
			}
		}

		if len(handler.Changes) == 0 {
			slog.Info("SnapshotCapturer: table has no rows, skipping", "table", c.currentTable)
			continue
		}

		slog.Info("SnapshotCapturer: table loaded", "table", c.currentTable, "rows", len(handler.Changes))

		c.changes = handler.Changes
		c.changeIndex = 0

		batchCtx := core.NewBatchContext()
		end := 1000
		if end > len(c.changes) {
			end = len(c.changes)
		}
		result := &core.CaptureResult{
			Changes:      c.changes[0:end],
			BatchID:      batchCtx.BatchID,
			NextCapturer: core.CapturerSnapshot,
		}
		c.changeIndex = end
		return result
	}

	// All tables processed
	c.completed = true
	slog.Info("SnapshotCapturer: snapshot completed", "total_tables", len(c.tables))
	return &core.CaptureResult{NextCapturer: core.CapturerCDC}
}

// collectingHandler implements TableHandler to accumulate changes.
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

func (h *collectingHandler) HandleTable(ctx context.Context, changes []core.Change) error {
	for _, c := range changes {
		h.Changes = append(h.Changes, core.CaptureChange{
			"table":       c.Table,
			"data":        c.Data,
			"operation":   int(c.Operation),
			"commit_time": c.CommitTime,
		})
	}
	return nil
}

// Ensure we implement Capturer interface
var _ core.Capturer = (*SnapshotCapturer)(nil)
