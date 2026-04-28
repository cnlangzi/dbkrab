package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
)

// CDCTable represents a CDC-enabled table.
type CDCTable struct {
	Schema string
	Name   string
}

// FullName returns the schema-qualified table name.
func (t CDCTable) FullName() string {
	return t.Schema + "." + t.Name
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

// TableProgress holds per-table snapshot progress.
type TableProgress struct {
	Table     string
	TotalRows int64
	ReadRows  int64
	Done      bool
}

// Progress represents the current state of a snapshot operation.
type Progress struct {
	TotalTables     int
	ProcessedTables int
	TotalRows       int64
	ReadRows        int64
	CurrentTable    string
	Tables          []TableProgress
	Started         bool
	Completed       bool
	Stopped         bool
	Error           string
	StartTime       time.Time
	EndTime         time.Time
}

// SnapshotCapturer fetches full table snapshots via Runtime.
//
// Lifecycle:
//  1. Restart(ctx) — captures MaxLSN, opens ONE snapshot-isolation transaction,
//     discovers PKs and approximate row counts for all tables.
//  2. Runtime calls Fetch(ctx) repeatedly — each call fetches one batch (BatchSize rows)
//     from the current table using the open transaction.
//  3. When a table is exhausted CDC will automatically pick up from min_lsn (cold start),
//     replaying changes idempotently — no offset checkpoint needed.
//  4. When all tables are done the transaction is committed and NextCapturer: CapturerCDC
//     is returned.
type SnapshotCapturer struct {
	querier       *Querier
	tables        []CDCTable
	pendingTables []CDCTable // staged tables from UpdateTables, applied in Restart

	mu           sync.Mutex
	pending      bool // set by Restart before DB init; reports as Started so status shows "running"
	started      bool
	completed    bool
	stopped      bool
	tx           *sql.Tx // open snapshot-isolation transaction; nil until Restart succeeds
	tableIndex   int
	rowOffset    int // row offset within current table
	currentTable string
	tableTotals  []int64           // approx total rows per table (sys.partitions)
	tableRead    []int64           // rows read so far per table
	tableDone    []bool            // whether each table has been fully read
	tablePKs     []*PrimaryKeyInfo // PK info per table (pre-discovered in Restart)
	lastError    string
	startTime    time.Time
	endTime      time.Time
}

// NewSnapshotCapturer creates a new SnapshotCapturer.
func NewSnapshotCapturer(querier *Querier, tables []CDCTable) *SnapshotCapturer {
	return &SnapshotCapturer{
		querier: querier,
		tables:  tables,
	}
}

// Restart initializes a fresh snapshot run.
// It captures MaxLSN, opens a single snapshot-isolation transaction covering all tables,
// discovers primary keys, and fetches approximate row counts.
// Returns an error if any DB setup step fails; no state is changed in that case.
// selectedTables specifies which tables to snapshot in this run.
func (c *SnapshotCapturer) Restart(ctx context.Context, selectedTables []CDCTable) error {
	// Phase 1: reset state and close any previous transaction.
	c.mu.Lock()
	oldTx := c.tx
	c.pending = true
	// Apply any staged tables from UpdateTables (keeps c.tables in sync with config)
	if c.pendingTables != nil {
		c.tables = c.pendingTables
		c.pendingTables = nil
	}
	c.started = false
	c.completed = false
	c.stopped = false
	c.tx = nil
	c.tableIndex = 0
	c.rowOffset = 0
	c.currentTable = ""
	c.tableTotals = nil
	c.tableRead = nil
	c.tableDone = nil
	c.tablePKs = nil
	c.lastError = ""
	c.mu.Unlock()

	if oldTx != nil {
		if err := oldTx.Rollback(); err != nil {
			slog.Warn("SnapshotCapturer: rollback old tx", "error", err)
		}
	}

	// Phase 2: DB initialisation (no mutex — blocking I/O).

	// 2a. Ensure snapshot isolation is enabled on the database.
	if err := c.querier.CheckSnapshotIsolation(ctx); err != nil {
		c.markError(fmt.Sprintf("snapshot isolation check: %v", err))
		return fmt.Errorf("snapshot isolation check: %w", err)
	}

	// 2b. Open snapshot-isolation transaction for consistent MVCC read view.
	//     IMPORTANT: Use context.Background() (not the caller's HTTP-request context) so
	//     the transaction is NOT automatically rolled back when the HTTP handler returns.

	// 2c. Open ONE snapshot-isolation transaction covering all table reads.
	//     Snapshot isolation gives a consistent MVCC read view — new DML on the source
	//     during snapshot will NOT be visible, ensuring MaxLSN stays valid.
	//     IMPORTANT: Use context.Background() (not the caller's HTTP-request context) so
	//     the transaction is NOT automatically rolled back when the HTTP handler returns.
	tx, err := c.querier.BeginSnapshotTx(context.Background())
	if err != nil {
		c.markError(fmt.Sprintf("begin snapshot tx: %v", err))
		return fmt.Errorf("begin snapshot tx: %w", err)
	}

	// 2d. Per-table setup: approximate row count + PK discovery.
	n := len(selectedTables)
	tableTotals := make([]int64, n)
	tableRead := make([]int64, n)
	tableDone := make([]bool, n)
	tablePKs := make([]*PrimaryKeyInfo, n)

	for i, t := range selectedTables {
		fullName := t.FullName()

		count, countErr := c.querier.GetApproxRowCount(ctx, t.Schema, t.Name)
		if countErr != nil {
			slog.Warn("SnapshotCapturer: approx row count unavailable, using 0",
				"table", fullName, "error", countErr)
		}
		tableTotals[i] = count

		pkInfo, pkErr := c.querier.DiscoverPrimaryKey(ctx, t.Schema, t.Name)
		if pkErr != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				slog.Warn("SnapshotCapturer: rollback after PK error", "error", rollErr)
			}
			c.markError(fmt.Sprintf("discover PK for %s: %v", fullName, pkErr))
			return fmt.Errorf("discover PK for %s: %w", fullName, pkErr)
		}
		tablePKs[i] = pkInfo
		slog.Debug("SnapshotCapturer: table initialised",
			"table", fullName, "approx_rows", count, "pk", pkInfo.Columns)
	}

	// Phase 3: commit initialised state under mutex.
	c.mu.Lock()
	c.tx = tx
	c.tableTotals = tableTotals
	c.tableRead = tableRead
	c.tableDone = tableDone
	c.tablePKs = tablePKs
	c.tables = selectedTables // use the selected tables for this run
	c.startTime = time.Now()
	c.endTime = time.Time{}
	c.mu.Unlock()

	slog.Info("SnapshotCapturer: ready", "tables", n)
	return nil
}

// markError records an error and clears pending (called on Restart failure).
func (c *SnapshotCapturer) markError(msg string) {
	c.mu.Lock()
	c.lastError = msg
	c.pending = false
	c.mu.Unlock()
}

// UpdateTables updates the table list for snapshot.
// Called by main.go when config reload signal is received.
// If a snapshot is currently running or pending, the new tables are staged in
// pendingTables and will be applied when Restart() is called for the next snapshot.
// Otherwise, tables are updated immediately for the next snapshot run.
func (c *SnapshotCapturer) UpdateTables(tables []CDCTable) {
	// Defensive copy to prevent caller from mutating our configuration.
	newTables := make([]CDCTable, len(tables))
	copy(newTables, tables)

	c.mu.Lock()
	// Check if snapshot is active (includes pending state during Restart initialization)
	isActive := c.pending || (c.started && !c.completed && !c.stopped)

	if isActive {
		// Stage the update for the next snapshot run
		c.pendingTables = newTables
		c.mu.Unlock()
		slog.Warn("SnapshotCapturer: tables staged for next snapshot",
			"count", len(newTables), "note", "will apply on next Restart() call")
	} else {
		// Apply immediately since no snapshot is running
		c.tables = newTables
		c.pendingTables = nil // clear any previously staged tables
		c.mu.Unlock()
		slog.Info("SnapshotCapturer: tables updated", "count", len(newTables))
	}
}
func (c *SnapshotCapturer) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	tx := c.tx
	c.tx = nil
	c.mu.Unlock()

	if tx != nil {
		if err := tx.Rollback(); err != nil {
			slog.Warn("SnapshotCapturer: rollback on stop", "error", err)
		}
	}
}

// Progress returns the current snapshot progress for the API/dashboard.
func (c *SnapshotCapturer) Progress() Progress {
	c.mu.Lock()
	defer c.mu.Unlock()

	tables := make([]TableProgress, len(c.tables))
	processedTables := 0
	var totalRows, readRows int64
	for i, t := range c.tables {
		tp := TableProgress{
			Table: t.FullName(),
		}
		if i < len(c.tableTotals) {
			tp.TotalRows = c.tableTotals[i]
			totalRows += c.tableTotals[i]
		}
		if i < len(c.tableRead) {
			tp.ReadRows = c.tableRead[i]
			readRows += c.tableRead[i]
		}
		if i < len(c.tableDone) {
			tp.Done = c.tableDone[i]
			if tp.Done {
				processedTables++
			}
		}
		tables[i] = tp
	}

	return Progress{
		TotalTables:     len(c.tables),
		ProcessedTables: processedTables,
		TotalRows:       totalRows,
		ReadRows:        readRows,
		CurrentTable:    c.currentTable,
		Tables:          tables,
		Started:         c.started || c.pending,
		Completed:       c.completed,
		Stopped:         c.stopped,
		Error:           c.lastError,
		StartTime:       c.startTime,
		EndTime:         c.endTime,
	}
}

// Fetch returns the next batch of snapshot changes.
// Each call reads exactly one batch (up to BatchSize rows) from the current table.
// Returns NextCapturer: CapturerCDC when all tables are exhausted.
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
	// Not yet initialised by Restart — stay idle.
	if !c.started && !c.pending {
		c.mu.Unlock()
		return &core.CaptureResult{NextCapturer: core.CapturerCDC}
	}
	if !c.started {
		c.pending = false
		c.started = true
		slog.Info("SnapshotCapturer: starting", "table_count", len(c.tables))
	}
	if c.tx == nil {
		// Restart hasn't finished DB setup yet — wait for next tick.
		c.mu.Unlock()
		return &core.CaptureResult{NextCapturer: core.CapturerSnapshot}
	}

	// Advance past already-completed tables.
	for c.tableIndex < len(c.tables) && c.tableDone[c.tableIndex] {
		c.tableIndex++
		c.rowOffset = 0
	}

	// All tables done — commit tx and switch to CDC.
	if c.tableIndex >= len(c.tables) {
		tx := c.tx
		c.tx = nil
		c.completed = true
		c.endTime = time.Now()
		c.mu.Unlock()

		if err := tx.Commit(); err != nil {
			slog.Warn("SnapshotCapturer: commit tx", "error", err)
		}
		slog.Info("SnapshotCapturer: completed", "total_tables", len(c.tables))
		return &core.CaptureResult{NextCapturer: core.CapturerCDC}
	}

	// Capture per-batch parameters.
	tableIdx := c.tableIndex
	table := c.tables[tableIdx]
	pkInfo := c.tablePKs[tableIdx]
	batchSize := c.querier.config.BatchSize
	offset := c.rowOffset
	tx := c.tx
	c.currentTable = table.FullName()

	c.mu.Unlock()

	// Execute paged query — no lock held during DB I/O.
	query := pkInfo.BuildPagedQuery(table.Schema, table.Name, batchSize, offset)
	slog.Debug("SnapshotCapturer: fetch batch",
		"table", c.currentTable, "offset", offset, "batch_size", batchSize)

	rows, queryErr := tx.QueryContext(ctx, query)

	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		if rows != nil {
			_ = rows.Close()
		}
		return &core.CaptureResult{NextCapturer: core.CapturerCDC}
	}
	if queryErr != nil {
		slog.Error("SnapshotCapturer: query failed",
			"table", c.currentTable, "offset", offset, "error", queryErr)
		c.lastError = fmt.Sprintf("table %s at offset %d: %v", c.currentTable, offset, queryErr)
		c.tableDone[tableIdx] = true
		c.tableIndex++
		c.rowOffset = 0
		c.mu.Unlock()
		return &core.CaptureResult{NextCapturer: core.CapturerSnapshot}
	}
	c.mu.Unlock()

	batchRows, scanErr := c.querier.ScanBatch(rows)

	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return &core.CaptureResult{NextCapturer: core.CapturerCDC}
	}
	if scanErr != nil {
		slog.Error("SnapshotCapturer: scan failed",
			"table", c.currentTable, "error", scanErr)
		c.lastError = fmt.Sprintf("scan %s: %v", c.currentTable, scanErr)
		c.tableDone[tableIdx] = true
		c.tableIndex++
		c.rowOffset = 0
		c.mu.Unlock()
		return &core.CaptureResult{NextCapturer: core.CapturerSnapshot}
	}

	isLastBatch := len(batchRows) < batchSize
	c.tableRead[tableIdx] += int64(len(batchRows))
	c.rowOffset += len(batchRows)
	readSoFar := c.tableRead[tableIdx]

	if len(batchRows) == 0 || isLastBatch {
		c.tableDone[tableIdx] = true
		c.tableIndex++
		c.rowOffset = 0
	}

	c.mu.Unlock()

	if isLastBatch || len(batchRows) == 0 {
		slog.Info("SnapshotCapturer: table completed",
			"table", c.currentTable, "read_rows", readSoFar)
	}

	if len(batchRows) == 0 {
		return &core.CaptureResult{NextCapturer: core.CapturerSnapshot}
	}

	// Build CaptureChange slice — snapshot rows are treated as INSERT.
	changes := make([]core.CaptureChange, len(batchRows))
	for i, row := range batchRows {
		changes[i] = core.CaptureChange{
			"table":       table.Name,
			"data":        row,
			"operation":   int(core.OpInsert),
			"commit_time": time.Time{},
		}
	}

	batchCtx := core.NewBatchContext()
	return &core.CaptureResult{
		Changes:      changes,
		BatchID:      batchCtx.BatchID,
		NextCapturer: core.CapturerSnapshot,
	}
}

// Ensure SnapshotCapturer satisfies the Capturer interface.
var _ core.Capturer = (*SnapshotCapturer)(nil)
