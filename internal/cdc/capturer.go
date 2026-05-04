package cdc

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/offset"
	"github.com/cnlangzi/dbkrab/internal/store"
)

// ChangeCapturer fetches CDC changes via incremental polling.
// It wraps CDCQuerier to perform timer-triggered poll cycles.
type ChangeCapturer struct {
	Querier       CDCQuerier       // Exported for testing
	Tables        []string         // Exported for testing
	OffsetMgr     *OffsetManager   // Exported for testing
	Store         store.Store      // Exported for testing
	SchemaCache   *TableSchemaCache // Primary key cache for tables
	Interval      time.Duration   // Exported for testing
	stopCh        chan struct{}
	stopped       bool
	lastPollTime  time.Time    // Last time Fetch() was called
	totalChanges  int          // Total CDC rows fetched (persisted to DB)
	totalInserted int          // Total rows actually written (persisted to DB)
	mu            sync.RWMutex // Protects Tables field for hot reload
}

// CDCQuerier interface for CDC database operations (allows mocking in tests)
type CDCQuerier interface {
	GetMinLSN(ctx context.Context, captureInstance string) ([]byte, error)
	GetMaxLSN(ctx context.Context) ([]byte, error)
	IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error)
	GetChanges(ctx context.Context, captureInstance string, tableName string, fromLSN []byte, toLSN []byte) ([]Change, error)
}

// NewChangeCapturer creates a new ChangeCapturer from high-level dependencies.
// It internally builds the Querier and OffsetManager from the provided config and db.
func NewChangeCapturer(db *sql.DB, cfg *config.Config, offsetStore offset.StoreInterface, appStore store.Store) (*ChangeCapturer, error) {
	interval, err := cfg.Interval()
	if err != nil {
		return nil, fmt.Errorf("parse cdc interval: %w", err)
	}

	querier := NewQuerier(db, config.ParseTimezone(cfg.MSSQL.Timezone))
	offsetMgr := NewOffsetManager(offsetStore, querier)

	c := &ChangeCapturer{
		Querier:    querier,
		Tables:    cfg.Tables,
		OffsetMgr: offsetMgr,
		Store:    appStore,
		Interval: interval,
		stopCh:    make(chan struct{}),
	}

	// Initialize schema cache and load primary keys
	c.SchemaCache = NewTableSchemaCache(db)
	if err := c.SchemaCache.Load(context.Background(), cfg.Tables); err != nil {
		slog.Warn("ChangeCapturer: failed to load schema cache", "error", err)
		// Continue without cache - not a fatal error
	}

	// Restore poller state from DB on startup
	if appStore != nil {
		if state, err := appStore.GetPollerState(); err == nil {
			if v, ok := state["total_changes"].(int); ok {
				c.totalChanges = v
			}
			if v, ok := state["total_inserted"].(int); ok {
				c.totalInserted = v
			}
			if v, ok := state["last_poll_time"].(string); ok && v != "" {
				if t, err := time.Parse("2006-01-02 15:04:05.999999999", v); err == nil {
					c.lastPollTime = t
				}
			}
		}
	}

	return c, nil
}

// Fetch performs one poll cycle and returns the changes.
// Returns CaptureResult with EOS=false when changes are available.
// Returns CaptureResult with EOS=true when no changes are available (idle cycle).
// The caller should call Fetch in a loop with appropriate timing.
func (c *ChangeCapturer) Fetch(ctx context.Context) *core.CaptureResult {
	batchCtx := core.NewBatchContext()
	fetchTime := time.Now()
	c.lastPollTime = fetchTime

	// Get global max LSN for consistent snapshot across tables
	globalMaxLSN, err := c.Querier.GetMaxLSN(ctx)
	if err != nil {
		slog.Error("ChangeCapturer: failed to get max LSN", "error", err)
		return &core.CaptureResult{BatchID: batchCtx.BatchID, NextCapturer: core.CapturerCDC}
	}

	var allChanges []core.CaptureChange
	maxLSNByTable := make(map[string][]byte) // table -> max LSN of changes

	// Poll each table (read tables under lock to support hot reload)
	c.mu.RLock()
	tables := c.Tables
	c.mu.RUnlock()

	for _, table := range tables {
		schema, tableName := ParseTableName(table)
		captureInstance := CaptureInstanceName(schema, tableName)

		// Get stored offset
		stored, err := c.OffsetMgr.Get(table)
		if err != nil {
			slog.Warn("ChangeCapturer: failed to get offset", "table", table, "error", err)
			continue
		}

		// Determine fromLSN
		fromLSN, err := c.getFromLSN(ctx, table, stored, globalMaxLSN)
		if err != nil {
			slog.Error("ChangeCapturer: GetFromLSN failed", "table", table, "error", err)
			continue
		}

		// No new data for this table
		if fromLSN == nil {
			continue
		}

		// Get changes
		cdcChanges, err := c.Querier.GetChanges(ctx, captureInstance, tableName, fromLSN, nil)
		if err != nil {
			slog.Error("ChangeCapturer: GetChanges failed", "table", table, "error", err)
			continue
		}

		// Convert to core.CaptureChange (map) and track max LSN per table
		var tableMaxLSN []byte
		for _, cc := range cdcChanges {
			hashID := ComputeChangeID(cc.TransactionID, cc.Table, cc.Data, cc.LSN, cc.Operation)
			change := core.CaptureChange{
				"table":          cc.Table,
				"transaction_id": cc.TransactionID,
				"lsn":            cc.LSN,
				"operation":      cc.Operation,
				"data":           cc.Data,
				"commit_time":    cc.CommitTime,
				"id":             hashID,
			}
			allChanges = append(allChanges, change)

			// Track max LSN for this table
			if len(cc.LSN) > 0 && (len(tableMaxLSN) == 0 || LSN(cc.LSN).Compare(tableMaxLSN) > 0) {
				tableMaxLSN = cc.LSN
			}
		}
		if len(tableMaxLSN) > 0 {
			maxLSNByTable[table] = tableMaxLSN
		}
	}

	// Write changes to cdc.db store
	var insertedCount int
	lastLSN := hex.EncodeToString(globalMaxLSN)
	if c.Store != nil && len(allChanges) > 0 {
		coreChanges := c.convertToCoreChanges(allChanges)
		n, err := c.Store.Write(coreChanges)
		if err != nil {
			slog.Warn("ChangeCapturer: failed to write to store", "error", err)
			return &core.CaptureResult{NextCapturer: core.CapturerCDC}
		}
		insertedCount = n

		if err := c.Store.Flush(); err != nil {
			slog.Warn("ChangeCapturer: failed to flush store", "error", err)
			return &core.CaptureResult{NextCapturer: core.CapturerCDC}
		}
	}

	// Always update poller state on every poll cycle for liveness monitoring.
	// This updates last_poll_time even when there are no changes (idle state).
	if c.Store != nil {
		if err := c.Store.UpdatePollerState(lastLSN, len(allChanges), insertedCount); err != nil {
			slog.Warn("ChangeCapturer: failed to update poller state", "error", err)
			return &core.CaptureResult{NextCapturer: core.CapturerCDC}
		}
	}

	// Update in-memory counters only after successful store operations
	c.totalChanges += len(allChanges)
	c.totalInserted += insertedCount

	// Save offsets for tables that had changes (only if store operations succeeded)
	for table, maxLSN := range maxLSNByTable {
		if err := c.OffsetMgr.SaveOffset(ctx, table, maxLSN); err != nil {
			slog.Warn("ChangeCapturer: failed to save offset", "table", table, "error", err)
		}
	}

	// Calculate pull duration
	pullDuration := time.Since(fetchTime)

	// Emit metrics
	if len(allChanges) > 0 {
		slog.Info("[ChangeCapturer poll cycle]",
			"cdc_fetched", len(allChanges),
			"cdc_inserted", insertedCount,
			"batch_ms", pullDuration.Milliseconds(),
		)
	}

	return &core.CaptureResult{
		Changes:      allChanges,
		BatchID:      batchCtx.BatchID,
		NextCapturer: core.CapturerCDC,
	}
}

// getFromLSN returns the starting LSN for CDC queries.
func (c *ChangeCapturer) getFromLSN(ctx context.Context, table string, stored offset.Offset, globalMaxLSN []byte) ([]byte, error) {
	// Cold start: no stored offset
	if stored.LastLSN == "" {
		schema, tableName := ParseTableName(table)
		captureInstance := CaptureInstanceName(schema, tableName)
		minLSN, err := c.Querier.GetMinLSN(ctx, captureInstance)
		if err != nil {
			return nil, err
		}
		return minLSN, nil
	}

	// Parse stored LSN
	lastLSNBytes, err := hex.DecodeString(stored.LastLSN)
	if err != nil || len(lastLSNBytes) == 0 {
		schema, tableName := ParseTableName(table)
		captureInstance := CaptureInstanceName(schema, tableName)
		minLSN, err := c.Querier.GetMinLSN(ctx, captureInstance)
		if err != nil {
			return nil, err
		}
		return minLSN, nil
	}

	// Check if new data available
	if len(globalMaxLSN) > 0 && LSN(lastLSNBytes).Compare(LSN(globalMaxLSN)) != 0 {
		// Use next_lsn if available, otherwise increment
		if stored.NextLSN != "" {
			nextLSNBytes, err := hex.DecodeString(stored.NextLSN)
			if err == nil && len(nextLSNBytes) > 0 {
				return nextLSNBytes, nil
			}
		}
		nextLSN, err := c.Querier.IncrementLSN(ctx, lastLSNBytes)
		if err != nil {
			return nil, err
		}
		return nextLSN, nil
	}

	// No new data
	return nil, nil
}

// UpdateTables updates the table list for CDC polling.
// Called by main.go when config reload signal is received.
func (c *ChangeCapturer) UpdateTables(tables []string) {
	// Defensive copy to prevent caller from mutating our configuration.
	newTables := append([]string(nil), tables...)

	c.mu.Lock()
	c.Tables = newTables
	c.mu.Unlock()

	// Reload schema cache with new tables
	if c.SchemaCache != nil {
		if err := c.SchemaCache.Load(context.Background(), newTables); err != nil {
			slog.Warn("ChangeCapturer: failed to reload schema cache", "error", err)
		}
	}

	// Log count and sample of tables (avoid noisy logs with large table lists)
	const maxLoggedTables = 5
	sample := newTables
	if len(newTables) > maxLoggedTables {
		sample = newTables[:maxLoggedTables]
	}
	slog.Info("ChangeCapturer: tables updated",
		"count", len(newTables),
		"tables_sample", sample,
	)
}

// Stop signals the capturer to stop.
func (c *ChangeCapturer) Stop() {
	if c.stopped {
		return // Already stopped, ignore
	}
	c.stopped = true
	close(c.stopCh)
}

// Status returns the current poller status from the CDC capturer.
func (c *ChangeCapturer) Status() map[string]interface{} {
	state := map[string]interface{}{
		"total_changes":  c.totalChanges,
		"total_inserted": c.totalInserted,
	}

	// Get last poll time from in-memory (updated on each Fetch)
	if !c.lastPollTime.IsZero() {
		state["last_poll_time"] = c.lastPollTime.Format("2006-01-02 15:04:05.999999999")
	}

	// Get global last LSN from offset manager (most advanced offset across all tables)
	if c.OffsetMgr != nil {
		offsets, err := c.OffsetMgr.GetAll()
		if err == nil && len(offsets) > 0 {
			var globalLastLSN []byte
			for _, off := range offsets {
				lsnBytes, err := hex.DecodeString(off.LastLSN)
				if err != nil || len(lsnBytes) == 0 {
					continue
				}
				if globalLastLSN == nil || LSN(lsnBytes).Compare(globalLastLSN) > 0 {
					globalLastLSN = lsnBytes
				}
			}
			if len(globalLastLSN) > 0 {
				state["last_lsn"] = hex.EncodeToString(globalLastLSN)
			}
		}
	}

	return state
}

// convertToCoreChanges converts []CaptureChange to []core.Change.
func (c *ChangeCapturer) convertToCoreChanges(captureChanges []core.CaptureChange) []core.Change {
	changes := make([]core.Change, 0, len(captureChanges))
	for _, cc := range captureChanges {
		table, _ := cc["table"].(string)
		txID, _ := cc["transaction_id"].(string)
		lsn, _ := cc["lsn"].([]byte)
		opVal, _ := cc["operation"].(int)
		data, _ := cc["data"].(map[string]interface{})
		commitTime, _ := cc["commit_time"].(time.Time)
		id, _ := cc["id"].(string)

		// Get primary key values from schema cache (field names) and extract actual values from data
		var tableKeysStr string
		if c.SchemaCache != nil && table != "" {
			pkFields, ok := c.SchemaCache.Get(table)
			if ok {
				var pkValues []string
				for _, field := range pkFields {
					if val, exists := data[strings.ToLower(field)]; exists {
						pkValues = append(pkValues, fmt.Sprintf("%v", val))
					}
				}
				// Only set tableKeysStr if ALL PK fields are present; otherwise
				// leave empty to avoid partial composite keys breaking exact-match filters
				if len(pkValues) == len(pkFields) {
					tableKeysStr = strings.Join(pkValues, ",")
				}
			}
		}

		changes = append(changes, core.Change{
			Table:         table,
			TransactionID: txID,
			LSN:           lsn,
			Operation:    core.Operation(opVal),
			Data:         data,
			CommitTime:   commitTime,
			ID:           id,
			TableKeys:    tableKeysStr,
		})
	}
	return changes
}

// LSN is a type alias for LSN comparison
type LSN []byte

// Compare returns comparison result
func (l LSN) Compare(other []byte) int {
	if len(l) != len(other) {
		if len(l) < len(other) {
			return -1
		}
		return 1
	}
	for i := range l {
		if l[i] < other[i] {
			return -1
		}
		if l[i] > other[i] {
			return 1
		}
	}
	return 0
}

// ComputeChangeID computes a content-based hash ID for a change.
// Uses FNV-1a hash which has better distribution than simple polynomial hash.
func ComputeChangeID(txID, table string, data map[string]interface{}, lsn []byte, op int) string {
	// Initialize FNV-1a hash
	const (
		fnvOffset uint64 = 14695981039346656037
		fnvPrime  uint64 = 1099511628211
	)

	hash := fnvOffset

	// Mix in txID
	for _, c := range txID {
		hash ^= uint64(c)
		hash *= fnvPrime
	}

	// Mix in table
	for _, c := range table {
		hash ^= uint64(c)
		hash *= fnvPrime
	}

	// Mix in LSN bytes
	for _, b := range lsn {
		hash ^= uint64(b)
		hash *= fnvPrime
	}

	// Mix in operation
	hash ^= uint64(op)
	hash *= fnvPrime

	// Mix in data keys and values for differentiation (sorted for deterministic hash)
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := data[k]
		for _, c := range k {
			hash ^= uint64(c)
			hash *= fnvPrime
		}
		switch val := v.(type) {
		case int:
			hash ^= uint64(val)
			hash *= fnvPrime
		case string:
			for _, c := range val {
				hash ^= uint64(c)
				hash *= fnvPrime
			}
		}
	}

	return fmt.Sprintf("%016x", hash)
}
