package cdc

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/offset"
)

// ChangeCapturer fetches CDC changes via incremental polling.
// It wraps CDCQuerier to perform timer-triggered poll cycles.
type ChangeCapturer struct {
	querier  CDCQuerier
	tables   []string
	offsets  OffsetGetter
	interval time.Duration
	stopCh   chan struct{}
	stopped  bool
}

// OffsetGetter interface for reading LSN offsets
type OffsetGetter interface {
	Get(table string) (offset.Offset, error)
}

// CDCQuerier interface for CDC database operations (allows mocking in tests)
type CDCQuerier interface {
	GetMinLSN(ctx context.Context, captureInstance string) ([]byte, error)
	GetMaxLSN(ctx context.Context) ([]byte, error)
	IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error)
	GetChanges(ctx context.Context, captureInstance string, tableName string, fromLSN []byte, toLSN []byte) ([]Change, error)
}

// NewChangeCapturer creates a new ChangeCapturer.
func NewChangeCapturer(querier CDCQuerier, tables []string, offsets OffsetGetter, interval time.Duration) *ChangeCapturer {
	return &ChangeCapturer{
		querier:  querier,
		tables:   tables,
		offsets:  offsets,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Fetch performs one poll cycle and returns the changes.
// Returns CaptureResult with EOS=false when changes are available.
// Returns CaptureResult with EOS=true when no changes are available (idle cycle).
// The caller should call Fetch in a loop with appropriate timing.
func (c *ChangeCapturer) Fetch(ctx context.Context) *core.CaptureResult {
	batchCtx := core.NewBatchContext()
	fetchTime := time.Now()

	// Get global max LSN for consistent snapshot across tables
	globalMaxLSN, err := c.querier.GetMaxLSN(ctx)
	if err != nil {
		slog.Error("ChangeCapturer: failed to get max LSN", "error", err)
		return &core.CaptureResult{BatchID: batchCtx.BatchID, EOS: false}
	}

	var allChanges []core.CaptureChange

	// Poll each table
	for _, table := range c.tables {
		schema, tableName := ParseTableName(table)
		captureInstance := CaptureInstanceName(schema, tableName)

		// Get stored offset
		stored, err := c.offsets.Get(table)
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
		cdcChanges, err := c.querier.GetChanges(ctx, captureInstance, tableName, fromLSN, nil)
		if err != nil {
			slog.Error("ChangeCapturer: GetChanges failed", "table", table, "error", err)
			continue
		}

		// Convert to core.CaptureChange (map)
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
		}
	}

	// Calculate pull duration
	pullDuration := time.Since(fetchTime)

	// Emit metrics
	if len(allChanges) > 0 {
		slog.Info("[ChangeCapturer poll cycle]",
			"cdc_fetched", len(allChanges),
			"batch_ms", pullDuration.Milliseconds(),
		)
	}

	return &core.CaptureResult{
		Changes: allChanges,
		BatchID: batchCtx.BatchID,
		EOS:     false,
	}
}

// getFromLSN returns the starting LSN for CDC queries.
func (c *ChangeCapturer) getFromLSN(ctx context.Context, table string, stored offset.Offset, globalMaxLSN []byte) ([]byte, error) {
	// Cold start: no stored offset
	if stored.LastLSN == "" {
		schema, tableName := ParseTableName(table)
		captureInstance := CaptureInstanceName(schema, tableName)
		minLSN, err := c.querier.GetMinLSN(ctx, captureInstance)
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
		minLSN, err := c.querier.GetMinLSN(ctx, captureInstance)
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
		nextLSN, err := c.querier.IncrementLSN(ctx, lastLSNBytes)
		if err != nil {
			return nil, err
		}
		return nextLSN, nil
	}

	// No new data
	return nil, nil
}

// Stop signals the capturer to stop.
func (c *ChangeCapturer) Stop() {
	if c.stopped {
		return // Already stopped, ignore
	}
	c.stopped = true
	close(c.stopCh)
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

	// Mix in data keys and values for differentiation
	for k, v := range data {
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
