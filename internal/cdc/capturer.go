package cdc

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/cnlangzi/dbkrab/internal/capture"
	"github.com/cnlangzi/dbkrab/internal/offset"
)

// ChangeCapturer fetches CDC changes via incremental polling.
// It wraps cdc.Querier to perform timer-triggered poll cycles.
type ChangeCapturer struct {
	querier  *Querier
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

// NewChangeCapturer creates a new ChangeCapturer.
func NewChangeCapturer(querier *Querier, tables []string, offsets OffsetGetter, interval time.Duration) *ChangeCapturer {
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
func (c *ChangeCapturer) Fetch(ctx context.Context) *capture.CaptureResult {
	batchCtx := capture.NewBatchContext()
	fetchTime := time.Now()

	// Get global max LSN for consistent snapshot across tables
	globalMaxLSN, err := c.querier.GetMaxLSN(ctx)
	if err != nil {
		slog.Error("ChangeCapturer: failed to get max LSN", "error", err)
		return &capture.CaptureResult{BatchID: batchCtx.BatchID, EOS: false}
	}

	var allChanges []capture.Change

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

		// Convert to capture.Change (map)
		for _, cc := range cdcChanges {
			hashID := ComputeChangeID(cc.TransactionID, cc.Table, cc.Data, cc.LSN, cc.Operation)
			change := capture.Change{
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

	return &capture.CaptureResult{
		Changes: allChanges,
		BatchID: batchCtx.BatchID,
		EOS:      false,
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
func ComputeChangeID(txID, table string, data map[string]interface{}, lsn []byte, op int) string {
	// Simple hash using txID + table + lsn hex + operation
	h := txID + table + hex.EncodeToString(lsn) + string(rune(op))
	// Use FNV hash for simplicity
	var hash uint64
	for i := range h {
		hash = hash*31 + uint64(h[i])
	}
	result := fmt.Sprintf("%x", hash)
	if len(result) < 16 {
		return result
	}
	return result[:16]
}
