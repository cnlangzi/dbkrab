package replay

import (
	"context"
	"log/slog"
	"sync"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// ReplayCapturer fetches CDC changes from the store for replay.
// It reads stored changes from cdc.db grouped by LSN.
type ReplayCapturer struct {
	store Store

	mu          sync.Mutex
	started     bool
	completed   bool
	stopped     bool
	stopCh      chan struct{}
	lsnIndex    int
	lsns        []string
	changes     []core.CaptureChange
	changeIndex int
}

// NewReplayCapturer creates a new ReplayCapturer.
func NewReplayCapturer(store Store) *ReplayCapturer {
	return &ReplayCapturer{
		store:  store,
		stopCh: make(chan struct{}),
	}
}

// Fetch returns the next batch of changes grouped by LSN.
// Returns CaptureResult with NextCapturer: CapturerCDC when all changes have been replayed.
func (c *ReplayCapturer) Fetch(ctx context.Context) *core.CaptureResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return &core.CaptureResult{NextCapturer: core.CapturerCDC}
	}

	// Initialize LSNs on first call
	if !c.started {
		c.started = true
		lsns, err := c.store.GetLSNs()
		if err != nil {
			slog.Error("ReplayCapturer: failed to get LSNs", "error", err)
			return &core.CaptureResult{NextCapturer: core.CapturerCDC}
		}
		c.lsns = lsns
		c.lsnIndex = 0

		if len(lsns) == 0 {
			slog.Info("ReplayCapturer: no changes to replay")
			c.completed = true
			return &core.CaptureResult{NextCapturer: core.CapturerCDC}
		}

		slog.Info("ReplayCapturer: starting", "lsn_count", len(lsns))
	}

	// Check if we have buffered changes to return
	if c.changeIndex < len(c.changes) {
		// Return buffered changes in batches
		batchSize := 1000
		endIndex := c.changeIndex + batchSize
		if endIndex > len(c.changes) {
			endIndex = len(c.changes)
		}
		batchCtx := core.NewBatchContext()
		result := &core.CaptureResult{
			Changes:      c.changes[c.changeIndex:endIndex],
			BatchID:      batchCtx.BatchID,
			NextCapturer: core.CapturerReplay, // Stay with replay until done
		}
		c.changeIndex = endIndex
		return result
	}

	// Load next LSN's changes
	for c.lsnIndex < len(c.lsns) {
		lsn := c.lsns[c.lsnIndex]
		c.lsnIndex++

		changes, err := c.store.GetChangesWithLSN(lsn)
		if err != nil {
			slog.Error("ReplayCapturer: failed to get changes for LSN", "lsn", lsn, "error", err)
			continue
		}

		if len(changes) == 0 {
			continue
		}

		// Filter UPDATE_BEFORE operations (only process final state)
		filteredChanges := make([]core.CaptureChange, 0, len(changes))
		for _, change := range changes {
			if change.Operation == OpUpdateBefore {
				continue
			}
			// Convert core.Change to core.CaptureChange
			filteredChanges = append(filteredChanges, core.CaptureChange{
				"table":          change.Table,
				"transaction_id": change.TransactionID,
				"lsn":            change.LSN,
				"operation":      int(change.Operation),
				"data":           change.Data,
				"commit_time":    change.CommitTime,
				"id":             change.ID,
			})
		}

		if len(filteredChanges) == 0 {
			continue
		}

		// Store changes for next batch
		c.changes = filteredChanges
		c.changeIndex = 0

		batchCtx := core.NewBatchContext()
		batchSize := 1000
		endIndex := batchSize
		if endIndex > len(c.changes) {
			endIndex = len(c.changes)
		}

		slog.Debug("ReplayCapturer: replaying LSN", "lsn", lsn, "changes", len(c.changes))

		return &core.CaptureResult{
			Changes:      c.changes[c.changeIndex:endIndex],
			BatchID:      batchCtx.BatchID,
			NextCapturer: core.CapturerReplay, // Stay with replay until done
		}
	}

	// All LSNs processed
	c.completed = true
	slog.Info("ReplayCapturer: replay completed", "total_lsns", len(c.lsns))
	return &core.CaptureResult{NextCapturer: core.CapturerCDC}
}

// Stop signals the capturer to stop.
func (c *ReplayCapturer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	close(c.stopCh)
}

// Progress returns the current replay progress.
func (c *ReplayCapturer) Progress() Progress {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Progress{
		TotalLSNs:     len(c.lsns),
		ProcessedLSNs: c.lsnIndex,
		Started:       c.started,
		Completed:     c.completed,
		Stopped:       c.stopped,
	}
}

// OpUpdateBefore is the operation type for UPDATE_BEFORE
const OpUpdateBefore = 3

// Ensure we implement Capturer interface
var _ core.Capturer = (*ReplayCapturer)(nil)
