package capture

import (
	"context"
	"fmt"
	"time"
)

// Change represents a single row change (captured from CDC).
// It uses plain types to avoid import cycles.
type Change map[string]interface{}

// CaptureResult carries data and metadata from a Capturer fetch operation.
type CaptureResult struct {
	Changes []Change
	BatchID string // Unique identifier for this fetch batch
	EOS     bool   // true = end of stream, no more data
}

// BatchContext carries observability context through the pipeline.
type BatchContext struct {
	BatchID   string
	StartTime time.Time
	SkillName string
}

// NewBatchContext creates a new BatchContext with a unique BatchID.
func NewBatchContext() *BatchContext {
	now := time.Now()
	return &BatchContext{
		BatchID:   fmt.Sprintf("%d", now.UnixMilli()),
		StartTime: now,
	}
}

// Capturer is the interface for data ingestion sources.
// Each implementation corresponds to a CDC mode: incremental (Change), Snapshot, or Replay.
// Capturer only handles data fetching — post-fetch processing (Transformer, DLQ, offset)
// is handled by Runtime.
type Capturer interface {
	// Fetch returns the next batch of changes.
	// Returns CaptureResult with EOS=true when no more data is available.
	// Capturer fills BatchContext with its own metadata (LSN range, table name, etc.)
	Fetch(ctx context.Context) *CaptureResult

	// Stop signals the capturer to stop fetching.
	// Called by Runtime when switching to a different Capturer.
	Stop()
}
