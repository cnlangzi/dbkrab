package core

import (
	"context"
)

// CaptureChange represents a single row change captured from CDC.
// This is the intermediate type used between Capturer and Runtime to avoid import cycles.
type CaptureChange map[string]interface{}

// CaptureResult carries data and metadata from a Capturer fetch operation.
type CaptureResult struct {
	Changes []CaptureChange
	BatchID string // Unique identifier for this fetch batch
	EOS     bool   // true = end of stream, no more data
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