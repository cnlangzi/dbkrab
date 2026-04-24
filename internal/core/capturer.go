package core

import (
	"context"
)

// CapturerName represents the name of a data capturer.
type CapturerName string

const (
	CapturerCDC     CapturerName = "cdc"
	CapturerSnapshot CapturerName = "snapshot"
	CapturerReplay  CapturerName = "replay"
)

// CaptureChange represents a single row change captured from CDC.
// This is the intermediate type used between Capturer and Runtime to avoid import cycles.
type CaptureChange map[string]interface{}

// CaptureResult carries data and metadata from a Capturer fetch operation.
type CaptureResult struct {
	Changes      []CaptureChange
	BatchID      string         // Unique identifier for this fetch batch
	NextCapturer CapturerName   // Which capturer to use for next fetch
}

// Capturer is the interface for data ingestion sources.
// Each implementation corresponds to a CDC mode: incremental (Change), Snapshot, or Replay.
// Capturer only handles data fetching — post-fetch processing (Transformer, DLQ, offset)
// is handled by Runtime.
type Capturer interface {
	// Fetch returns the next batch of changes and indicates which capturer to use next.
	Fetch(ctx context.Context) *CaptureResult

	// Stop signals the capturer to stop fetching.
	// Called by Runtime when the overall system is shutting down.
	Stop()
}