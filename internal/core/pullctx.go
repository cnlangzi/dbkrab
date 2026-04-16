package core

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// PullContext carries observability context through the pipeline.
// It is created at the start of each poll cycle and passed to handlers,
// skills, and sinks for logging correlation.
type PullContext struct {
	// PullID is a unique identifier for each poll cycle (root trace ID)
	// Format: {uuid-short-8chars}-{timestamp-ms}
	PullID string

	// StartTime is when this poll cycle started
	StartTime time.Time
}

// NewPullContext creates a new PullContext with a unique PullID
func NewPullContext() *PullContext {
	now := time.Now()
	u := uuid.New()
	// Generate pull_id: short-uuid-timestamp (e.g., "a1b2c3d4-1712345678")
	pullID := fmt.Sprintf("%s-%d", u.String()[:8], now.UnixMilli())

	return &PullContext{
		PullID:    pullID,
		StartTime: now,
	}
}

// TransactionWithPull wraps a Transaction with its PullContext
type TransactionWithPull struct {
	Transaction *Transaction
	PullCtx     *PullContext
}