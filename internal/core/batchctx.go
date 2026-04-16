package core

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// BatchContext carries observability context through the pipeline.
// It is created at the start of each poll cycle and passed to handlers,
// skills, and sinks for logging correlation.
type BatchContext struct {
	// SkillName is the skill being executed (for sink logging)
	SkillName string

	// BatchID is a unique identifier for each poll cycle (root trace ID)
	// Format: {uuid-short-8chars}-{timestamp-ms}
	BatchID string

	// StartTime is when this poll cycle started
	StartTime time.Time
}

// NewBatchContext creates a new BatchContext with a unique BatchID
func NewBatchContext() *BatchContext {
	now := time.Now()
	u := uuid.New()
	// Generate batch_id: short-uuid-timestamp (e.g., "a1b2c3d4-1712345678")
	batchID := fmt.Sprintf("%s-%d", u.String()[:8], now.UnixMilli())

	return &BatchContext{
		BatchID:    batchID,
		StartTime: now,
	}
}

// TransactionWithBatch wraps a Transaction with its BatchContext
type TransactionWithBatch struct {
	Transaction *Transaction
	BatchCtx    *BatchContext
}
