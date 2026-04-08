package core

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Operation represents the type of change
type Operation int

const (
	OpDelete Operation = 1
	OpInsert Operation = 2
	OpUpdateBefore Operation = 3
	OpUpdateAfter  Operation = 4
)

// String returns the operation name
func (o Operation) String() string {
	switch o {
	case OpDelete:
		return "DELETE"
	case OpInsert:
		return "INSERT"
	case OpUpdateBefore:
		return "UPDATE_BEFORE"
	case OpUpdateAfter:
		return "UPDATE_AFTER"
	default:
		return "UNKNOWN"
	}
}

// Change represents a single row change
type Change struct {
	Table         string                 `json:"table"`
	TransactionID string                 `json:"transaction_id"`
	LSN           []byte                 `json:"lsn"`
	Operation     Operation              `json:"operation"`
	Data          map[string]interface{} `json:"data"`
	CommitTime    time.Time              `json:"commit_time"` // Transaction commit time from source database
}

// Transaction represents a group of changes in the same transaction
type Transaction struct {
	TraceID  string      `json:"trace_id"` // Human-readable trace ID for log correlation
	ID      string      `json:"id"`      // Internal transaction ID
	Changes []Change   `json:"changes"`
	CreatedAt time.Time `json:"created_at"`
}

// AddChange adds a change to the transaction
func (t *Transaction) AddChange(c Change) {
	t.Changes = append(t.Changes, c)
}

// NewTransaction creates a new transaction with a unique trace ID
func NewTransaction(id string) *Transaction {
	return &Transaction{
		TraceID:  generateTraceID(),
		ID:      id,
		Changes: make([]Change, 0),
		CreatedAt: time.Now(),
	}
}

// generateTraceID creates a short, unique trace ID for log correlation
// Format: {short-uuid}-{timestamp}
func generateTraceID() string {
	u := uuid.New()
	return fmt.Sprintf("%s-%d", u.String()[:8], time.Now().UnixNano())
}