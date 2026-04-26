package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/cnlangzi/dbkrab/internal/json"
	"github.com/google/uuid"
)

// Operation represents the type of change
type Operation int

const (
	OpDelete       Operation = 1
	OpInsert       Operation = 2
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
	ID            string                 `json:"id"`          // Content-based hash ID for deduplication
}

// Transaction represents a group of changes in the same transaction
type Transaction struct {
	TraceID        string    `json:"trace_id"` // Human-readable trace ID for log correlation
	ID             string    `json:"id"`       // Internal transaction ID
	Changes        []Change  `json:"changes"`
	CreatedAt      time.Time `json:"created_at"`
	CommitTime     time.Time `json:"commit_time"`     // Transaction commit time (from first change)
	FirstSeenTime  time.Time `json:"first_seen_time"` // When first change was added to buffer
	InvolvedTables []string  `json:"involved_tables"` // Tables that have changes in this transaction
}

// AddChange adds a change to the transaction
func (t *Transaction) AddChange(c Change) {
	t.Changes = append(t.Changes, c)
}

// NewTransaction creates a new transaction with a unique trace ID
func NewTransaction(id string) *Transaction {
	return &Transaction{
		TraceID:   generateTraceID(),
		ID:        id,
		Changes:   make([]Change, 0),
		CreatedAt: time.Now(),
	}
}

// generateTraceID creates a short, unique trace ID for log correlation
// Format: {short-uuid}-{timestamp}
func generateTraceID() string {
	u := uuid.New()
	return fmt.Sprintf("%s-%d", u.String()[:8], time.Now().UnixNano())
}

// ComputeChangeID computes a content-based hash ID for a change.
// It uses the same algorithm as store/sqlite to ensure consistency.
// The ID is deterministic: same input always produces the same ID.
func ComputeChangeID(txID, table string, data map[string]interface{}, lsn []byte, op Operation) string {
	// Convert LSN bytes to hex string with 0x prefix for deterministic sorting
	var lsnStr string
	if len(lsn) > 0 {
		lsnStr = "0x" + hex.EncodeToString(lsn)
	}

	// Serialize data map to JSON with sorted keys for deterministic output
	var dataJSON []byte
	if len(data) > 0 {
		// Sort keys for deterministic serialization
		keys := make([]string, 0, len(data))
		for k := range data {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Build ordered map for JSON serialization
		orderedData := make(map[string]interface{}, len(data))
		for _, k := range keys {
			orderedData[k] = data[k]
		}
		dataJSON, _ = json.Marshal(orderedData)
	} else {
		dataJSON = []byte("{}")
	}

	// Compute hash: SHA256(transaction_id + table_name + data + lsn + operation)
	hashInput := txID + table + string(dataJSON) + lsnStr + op.String()
	hash := sha256.Sum256([]byte(hashInput))
	return hex.EncodeToString(hash[:16]) // first 16 bytes = 32 hex chars
}
