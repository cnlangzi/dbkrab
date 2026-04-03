package core

import "time"

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
}

// Transaction represents a group of changes in the same transaction
type Transaction struct {
	ID        string    `json:"id"`
	Changes   []Change  `json:"changes"`
	CreatedAt time.Time `json:"created_at"`
}

// AddChange adds a change to the transaction
func (t *Transaction) AddChange(c Change) {
	t.Changes = append(t.Changes, c)
}

// NewTransaction creates a new transaction
func NewTransaction(id string) *Transaction {
	return &Transaction{
		ID:        id,
		Changes:   make([]Change, 0),
		CreatedAt: time.Now(),
	}
}