package store

import (
	"github.com/cnlangzi/dbkrab/internal/core"
)

// Store defines the interface for application persistence (transactions, poller state, etc.)
// It handles writing CDC transactions and provides observability into poller state.
type Store interface {
	// Write writes a transaction to the store
	Write(tx *core.Transaction) error

	// WriteOps writes transformed DataSets from SQL plugins to the store
	WriteOps(ops []core.Sink) error

	// GetChanges retrieves changes from the store
	GetChanges(limit int) ([]map[string]interface{}, error)

	// GetChangesWithFilter retrieves changes with optional filters
	GetChangesWithFilter(limit int, tableName, operation, txID string) ([]map[string]interface{}, error)

	// UpdatePollerState updates the poller state after successful poll
	UpdatePollerState(lastLSN string, changeCount int) error

	// GetPollerState returns the current poller state
	GetPollerState() (map[string]interface{}, error)

	// Close closes the store and releases resources
	Close() error
}
