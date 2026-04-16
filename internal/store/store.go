package store

import (
	"github.com/cnlangzi/dbkrab/internal/core"
)

// Store defines the interface for application persistence (transactions, poller state, etc.)
// It handles writing CDC transactions and provides observability into poller state.
type Store interface {
	// Write writes a transaction to the store.
	// Returns the number of rows actually inserted (0 for duplicates via INSERT OR IGNORE).
	Write(tx *core.Transaction) (int, error)

	// WriteOps writes transformed DataSets from SQL plugins to the store
	WriteOps(ops []core.Sink) error

	// GetChanges retrieves changes from the store
	GetChanges(limit int) ([]map[string]interface{}, error)

	// GetChangesWithFilter retrieves changes with optional filters
	GetChangesWithFilter(limit int, tableName, operation, txID string) ([]map[string]interface{}, error)

	// UpdatePollerState updates the poller state after successful poll.
	// fetchedCount is the number of CDC rows fetched; insertedCount is actually written rows.
	UpdatePollerState(lastLSN string, fetchedCount, insertedCount int) error

	// GetPollerState returns the current poller state
	GetPollerState() (map[string]interface{}, error)

	// Flush ensures all buffered writes are committed to the underlying database.
	Flush() error

	// Close closes the store and releases resources
	Close() error

	// GetLSNs returns all unique LSNs from the store, ordered by LSN
	GetLSNs() ([]string, error)

	// GetChangesWithLSN returns all changes for a specific LSN, as core.Change
	GetChangesWithLSN(lsn string) ([]core.Change, error)
}
