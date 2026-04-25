package cdc

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/cnlangzi/dbkrab/internal/offset"
)

// OffsetManager handles LSN offset persistence for ChangeCapturer.
// It wraps offset.StoreInterface and provides CDC-specific LSN operations.
type OffsetManager struct {
	store   offset.StoreInterface
	querier LSNQuerier
}

// LSNQuerier interface for LSN operations (allows mocking in tests)
type LSNQuerier interface {
	IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error)
}

// NewOffsetManager creates a new OffsetManager.
func NewOffsetManager(store offset.StoreInterface, querier LSNQuerier) *OffsetManager {
	return &OffsetManager{
		store:   store,
		querier: querier,
	}
}

// Get returns the stored offset for a table.
func (m *OffsetManager) Get(table string) (offset.Offset, error) {
	return m.store.Get(table)
}

// SaveOffset saves the offset for a table after processing.
// It calculates next_lsn = IncrementLSN(lsn) and stores both.
func (m *OffsetManager) SaveOffset(ctx context.Context, table string, lsn []byte) error {
	if len(lsn) == 0 {
		return fmt.Errorf("cannot save empty LSN")
	}

	// Calculate next_lsn
	nextLSN, err := m.querier.IncrementLSN(ctx, lsn)
	if err != nil {
		return fmt.Errorf("increment LSN: %w", err)
	}

	// Hex encode for storage
	lastLSNStr := hex.EncodeToString(lsn)
	nextLSNStr := hex.EncodeToString(nextLSN)

	// Persist
	if err := m.store.Set(table, lastLSNStr, nextLSNStr); err != nil {
		return fmt.Errorf("set offset: %w", err)
	}

	return nil
}

// Flush persists any buffered writes.
func (m *OffsetManager) Flush() error {
	return m.store.Flush()
}

// GetAll returns all stored offsets.
func (m *OffsetManager) GetAll() (map[string]offset.Offset, error) {
	return m.store.GetAll()
}
