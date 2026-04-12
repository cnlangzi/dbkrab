package offset

import (
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite"
)

// UnifiedStore manages LSN offsets using the unified SQLite database.
// Reads and writes are immediate (no buffering), so Load/Save are no-ops.
type UnifiedStore struct {
	db *sqlite.DB
	mu sync.RWMutex
}

// NewUnifiedStore creates a new unified offset store that uses the shared DB
func NewUnifiedStore(db *sqlite.DB) *UnifiedStore {
	return &UnifiedStore{
		db: db,
	}
}

// Load is a no-op: data is already in the DB.
func (s *UnifiedStore) Load() error { return nil }

// Save is a no-op: writes are immediate.
func (s *UnifiedStore) Save() error { return nil }

// Get returns the LSN for a table
func (s *UnifiedStore) Get(table string) (Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var lsn string
	var updatedAt time.Time

	err := s.db.Reader.QueryRow(
		"SELECT lsn, updated_at FROM offsets WHERE table_name = ?",
		table,
	).Scan(&lsn, &updatedAt)

	if err == sql.ErrNoRows {
		return Offset{}, nil
	}
	if err != nil {
		return Offset{}, err
	}

	return Offset{
		LSN:       lsn,
		UpdatedAt: updatedAt,
	}, nil
}

// Set updates the LSN for a table
func (s *UnifiedStore) Set(table string, lsn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Writer.Exec(`
		INSERT INTO offsets (table_name, lsn, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(table_name) DO UPDATE SET lsn = excluded.lsn, updated_at = excluded.updated_at
	`, table, lsn, time.Now())

	return err
}

// GetAll returns all offsets
func (s *UnifiedStore) GetAll() (map[string]Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Reader.Query("SELECT table_name, lsn, updated_at FROM offsets")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Warn("rows.Close error", "error", err)
		}
	}()

	result := make(map[string]Offset)
	for rows.Next() {
		var table string
		var lsn string
		var updatedAt time.Time

		if err := rows.Scan(&table, &lsn, &updatedAt); err != nil {
			return nil, err
		}

		result[table] = Offset{
			LSN:       lsn,
			UpdatedAt: updatedAt,
		}
	}

	return result, rows.Err()
}

// Close closes the store (no-op for unified store, DB lifecycle managed externally)
func (s *UnifiedStore) Close() error {
	return nil
}

// Ensure UnifiedStore implements StoreInterface
var _ StoreInterface = (*UnifiedStore)(nil)
