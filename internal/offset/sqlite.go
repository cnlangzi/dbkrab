package offset

import (
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite"
)

// SQLiteStore manages LSN offsets using the unified SQLite database.
// Reads and writes are immediate (no buffering), so Load/Save are no-ops.
type SQLiteStore struct {
	db *sqlite.DB
	mu sync.RWMutex
}

// NewSQLiteStore creates a new SQLite offset store that uses the shared DB.
func NewSQLiteStore(db *sqlite.DB) *SQLiteStore {
	return &SQLiteStore{
		db: db,
	}
}

// Load is a no-op: data is already in the DB.
func (s *SQLiteStore) Load() error { return nil }

// Save is a no-op: writes are immediate.
func (s *SQLiteStore) Save() error { return nil }

// Get returns the LSN for a table
func (s *SQLiteStore) Get(table string) (Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var lsn string
	var hasNewData bool
	var updatedAt time.Time

	err := s.db.Reader.QueryRow(
		"SELECT lsn, has_new_data, updated_at FROM offsets WHERE table_name = ?",
		table,
	).Scan(&lsn, &hasNewData, &updatedAt)

	if err == sql.ErrNoRows {
		return Offset{}, nil
	}
	if err != nil {
		return Offset{}, err
	}

	return Offset{
		LSN:        lsn,
		HasNewData: hasNewData,
		UpdatedAt:  updatedAt,
	}, nil
}

// Set updates the LSN for a table
func (s *SQLiteStore) Set(table string, lsn string, hasNewData bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Writer.Exec(`
		INSERT INTO offsets (table_name, lsn, has_new_data, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(table_name) DO UPDATE SET lsn = excluded.lsn, has_new_data = excluded.has_new_data, updated_at = excluded.updated_at
	`, table, lsn, hasNewData, time.Now())

	return err
}

// GetAll returns all offsets
func (s *SQLiteStore) GetAll() (map[string]Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Reader.Query("SELECT table_name, lsn, has_new_data, updated_at FROM offsets")
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
		var hasNewData bool
		var updatedAt time.Time

		if err := rows.Scan(&table, &lsn, &hasNewData, &updatedAt); err != nil {
			return nil, err
		}

		result[table] = Offset{
			LSN:        lsn,
			HasNewData: hasNewData,
			UpdatedAt:  updatedAt,
		}
	}

	return result, rows.Err()
}

// Close closes the store (no-op for unified store, DB lifecycle managed externally)
func (s *SQLiteStore) Close() error {
	return nil
}

// Ensure SQLiteStore implements StoreInterface
var _ StoreInterface = (*SQLiteStore)(nil)