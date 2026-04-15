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

	var lastLSN, nextLSN, maxLSN string
	var updatedAt time.Time

	err := s.db.Reader.QueryRow(
		"SELECT last_lsn, next_lsn, max_lsn, updated_at FROM offsets WHERE table_name = ?",
		table,
	).Scan(&lastLSN, &nextLSN, &maxLSN, &updatedAt)

	if err == sql.ErrNoRows {
		return Offset{}, nil
	}
	if err != nil {
		return Offset{}, err
	}

	return Offset{
		LastLSN:   lastLSN,
		NextLSN:   nextLSN,
		MaxLSN:    maxLSN,
		UpdatedAt: updatedAt,
	}, nil
}

// Set updates the LSN for a table using three-value approach
// lastLSN: last LSN from fetched data
// nextLSN: incrementLSN(lastLSN) - next start point
// maxLSN: GetMaxLSN() result stored at save time
func (s *SQLiteStore) Set(table string, lastLSN string, nextLSN string, maxLSN string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Writer.Exec(`
		INSERT INTO offsets (table_name, last_lsn, next_lsn, max_lsn, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(table_name) DO UPDATE SET 
			last_lsn = excluded.last_lsn, 
			next_lsn = excluded.next_lsn, 
			max_lsn = excluded.max_lsn, 
			updated_at = excluded.updated_at
	`, table, lastLSN, nextLSN, maxLSN, time.Now())

	return err
}

// GetAll returns all offsets
func (s *SQLiteStore) GetAll() (map[string]Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Reader.Query("SELECT table_name, last_lsn, next_lsn, max_lsn, updated_at FROM offsets")
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
		var lastLSN, nextLSN, maxLSN string
		var updatedAt time.Time

		if err := rows.Scan(&table, &lastLSN, &nextLSN, &maxLSN, &updatedAt); err != nil {
			return nil, err
		}

		result[table] = Offset{
			LastLSN:   lastLSN,
			NextLSN:   nextLSN,
			MaxLSN:    maxLSN,
			UpdatedAt: updatedAt,
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