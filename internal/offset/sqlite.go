package offset

import (
	"database/sql"
	"log"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteStore manages LSN offsets using SQLite database
type SQLiteStore struct {
	db     *sql.DB
	mu     sync.RWMutex
	closed bool
}

// NewSQLiteStore creates a new SQLite offset store
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Enable WAL mode for better concurrency
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			// Log but don't mask the original error
		}
		return nil, err
	}

	// Create table if not exists
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS offsets (
			table_name TEXT PRIMARY KEY,
			lsn TEXT NOT NULL,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			// Log but don't mask the original error
		}
		return nil, err
	}

	return &SQLiteStore{
		db: db,
	}, nil
}

// Load is a no-op for SQLite (data is always persisted)
func (s *SQLiteStore) Load() error {
	return nil
}

// Save is a no-op for SQLite (writes are immediate)
func (s *SQLiteStore) Save() error {
	return nil
}

// Get returns the LSN for a table
func (s *SQLiteStore) Get(table string) (Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return Offset{}, ErrStoreClosed
	}

	var lsn string
	var updatedAt time.Time

	err := s.db.QueryRow(
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
func (s *SQLiteStore) Set(table string, lsn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO offsets (table_name, lsn, updated_at)
		VALUES (?, ?, ?)
	`, table, lsn, time.Now())

	return err
}

// GetAll returns all offsets
func (s *SQLiteStore) GetAll() (map[string]Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStoreClosed
	}

	rows, err := s.db.Query("SELECT table_name, lsn, updated_at FROM offsets")
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Log but don't return error for Close
			log.Printf("rows.Close error: %v", closeErr)
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

// Close closes the database connection
func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	return s.db.Close()
}
