package offset

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite"
	"github.com/yaitoo/sqle"
	"github.com/yaitoo/sqle/migrate"
)

//go:embed migrations
var migrationsFS embed.FS

// SQLiteStore manages LSN offsets using a dedicated SQLite database.
// This DB is separate from the CDC store DB for transactional safety.
// Writes are immediate (no buffering), but Flush() is exposed for interface compatibility.
type SQLiteStore struct {
	db     *sqlite.DB
	mu     sync.RWMutex
	closed bool
}

// New creates a new standalone SQLite offset store with its own DB file.
// It runs migrations separately from the CDC store DB.
func New(ctx context.Context, dbPath string) (*SQLiteStore, error) {
	db, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("open offset database: %w", err)
	}

	if err := runMigrations(db); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			slog.Warn("db.Close error on migration failure", "error", closeErr)
		}
		return nil, fmt.Errorf("run offset migrations: %w", err)
	}

	slog.Info("offset store initialized", "path", dbPath)

	return &SQLiteStore{
		db: db,
	}, nil
}

// NewWithDB creates a new offset store using an existing *sqlite.DB.
// Migrations are run on the given DB. The caller manages the DB lifecycle.
// Use this for testing or when the DB connection is shared.
func NewWithDB(db *sqlite.DB) (*SQLiteStore, error) {
	if err := runMigrations(db); err != nil {
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	return &SQLiteStore{
		db: db,
	}, nil
}

// runMigrations discovers and applies offset schema migrations using sqle/migrate.
func runMigrations(db *sqlite.DB) error {
	sqleDB := sqle.Open(db.Writer.DB)
	migrator := migrate.New(sqleDB)
	if err := migrator.Discover(migrationsFS, migrate.WithModule("dbkrab-offset")); err != nil {
		return fmt.Errorf("discover migrations: %w", err)
	}

	if err := migrator.Init(context.Background()); err != nil {
		return fmt.Errorf("init migrations: %w", err)
	}

	return migrator.Migrate(context.Background())
}

// Load is a no-op: data is already in the DB.
func (s *SQLiteStore) Load() error { return nil }

// Save is a no-op: writes are immediate.
func (s *SQLiteStore) Save() error { return nil }

// Flush ensures all buffered writes are committed to the database.
// For the offset store, writes are immediate, but this method is kept for interface compatibility.
func (s *SQLiteStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	return s.db.Flush()
}

// Get returns the LSN for a table
func (s *SQLiteStore) Get(table string) (Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return Offset{}, ErrStoreClosed
	}

	var lastLSN, nextLSN string
	var updatedAt time.Time

	err := s.db.Reader.QueryRow(
		"SELECT last_lsn, next_lsn, updated_at FROM offsets WHERE table_name = ?",
		table,
	).Scan(&lastLSN, &nextLSN, &updatedAt)

	if err == sql.ErrNoRows {
		return Offset{}, nil
	}
	if err != nil {
		return Offset{}, err
	}

	return Offset{
		LastLSN:   lastLSN,
		NextLSN:   nextLSN,
		UpdatedAt: updatedAt,
	}, nil
}

// Set updates the LSN for a table
// lastLSN: last LSN from fetched data
// nextLSN: incrementLSN(lastLSN) - cached next start point
func (s *SQLiteStore) Set(table string, lastLSN string, nextLSN string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	_, err := s.db.Writer.Exec(`
		INSERT INTO offsets (table_name, last_lsn, next_lsn, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(table_name) DO UPDATE SET 
			last_lsn = excluded.last_lsn, 
			next_lsn = excluded.next_lsn, 
			updated_at = excluded.updated_at
	`, table, lastLSN, nextLSN, time.Now())

	return err
}

// GetAll returns all offsets
func (s *SQLiteStore) GetAll() (map[string]Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStoreClosed
	}

	rows, err := s.db.Reader.Query("SELECT table_name, last_lsn, next_lsn, updated_at FROM offsets")
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
		var lastLSN, nextLSN string
		var updatedAt time.Time

		if err := rows.Scan(&table, &lastLSN, &nextLSN, &updatedAt); err != nil {
			return nil, err
		}

		result[table] = Offset{
			LastLSN:   lastLSN,
			NextLSN:   nextLSN,
			UpdatedAt: updatedAt,
		}
	}

	return result, rows.Err()
}

// Close closes the store and the underlying database connection.
// This should be called when the offset store owns its DB (created via New, not NewWithDB).
func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	return s.db.Close()
}

// Ensure SQLiteStore implements StoreInterface
var _ StoreInterface = (*SQLiteStore)(nil)