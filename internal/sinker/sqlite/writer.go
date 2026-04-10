package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"sync"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/sqliteutil"

	_ "github.com/mattn/go-sqlite3"
)

// Sinker implements sinker.Sinker for SQLite.
type Sinker struct {
	name          string
	db            *sql.DB
	dbType        string
	migrationFS   fs.FS
	migrationsDir string
	mu            sync.Mutex
	closed        bool
}

// NewSinker creates a new SQLite sinker.
// migrationsFS and migrationsDir are used for auto-running migrations.
func NewSinker(name, dbType, file string, migrationsFS fs.FS, migrationsDir string) (*Sinker, error) {
	db, err := sql.Open("sqlite3", file+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(1)

	s := &Sinker{
		name:          name,
		db:            db,
		dbType:        dbType,
		migrationFS:   migrationsFS,
		migrationsDir: migrationsDir,
	}

	// Run migrations on startup if configured
	if s.migrationFS != nil && s.migrationsDir != "" {
		if err := RunMigrationsFS(db, s.migrationFS, s.migrationsDir); err != nil {
			db.Close()
			return nil, fmt.Errorf("run migrations: %w", err)
		}
	}

	return s, nil
}

// DatabaseName returns the database name.
func (s *Sinker) DatabaseName() string {
	return s.name
}

// DatabaseType returns the database type.
func (s *Sinker) DatabaseType() string {
	return s.dbType
}

// Write writes a batch of sink operations to the database.
func (s *Sinker) Write(ctx context.Context, ops []core.Sink) error {
	if len(ops) == 0 {
		return nil
	}

	slog.Debug("SQLiteSinker.Write: starting batch write",
		"database", s.name,
		"operations", len(ops))

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, op := range ops {
		if err := s.writeOp(ctx, tx, op); err != nil {
			return fmt.Errorf("write op %s: %w", op.Config.Output, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (s *Sinker) writeOp(ctx context.Context, tx *sql.Tx, op core.Sink) error {
	config := sqliteutil.TableConfig{
		Output:     op.Config.Output,
		PrimaryKey: op.Config.PrimaryKey,
		OnConflict: op.Config.OnConflict,
	}

	switch op.OpType {
	case core.OpInsert:
		return sqliteutil.InsertInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	case core.OpUpdateAfter:
		return sqliteutil.UpdateInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	case core.OpDelete:
		return sqliteutil.DeleteInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	default:
		return fmt.Errorf("unknown operation type: %v", op.OpType)
	}
}

// Close closes the database connection.
func (s *Sinker) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	return s.db.Close()
}
