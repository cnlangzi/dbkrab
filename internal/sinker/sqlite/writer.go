package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/sqliteutil"
	"github.com/yaitoo/sqle"
	"github.com/yaitoo/sqle/migrate"
)

// Sinker implements sinker.Sinker for SQLite.
type Sinker struct {
	name       string
	db         *DB
	migrations string // path to migration SQL files
	mu         sync.Mutex
	closed     bool
}

// NewSinker creates a new SQLite sinker.
func NewSinker(name string, dsn string, migrations string) (*Sinker, error) {
	db, err := NewSinkerDB(context.Background(), dsn, migrations)
	if err != nil {
		return nil, fmt.Errorf("create sqlite db: %w", err)
	}

	return &Sinker{
		name:       name,
		db:         db,
		migrations: migrations,
	}, nil
}

// DatabaseName returns the database name.
func (s *Sinker) DatabaseName() string {
	return s.name
}

// DatabaseType returns the database type.
func (s *Sinker) DatabaseType() string {
	return "sqlite"
}

// Write writes a batch of sink operations to the database.
func (s *Sinker) Write(ctx context.Context, ops []core.Sink) error {
	if len(ops) == 0 {
		return nil
	}

	slog.Debug("SQLiteSinker.Write: starting batch write",
		"database", s.name,
		"operations", len(ops))

	// Set busy_timeout to 5 seconds to handle concurrent writes (e.g., from poller)
	_, _ = s.db.Writer.ExecContext(ctx, "PRAGMA busy_timeout = 5000")

	tx, err := s.db.Writer.BeginTx(ctx, nil)
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

// ExecContext executes a raw SQL query
func (s *Sinker) ExecContext(ctx context.Context, query string) error {
	_, err := s.db.Writer.ExecContext(ctx, query)
	return err
}

func (s *Sinker) writeOp(ctx context.Context, tx sqliteutil.TxExec, op core.Sink) error {
	config := sqliteutil.TableConfig{
		Output:     op.Config.Output,
		PrimaryKey: op.Config.PrimaryKey,
		OnConflict: op.Config.OnConflict,
	}

	slog.Debug("SQLiteSinker.writeOp: processing",
		"database", s.name,
		"output", op.Config.Output,
		"opType", op.OpType,
		"opTypeName", op.OpType.String(),
		"primaryKey", op.Config.PrimaryKey,
		"columns", len(op.DataSet.Columns),
		"rows", len(op.DataSet.Rows))

	switch op.OpType {
	case core.OpInsert:
		return sqliteutil.InsertInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	case core.OpUpdateAfter:
		return sqliteutil.UpdateInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	case core.OpDelete:
		return sqliteutil.DeleteInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	default:
		// Defensively handle unknown operations by logging and dropping.
		slog.Error("SQLiteSinker.writeOp: unsupported operation type, dropping",
			"database", s.name,
			"output", op.Config.Output,
			"opType", op.OpType,
			"opTypeName", op.OpType.String(),
			"primaryKey", op.Config.PrimaryKey,
			"columns", len(op.DataSet.Columns),
			"rows", len(op.DataSet.Rows),
			"hint", "supported types are: OpInsert(2), OpUpdateAfter(4), OpDelete(1)")
		return nil
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

// Migrate runs the migration for this sinker's database.
// It re-discovers and re-applies migrations from the configured migrations path.
func (s *Sinker) Migrate(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.migrations == "" {
		return errors.New("no migrations path configured")
	}

	slog.Info("SQLiteSinker.Migrate: starting migration",
		"database", s.name,
		"migrations", s.migrations)

	sqleDB := sqle.Open(s.db.Writer.DB)
	migrator := migrate.New(sqleDB)
	if err := migrator.Discover(os.DirFS(s.migrations), migrate.WithModule("dbkrab")); err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	if err := migrator.Init(ctx); err != nil {
		return fmt.Errorf("init migrations: %w", err)
	}
	if err := migrator.Migrate(ctx); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	slog.Info("SQLiteSinker.Migrate: migration completed",
		"database", s.name)

	return nil
}

// Reset clears all user tables in the sink, disabling foreign key checks
// during the clear operation and flushing changes to disk upon completion.
// This is used by snapshot startup to prepare sinks before loading data.
func (s *Sinker) Reset(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	slog.Info("SQLiteSinker.Reset: starting reset",
		"database", s.name)

	// Step 1: Query user tables from sqlite_master
	rows, err := s.db.Writer.QueryContext(ctx, `
		SELECT name FROM sqlite_master
		WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'sqle_%'
		ORDER BY name`)
	if err != nil {
		return fmt.Errorf("query tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	slog.Debug("SQLiteSinker.Reset: found tables",
		"database", s.name,
		"table_count", len(tables))

	// Step 2: Capture original foreign_keys setting and disable for reset.
	// Use Writer (not raw DB) to stay within the buffer writer's locking.
	// Flush immediately after so the PRAGMA commits before any DELETE runs.
	var fkEnabled bool
	row := s.db.Writer.QueryRowContext(ctx, "PRAGMA foreign_keys")
	if err := row.Scan(&fkEnabled); err != nil {
		return fmt.Errorf("read foreign_keys setting: %w", err)
	}

	if fkEnabled {
		if _, err := s.db.Writer.ExecContext(context.Background(), "PRAGMA foreign_keys = off"); err != nil {
			return fmt.Errorf("disable foreign keys: %w", err)
		}
		if err := s.db.Flush(); err != nil {
			return fmt.Errorf("flush after disabling foreign keys: %w", err)
		}
		// Ensure FK is re-enabled even if context is canceled mid-reset.
		defer func() {
			_, _ = s.db.Writer.ExecContext(context.Background(), "PRAGMA foreign_keys = on")
		}()
	}

	// Step 3: Delete from each table, continuing on per-table errors
	for _, tableName := range tables {
		slog.Debug("SQLiteSinker.Reset: clearing table",
			"database", s.name,
			"table", tableName)

		// Use QuoteIdent to properly escape table name (handles backticks, etc.)
		query := fmt.Sprintf("DELETE FROM %s", QuoteIdent(tableName))
		if _, err := s.db.Writer.ExecContext(ctx, query); err != nil {
			// Log per-table error but continue with remaining tables
			slog.Error("SQLiteSinker.Reset: failed to clear table, continuing",
				"database", s.name,
				"table", tableName,
				"error", err)
			continue
		}

		slog.Debug("SQLiteSinker.Reset: cleared table",
			"database", s.name,
			"table", tableName)
	}

	// Step 4: Flush to ensure changes are persisted (non-fatal on failure)
	if err := s.db.Flush(); err != nil {
		slog.Warn("SQLiteSinker.Reset: flush failed, continuing",
			"database", s.name,
			"error", err)
	}

	slog.Info("SQLiteSinker.Reset: completed",
		"database", s.name,
		"tables_cleared", len(tables))

	return nil
}
