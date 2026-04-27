package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
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

	// Get all valid tables from database (without lock, safe read)
	tables, err := s.getValidTables(ctx)
	if err != nil {
		return err
	}

	// Delegate to truncateTables with lock held and fail-fast=false for Reset
	return s.truncateTables(ctx, tables, false)
}

// getValidTables returns all valid user tables from the database
func (s *Sinker) getValidTables(ctx context.Context) ([]string, error) {
	rows, err := s.db.Writer.QueryContext(ctx, `
		SELECT name FROM sqlite_master
		WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'sqle_%'
		ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Warn("SQLiteSinker.getValidTables: failed to close rows", "error", err)
		}
	}()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	slog.Debug("SQLiteSinker: found tables",
		"database", s.name,
		"table_count", len(tables))

	return tables, nil
}

// Truncate deletes all data from the specified tables.
// It disables foreign key checks during the operation.
// Only tables that exist in the database and don't start with sqle_ will be truncated.
func (s *Sinker) Truncate(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return errors.New("no tables specified")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	slog.Info("SQLiteSinker.Truncate: starting truncate",
		"database", s.name,
		"requested_tables", tables)

	// Step 1: Query valid tables from database as whitelist (without lock)
	validTables, err := s.getValidTables(ctx)
	if err != nil {
		return err
	}

	// Build whitelist map for O(1) lookup
	validTableSet := make(map[string]struct{}, len(validTables))
	for _, t := range validTables {
		validTableSet[t] = struct{}{}
	}

	slog.Debug("SQLiteSinker.Truncate: valid tables",
		"database", s.name,
		"valid_count", len(validTables))

	// Step 2: Filter requested tables against whitelist
	var tablesToTruncate []string
	var skipped []string
	for _, table := range tables {
		// Double-check: skip any table starting with sqle_ (defense in depth)
		if strings.HasPrefix(table, "sqle_") {
			skipped = append(skipped, table)
			continue
		}
		// Check if table exists in database
		if _, ok := validTableSet[table]; !ok {
			skipped = append(skipped, table)
			continue
		}
		tablesToTruncate = append(tablesToTruncate, table)
	}

	if len(skipped) > 0 {
		slog.Warn("SQLiteSinker.Truncate: skipped invalid tables",
			"database", s.name,
			"skipped", skipped)
	}

	if len(tablesToTruncate) == 0 {
		return errors.New("no valid tables to truncate after filtering")
	}

	// Step 3: Execute truncate with fail-fast for Truncate operation
	return s.truncateTables(ctx, tablesToTruncate, true)
}

// truncateTables performs the actual table truncation (called with lock held)
// failFast: if true, return on first error; if false, log and continue (best-effort)
func (s *Sinker) truncateTables(ctx context.Context, tables []string, failFast bool) error {
	// Capture original foreign_keys setting and disable for truncate.
	var fkEnabled bool
	row := s.db.Writer.QueryRowContext(ctx, "PRAGMA foreign_keys")
	if err := row.Scan(&fkEnabled); err != nil {
		return fmt.Errorf("read foreign_keys setting: %w", err)
	}

	if fkEnabled {
		// Disable foreign keys with the same context for consistent cancellation handling
		if _, err := s.db.Writer.ExecContext(ctx, "PRAGMA foreign_keys = off"); err != nil {
			return fmt.Errorf("disable foreign keys: %w", err)
		}
		if err := s.db.Flush(); err != nil {
			return fmt.Errorf("flush after disabling foreign keys: %w", err)
		}
		// Ensure FK is re-enabled after truncate - log any error
		defer func() {
			if _, err := s.db.Writer.ExecContext(ctx, "PRAGMA foreign_keys = on"); err != nil {
				slog.Warn("SQLiteSinker.Truncate: failed to re-enable foreign keys",
					"database", s.name, "error", err)
			}
		}()
	}

	// Delete from each valid table
	var deletedCount int
	for _, tableName := range tables {
		query := fmt.Sprintf("DELETE FROM %s", QuoteIdent(tableName))
		if _, err := s.db.Writer.ExecContext(ctx, query); err != nil {
			slog.Warn("SQLiteSinker.Truncate: failed to delete from table",
				"database", s.name,
				"table", tableName,
				"error", err)
			if failFast {
				return fmt.Errorf("delete from %s: %w", tableName, err)
			}
			// Best-effort: continue with remaining tables
			continue
		}

		deletedCount++
		slog.Debug("SQLiteSinker.Truncate: truncated table",
			"database", s.name,
			"table", tableName)
	}

	// Flush to ensure changes are persisted
	if err := s.db.Flush(); err != nil {
		if failFast {
			return fmt.Errorf("flush after truncate: %w", err)
		}
		slog.Warn("SQLiteSinker.Truncate: flush failed",
			"database", s.name,
			"error", err)
	}

	slog.Info("SQLiteSinker.Truncate: completed",
		"database", s.name,
		"tables_truncated", deletedCount)

	return nil
}
