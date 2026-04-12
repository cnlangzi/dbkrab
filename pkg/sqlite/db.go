package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cnlangzi/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// DB is a wrapper around github.com/cnlangzi/sqlite.DB
// providing read/write separation and migration support.
type DB = sqlite.DB

// Config holds SQLite configuration options.
type Config struct {
	// File is the path to the SQLite file. Use ":memory:" for in-memory database.
	File string

	// ModuleName is used for migration discovery.
	ModuleName string

	// MigrationPath is the directory path for migration files.
	MigrationPath string

	// InMemory indicates if this is an in-memory database.
	InMemory bool

	// MaxOpenConns sets maximum open connections for Reader.
	MaxOpenConnsReader int

	// MaxIdleConns sets maximum idle connections for Reader.
	MaxIdleConnsReader int
}

// New creates a new SQLite DB with read/write separation and migration support.
func New(ctx context.Context, config Config) (*DB, error) {
	if config.File == "" {
		config.File = ":memory:"
	}

	inmemory := strings.HasPrefix(config.File, ":memory:")
	dsn := buildDSN(config.File, inmemory)

	db, err := sqlite.Open(ctx, dsn)
	if err != nil {
		return nil, err
	}

	// Run migrations if MigrationPath is provided
	if config.MigrationPath != "" {
		if err := runMigrations(db.Writer.DB, config.MigrationPath); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	return db, nil
}

// NewInMemory creates an in-memory SQLite DB with shared cache.
func NewInMemory(ctx context.Context, moduleName string, migrations fs.FS) (*DB, error) {
	db, err := sqlite.Open(ctx, ":memory:")
	if err != nil {
		return nil, err
	}

	// Run migrations if provided
	if migrations != nil {
		if err := runMigrationsFS(db.Writer.DB, migrations); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	return db, nil
}

// NewFile creates a SQLite DB from a file path.
func NewFile(ctx context.Context, file string, moduleName string, migrationPath string) (*DB, error) {
	// Ensure file exists
	_, err := os.Stat(file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = os.WriteFile(file, nil, 0666)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return New(ctx, Config{
		File:          file,
		InMemory:      false,
		ModuleName:    moduleName,
		MigrationPath: migrationPath,
	})
}

// runMigrations runs SQL migration files from a directory path.
func runMigrations(db *sql.DB, migrationPath string) error {
	entries, err := os.ReadDir(migrationPath)
	if err != nil {
		return fmt.Errorf("read migration directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		filePath := filepath.Join(migrationPath, entry.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", entry.Name(), err)
		}

		if _, err := db.Exec(string(content)); err != nil {
			return fmt.Errorf("execute migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// runMigrationsFS runs SQL migration files from an embedded FS.
func runMigrationsFS(db *sql.DB, migrations fs.FS) error {
	entries, err := fs.ReadDir(migrations, ".")
	if err != nil {
		return fmt.Errorf("read migration directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		content, err := fs.ReadFile(migrations, entry.Name())
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", entry.Name(), err)
		}

		if _, err := db.Exec(string(content)); err != nil {
			return fmt.Errorf("execute migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func buildDSN(file string, inmemory bool) string {
	if inmemory {
		return ":memory:"
	}
	return file
}

// Execer is an interface for executing queries
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// InsertInTx inserts rows into a table within a transaction.
func InsertInTx(tx Execer, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		QuoteIdent(table),
		QuoteIdentList(columns),
		strings.Join(placeholders, ","))

	for _, row := range rows {
		if _, err := tx.Exec(query, row...); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
	}

	return nil
}

// UpdateInTx updates rows in a table within a transaction.
func UpdateInTx(tx Execer, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	setClause := make([]string, len(columns))
	for i, col := range columns {
		setClause[i] = fmt.Sprintf("%s = ?", QuoteIdent(col))
	}

	query := fmt.Sprintf("UPDATE %s SET %s", QuoteIdent(table), strings.Join(setClause, ","))

	for _, row := range rows {
		if _, err := tx.Exec(query, row...); err != nil {
			return fmt.Errorf("update: %w", err)
		}
	}

	return nil
}

// DeleteInTx deletes rows from a table within a transaction.
func DeleteInTx(tx Execer, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	if len(columns) == 0 {
		return errors.New("delete requires at least one column")
	}

	whereClause := make([]string, len(columns))
	for i, col := range columns {
		whereClause[i] = fmt.Sprintf("%s = ?", QuoteIdent(col))
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		QuoteIdent(table),
		strings.Join(whereClause, " AND "))

	for _, row := range rows {
		if _, err := tx.Exec(query, row...); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}

	return nil
}

func QuoteIdent(s string) string {
	var buf strings.Builder
	buf.Grow(len(s) + 2)
	buf.WriteByte('`')
	for _, c := range s {
		buf.WriteRune(c)
		if c == '`' {
			buf.WriteByte('`')
		}
	}
	buf.WriteByte('`')
	return buf.String()
}

func QuoteIdentList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = QuoteIdent(col)
	}
	return strings.Join(quoted, ",")
}
