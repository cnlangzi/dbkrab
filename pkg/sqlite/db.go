package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var ErrUnreachable = errors.New("sqlite: is unreachable")

// DB wraps SQLite database with read/write separation.
// Writer is used for writes, Reader for reads.
type DB struct {
	Writer *sql.DB
	Reader *sql.DB
	ctx    context.Context
}

// Config holds SQLite configuration options.
type Config struct {
	// File is the path to the SQLite file. Use ":memory:" for in-memory database.
	File string

	// ModuleName is used for migration discovery.
	ModuleName string

	// MigrationPath is the directory path for migration files (used with os.DirFS).
	MigrationPath string

	// InMemory indicates if this is an in-memory database.
	InMemory bool

	// MaxOpenConns sets maximum open connections (default: Writer=1, Reader=runtime.NumCPU()*2)
	MaxOpenConnsWriter int
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

	d := &DB{
		ctx: ctx,
	}

	var err error
	d.Writer, err = setupWriter(config.File, inmemory, config.MaxOpenConnsWriter)
	if err != nil {
		return nil, err
	}

	// Run migrations if MigrationPath is provided
	if config.MigrationPath != "" {
		if err := runMigrations(d.Writer, config.MigrationPath); err != nil {
			_ = d.Writer.Close()
			return nil, err
		}
	}

	d.Reader, err = setupReader(config.File, inmemory, config.MaxOpenConnsReader, config.MaxIdleConnsReader)
	if err != nil {
		return nil, err
	}

	return d, nil
}

// NewInMemory creates an in-memory SQLite DB with shared cache for read/write separation.
// Note: For in-memory DBs, we use a single connection to avoid transaction isolation issues.
func NewInMemory(ctx context.Context, moduleName string, migrations fs.FS) (*DB, error) {
	db, err := setupWriter(":memory:", true, 0)
	if err != nil {
		return nil, err
	}

	// Run migrations if provided
	if migrations != nil {
		if err := runMigrationsFS(db, migrations); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	return &DB{
		Writer: db,
		Reader: db, // Use same connection for in-memory DB
		ctx:    ctx,
	}, nil
}

// NewFile creates a SQLite DB with read/write separation from a file.
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
// Files are executed in alphabetical order.
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
// Files are executed in alphabetical order by filename.
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

func setupWriter(file string, inmemory bool, maxOpenConns int) (*sql.DB, error) {
	dsn := buildWriterDSN(file, inmemory)

	d, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	if maxOpenConns > 0 {
		d.SetMaxOpenConns(maxOpenConns)
	} else {
		d.SetMaxOpenConns(1)
	}

	if err := pingWithRetry(d); err != nil {
		return nil, err
	}

	return d, nil
}

func setupReader(file string, inmemory bool, maxOpenConns, maxIdleConns int) (*sql.DB, error) {
	dsn := buildReaderDSN(file, inmemory)

	d, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	if maxOpenConns > 0 {
		d.SetMaxOpenConns(maxOpenConns)
	} else {
		d.SetMaxOpenConns(max(10, runtime.NumCPU()*2))
	}

	if maxIdleConns > 0 {
		d.SetMaxIdleConns(maxIdleConns)
	} else {
		d.SetMaxIdleConns(max(10, runtime.NumCPU()*2))
	}

	if err := pingWithRetry(d); err != nil {
		return nil, err
	}

	return d, nil
}

func buildWriterDSN(file string, inmemory bool) string {
	params := url.Values{}

	if inmemory {
		params.Add("cache", "shared")
		params.Add("mode", "memory")
		params.Add("_busy_timeout", "5000")
		params.Add("_synchronous", "OFF")
		params.Add("_journal_mode", "MEMORY")
	} else {
		params.Add("cache", "shared")
		params.Add("mode", "rwc")
		params.Add("_journal_mode", "WAL")
		params.Add("_synchronous", "NORMAL")
		params.Add("_busy_timeout", "5000")
		params.Add("_pragma", "temp_store(MEMORY)")
		params.Add("_pragma", "cache_size(-100000)")
	}

	return file + "?" + params.Encode()
}

func buildReaderDSN(file string, inmemory bool) string {
	params := url.Values{}

	if inmemory {
		params.Add("cache", "shared")
		params.Add("mode", "ro")
		params.Add("_query_only", "true")
		params.Add("_pragma", "read_uncommitted(true)")
		params.Add("_busy_timeout", "5000")
	} else {
		params.Add("mode", "ro")
		params.Add("cache", "private")
		params.Add("_journal_mode", "WAL")
		params.Add("_busy_timeout", "5000")
		params.Add("_query_only", "true")
		params.Add("_synchronous", "OFF")
		params.Add("_pragma", "mmap_size(1000000000)")
		params.Add("_pragma", "temp_store(MEMORY)")
	}

	return file + "?" + params.Encode()
}

func pingWithRetry(d *sql.DB) error {
	retries := 0
	for {
		err := d.Ping()
		if err == nil {
			return nil
		}

		retries++
		if retries > 3 {
			return ErrUnreachable
		}
		time.Sleep(1 * time.Second)
	}
}

// Close closes both Writer and Reader connections.
func (db *DB) Close() error {
	var errs []error

	if err := db.Writer.Close(); err != nil {
		errs = append(errs, err)
	}

	if err := db.Reader.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Exec executes a write operation on Writer.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.Writer.Exec(query, args...)
}

// ExecContext executes a write operation on Writer with context.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.Writer.ExecContext(ctx, query, args...)
}

// Query executes a read operation on Reader.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.Reader.Query(query, args...)
}

// QueryContext executes a read operation on Reader with context.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.Reader.QueryContext(ctx, query, args...)
}

// QueryRow executes a read operation on Reader and returns a single row.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.Reader.QueryRow(query, args...)
}

// QueryRowContext executes a read operation on Reader with context and returns a single row.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.Reader.QueryRowContext(ctx, query, args...)
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
		quoteIdent(table),
		quoteIdentList(columns),
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
		setClause[i] = fmt.Sprintf("%s = ?", quoteIdent(col))
	}

	query := fmt.Sprintf("UPDATE %s SET %s", quoteIdent(table), strings.Join(setClause, ","))

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
		whereClause[i] = fmt.Sprintf("%s = ?", quoteIdent(col))
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		quoteIdent(table),
		strings.Join(whereClause, " AND "))

	for _, row := range rows {
		if _, err := tx.Exec(query, row...); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}

	return nil
}

func quoteIdent(s string) string {
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

func quoteIdentList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = quoteIdent(col)
	}
	return strings.Join(quoted, ",")
}
