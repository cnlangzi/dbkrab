package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
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

	// FS for migration files (optional).
	FS fs.FS

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

	// Run migrations if FS is provided
	if config.FS != nil {
		if err := RunMigrations(d.Writer, config.FS, config.ModuleName); err != nil {
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
		if err := RunMigrations(db, migrations, moduleName); err != nil {
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
func NewFile(ctx context.Context, file string, moduleName string, migrations fs.FS) (*DB, error) {
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
		File:        file,
		InMemory:    false,
		ModuleName:  moduleName,
		FS:          migrations,
	})
}

// RunMigrations discovers and runs pending migrations from embedded FS.
// This is exported so other packages can use it.
func RunMigrations(db *sql.DB, migrationsFS fs.FS, moduleName string) error {
	// Create schema_migrations table if not exists
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}

	// Discover migration files
	var migrationFiles []string
	err = fs.WalkDir(migrationsFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasSuffix(name, ".sql") && isMigrationFile(name) {
			migrationFiles = append(migrationFiles, path)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Sort migrations
	sort.Sort(byFilename(migrationFiles))

	// Get applied migrations
	applied := make(map[string]bool)
	rows, err := db.Query("SELECT version FROM schema_migrations")
	if err != nil {
		return err
	}
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			_ = rows.Close()
			return err
		}
		applied[version] = true
	}
	_ = rows.Close()

	// Run pending migrations
	for _, file := range migrationFiles {
		version := extractVersion(file)
		if applied[version] {
			continue
		}

		content, err := fs.ReadFile(migrationsFS, file)
		if err != nil {
			return err
		}

		if err := execMigration(db, version, content); err != nil {
			return err
		}
	}

	return nil
}

// isMigrationFile checks if filename matches migration pattern (e.g., 001_description.sql)
func isMigrationFile(name string) bool {
	matched, _ := regexp.MatchString(`^\d+_.*\.sql$`, name)
	return matched
}

type byFilename []string

func (b byFilename) Len() int           { return len(b) }
func (b byFilename) Less(i, j int) bool { return b[i] < b[j] }
func (b byFilename) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

func extractVersion(path string) string {
	name := filepath.Base(path)
	re := regexp.MustCompile(`^(\d+)`)
	matches := re.FindStringSubmatch(name)
	if len(matches) > 1 {
		return matches[1]
	}
	return name
}

func execMigration(db *sql.DB, version string, content []byte) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Execute each statement
	statements := splitStatements(string(content))
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	// Record migration
	if _, err := tx.Exec("INSERT INTO schema_migrations (version) VALUES (?)", version); err != nil {
		return err
	}

	return tx.Commit()
}

func splitStatements(content string) []string {
	var statements []string
	var current bytes.Buffer

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")

		if strings.HasSuffix(trimmed, ";") {
			statements = append(statements, current.String())
			current.Reset()
		}
	}

	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
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
