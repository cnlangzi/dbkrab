package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/yaitoo/sqle"
	"github.com/yaitoo/sqle/migrate"

	_ "github.com/mattn/go-sqlite3"
)

var ErrUnreachable = errors.New("sqlite: is unreachable")

// DB wraps SQLite database with read/write separation.
// Writer is used for writes, Reader for reads.
type DB struct {
	Writer *sqle.DB
	Reader *sqle.DB
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
		m := migrate.New(d.Writer)
		if err := m.Discover(config.FS, migrate.WithSuffix("sqlite"), migrate.WithModule(config.ModuleName)); err != nil {
			return nil, err
		}
		if err := m.Init(ctx); err != nil {
			return nil, err
		}
		if err := m.Migrate(ctx); err != nil {
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
func NewInMemory(ctx context.Context, moduleName string, migrations fs.FS) (*DB, error) {
	return New(ctx, Config{
		File:        ":memory:",
		InMemory:    true,
		ModuleName: moduleName,
		FS:          migrations,
	})
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

func setupWriter(file string, inmemory bool, maxOpenConns int) (*sqle.DB, error) {
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

	return sqle.Open(d), nil
}

func setupReader(file string, inmemory bool, maxOpenConns, maxIdleConns int) (*sqle.DB, error) {
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

	return sqle.Open(d), nil
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
func (db *DB) Exec(query string, args ...interface{}) (sqle.Result, error) {
	return db.Writer.Exec(query, args...)
}

// ExecContext executes a write operation on Writer with context.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sqle.Result, error) {
	return db.Writer.ExecContext(ctx, query, args...)
}

// Query executes a read operation on Reader.
func (db *DB) Query(query string, args ...interface{}) (sqle.Rows, error) {
	return db.Reader.Query(query, args...)
}

// QueryContext executes a read operation on Reader with context.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (sqle.Rows, error) {
	return db.Reader.QueryContext(ctx, query, args...)
}

// QueryRow executes a read operation on Reader and returns a single row.
func (db *DB) QueryRow(query string, args ...interface{}) sqle.Row {
	return db.Reader.QueryRow(query, args...)
}

// QueryRowContext executes a read operation on Reader with context and returns a single row.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) sqle.Row {
	return db.Reader.QueryRowContext(ctx, query, args...)
}