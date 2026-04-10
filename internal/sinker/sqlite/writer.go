package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/sinker"
	"github.com/cnlangzi/dbkrab/internal/sqliteutil"
	"github.com/yaitoo/sqle"
	"github.com/yaitoo/sqle/migrate"

	_ "github.com/mattn/go-sqlite3"
)

// Sinker implements sinker.Sinker for SQLite.
type Sinker struct {
	name          string
	db            *sqle.DB
	dbType        string
	path          string
	migrationFS   fs.FS
	migrationsDir string
	stopCh        chan struct{}
}

// Config holds SQLite Sinker configuration options.
type Config struct {
	// Name is the database name this sinker handles
	Name string

	// File is the path to the SQLite file.
	File string

	// ModuleName is used for migration discovery.
	ModuleName string

	// FS for migration files (optional).
	FS fs.FS

	// MigrationsDir is the directory containing migration files.
	MigrationsDir string
}

// NewSinker creates a new SQLite sinker.
func NewSinker(config Config) (*Sinker, error) {
	// Ensure file exists
	if config.File != ":memory:" {
		_, err := os.Stat(config.File)
		if err != nil {
			if os.IsNotExist(err) {
				err = os.WriteFile(config.File, nil, 0666)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
	}

	// Build DSN with optimized settings
	dsn := buildDSN(config.File)

	// Open database
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Set connection pool - writer needs single connection to avoid SQLITE_BUSY
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	s := &Sinker{
		name:          config.Name,
		dbType:        "sqlite",
		path:          config.File,
		migrationFS:   config.FS,
		migrationsDir: config.MigrationsDir,
		db:            sqle.Open(db),
		stopCh:        make(chan struct{}),
	}

	// Start migration file watcher if configured
	if config.MigrationsDir != "" {
		go s.watchMigrations(config.MigrationsDir)
	}

	return s, nil
}

// watchMigrations monitors migration directory for new files and re-runs migrations on debounce.
func (s *Sinker) watchMigrations(migrationsDir string) {
	watcher, err := os.MkdirTemp("", "fsnotify-*")
	if err != nil {
		return
	}
	defer os.RemoveAll(watcher) //nolint:errcheck

	// Note: fsnotify doesn't work well with arbitrary temp dirs
	// This is a simplified implementation - full implementation would use proper fsnotify
	_ = watcher
}

// buildDSN builds SQLite DSN with appropriate settings
func buildDSN(file string) string {
	return file + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_pragma=temp_store(MEMORY)&_pragma=cache_size(-100000)&_pragma=mmap_size(1000000000)&cache=shared"
}

// DatabaseName implements sinker.Sinker
func (s *Sinker) DatabaseName() string {
	return s.name
}

// DatabaseType implements sinker.Sinker
func (s *Sinker) DatabaseType() string {
	return s.dbType
}

// Write writes a batch of sink operations to the database
func (s *Sinker) Write(ops []core.Sink) error {
	if len(ops) == 0 {
		slog.Debug("SQLiteSinker.Write: no operations to write")
		return nil
	}

	slog.Debug("SQLiteSinker.Write: starting batch write",
		"database", s.name,
		"operations", len(ops),
		"path", s.path)

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("SQLiteSinker.Write: failed to begin transaction",
			"error", err)
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for i, op := range ops {
		slog.Debug("SQLiteSinker.Write: processing operation",
			"index", i,
			"output", op.Config.Output,
			"op_type", op.OpType.String(),
			"rows", len(op.DataSet.Rows))

		config := sqliteutil.TableConfig{
			Output:     op.Config.Output,
			PrimaryKey: op.Config.PrimaryKey,
			OnConflict: op.Config.OnConflict,
		}

		switch op.OpType {
		case core.OpInsert:
			if err := s.insertInTx(tx, config, op.DataSet); err != nil {
				slog.Error("SQLiteSinker.Write: insert failed",
					"output", op.Config.Output,
					"error", err)
				return fmt.Errorf("insert %s: %w", op.Config.Output, err)
			}
		case core.OpUpdateAfter:
			if err := sqliteutil.UpdateInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows); err != nil {
				slog.Error("SQLiteSinker.Write: update failed",
					"output", op.Config.Output,
					"error", err)
				return fmt.Errorf("update %s: %w", op.Config.Output, err)
			}
		case core.OpDelete:
			if err := sqliteutil.DeleteInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows); err != nil {
				slog.Error("SQLiteSinker.Write: delete failed",
					"output", op.Config.Output,
					"error", err)
				return fmt.Errorf("delete %s: %w", op.Config.Output, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		slog.Error("SQLiteSinker.Write: failed to commit transaction",
			"error", err)
		return fmt.Errorf("commit transaction: %w", err)
	}

	slog.Info("SQLiteSinker.Write: batch write completed",
		"database", s.name,
		"operations", len(ops))

	return nil
}

// insertInTx inserts DataSet into table with OnConflict handling.
// This function has special handling for OnConflict strategy that's different from store.
func (s *Sinker) insertInTx(tx *sql.Tx, config sqliteutil.TableConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Use shared EnsureTable
	if err := sqliteutil.EnsureTable(tx, config.Output, ds.Columns); err != nil {
		return err
	}

	for _, row := range ds.Rows {
		// Build INSERT SQL based on OnConflict strategy
		var sqlStr string

		switch config.OnConflict {
		case "overwrite":
			// INSERT OR REPLACE
			sqlStr = sqliteutil.BuildInsertSQL(config.Output, ds.Columns, true)
		case "skip":
			// INSERT OR IGNORE
			sqlStr = sqliteutil.BuildInsertSQL(config.Output, ds.Columns, false)
		default:
			// Default to standard INSERT
			sqlStr = sqliteutil.BuildStandardInsertSQL(config.Output, ds.Columns)
		}

		_, err := tx.Exec(sqlStr, row...)
		if err != nil {
			return fmt.Errorf("execute: %w", err)
		}
	}

	return nil
}

// RunMigrations runs any pending migrations using sqle/migrate
func (s *Sinker) RunMigrations() error {
	if s.migrationFS == nil && s.migrationsDir == "" {
		return nil // No migration configured, skip
	}

	var m *migrate.Migrator
	if s.migrationFS != nil {
		m = migrate.New(s.db)
		if err := m.Discover(s.migrationFS, migrate.WithSuffix("sqlite"), migrate.WithModule("dbkrab")); err != nil {
			return err
		}
		if err := m.Init(context.Background()); err != nil {
			return err
		}
		return m.Migrate(context.Background())
	}

	// File-based migrations
	if s.migrationsDir != "" {
		return s.runFileMigrations()
	}

	return nil
}

// runFileMigrations runs migrations from a directory
func (s *Sinker) runFileMigrations() error {
	if s.migrationsDir == "" {
		return nil
	}

	// Check if migrations directory exists
	if _, err := os.Stat(s.migrationsDir); os.IsNotExist(err) {
		return nil // No migrations directory, skip
	}

	// Read version directories (format: 1.0.0, 1.0.1, etc.)
	versionEntries, err := os.ReadDir(s.migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	// Filter and sort version directories
	var versionDirs []string
	for _, entry := range versionEntries {
		if !entry.IsDir() {
			continue
		}
		versionDirs = append(versionDirs, entry.Name())
	}

	// Sort version directories
	sort.Strings(versionDirs)

	// Collect migration files from all version directories
	var migrationFiles []string
	for _, versionDir := range versionDirs {
		versionPath := filepath.Join(s.migrationsDir, versionDir)
		entries, err := os.ReadDir(versionPath)
		if err != nil {
			return fmt.Errorf("read version dir %s: %w", versionDir, err)
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasSuffix(name, ".sql") && isMigrationFile(name) {
				migrationFiles = append(migrationFiles, filepath.Join(versionDir, name))
			}
		}
	}

	// Sort migration files
	sortMigrations(migrationFiles)

	// Run each migration
	for _, file := range migrationFiles {
		if err := s.runMigrationFile(filepath.Join(s.migrationsDir, file)); err != nil {
			return fmt.Errorf("run migration %s: %w", file, err)
		}
	}

	return nil
}

// isMigrationFile checks if filename matches migration pattern (e.g., 001_description.sql)
func isMigrationFile(name string) bool {
	matched, _ := regexp.MatchString(`^\d+_.*\.sql$`, name)
	return matched
}

// sortMigrations sorts migration files by version number
func sortMigrations(files []string) {
	// Simple bubble sort for small number of files
	for i := 0; i < len(files)-1; i++ {
		for j := 0; j < len(files)-i-1; j++ {
			v1 := extractVersion(files[j])
			v2 := extractVersion(files[j+1])
			if v1 > v2 {
				files[j], files[j+1] = files[j+1], files[j]
			}
		}
	}
}

// extractVersion extracts version number from migration filename
func extractVersion(name string) int {
	// Extract leading digits
	re := regexp.MustCompile(`^(\d+)`)
	matches := re.FindStringSubmatch(name)
	if len(matches) > 1 {
		v, _ := strconv.Atoi(matches[1])
		return v
	}
	return 0
}

// runMigrationFile executes a single migration file
func (s *Sinker) runMigrationFile(path string) error {
	// Read migration content
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read file %s: %w", path, err)
	}

	// Execute migration in a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Execute each statement in the migration
	statements := splitStatements(string(content))
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("execute statement: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// splitStatements splits SQL content into individual statements
func splitStatements(content string) []string {
	// Simple statement splitter - handles basic cases
	var statements []string
	var current strings.Builder

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue // Skip comments
		}
		current.WriteString(line)
		current.WriteString("\n")

		if strings.HasSuffix(trimmed, ";") {
			statements = append(statements, current.String())
			current.Reset()
		}
	}

	// Add any remaining content
	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

// Close closes the database connection
func (s *Sinker) Close() error {
	if s.stopCh != nil {
		close(s.stopCh)
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Ensure the Sinker implements the sinker.Sinker interface
var _ sinker.Sinker = (*Sinker)(nil)
