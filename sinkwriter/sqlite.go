package sinkwriter

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/fsnotify/fsnotify"
	_ "modernc.org/sqlite"
)

// SQLiteWriter writes sink operations to a SQLite database.
// It handles its own SQL syntax and migration strategy.
type SQLiteWriter struct {
	name          string
	db            *sql.DB
	dbType        string
	path          string
	migrationPath string
	stopCh        chan struct{}
}

// NewSQLiteWriter creates a new SQLite sink writer.
// path: 直接使用作为数据库文件路径，不再做二次处理
func NewSQLiteWriter(name, dbType, path, migrationPath string) (*SQLiteWriter, error) {
	// Determine actual db file path and directory
	var dbPath string
	var basePath string

	if strings.HasSuffix(strings.ToLower(path), ".db") {
		// path is the direct database file path
		dbPath = path
		basePath = filepath.Dir(path)
	} else {
		// path is a directory, use name.db inside it
		basePath = path
		dbPath = filepath.Join(path, name+".db")
	}

	// Create directory if not exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Initialize database connection
	db, err := sql.Open("sqlite", buildDSN(dbPath))
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	w := &SQLiteWriter{
		name:          name,
		dbType:        dbType,
		path:          basePath,
		migrationPath: migrationPath,
		db:            db,
		stopCh:        make(chan struct{}),
	}

	// Start migration file watcher if configured
	if migrationPath != "" {
		go w.watchMigrations(migrationPath)
	}

	return w, nil
}

// watchMigrations monitors migration directory for new files and re-runs migrations on debounce.
func (w *SQLiteWriter) watchMigrations(migrationsDir string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return
	}
	defer watcher.Close() //nolint:errcheck

	if err := watcher.Add(migrationsDir); err != nil {
		return
	}

	var debounceTimer *time.Timer
	const debounceDuration = 200 * time.Millisecond

	for {
		select {
		case <-w.stopCh:
			return
		case event := <-watcher.Events:
			// Only care about new files
			if event.Op&fsnotify.Create == 0 {
				continue
			}
			// Reset debounce timer (sliding window)
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.AfterFunc(debounceDuration, func() {
				if err := w.RunMigrations(); err != nil {
					// migrations error, log it
				}
			})
		case err := <-watcher.Errors:
			// ignore watcher errors
			_ = err
		}
	}
}

// buildDSN builds SQLite DSN with appropriate settings
func buildDSN(dbPath string) string {
	return dbPath + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_pragma=temp_store(MEMORY)&_pragma=cache_size(-100000)&_pragma=mmap_size(1000000000)&cache=shared"
}

// DatabaseName implements SinkWriter
func (w *SQLiteWriter) DatabaseName() string {
	return w.name
}

// DatabaseType implements SinkWriter
func (w *SQLiteWriter) DatabaseType() string {
	return w.dbType
}

// Write writes a batch of sink operations to the database
func (w *SQLiteWriter) Write(ops []core.Sink) error {
	if len(ops) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, op := range ops {
		switch op.OpType {
		case core.OpInsert:
			if err := w.insertInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("insert %s: %w", op.Config.Output, err)
			}
		case core.OpUpdateAfter:
			if err := w.updateInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("update %s: %w", op.Config.Output, err)
			}
		case core.OpDelete:
			if err := w.deleteInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("delete %s: %w", op.Config.Output, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// insertInTx inserts DataSet into table
func (w *SQLiteWriter) insertInTx(tx *sql.Tx, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Ensure table exists
	if err := w.ensureTable(tx, config.Output, ds.Columns); err != nil {
		return err
	}

	for _, row := range ds.Rows {
		// Build INSERT SQL based on OnConflict strategy
		var sqlStr string

		switch config.OnConflict {
		case "overwrite":
			// INSERT OR REPLACE
			sqlStr = w.buildInsertSQL(config.Output, ds.Columns, row, true)
		case "skip":
			// INSERT OR IGNORE
			sqlStr = w.buildInsertSQL(config.Output, ds.Columns, row, false)
		default:
			// Default to standard INSERT
			sqlStr = w.buildStandardInsertSQL(config.Output, ds.Columns, row)
		}

		_, err := tx.Exec(sqlStr, row...)
		if err != nil {
			return fmt.Errorf("execute: %w", err)
		}
	}

	return nil
}

// buildInsertSQL builds INSERT SQL with OR REPLACE or OR IGNORE
func (w *SQLiteWriter) buildInsertSQL(table string, columns []string, row []interface{}, replace bool) string {
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = fmt.Sprintf("[%s]", col)
	}
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}

	verb := "INSERT OR IGNORE"
	if replace {
		verb = "INSERT OR REPLACE"
	}

	return fmt.Sprintf("%s INTO %s (%s) VALUES (%s)",
		verb,
		table,
		strings.Join(escapedCols, ", "),
		strings.Join(placeholders, ", "))
}

// buildStandardInsertSQL builds standard INSERT SQL
func (w *SQLiteWriter) buildStandardInsertSQL(table string, columns []string, row []interface{}) string {
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = fmt.Sprintf("[%s]", col)
	}
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(escapedCols, ", "),
		strings.Join(placeholders, ", "))
}

// updateInTx updates records in table
func (w *SQLiteWriter) updateInTx(tx *sql.Tx, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Find PK index
	pkIndex := -1
	for i, col := range ds.Columns {
		if col == config.PrimaryKey {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return fmt.Errorf("primary key %s not found", config.PrimaryKey)
	}

	for _, row := range ds.Rows {
		// Build UPDATE SQL
		var setClauses []string
		var values []any
		for i, col := range ds.Columns {
			if col == config.PrimaryKey {
				continue
			}
			setClauses = append(setClauses, fmt.Sprintf("[%s] = ?", col))
			values = append(values, row[i])
		}

		pkValue := row[pkIndex]
		sqlStr := fmt.Sprintf("UPDATE %s SET %s WHERE [%s] = ?",
			config.Output,
			strings.Join(setClauses, ", "),
			config.PrimaryKey)
		values = append(values, pkValue)

		_, err := tx.Exec(sqlStr, values...)
		if err != nil {
			return fmt.Errorf("execute: %w", err)
		}
	}

	return nil
}

// deleteInTx deletes records from table
func (w *SQLiteWriter) deleteInTx(tx *sql.Tx, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Find PK index
	pkIndex := -1
	for i, col := range ds.Columns {
		if col == config.PrimaryKey {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return fmt.Errorf("primary key %s not found", config.PrimaryKey)
	}

	pkValues := make([]any, len(ds.Rows))
	for i, row := range ds.Rows {
		pkValues[i] = row[pkIndex]
	}

	// Build IN clause
	placeholders := make([]string, len(ds.Rows))
	for i := range ds.Rows {
		placeholders[i] = "?"
	}

	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE [%s] IN (%s)",
		config.Output,
		config.PrimaryKey,
		strings.Join(placeholders, ", "))

	_, err := tx.Exec(sqlStr, pkValues...)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}

	return nil
}

// ensureTable ensures the table exists with the given columns
func (w *SQLiteWriter) ensureTable(tx *sql.Tx, table string, columns []string) error {
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = fmt.Sprintf("[%s] TEXT", col)
	}

	sqlStr := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		table,
		strings.Join(escapedCols, ", "))

	_, err := tx.Exec(sqlStr)
	return err
}

// RunMigrations runs any pending migrations in the migrations folder
func (w *SQLiteWriter) RunMigrations() error {
	migrationsDir := w.MigrationsPath()
	if migrationsDir == "" {
		return nil // No migration configured, skip
	}

	// Check if migrations directory exists
	if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
		return nil // No migrations directory, skip
	}

	// Read version directories (format: 1.0.0, 1.0.1, etc.)
	versionEntries, err := os.ReadDir(migrationsDir)
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
		versionPath := filepath.Join(migrationsDir, versionDir)
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
		if err := w.runMigrationFile(filepath.Join(migrationsDir, file)); err != nil {
			return fmt.Errorf("run migration %s: %w", file, err)
		}
	}

	return nil
}

// MigrationsPath returns the migrations directory path
// Returns empty string if not configured (no automatic fallback)
func (w *SQLiteWriter) MigrationsPath() string {
	return w.migrationPath
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
func (w *SQLiteWriter) runMigrationFile(path string) error {
	// Read migration content
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read file %s: %w", path, err)
	}

	// Execute migration in a transaction
	tx, err := w.db.Begin()
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
func (w *SQLiteWriter) Close() error {
	if w.stopCh != nil {
		close(w.stopCh)
	}
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}
