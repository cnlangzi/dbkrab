package sql

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
)

// SQLiteSink writes business data to a dedicated SQLite file per skill.
// It uses connection pooling for read/write separation and supports migrations.
type SQLiteSink struct {
	pool     *Pool
	skill    *Skill
	watchDir string // directory to watch for migration changes
}

// NewSQLiteSink creates a new SQLite sink for a skill
func NewSQLiteSink(skill *Skill, pool *Pool) *SQLiteSink {
	return &SQLiteSink{
		pool:     pool,
		skill:    skill,
		watchDir: pool.MigrationsPath(),
	}
}

// Write writes transformed DataSets to the sink database
func (s *SQLiteSink) Write(ctx context.Context, ops []core.Sink) error {
	if len(ops) == 0 {
		return nil
	}

	tx, err := s.pool.Write().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, op := range ops {
		switch op.OpType {
		case core.OpInsert:
			if err := s.insertInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("insert %s: %w", op.Config.Output, err)
			}
		case core.OpUpdateAfter:
			if err := s.updateInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("update %s: %w", op.Config.Output, err)
			}
		case core.OpDelete:
			if err := s.deleteInTx(tx, op.Config, op.DataSet); err != nil {
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
func (s *SQLiteSink) insertInTx(tx *sql.Tx, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Ensure table exists
	if err := s.ensureTable(tx, config.Output, ds.Columns); err != nil {
		return err
	}

	for _, row := range ds.Rows {
		// Build INSERT SQL based on OnConflict strategy
		var sqlStr string
		var args []any

		switch config.OnConflict {
		case "overwrite":
			// INSERT OR REPLACE
			escapedCols := make([]string, len(ds.Columns))
			for i, col := range ds.Columns {
				escapedCols[i] = fmt.Sprintf("[%s]", col)
			}
			placeholders := make([]string, len(ds.Columns))
			for i := range ds.Columns {
				placeholders[i] = "?"
			}
			sqlStr = fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedCols, ", "),
				strings.Join(placeholders, ", "))
			args = row
		case "skip":
			// INSERT OR IGNORE
			escapedCols := make([]string, len(ds.Columns))
			for i, col := range ds.Columns {
				escapedCols[i] = fmt.Sprintf("[%s]", col)
			}
			placeholders := make([]string, len(ds.Columns))
			for i := range ds.Columns {
				placeholders[i] = "?"
			}
			sqlStr = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedCols, ", "),
				strings.Join(placeholders, ", "))
			args = row
		default:
			// Default to INSERT with ON CONFLICT clause
			escapedCols := make([]string, len(ds.Columns))
			for i, col := range ds.Columns {
				escapedCols[i] = fmt.Sprintf("[%s]", col)
			}
			placeholders := make([]string, len(ds.Columns))
			for i := range ds.Columns {
				placeholders[i] = "?"
			}
			sqlStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedCols, ", "),
				strings.Join(placeholders, ", "))
			args = row
		}

		_, err := tx.Exec(sqlStr, args...)
		if err != nil {
			return fmt.Errorf("execute: %w", err)
		}
	}

	return nil
}

// updateInTx updates records in table
func (s *SQLiteSink) updateInTx(tx *sql.Tx, config core.SinkConfig, ds *core.DataSet) error {
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
func (s *SQLiteSink) deleteInTx(tx *sql.Tx, config core.SinkConfig, ds *core.DataSet) error {
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
func (s *SQLiteSink) ensureTable(tx *sql.Tx, table string, columns []string) error {
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
func (s *SQLiteSink) RunMigrations() error {
	migrationsDir := s.pool.MigrationsPath()

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
		if err := s.runMigrationFile(filepath.Join(migrationsDir, file)); err != nil {
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
func (s *SQLiteSink) runMigrationFile(path string) error {
	// Read migration content
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read file %s: %w", path, err)
	}

	// Execute migration in a transaction
	tx, err := s.pool.Write().Begin()
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

// Close closes the sink
func (s *SQLiteSink) Close() error {
	return s.pool.Close()
}

// Query executes a read query against the sink database
func (s *SQLiteSink) Query(query string, args ...any) (*sql.Rows, error) {
	return s.pool.Read().Query(query, args...)
}

// QueryRow executes a read query that returns a single row
func (s *SQLiteSink) QueryRow(query string, args ...any) *sql.Row {
	return s.pool.Read().QueryRow(query, args...)
}

// GetPool returns the underlying pool
func (s *SQLiteSink) GetPool() *Pool {
	return s.pool
}

// LastModified returns the last modification time of the sink database
func (s *SQLiteSink) LastModified() (time.Time, error) {
	dbPath := filepath.Join(s.pool.Path(), s.pool.Name()+".db")
	info, err := os.Stat(dbPath)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}
