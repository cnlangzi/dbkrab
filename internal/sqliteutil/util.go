// Package sqliteutil provides shared utilities for SQLite operations.
package sqliteutil

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// EnsureTable creates a table with the given columns if it doesn't exist.
func EnsureTable(tx *sql.Tx, table string, columns []string) error {
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

// InsertInTx inserts DataSet into table using INSERT OR REPLACE strategy.
func InsertInTx(tx *sql.Tx, config TableConfig, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	if err := EnsureTable(tx, config.Output, columns); err != nil {
		return err
	}

	for _, row := range rows {
		sqlStr := BuildInsertSQL(config.Output, columns, true)
		_, err := tx.Exec(sqlStr, row...)
		if err != nil {
			return fmt.Errorf("execute: %w", err)
		}
	}

	return nil
}

// UpdateInTx updates records in table.
func UpdateInTx(tx *sql.Tx, config TableConfig, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// Find PK index
	pkIndex := -1
	for i, col := range columns {
		if col == config.PrimaryKey {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return fmt.Errorf("primary key %s not found", config.PrimaryKey)
	}

	for _, row := range rows {
		var setClauses []string
		var values []any
		for i, col := range columns {
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

// DeleteInTx deletes records from table.
func DeleteInTx(tx *sql.Tx, config TableConfig, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// Find PK index
	pkIndex := -1
	for i, col := range columns {
		if col == config.PrimaryKey {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return fmt.Errorf("primary key %s not found", config.PrimaryKey)
	}

	pkValues := make([]any, len(rows))
	for i, row := range rows {
		pkValues[i] = row[pkIndex]
	}

	placeholders := make([]string, len(rows))
	for i := range rows {
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

// TableConfig holds configuration for table operations.
type TableConfig struct {
	Output      string
	PrimaryKey  string
	OnConflict  string // "overwrite", "skip", or ""
}

// BuildInsertSQL builds INSERT SQL with optional OR REPLACE/OR IGNORE.
func BuildInsertSQL(table string, columns []string, replace bool) string {
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

// BuildStandardInsertSQL builds standard INSERT SQL.
func BuildStandardInsertSQL(table string, columns []string) string {
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
