package sqlite

import (
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// RunMigrationsFS discovers and runs pending migrations from embedded FS.
// This is called internally during sinker initialization.
func RunMigrationsFS(db *sql.DB, migrationsFS fs.FS, migrationsDir string) error {
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
			return fmt.Errorf("exec %s: %w", stmt[:min(50, len(stmt))], err)
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
	var current strings.Builder

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
