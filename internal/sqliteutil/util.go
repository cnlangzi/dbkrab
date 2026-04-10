// Package sqliteutil provides shared utilities for SQLite operations.
package sqliteutil

import (
	"database/sql"
	"fmt"
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
