package sqliteutil

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// TxExec is the interface for executing statements within a transaction.
// Both *sql.Tx and *DB (from this package) implement this interface.
type TxExec interface {
	Exec(query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// normalizeRowValues converts datetime string values back to time.Time for proper SQLite serialization.
// When datetime columns are stored as JSON strings, they need to be converted back to time.Time
// so the SQLite driver can serialize them correctly as DATETIME.
func normalizeRowValues(columns []string, row []interface{}) []interface{} {
	// Detect datetime columns by name pattern
	datetimeCols := make(map[int]bool)
	for i, col := range columns {
		colLower := strings.ToLower(col)
		if strings.Contains(colLower, "date") || strings.Contains(colLower, "time") || strings.Contains(colLower, "dt") || strings.Contains(colLower, "ts") {
			datetimeCols[i] = true
		}
	}

	// Process each value
	result := make([]interface{}, len(row))
	for i, val := range row {
		if datetimeCols[i] {
			// Try to parse datetime string back to time.Time
			if str, ok := val.(string); ok && str != "" {
				// Try various formats
				var t time.Time
				var err error

				// Try RFC3339Nano format first (from JSON serialization)
				if t, err = time.Parse(time.RFC3339Nano, str); err == nil {
					result[i] = t
					continue
				}
				// Try RFC3339
				if t, err = time.Parse(time.RFC3339, str); err == nil {
					result[i] = t
					continue
				}
				// Try Go's driver format "2006-01-02 15:04:05.999999999 -0700 MST"
				if t, err = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", str); err == nil {
					result[i] = t
					continue
				}
				if t, err = time.Parse("2006-01-02 15:04:05.999 -0700 MST", str); err == nil {
					result[i] = t
					continue
				}
				if t, err = time.Parse("2006-01-02 15:04:05 -0700 MST", str); err == nil {
					result[i] = t
					continue
				}
				// If parsing fails, keep original string
				slog.Debug("normalizeRowValues: failed to parse datetime", "col", columns[i], "val", str)
			}
		}
		result[i] = val
	}
	return result
}

// InsertInTx inserts DataSet into table using INSERT OR REPLACE strategy.
// Tables must be created via migrations before calling this function.
// This function will NOT create tables - it assumes the table already exists.
func InsertInTx(tx TxExec, config TableConfig, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	slog.Debug("sqliteutil.InsertInTx: starting insert",
		"table", config.Output,
		"primaryKey", config.PrimaryKey,
		"columns", columns,
		"rows", len(rows))

	for _, row := range rows {
		sqlStr := BuildInsertSQL(config.Output, columns, true)
		slog.Debug("sqliteutil.InsertInTx: executing",
			"table", config.Output,
			"sql", sqlStr,
			"rowLen", len(row))

		// Pass row directly - driver handles time.Time serialization
		normalizedRow := normalizeRowValues(columns, row)
		fmt.Printf("DEBUG Exec: table=%s, sql=%s, normalizedRow len=%d\n", config.Output, sqlStr, len(normalizedRow))
		result, err := tx.Exec(sqlStr, normalizedRow...)
		if err != nil {
			slog.Error("sqliteutil.InsertInTx: exec failed",
				"table", config.Output,
				"sql", sqlStr,
				"err", err)
			return fmt.Errorf("execute: %w", err)
		}

		if result != nil {
			rowsAffected, _ := result.RowsAffected()
			slog.Debug("sqliteutil.InsertInTx: rows affected",
				"table", config.Output,
				"rowsAffected", rowsAffected)
		}

		slog.Debug("sqliteutil.InsertInTx: statement buffered",
			"table", config.Output)
	}

	return nil
}

// UpdateInTx updates records in table.
func UpdateInTx(tx TxExec, config TableConfig, columns []string, rows [][]interface{}) error {
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

	slog.Debug("sqliteutil.UpdateInTx: starting update",
		"table", config.Output,
		"primaryKey", config.PrimaryKey,
		"pkIndex", pkIndex,
		"columns", columns,
		"rows", len(rows))

	for _, row := range rows {
		pkValue := row[pkIndex]

		var sqlStr string
		var values []any

		// For UPDATE operations:
		// - "overwrite" (INSERT OR REPLACE): update if exists, insert if not
		// - "skip" or "" (INSERT OR IGNORE): do nothing if exists, insert if not
		// - otherwise: plain UPDATE (does nothing if row doesn't exist)
		switch config.OnConflict {
		case "overwrite":
			// Use INSERT OR REPLACE: inserts if not exists, replaces if exists
			var placeholders []string
			for i := range columns {
				placeholders = append(placeholders, "?")
				values = append(values, row[i])
			}
			sqlStr = fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedColumns(columns), ", "),
				strings.Join(placeholders, ", "))
		case "skip", "":
			// Use INSERT OR IGNORE: does nothing if row with PK already exists
			var placeholders []string
			for i := range columns {
				placeholders = append(placeholders, "?")
				values = append(values, row[i])
			}
			sqlStr = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedColumns(columns), ", "),
				strings.Join(placeholders, ", "))
		default:
			// Plain UPDATE: only updates if row exists
			var setClauses []string
			for i, col := range columns {
				if col == config.PrimaryKey {
					continue
				}
				setClauses = append(setClauses, fmt.Sprintf("[%s] = ?", col))
				values = append(values, row[i])
			}
			sqlStr = fmt.Sprintf("UPDATE %s SET %s WHERE [%s] = ?",
				config.Output,
				strings.Join(setClauses, ", "),
				config.PrimaryKey)
			values = append(values, pkValue)
		}

		slog.Debug("sqliteutil.UpdateInTx: executing",
			"table", config.Output,
			"sql", sqlStr,
			"pkValue", pkValue,
			"valuesCount", len(values))

		// Pass values directly - driver handles time.Time serialization
		normalizedValues := normalizeRowValues(columns, values)
		result, err := tx.Exec(sqlStr, normalizedValues...)
		if err != nil {
			slog.Error("sqliteutil.UpdateInTx: exec failed",
				"table", config.Output,
				"sql", sqlStr,
				"pkValue", pkValue,
				"err", err)
			return fmt.Errorf("execute: %w", err)
		}

		if result != nil {
			rowsAffected, _ := result.RowsAffected()
			slog.Debug("sqliteutil.UpdateInTx: rows affected",
				"table", config.Output,
				"pkValue", pkValue,
				"rowsAffected", rowsAffected)
		}

		slog.Debug("sqliteutil.UpdateInTx: statement buffered",
			"table", config.Output,
			"pkValue", pkValue)
	}

	return nil
}

// escapedColumns returns column names properly escaped for SQL
func escapedColumns(columns []string) []string {
	escaped := make([]string, len(columns))
	for i, col := range columns {
		escaped[i] = fmt.Sprintf("[%s]", col)
	}
	return escaped
}

// DeleteInTx deletes records from table.
func DeleteInTx(tx TxExec, config TableConfig, columns []string, rows [][]interface{}) error {
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
	Output     string
	PrimaryKey string
	OnConflict string // "overwrite", "skip", or ""
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
