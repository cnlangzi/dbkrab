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

// normalizeRowValues ensures all time.Time values in a row are formatted as
// "2006-01-02 15:04:05" strings before SQLite insertion. This keeps the storage
// format consistent with NullTime.Value() output. Non-time values pass through
// unchanged — in particular, strings produced by NullTime.Value() are already
// in the correct format and must not be re-parsed into time.Time.
func normalizeRowValues(columns []string, row []interface{}) []interface{} {
	result := make([]interface{}, len(row))
	for i, val := range row {
		if t, ok := val.(time.Time); ok {
			result[i] = t.UTC().Format("2006-01-02 15:04:05")
		} else {
			result[i] = val
		}
	}
	return result
}

// InsertInTx inserts DataSet into table.
// For "overwrite" strategy:
//   - If record exists: UPDATE only columns provided (preserve other columns)
//   - If record doesn't exist: INSERT new record
//
// For "skip"/"": INSERT OR IGNORE (do nothing if exists)
// For other cases: standard INSERT
func InsertInTx(tx TxExec, config TableConfig, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	slog.Debug("sqliteutil.InsertInTx: starting insert",
		"table", config.Output,
		"primaryKey", config.PrimaryKey,
		"onConflict", config.OnConflict,
		"columns", columns,
		"rows", len(rows))

	pkColumn := config.PrimaryKey

	// Find PK index
	pkIndex := -1
	for i, col := range columns {
		if col == pkColumn {
			pkIndex = i
			break
		}
	}

	// Normalize onConflict first, then validate based on normalized value
	normalizedOnConflict := config.OnConflict
	if normalizedOnConflict != "overwrite" && normalizedOnConflict != "skip" {
		normalizedOnConflict = "overwrite"
	}

	// For overwrite strategy, PK must exist in columns
	if pkIndex == -1 && normalizedOnConflict == "overwrite" {
		return fmt.Errorf("primary key %s not found in columns", pkColumn)
	}

	for _, row := range rows {
		onConflict := normalizedOnConflict

		switch onConflict {
		case "overwrite":
			// "overwrite" strategy: partial update
			// - If record exists: UPDATE only provided columns (preserve other columns)
			// - If record doesn't exist: INSERT new record
			pkValue := row[pkIndex]

			// Build SET clause (exclude PK column from update)
			var setClauses []string
			var updateValues []any
			for colIdx, col := range columns {
				if col == pkColumn {
					continue // Skip PK in SET clause
				}
				setClauses = append(setClauses, fmt.Sprintf("[%s] = ?", col))
				updateValues = append(updateValues, row[colIdx])
			}

			// check if we can perform UPDATE (need non-PK columns)
			canUpdate := len(setClauses) > 0

			if canUpdate {
				updateValues = append(updateValues, pkValue)

				updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE [%s] = ?",
					config.Output,
					strings.Join(setClauses, ", "),
					pkColumn)

				slog.Debug("sqliteutil.InsertInTx: executing update",
					"table", config.Output,
					"sql", updateSQL,
					"pkValue", pkValue)
				// Normalize values for UPDATE
				normalizedUpdateValues := normalizeRowValues(columns, updateValues)
				result, err := tx.Exec(updateSQL, normalizedUpdateValues...)
				if err != nil {
					slog.Error("sqliteutil.InsertInTx: update failed",
						"table", config.Output,
						"sql", updateSQL,
						"err", err)
					return fmt.Errorf("update: %w", err)
				}

				rowsAffected, _ := result.RowsAffected()
				if rowsAffected > 0 {
					// Successfully updated existing row
					slog.Debug("sqliteutil.InsertInTx: updated existing row",
						"table", config.Output,
						"pkValue", pkValue)
					continue
				}
			}

			// No row exists (or can't update), INSERT new record
			var placeholders []string
			for range columns {
				placeholders = append(placeholders, "?")
			}
			insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedColumns(columns), ", "),
				strings.Join(placeholders, ", "))

			slog.Debug("sqliteutil.InsertInTx: executing insert",
				"table", config.Output,
				"sql", insertSQL,
				"pkValue", pkValue)
			// Normalize values for INSERT
			normalizedRow := normalizeRowValues(columns, row)
			_, err := tx.Exec(insertSQL, normalizedRow...)
			if err != nil {
				slog.Error("sqliteutil.InsertInTx: insert failed",
					"table", config.Output,
					"sql", insertSQL,
					"err", err)
				return fmt.Errorf("insert: %w", err)
			}
		default:
			// Default strategy: INSERT OR REPLACE
			sqlStr := BuildInsertSQL(config.Output, columns, true)
			slog.Debug("sqliteutil.InsertInTx: executing",
				"table", config.Output,
				"sql", sqlStr,
				"rowLen", len(row))

			// Pass row directly - driver handles time.Time serialization
			normalizedRow := normalizeRowValues(columns, row)
			slog.Debug("sqliteutil.InsertInTx: exec insert or replace", "table", config.Output, "sql", sqlStr, "args_len", len(normalizedRow))
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
		}

		slog.Debug("sqliteutil.InsertInTx: statement buffered",
			"table", config.Output)
	}

	return nil
}

// UpdateInTx updates records in table.
// For "overwrite" strategy:
//   - If record exists: UPDATE only columns in this sink (preserve other columns)
//   - If record doesn't exist: INSERT the record
//
// This achieves partial update without overwriting the entire row.
func UpdateInTx(tx TxExec, config TableConfig, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	pkColumn := config.PrimaryKey

	// Find PK index
	pkIndex := -1
	for i, col := range columns {
		if col == pkColumn {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return fmt.Errorf("primary key %s not found", pkColumn)
	}

	// Normalize onConflict before use
	normalizedOnConflict := config.OnConflict
	if normalizedOnConflict != "overwrite" && normalizedOnConflict != "skip" {
		normalizedOnConflict = "overwrite"
	}

	slog.Debug("sqliteutil.UpdateInTx: starting update",
		"table", config.Output,
		"primaryKey", pkColumn,
		"pkIndex", pkIndex,
		"onConflict", normalizedOnConflict,
		"columns", columns,
		"rows", len(rows))

	for _, row := range rows {
		pkValue := row[pkIndex]

		onConflict := normalizedOnConflict

		switch onConflict {
		case "overwrite":
			// "overwrite" strategy:
			// - If record exists: UPDATE only columns in this sink (preserve other columns)
			// - If record doesn't exist: INSERT the record
			// This achieves partial update without overwriting entire row

			// Build SET clause (exclude PK column from update)
			var setClauses []string
			var updateValues []any
			for colIdx, col := range columns {
				if col == pkColumn {
					continue // Skip PK in SET clause
				}
				setClauses = append(setClauses, fmt.Sprintf("[%s] = ?", col))
				updateValues = append(updateValues, row[colIdx])
			}

			// Check if we can perform UPDATE (need non-PK columns)
			canUpdate := len(setClauses) > 0

			if canUpdate {
				updateValues = append(updateValues, pkValue)

				updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE [%s] = ?",
					config.Output,
					strings.Join(setClauses, ", "),
					pkColumn)

				slog.Debug("sqliteutil.UpdateInTx: executing update",
					"table", config.Output,
					"sql", updateSQL,
					"pkValue", pkValue)
				// Normalize values for UPDATE
				normalizedUpdateValues := normalizeRowValues(columns, updateValues)
				result, err := tx.Exec(updateSQL, normalizedUpdateValues...)
				if err != nil {
					slog.Error("sqliteutil.UpdateInTx: update failed",
						"table", config.Output,
						"sql", updateSQL,
						"err", err)
					return fmt.Errorf("update: %w", err)
				}

				rowsAffected, _ := result.RowsAffected()
				if rowsAffected > 0 {
					// Successfully updated existing row
					slog.Debug("sqliteutil.UpdateInTx: updated existing row",
						"table", config.Output,
						"pkValue", pkValue)
					continue
				}
			}

			// No row exists (or can't update), INSERT new record
			var placeholders []string
			for range columns {
				placeholders = append(placeholders, "?")
			}
			insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedColumns(columns), ", "),
				strings.Join(placeholders, ", "))

			slog.Debug("sqliteutil.UpdateInTx: executing insert",
				"table", config.Output,
				"sql", insertSQL,
				"pkValue", pkValue)
			// Normalize values for INSERT
			normalizedRow := normalizeRowValues(columns, row)
			_, err := tx.Exec(insertSQL, normalizedRow...)
			if err != nil {
				slog.Error("sqliteutil.UpdateInTx: insert failed",
					"table", config.Output,
					"sql", insertSQL,
					"err", err)
				return fmt.Errorf("insert: %w", err)
			}
		case "skip", "":
			// Use INSERT OR IGNORE: does nothing if row with PK already exists
			var placeholders []string
			var values []any
			for i := range columns {
				placeholders = append(placeholders, "?")
				values = append(values, row[i])
			}
			sqlStr := fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedColumns(columns), ", "),
				strings.Join(placeholders, ", "))

			slog.Debug("sqliteutil.UpdateInTx: executing insert or ignore",
				"table", config.Output,
				"sql", sqlStr,
				"pkValue", pkValue)
			normalizedValues := normalizeRowValues(columns, values)
			_, err := tx.Exec(sqlStr, normalizedValues...)
			if err != nil {
				slog.Error("sqliteutil.UpdateInTx: exec failed",
					"table", config.Output,
					"sql", sqlStr,
					"pkValue", pkValue,
					"err", err)
				return fmt.Errorf("execute: %w", err)
			}
		default:
			// Plain UPDATE: only updates if row exists
			var setClauses []string
			var values []any
			for i, col := range columns {
				if col == pkColumn {
					continue
				}
				setClauses = append(setClauses, fmt.Sprintf("[%s] = ?", col))
				values = append(values, row[i])
			}
			sqlStr := fmt.Sprintf("UPDATE %s SET %s WHERE [%s] = ?",
				config.Output,
				strings.Join(setClauses, ", "),
				pkColumn)
			values = append(values, pkValue)

			slog.Debug("sqliteutil.UpdateInTx: executing plain update",
				"table", config.Output,
				"sql", sqlStr,
				"pkValue", pkValue,
				"valuesCount", len(values))

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
