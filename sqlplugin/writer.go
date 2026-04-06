package sqlplugin

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Writer writes DataSet results to SQLite sinks
type Writer struct {
	db *sql.DB
}

// NewWriter creates a new sink writer
func NewWriter(db *sql.DB) *Writer {
	return &Writer{db: db}
}

// WriteUpsert writes a DataSet to a table using INSERT OR REPLACE
func (w *Writer) WriteUpsert(table string, pk string, ds *DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	ctx := context.Background()

	// Begin transaction
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return NewWriteError(table, fmt.Errorf("begin transaction: %w", err))
	}
	defer tx.Rollback()

	// Build and execute INSERT OR REPLACE for each row
	for _, row := range ds.Rows {
		sql, err := w.buildUpsertSQL(table, pk, ds.Columns, row)
		if err != nil {
			return NewWriteError(table, err)
		}

		_, err = tx.Exec(sql)
		if err != nil {
			return NewWriteError(table, fmt.Errorf("execute upsert: %w", err))
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return NewWriteError(table, fmt.Errorf("commit transaction: %w", err))
	}

	return nil
}

// WriteUpsertInTx writes a DataSet to a table using INSERT OR REPLACE within an existing transaction
func (w *Writer) WriteUpsertInTx(tx *sql.Tx, table string, pk string, ds *DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Build and execute INSERT OR REPLACE for each row
	for _, row := range ds.Rows {
		sql, err := w.buildUpsertSQL(table, pk, ds.Columns, row)
		if err != nil {
			return NewWriteError(table, err)
		}

		_, err = tx.Exec(sql)
		if err != nil {
			return NewWriteError(table, fmt.Errorf("execute upsert: %w", err))
		}
	}

	return nil
}

// buildUpsertSQL builds an INSERT OR REPLACE SQL statement
func (w *Writer) buildUpsertSQL(table string, pk string, columns []string, row []interface{}) (string, error) {
	// Escape column names
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = fmt.Sprintf("[%s]", col)
	}

	// Build column list
	colList := strings.Join(escapedCols, ", ")

	// Build VALUES placeholders
	placeholders := make([]string, len(row))
	for i := range row {
		placeholders[i] = fmt.Sprintf("@p%d", i)
	}
	valuesList := strings.Join(placeholders, ", ")

	// Build INSERT OR REPLACE
	sql := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		table, colList, valuesList)

	return sql, nil
}

// WriteDelete writes delete operations to a table
func (w *Writer) WriteDelete(table string, pk string, ds *DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	ctx := context.Background()

	// Begin transaction
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return NewWriteError(table, fmt.Errorf("begin transaction: %w", err))
	}
	defer tx.Rollback()

	// Collect primary key values
	pkIndex := -1
	for i, col := range ds.Columns {
		if col == pk {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return NewWriteError(table, fmt.Errorf("primary key %s not found in columns", pk))
	}

	// Build DELETE statement with IN clause
	var pkValues []interface{}
	for _, row := range ds.Rows {
		if pkIndex < len(row) {
			pkValues = append(pkValues, row[pkIndex])
		}
	}

	if len(pkValues) == 0 {
		return nil
	}

	sql, err := w.buildDeleteSQL(table, pk, pkValues)
	if err != nil {
		return NewWriteError(table, err)
	}

	_, err = tx.Exec(sql, pkValues...)
	if err != nil {
		return NewWriteError(table, fmt.Errorf("execute delete: %w", err))
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return NewWriteError(table, fmt.Errorf("commit transaction: %w", err))
	}

	return nil
}

// WriteDeleteInTx writes delete operations within an existing transaction
func (w *Writer) WriteDeleteInTx(tx *sql.Tx, table string, pk string, ds *DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Collect primary key values
	pkIndex := -1
	for i, col := range ds.Columns {
		if col == pk {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return NewWriteError(table, fmt.Errorf("primary key %s not found in columns", pk))
	}

	var pkValues []interface{}
	for _, row := range ds.Rows {
		if pkIndex < len(row) {
			pkValues = append(pkValues, row[pkIndex])
		}
	}

	if len(pkValues) == 0 {
		return nil
	}

	sql, err := w.buildDeleteSQL(table, pk, pkValues)
	if err != nil {
		return NewWriteError(table, err)
	}

	_, err = tx.Exec(sql, pkValues...)
	if err != nil {
		return NewWriteError(table, fmt.Errorf("execute delete: %w", err))
	}

	return nil
}

// buildDeleteSQL builds a DELETE SQL statement with IN clause
func (w *Writer) buildDeleteSQL(table string, pk string, pkValues []interface{}) (string, error) {
	// Build the IN clause placeholders
	placeholders := make([]string, len(pkValues))
	for i := range pkValues {
		placeholders[i] = fmt.Sprintf("@p%d", i)
	}
	valuesList := strings.Join(placeholders, ", ")

	sql := fmt.Sprintf("DELETE FROM %s WHERE [%s] IN (%s)",
		table, pk, valuesList)

	return sql, nil
}

// WriteMultiple writes multiple DataSets to their respective tables
func (w *Writer) WriteMultiple(tableData map[string]*DataSet, pkColumn string) error {
	for table, ds := range tableData {
		if err := w.WriteUpsert(table, pkColumn, ds); err != nil {
			return err
		}
	}
	return nil
}

// WriteMultipleInTx writes multiple DataSets within a single transaction
func (w *Writer) WriteMultipleInTx(tx *sql.Tx, tableData map[string]*DataSet, pkColumn string) error {
	for table, ds := range tableData {
		if err := w.WriteUpsertInTx(tx, table, pkColumn, ds); err != nil {
			return err
		}
	}
	return nil
}

// EnsureTable ensures a table exists with the given schema
func (w *Writer) EnsureTable(table string, columns []string) error {
	if len(columns) == 0 {
		return nil
	}

	// Escape column names
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = fmt.Sprintf("[%s] TEXT", col)
	}

	colDefs := strings.Join(escapedCols, ", ")

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, colDefs)

	_, err := w.db.Exec(sql)
	if err != nil {
		return NewWriteError(table, fmt.Errorf("create table: %w", err))
	}

	return nil
}
