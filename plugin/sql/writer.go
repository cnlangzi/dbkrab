package sql

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

// DB returns the underlying database connection
func (w *Writer) DB() *sql.DB {
	return w.db
}

// SinkOp represents a single sink operation (write to SQLite)
type SinkOp struct {
	Config  SinkConfig
	DataSet *DataSet
	OpType  Operation
}

// WriteBatch writes multiple sink operations in a single transaction
func (w *Writer) WriteBatch(ops []*SinkOp) error {
	if len(ops) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return NewWriteError("batch", fmt.Errorf("begin transaction: %w", err))
	}
	defer func() { _ = tx.Rollback() }()

	for _, op := range ops {
		switch op.OpType {
		case Insert:
			if err := w.insertInTx(tx, &op.Config, op.DataSet); err != nil {
				return err
			}
		case Update:
			if err := w.updateInTx(tx, &op.Config, op.DataSet); err != nil {
				return err
			}
		case Delete:
			if err := w.deleteInTx(tx, &op.Config, op.DataSet); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return NewWriteError("batch", fmt.Errorf("commit transaction: %w", err))
	}

	return nil
}

// insertInTx inserts DataSet into table using the configured conflict strategy
func (w *Writer) insertInTx(tx *sql.Tx, config *SinkConfig, ds *DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	strategy := config.GetOnConflict()

	for _, row := range ds.Rows {
		sqlStr, err := w.buildInsertSQL(config.Output, config.PrimaryKey, ds.Columns, row, strategy)
		if err != nil {
			return NewWriteError(config.Output, err)
		}

		_, err = tx.Exec(sqlStr, row...)
		if err != nil {
			// For error strategy, check if it's a constraint violation
			if strategy == ConflictError && isConstraintError(err) {
				return NewWriteError(config.Output,
					fmt.Errorf("constraint error on insert: record already exists (pk=%v)", getPKValue(ds.Columns, row, config.PrimaryKey)))
			}
			return NewWriteError(config.Output, fmt.Errorf("execute insert: %w", err))
		}
	}

	return nil
}

// updateInTx updates records in table using the configured conflict strategy
func (w *Writer) updateInTx(tx *sql.Tx, config *SinkConfig, ds *DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	strategy := config.GetOnConflict()

	for _, row := range ds.Rows {
		sqlStr, args, err := w.buildUpdateSQL(config.Output, config.PrimaryKey, ds.Columns, row, strategy)
		if err != nil {
			return NewWriteError(config.Output, err)
		}

		result, err := tx.Exec(sqlStr, args...)
		if err != nil {
			return NewWriteError(config.Output, fmt.Errorf("execute update: %w", err))
		}

		// For error strategy, check if any row was actually updated
		if strategy == ConflictError {
			rowsAffected, _ := result.RowsAffected()
			if rowsAffected == 0 {
				return NewWriteError(config.Output,
					fmt.Errorf("no record found to update (pk=%v)", getPKValue(ds.Columns, row, config.PrimaryKey)))
			}
		}
	}

	return nil
}

// deleteInTx deletes records from table
func (w *Writer) deleteInTx(tx *sql.Tx, config *SinkConfig, ds *DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	strategy := config.GetOnConflict()

	// Collect primary key values
	pkIndex := -1
	for i, col := range ds.Columns {
		if col == config.PrimaryKey {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return NewWriteError(config.Output, fmt.Errorf("primary key %s not found in columns", config.PrimaryKey))
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

	sqlStr, err := w.buildDeleteSQL(config.Output, config.PrimaryKey, len(pkValues), strategy)
	if err != nil {
		return NewWriteError(config.Output, err)
	}

	result, err := tx.Exec(sqlStr, pkValues...)
	if err != nil {
		return NewWriteError(config.Output, fmt.Errorf("execute delete: %w", err))
	}

	// For error strategy, check if any row was actually deleted
	if strategy == ConflictError {
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			return NewWriteError(config.Output,
				fmt.Errorf("no record found to delete (pk=%v)", pkValues))
		}
	}

	return nil
}

// buildInsertSQL builds INSERT SQL based on conflict strategy
// row is needed for ConflictError to properly check if record exists
func (w *Writer) buildInsertSQL(table, pk string, columns []string, row []interface{}, strategy OnConflictStrategy) (string, error) {
	// Escape column names
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = fmt.Sprintf("[%s]", col)
	}

	colList := strings.Join(escapedCols, ", ")

	// Build VALUES placeholders
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}
	valuesList := strings.Join(placeholders, ", ")

	var sql string
	switch strategy {
	case ConflictOverwrite:
		// INSERT OR REPLACE - replaces existing record
		sql = fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
			table, colList, valuesList)
	case ConflictSkip:
		// INSERT OR IGNORE - does nothing if exists
		sql = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (%s)",
			table, colList, valuesList)
	case ConflictError:
		// For ConflictError, we use standard INSERT (will error on constraint violation)
		// The error will be caught in insertInTx and converted to a meaningful message
		sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table, colList, valuesList)
	default:
		// Default to skip
		sql = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (%s)",
			table, colList, valuesList)
	}

	return sql, nil
}

// buildUpdateSQL builds UPDATE SQL based on conflict strategy
func (w *Writer) buildUpdateSQL(table, pk string, columns []string, row []interface{}, strategy OnConflictStrategy) (string, []interface{}, error) {
	// Find PK index
	pkIndex := -1
	for i, col := range columns {
		if col == pk {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return "", nil, fmt.Errorf("primary key %s not found in columns", pk)
	}

	pkValue := row[pkIndex]

	// Build SET clause (exclude PK column from update)
	var setClauses []string
	var values []interface{}
	for i, col := range columns {
		if col == pk {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("[%s] = ?", col))
		values = append(values, row[i])
	}

	if len(setClauses) == 0 {
		return "", nil, fmt.Errorf("no columns to update (only primary key)")
	}

	setClause := strings.Join(setClauses, ", ")
	pkEscaped := fmt.Sprintf("[%s]", pk)

	var sql string
	switch strategy {
	case ConflictOverwrite:
		// Direct UPDATE
		sql = fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", table, setClause, pkEscaped)
		values = append(values, pkValue)
	case ConflictSkip:
		// UPDATE ... WHERE EXISTS (only if record exists)
		sql = fmt.Sprintf("UPDATE %s SET %s WHERE %s = ? AND EXISTS (SELECT 1 FROM %s WHERE %s = ?)",
			table, setClause, pkEscaped, table, pkEscaped)
		values = append(values, pkValue, pkValue)
	case ConflictError:
		// UPDATE ... WHERE pk = ? - will error if no row affected
		sql = fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", table, setClause, pkEscaped)
		values = append(values, pkValue)
	default:
		sql = fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", table, setClause, pkEscaped)
		values = append(values, pkValue)
	}

	return sql, values, nil
}

// buildDeleteSQL builds DELETE SQL based on conflict strategy
// For DELETE, ConflictSkip and ConflictOverwrite behave the same (idempotent)
// ConflictError checks if rows were actually deleted
func (w *Writer) buildDeleteSQL(table, pk string, numValues int, strategy OnConflictStrategy) (string, error) {
	// Build the IN clause placeholders
	placeholders := make([]string, numValues)
	for i := range numValues {
		placeholders[i] = "?"
	}
	valuesList := strings.Join(placeholders, ", ")

	pkEscaped := fmt.Sprintf("[%s]", pk)

	// All DELETE strategies use the same SQL - difference is in error handling
	// ConflictSkip/Overwrite: no error even if no rows deleted
	// ConflictError: error if no rows deleted
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", table, pkEscaped, valuesList)

	return sql, nil
}

// getPKValue extracts primary key value from a row
func getPKValue(columns []string, row []interface{}, pk string) interface{} {
	for i, col := range columns {
		if col == pk && i < len(row) {
			return row[i]
		}
	}
	return nil
}

// isConstraintError checks if the error is a constraint violation
func isConstraintError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "constraint") ||
		strings.Contains(errStr, "unique") ||
		strings.Contains(errStr, "primary key")
}

// WriteUpsert writes a DataSet to a table using INSERT OR REPLACE (legacy method)
func (w *Writer) WriteUpsert(table string, pk string, ds *DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return NewWriteError(table, fmt.Errorf("begin transaction: %w", err))
	}
	defer func() { _ = tx.Rollback() }()

	for _, row := range ds.Rows {
		sqlStr, err := w.buildInsertSQL(table, pk, ds.Columns, row, ConflictOverwrite)
		if err != nil {
			return NewWriteError(table, err)
		}

		_, err = tx.Exec(sqlStr, row...)
		if err != nil {
			return NewWriteError(table, fmt.Errorf("execute upsert: %w", err))
		}
	}

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

	for _, row := range ds.Rows {
		sqlStr, err := w.buildInsertSQL(table, pk, ds.Columns, row, ConflictOverwrite)
		if err != nil {
			return NewWriteError(table, err)
		}

		_, err = tx.Exec(sqlStr, row...)
		if err != nil {
			return NewWriteError(table, fmt.Errorf("execute upsert: %w", err))
		}
	}

	return nil
}

// WriteDelete writes delete operations to a table
func (w *Writer) WriteDelete(table string, pk string, ds *DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return NewWriteError(table, fmt.Errorf("begin transaction: %w", err))
	}
	defer func() { _ = tx.Rollback() }()

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

	sqlStr, err := w.buildDeleteSQL(table, pk, len(pkValues), ConflictOverwrite)
	if err != nil {
		return NewWriteError(table, err)
	}

	_, err = tx.Exec(sqlStr, pkValues...)
	if err != nil {
		return NewWriteError(table, fmt.Errorf("execute delete: %w", err))
	}

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

	sqlStr, err := w.buildDeleteSQL(table, pk, len(pkValues), ConflictOverwrite)
	if err != nil {
		return NewWriteError(table, err)
	}

	_, err = tx.Exec(sqlStr, pkValues...)
	if err != nil {
		return NewWriteError(table, fmt.Errorf("execute delete: %w", err))
	}

	return nil
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
