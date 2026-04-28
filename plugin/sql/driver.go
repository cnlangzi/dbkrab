package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/cnlangzi/dbkrab/internal/types"
)

// DriverType represents the type of database driver
type DriverType string

const (
	DriverMSSQL  DriverType = "mssql"
	DriverSQLite DriverType = "sqlite"
)

// DriverExecutor defines the interface for database-specific SQL execution
type DriverExecutor interface {
	// Execute runs a parameterized SQL query
	Execute(sqlTmpl string, params map[string]interface{}) (*DataSet, error)

	// ExecuteBatch runs multiple SQL statements in a transaction
	ExecuteBatch(statements []string, params []map[string]interface{}) error

	// DB returns the underlying database connection
	DB() *sql.DB
}

// MSSQLExecutor implements DriverExecutor for MSSQL
type MSSQLExecutor struct {
	db        *sql.DB
	extractor *ParamExtractor
}

// NewMSSQLExecutor creates a new MSSQL executor
func NewMSSQLExecutor(db *sql.DB) *MSSQLExecutor {
	return &MSSQLExecutor{
		db:        db,
		extractor: NewParamExtractor(),
	}
}

// Execute runs a parameterized SQL query on MSSQL
func (e *MSSQLExecutor) Execute(sqlTmpl string, params map[string]interface{}) (*DataSet, error) {
	// Convert @name to MSSQL format with named args
	sqlStr, namedArgs, err := e.extractor.ExtractNamed(sqlTmpl, params)
	if err != nil {
		return nil, err
	}

	return e.query(sqlStr, namedArgs)
}

// ExecuteBatch executes multiple statements in a transaction
func (e *MSSQLExecutor) ExecuteBatch(statements []string, params []map[string]interface{}) error {
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for i, stmt := range statements {
		var p map[string]interface{}
		if i < len(params) {
			p = params[i]
		}

		sqlStr, namedArgs, err := e.extractor.ExtractNamed(stmt, p)
		if err != nil {
			return err
		}

		_, err = tx.Exec(sqlStr, namedArgs...)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DB returns the underlying database connection
func (e *MSSQLExecutor) DB() *sql.DB {
	return e.db
}

func (e *MSSQLExecutor) query(sqlStr string, args []interface{}) (*DataSet, error) {
	// Use context with timeout to prevent long-running queries
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := e.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, NewExecutionError(sqlStr, map[string]interface{}{"args": args}, err)
	}
	defer func() { _ = rows.Close() }()

	// Get column names and types
	columns, err := rows.Columns()
	if err != nil {
		return nil, NewExecutionError(sqlStr, map[string]interface{}{"args": args}, err)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, NewExecutionError(sqlStr, map[string]interface{}{"args": args}, err)
	}

	codec := types.NewMSSQLCodec()
	dest := codec.CreateDest(colTypes)

	// Scan results
	var resultRows [][]interface{}
	for rows.Next() {
		err := rows.Scan(dest...)
		if err != nil {
			return nil, NewExecutionError(sqlStr, map[string]interface{}{"args": args}, err)
		}

		// Extract values from scanners
		row := make([]interface{}, len(dest))
		for i, d := range dest {
			if s, ok := d.(types.DBType); ok {
				row[i], _ = s.Value()
			} else {
				row[i] = d
			}
		}
		resultRows = append(resultRows, row)
	}

	return &DataSet{
		Columns: columns,
		Rows:    resultRows,
	}, nil
}

// SQLiteExecutor implements DriverExecutor for SQLite
type SQLiteExecutor struct {
	db        *sql.DB
	extractor *ParamExtractor
}

// NewSQLiteExecutor creates a new SQLite executor
func NewSQLiteExecutor(db *sql.DB) *SQLiteExecutor {
	return &SQLiteExecutor{
		db:        db,
		extractor: NewParamExtractor(),
	}
}

// Execute runs a parameterized SQL query on SQLite
func (e *SQLiteExecutor) Execute(sqlTmpl string, params map[string]interface{}) (*DataSet, error) {
	// Convert @name to ? placeholders
	sqlStr, args, err := e.extractor.Extract(sqlTmpl, params, OptionPlaceholders)
	if err != nil {
		return nil, err
	}

	return e.query(sqlStr, args)
}

// ExecuteBatch executes multiple statements in a transaction
func (e *SQLiteExecutor) ExecuteBatch(statements []string, params []map[string]interface{}) error {
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for i, stmt := range statements {
		var p map[string]interface{}
		if i < len(params) {
			p = params[i]
		}

		sqlStr, args, err := e.extractor.Extract(stmt, p, OptionPlaceholders)
		if err != nil {
			return err
		}

		_, err = tx.Exec(sqlStr, args...)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DB returns the underlying database connection
func (e *SQLiteExecutor) DB() *sql.DB {
	return e.db
}

func (e *SQLiteExecutor) query(sqlStr string, args []interface{}) (*DataSet, error) {
	rows, err := e.db.Query(sqlStr, args...)
	if err != nil {
		return nil, NewExecutionError(sqlStr, map[string]interface{}{"args": args}, err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return nil, NewExecutionError(sqlStr, map[string]interface{}{"args": args}, err)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, NewExecutionError(sqlStr, map[string]interface{}{"args": args}, err)
	}

	codec := types.NewSQLiteCodec()
	dest := codec.CreateDest(colTypes)

	var resultRows [][]interface{}
	for rows.Next() {
		err := rows.Scan(dest...)
		if err != nil {
			return nil, NewExecutionError(sqlStr, map[string]interface{}{"args": args}, err)
		}

		row := make([]interface{}, len(dest))
		for i, d := range dest {
			if s, ok := d.(types.DBType); ok {
				row[i], _ = s.Value()
			} else {
				row[i] = d
			}
		}
		resultRows = append(resultRows, row)
	}

	return &DataSet{
		Columns: columns,
		Rows:    resultRows,
	}, nil
}
