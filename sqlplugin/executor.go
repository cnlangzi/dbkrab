package sqlplugin

import (
	"database/sql"
	"fmt"
	"strings"
)

// Executor executes SQL templates with parameter substitution
// It supports both string substitution and driver-based parameterized execution
type Executor struct {
	db     *sql.DB
	driver DriverExecutor
}

// NewExecutor creates a new SQL template executor
// For backwards compatibility, uses string substitution by default
func NewExecutor(db *sql.DB) *Executor {
	return &Executor{db: db}
}

// NewExecutorWithDriver creates a new executor with a specific driver
// dbType: "mssql" or "sqlite"
func NewExecutorWithDriver(db *sql.DB, dbType DriverType) *Executor {
	var driver DriverExecutor
	switch dbType {
	case DriverMSSQL:
		driver = NewMSSQLExecutor(db)
	case DriverSQLite:
		driver = NewSQLiteExecutor(db)
	default:
		panic("unsupported driver type: " + string(dbType) + "")
	}
	return &Executor{db: db, driver: driver}
}

// Execute executes a SQL template with parameters using string substitution
// For backwards compatibility - considers using ExecuteDriver for parameterized queries
func (e *Executor) Execute(sqlTmpl string, params CDCParameters) (*DataSet, error) {
	// Substitute parameters
	sql, err := e.substituteParams(sqlTmpl, params)
	if err != nil {
		return nil, NewExecutionError(sqlTmpl, nil, err)
	}

	// Execute query
	rows, err := e.db.Query(sql)
	if err != nil {
		return nil, NewExecutionError(sqlTmpl, nil, err)
	}
	defer func() { _ = rows.Close() }()

	// Scan results
	ds, err := e.scanRows(rows)
	if err != nil {
		return nil, NewExecutionError(sqlTmpl, nil, err)
	}

	return ds, nil
}

// ExecuteDriver executes a SQL template using driver-based parameterized queries
// This is the preferred method for security (SQL injection prevention)
func (e *Executor) ExecuteDriver(sqlTmpl string, params map[string]interface{}) (*DataSet, error) {
	if e.driver == nil {
		panic("driver not configured - use NewExecutorWithDriver")
	}
	return e.driver.Execute(sqlTmpl, params)
}

// ExecuteStages executes multi-step SQL stages in memory
// Each stage receives the previous stage's DataSet as parameters
// No temp tables in MSSQL - data flows as DataSet objects in Go memory
func (e *Executor) ExecuteStages(skill *Skill, params CDCParameters) (*DataSet, error) {
	if len(skill.Stages) == 0 {
		return nil, nil
	}

	var currentDataSet *DataSet

	for _, stage := range skill.Stages {
		if stage.SQL == "" {
			continue
		}

		// Build stage-specific params by merging CDC params with previous stage results
		stageParams := e.buildStageParams(params, stage.Name, currentDataSet)

		// Substitute parameters
		sql, err := e.substituteParams(stage.SQL, stageParams)
		if err != nil {
			return nil, NewExecutionError(stage.SQL, nil, err)
		}

		// Execute the stage SQL
		ds, err := e.executeQuery(sql)
		if err != nil {
			return nil, NewExecutionError(stage.SQL, nil, err)
		}

		currentDataSet = ds
	}

	return currentDataSet, nil
}

// buildStageParams builds parameters for a stage, merging CDC params with previous stage results
// Previous stage results are injected as @<stage_name>_<field> parameters
func (e *Executor) buildStageParams(cdcParams CDCParameters, stageName string, prevDataSet *DataSet) CDCParameters {
	params := CDCParameters{
		CDCLSN:       cdcParams.CDCLSN,
		CDCTxID:      cdcParams.CDCTxID,
		CDCTable:     cdcParams.CDCTable,
		CDCOperation: cdcParams.CDCOperation,
		Fields:       make(map[string]interface{}),
	}

	// Copy CDC fields
	for k, v := range cdcParams.Fields {
		params.Fields[k] = v
	}

	// Add previous stage results as @<stageName>_<field>
	if prevDataSet != nil && stageName != "" && len(prevDataSet.Rows) > 0 {
		row := prevDataSet.Rows[0] // Use first row
		for i, col := range prevDataSet.Columns {
			if i < len(row) {
				params.Fields[fmt.Sprintf("%s_%s", stageName, col)] = row[i]
			}
		}
	}

	return params
}

// executeQuery executes a SELECT query and returns DataSet
func (e *Executor) executeQuery(sql string) (*DataSet, error) {
	rows, err := e.db.Query(sql)
	if err != nil {
		return nil, NewExecutionError(sql, nil, err)
	}
	defer func() { _ = rows.Close() }()

	return e.scanRows(rows)
}

// substituteParams replaces parameter placeholders with actual values
func (e *Executor) substituteParams(sqlTmpl string, params CDCParameters) (string, error) {
	sql := sqlTmpl

	// Replace CDC system parameters
	sql = replaceParam(sql, `@cdc_lsn`, params.CDCLSN)
	sql = replaceParam(sql, `@cdc_tx_id`, params.CDCTxID)
	sql = replaceParam(sql, `@cdc_table`, params.CDCTable)
	sql = replaceParam(sql, `@cdc_operation`, fmt.Sprintf("%d", params.CDCOperation))

	// Replace table ID parameters (e.g., @order_ids -> (1, 2, 3))
	for paramName, value := range params.Fields {
		if strings.HasSuffix(paramName, "_ids") {
			if ids, ok := value.([]interface{}); ok {
				sql = replaceIDsParam(sql, "@"+paramName, ids)
			}
		} else {
			// Replace field parameters
			sql = replaceParam(sql, "@"+paramName, value)
		}
	}

	return sql, nil
}

// replaceParam replaces a single parameter
func replaceParam(sql, placeholder string, value interface{}) string {
	if value == nil {
		return sql
	}
	return strings.ReplaceAll(sql, placeholder, fmt.Sprintf("%v", value))
}

// replaceIDsParam replaces an IDs parameter with SQL IN clause
func replaceIDsParam(sql, placeholder string, ids []interface{}) string {
	if len(ids) == 0 {
		return strings.ReplaceAll(sql, placeholder, "(NULL)")
	}

	var sb strings.Builder
	sb.WriteString("(")
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%v", id)
	}
	sb.WriteString(")")
	return strings.ReplaceAll(sql, placeholder, sb.String())
}

// scanRows scans SQL rows into a DataSet
func (e *Executor) scanRows(rows *sql.Rows) (*DataSet, error) {
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	// Scan all rows
	var rowValues [][]interface{}
	for rows.Next() {
		// Create a slice of interfaces for each row
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		rowValues = append(rowValues, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return &DataSet{
		Columns: columns,
		Rows:    rowValues,
	}, nil
}

// ExecuteInline executes inline SQL (without template substitution for stages)
func (e *Executor) ExecuteInline(sql string) (*DataSet, error) {
	rows, err := e.db.Query(sql)
	if err != nil {
		return nil, NewExecutionError(sql, nil, err)
	}
	defer func() { _ = rows.Close() }()

	return e.scanRows(rows)
}

// Exec executes a non-query SQL statement
func (e *Executor) Exec(sql string) (sql.Result, error) {
	return e.db.Exec(sql)
}

// ReplaceParameters substitutes parameters in a SQL template (public method for testing)
func (e *Executor) ReplaceParameters(sqlTmpl string, params CDCParameters) (string, error) {
	return e.substituteParams(sqlTmpl, params)
}
