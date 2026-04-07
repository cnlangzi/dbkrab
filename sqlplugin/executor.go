package sqlplugin

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// Executor executes SQL templates with parameter substitution
type Executor struct {
	db *sql.DB
}

// NewExecutor creates a new SQL template executor
func NewExecutor(db *sql.DB) *Executor {
	return &Executor{db: db}
}

// parameterPatterns defines the supported parameter patterns
var _ = parameterPatterns
var parameterPatterns = //nolint:unused
 map[string]*regexp.Regexp{
	"cdc_lsn":      regexp.MustCompile(`@cdc_lsn\b`),
	"cdc_tx_id":    regexp.MustCompile(`@cdc_tx_id\b`),
	"cdc_table":    regexp.MustCompile(`@cdc_table\b`),
	"cdc_operation": regexp.MustCompile(`@cdc_operation\b`),
	"table_ids":    regexp.MustCompile(`@(\w+)_ids\b`),
	"field":        regexp.MustCompile(`@(\w+)\b`),
}

// Execute executes a SQL template with parameters
func (e *Executor) Execute(sqlTmpl string, params CDCParameters) (*DataSet, error) {
	// Substitute parameters
	sql, err := e.substituteParams(sqlTmpl, params)
	if err != nil {
		return nil, NewExecutionError(sqlTmpl, map[string]interface{}{
			"cdc_lsn": params.CDCLSN,
			"cdc_tx_id": params.CDCTxID,
			"cdc_table": params.CDCTable,
			"cdc_operation": params.CDCOperation,
			"fields": params.Fields,
		}, err)
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

// ExecuteStages executes multi-step SQL stages
func (e *Executor) ExecuteStages(skill *Skill, params CDCParameters) (map[string]*DataSet, error) {
	results := make(map[string]*DataSet)

	for _, stage := range skill.Stages {
		if stage.SQL == "" {
			continue
		}

		// Substitute parameters
		sql, err := e.substituteParams(stage.SQL, params)
		if err != nil {
			return nil, NewExecutionError(stage.SQL, nil, err)
		}

		// Execute the stage SQL
		_, err = e.db.Exec(sql)
		if err != nil {
			return nil, NewExecutionError(stage.SQL, nil, err)
		}

		// If stage has a name/temp_table, it's a temporary table we can query
		stageName := stage.Name
		if stageName == "" && stage.TempTable != "" {
			stageName = stage.TempTable
		}

		// Query the temp table to get results for the next stage
		if stageName != "" {
			// Check if the temp table exists and has data
			query := fmt.Sprintf("SELECT * FROM %s", stageName)
			rows, err := e.db.Query(query)
			if err != nil {
				// Table might not exist, skip
				continue
			}
			defer func() { _ = rows.Close() }()

			ds, err := e.scanRows(rows)
			if err != nil {
				return nil, err
			}
			results[stageName] = ds
		}
	}

	return results, nil
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
