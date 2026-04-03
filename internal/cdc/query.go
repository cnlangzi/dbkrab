package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
)

// validCaptureInstance validates capture instance name to prevent SQL injection
var validCaptureInstance = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// Change represents a single row change (raw from CDC)
type Change struct {
	Table         string
	TransactionID string
	LSN           []byte
	Operation     int // 1=DELETE, 2=INSERT, 3=UPDATE(before), 4=UPDATE(after)
	Data          map[string]interface{}
}

// Querier handles CDC queries against MSSQL
type Querier struct {
	db *sql.DB
}

// NewQuerier creates a new CDC querier
func NewQuerier(db *sql.DB) *Querier {
	return &Querier{db: db}
}

// GetMinLSN returns the minimum LSN for a capture instance
func (q *Querier) GetMinLSN(ctx context.Context, captureInstance string) ([]byte, error) {
	// Validate capture instance to prevent SQL injection
	if !validCaptureInstance.MatchString(captureInstance) {
		return nil, fmt.Errorf("invalid capture instance name: %s", captureInstance)
	}
	var lsn []byte
	query := fmt.Sprintf("SELECT sys.fn_cdc_get_min_lsn('%s')", captureInstance)
	err := q.db.QueryRowContext(ctx, query).Scan(&lsn)
	return lsn, err
}

// GetMaxLSN returns the current max LSN
func (q *Querier) GetMaxLSN(ctx context.Context) ([]byte, error) {
	var lsn []byte
	err := q.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&lsn)
	return lsn, err
}

// GetChanges queries CDC changes for a table
func (q *Querier) GetChanges(ctx context.Context, captureInstance string, fromLSN, toLSN []byte) ([]Change, error) {
	// Validate capture instance to prevent SQL injection
	if !validCaptureInstance.MatchString(captureInstance) {
		return nil, fmt.Errorf("invalid capture instance name: %s", captureInstance)
	}

	// Build the CDC function name
	fnName := fmt.Sprintf("cdc.fn_cdc_get_all_changes_%s", captureInstance)
	
	query := fmt.Sprintf(`
		SELECT __$start_lsn, __$transaction_id, __$operation, __$update_mask, *
		FROM %s(@from_lsn, @to_lsn, N'all')
		ORDER BY __$start_lsn
	`, fnName)

	rows, err := q.db.QueryContext(ctx, query,
		sql.Named("from_lsn", fromLSN),
		sql.Named("to_lsn", toLSN),
	)
	if err != nil {
		return nil, fmt.Errorf("query CDC: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("rows.Close error: %v", err)
		}
	}()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	var changes []Change
	for rows.Next() {
		// Create values slice for scanning
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		// Extract CDC metadata (first 4 columns)
		lsn, _ := values[0].([]byte)
		txID := fmt.Sprintf("%x", values[1])
		op, _ := values[2].(int32)
		// updateMask is values[3], we skip it for now

		// Build data map from remaining columns
		data := make(map[string]interface{})
		for i := 4; i < len(columns); i++ {
			colName := columns[i]
			// Convert column name to lowercase for consistency
			data[strings.ToLower(colName)] = values[i]
		}

		changes = append(changes, Change{
			Table:         captureInstance,
			TransactionID: txID,
			LSN:           lsn,
			Operation:     int(op),
			Data:          data,
		})
	}

	return changes, rows.Err()
}

// EnableCDC enables CDC on the database
func (q *Querier) EnableCDC(ctx context.Context) error {
	_, err := q.db.ExecContext(ctx, "EXEC sp_cdc_enable_db")
	return err
}

// EnableTableCDC enables CDC on a specific table
func (q *Querier) EnableTableCDC(ctx context.Context, schema, table string) error {
	query := `
		EXEC sp_cdc_enable_table
			@source_schema = @schema,
			@source_name = @table,
			@role_name = NULL
	`
	_, err := q.db.ExecContext(ctx, query,
		sql.Named("schema", schema),
		sql.Named("table", table),
	)
	return err
}

// ParseTableName extracts schema and table name from "schema.table" format
func ParseTableName(fullName string) (schema, table string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "dbo", parts[0]
}

// CaptureInstanceName returns the CDC capture instance name
func CaptureInstanceName(schema, table string) string {
	return schema + "_" + table
}