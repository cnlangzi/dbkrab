package cdc

import (
	"time"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
)

// numericPattern matches numeric strings (integers, decimals, scientific notation)
// Examples: "123", "999.99", "700.0000", "-123.45", "1.23e10"
var numericPattern = regexp.MustCompile(`^-?\d+(\.\d+)?([eE][+-]?\d+)?$`)

// validCaptureInstance validates capture instance name to prevent SQL injection
var validCaptureInstance = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// Change represents a single row change (raw from CDC)
type Change struct {
	Table         string
	TransactionID string
	LSN           []byte
	Operation     int // 1=DELETE, 2=INSERT, 3=UPDATE(before), 4=UPDATE(after)
	CommitTime    time.Time // Transaction commit time from LSN
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
	
	// Get max LSN if toLSN is not provided
	if len(toLSN) == 0 {
		var err error
		toLSN, err = q.GetMaxLSN(ctx)
		if err != nil {
			return nil, fmt.Errorf("get max LSN: %w", err)
		}
	}

	// Note: Use * only to avoid duplicate columns (CDC function already returns metadata)
	// Also convert LSN to transaction time
	query := fmt.Sprintf(`
		SELECT *, sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS __$commit_time
		FROM %s(@from_lsn, @to_lsn, N'all')
		ORDER BY __$start_lsn
	`, fnName)

	rows, err := q.db.QueryContext(ctx, query,
		sql.Named("from_lsn", fromLSN),
		sql.Named("to_lsn", toLSN),
	)
	if err != nil {
		slog.Error("GetChanges query error", "captureInstance", captureInstance, "error", err)
		return nil, fmt.Errorf("query CDC: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Warn("rows.Close error", "error", err)
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

		// Extract CDC metadata by column name
		colIndex := make(map[string]int)
		for i, col := range columns {
			colIndex[col] = i
		}

		// Get LSN
		var lsn []byte
		if idx, ok := colIndex["__$start_lsn"]; ok {
			lsn, _ = values[idx].([]byte)
		}
		
		// Get transaction ID
		var txID string
		if idx, ok := colIndex["__$transaction_id"]; ok {
			if txBytes, ok := values[idx].([]byte); ok && len(txBytes) > 0 {
				txID = formatMSSQLGUID(txBytes)
			} else {
				txID = fmt.Sprintf("%v", values[idx])
			}
		}
		
		// Get operation (MSSQL returns int64)
		var op int64
		if idx, ok := colIndex["__$operation"]; ok {
			op, _ = values[idx].(int64)
		}

		// Get commit time from LSN
		// MSSQL sys.fn_cdc_map_lsn_to_time() returns timestamp with timezone info.
		// The Go MSSQL driver correctly interprets the timezone, so we just convert to UTC.
		var commitTime time.Time
		if idx, ok := colIndex["__$commit_time"]; ok {
			switch v := values[idx].(type) {
			case time.Time:
				commitTime = v.UTC()
			case string:
				if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
					commitTime = parsed.UTC()
				}
			}
		}

		// Build data map from all columns (including metadata for completeness)
		data := make(map[string]interface{})
		for i, col := range columns {
			// Skip CDC metadata columns for data map
			if strings.HasPrefix(col, "__$") {
				continue
			}
			
			// Convert MSSQL numeric bytes to string to avoid Base64 encoding in JSON
			// MSSQL driver returns DECIMAL/NUMERIC as []byte which gets Base64 encoded
			val := convertMSSQLValue(values[i])
			
			// Convert column name to lowercase for consistency
			data[strings.ToLower(col)] = val
		}

		changes = append(changes, Change{
			Table:         captureInstance,
			TransactionID: txID,
			LSN:           lsn,
			Operation:     int(op),
			CommitTime:    commitTime,
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

// formatMSSQLGUID formats a 16-byte MSSQL GUID to standard string format
// MSSQL uniqueidentifier is stored with mixed byte order:
// - First 4 bytes: little-endian
// - Next 2 bytes: little-endian
// - Next 2 bytes: little-endian
// - Last 8 bytes: big-endian
// This function returns the canonical GUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
func formatMSSQLGUID(b []byte) string {
	if len(b) != 16 {
		// Not a valid GUID, return hex encoding
		return hex.EncodeToString(b)
	}
	
	// MSSQL GUID byte order: 3-2-1-0, 5-4, 7-6, 8-9-10-11-12-13-14-15
	// (first 3 groups are little-endian, last group is big-endian)
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0], // First 4 bytes (LE)
		b[5], b[4],             // Next 2 bytes (LE)
		b[7], b[6],             // Next 2 bytes (LE)
		b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15], // Last 8 bytes (BE)
	)
}

// convertMSSQLValue converts MSSQL driver returned values to appropriate types
// MSSQL driver returns DECIMAL/NUMERIC/FLOAT as []byte which would be Base64 encoded in JSON
// This function converts numeric []byte to string to preserve precision
func convertMSSQLValue(val interface{}) interface{} {
	// Check if value is []byte
	bytesVal, ok := val.([]byte)
	if !ok {
		// Not []byte, return as-is (string, int64, nil, etc.)
		return val
	}
	
	// Convert []byte to string
	strVal := string(bytesVal)
	
	// Check if it looks like a numeric value
	if numericPattern.MatchString(strVal) {
		// It's a numeric string, return as string to preserve precision
		// Example: "999.99", "700.0000", "824.000"
		return strVal
	}
	
	// Not numeric, return original []byte
	// (could be GUID, binary data, etc. which will be Base64 encoded as intended)
	return bytesVal
}