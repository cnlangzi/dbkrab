package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
)

// validCaptureInstance validates capture instance name to prevent SQL injection
var validCaptureInstance = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// Change represents a single row change (raw from CDC)
type Change struct {
	Table         string
	TransactionID string
	LSN           []byte
	Operation     int       // 1=DELETE, 2=INSERT, 3=UPDATE(before), 4=UPDATE(after)
	CommitTime    time.Time // Transaction commit time from LSN
	Data          map[string]interface{}
}

// Querier handles CDC queries against MSSQL
type Querier struct {
	db       *sql.DB
	timezone *time.Location // SQL Server timezone for CDC timestamp conversion
	factory  *ScannerFactory
}

// NewQuerier creates a new CDC querier
// timezone should be the SQL Server's timezone (e.g., Asia/Shanghai for UTC+8)
// If timezone is nil, defaults to time.Local
func NewQuerier(db *sql.DB, timezone *time.Location) *Querier {
	if timezone == nil {
		timezone = time.Local
	}
	return &Querier{
		db:       db,
		timezone: timezone,
		factory:  NewScannerFactory(timezone),
	}
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
// captureInstance is used for MSSQL CDC queries (must include schema prefix)
// tableName is the original table name used for returned Change.Table (without schema prefix)
func (q *Querier) GetChanges(ctx context.Context, captureInstance string, tableName string, fromLSN, toLSN []byte) ([]Change, error) {
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

	// Get column names and types
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}

	// Create column index map for metadata extraction
	colIndex := make(map[string]int)
	for i, col := range columns {
		colIndex[col] = i
	}

	// Create typed dest slice using ScannerFactory
	dest := q.factory.CreateDest(columns, colTypes)

	var changes []Change
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		// Extract LSN
		var lsn []byte
		if idx, ok := colIndex["__$start_lsn"]; ok {
			if scanner, ok := dest[idx].(Scanner); ok {
				if val, err := scanner.Value(); err == nil && val != nil {
					if b, ok := val.([]byte); ok {
						lsn = b
					}
				}
			}
		}

		// Extract transaction ID
		var txID string
		if idx, ok := colIndex["__$transaction_id"]; ok {
			if scanner, ok := dest[idx].(Scanner); ok {
				if val, err := scanner.Value(); err == nil && val != nil {
					switch v := val.(type) {
					case string:
						txID = v
					case []byte:
						txID = FormatMSSQLGUID(v)
					default:
						txID = fmt.Sprintf("%v", v)
					}
				}
			}
		}

		// Extract operation
		var op int64
		if idx, ok := colIndex["__$operation"]; ok {
			if scanner, ok := dest[idx].(Scanner); ok {
				if val, err := scanner.Value(); err == nil && val != nil {
					switch v := val.(type) {
					case int64:
						op = v
					case int32:
						op = int64(v)
					default:
						// Try to parse from other numeric types
						fmt.Sscanf(fmt.Sprintf("%v", v), "%d", &op)
					}
				}
			}
		}

		// Extract commit time
		var commitTime time.Time
		if idx, ok := colIndex["__$commit_time"]; ok {
			if scanner, ok := dest[idx].(Scanner); ok {
				if val, err := scanner.Value(); err == nil && val != nil {
					// DateTime.Value returns RFC3339Nano string, handle both string and time.Time
					switch v := val.(type) {
					case time.Time:
						commitTime = v
					case string:
						if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
							commitTime = parsed
						}
					}
				}
			}
		}

		// Build data map from non-metadata columns
		data := make(map[string]interface{})
		for i, col := range columns {
			// Skip CDC metadata columns for data map
			if strings.HasPrefix(col, "__$") {
				continue
			}

			if scanner, ok := dest[i].(Scanner); ok {
				if val, err := scanner.Value(); err == nil {
					data[strings.ToLower(col)] = val
				}
			}
		}

		// Extract original table name from captureInstance (format: schema_table)
		tableName := captureInstance
		if idx := strings.Index(captureInstance, "_"); idx > 0 {
			tableName = captureInstance[idx+1:]
		}

		changes = append(changes, Change{
			Table:         tableName,
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
