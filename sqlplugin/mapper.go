package sqlplugin

import (
	"database/sql"
	"fmt"
	"strings"
)

// Mapper maps CDC data to SQL parameters
type Mapper struct{}

// NewMapper creates a new CDC parameter mapper
func NewMapper() *Mapper {
	return &Mapper{}
}

// MapParameters maps CDC transaction to SQL parameters for a specific table
// For single-table CDC: directly map parameters
// For multi-table CDC: use WriteCDCBatch to write to #cdc_batch first
func (m *Mapper) MapParameters(tx *SQLTransaction, tableName string, opType Operation) (CDCParameters, error) {
	params := CDCParameters{
		Fields: make(map[string]interface{}),
	}

	// Get all changes for this table
	var tableIDs []interface{}

	// Query #cdc_batch for multi-table CDC, or use direct parameters
	if tx.cdcBatch {
		// Multi-table: query from #cdc_batch
		sqlStr := "SELECT cdc_lsn, cdc_tx_id, cdc_table, cdc_operation, table_id FROM #cdc_batch WHERE cdc_table = @table"
		rows, err := tx.Query(sqlStr, sql.NamedArg{Name: "table", Value: tableName})
		if err != nil {
			return params, fmt.Errorf("query cdc_batch: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var id interface{}
			if err := rows.Scan(&params.CDCLSN, &params.CDCTxID, &params.CDCTable, &params.CDCOperation, &id); err != nil {
				return params, fmt.Errorf("scan cdc_batch row: %w", err)
			}
			tableIDs = append(tableIDs, id)
		}
		params.TableIDs = tableIDs
	} else {
		// Single-table: use direct CDC parameters
		params.CDCLSN = tx.LSN
		params.CDCTxID = tx.TxID
		params.CDCTable = tableName
		params.CDCOperation = int(opType)
		params.TableIDs = tx.TableIDs
		params.Fields = tx.Fields
	}

	// Extract field parameters (e.g., @order_id, @amount)
	if len(tx.Fields) > 0 {
		for k, v := range tx.Fields {
			fieldName := m.fieldToParamName(k)
			params.Fields[fieldName] = v
		}
	}

	// Add table IDs with table prefix
	if len(tableIDs) > 0 {
		shortTable := m.shortTableName(tableName)
		params.Fields[fmt.Sprintf("%s_ids", shortTable)] = tableIDs
	}

	return params, nil
}

// fieldToParamName converts field name to parameter name format (e.g., order_id -> order_id)
func (m *Mapper) fieldToParamName(field string) string {
	return strings.ToLower(field)
}

// shortTableName extracts short table name (e.g., dbo.orders -> orders)
func (m *Mapper) shortTableName(table string) string {
	parts := strings.Split(table, ".")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return table
}

// SQLTransaction represents a transaction context for SQL execution
type SQLTransaction struct {
	TxID      string
	LSN       string
	TableIDs  []interface{}
	Fields    map[string]interface{}
	cdcBatch  bool
	db        *sql.DB
}

// NewSQLTransaction creates a new SQL transaction
func NewSQLTransaction(txID string, lsn string) *SQLTransaction {
	return &SQLTransaction{
		TxID:   txID,
		LSN:    lsn,
		Fields: make(map[string]interface{}),
	}
}

// Query executes a query with named parameters
func (tx *SQLTransaction) Query(sqlStr string, args ...sql.NamedArg) (*sql.Rows, error) {
	if tx.db == nil {
		return nil, fmt.Errorf("database not set")
	}
	// Convert NamedArg to any for Query
	var argsAny []interface{}
	for _, arg := range args {
		argsAny = append(argsAny, arg.Value)
	}
	return tx.db.Query(sqlStr, argsAny...)
}

// Exec executes a statement with named parameters
func (tx *SQLTransaction) Exec(sqlStr string, args ...sql.NamedArg) (sql.Result, error) {
	if tx.db == nil {
		return nil, fmt.Errorf("database not set")
	}
	// Convert NamedArg to any for Exec
	var argsAny []interface{}
	for _, arg := range args {
		argsAny = append(argsAny, arg.Value)
	}
	return tx.db.Exec(sqlStr, argsAny...)
}

// SetDatabase sets the database connection
func (tx *SQLTransaction) SetDatabase(db *sql.DB) {
	tx.db = db
}

// WriteCDCBatch writes CDC data to #cdc_batch temporary table for multi-table transactions
func (m *Mapper) WriteCDCBatch(tx *SQLTransaction, changes map[string][]ChangeItem) error {
	if tx.db == nil {
		return fmt.Errorf("database not set")
	}

	// Create temporary table if not exists
	_, err := tx.db.Exec(`
		IF OBJECT_ID('tempdb..#cdc_batch') IS NULL
		CREATE TABLE #cdc_batch (
			cdc_lsn NVARCHAR(MAX),
			cdc_tx_id NVARCHAR(MAX),
			cdc_table NVARCHAR(MAX),
			cdc_operation INT,
			table_id NVARCHAR(MAX)
		)
	`)
	if err != nil {
		return fmt.Errorf("create cdc_batch: %w", err)
	}

	// Clear existing batch
	_, err = tx.db.Exec("TRUNCATE TABLE #cdc_batch")
	if err != nil {
		return fmt.Errorf("truncate cdc_batch: %w", err)
	}

	// Write each change to the batch
	for table, items := range changes {
		for _, item := range items {
			_, err := tx.db.Exec(`
				INSERT INTO #cdc_batch (cdc_lsn, cdc_tx_id, cdc_table, cdc_operation, table_id)
				VALUES (@lsn, @txId, @table, @op, @id)
			`,
				sql.NamedArg{Name: "lsn", Value: item.LSN},
				sql.NamedArg{Name: "txId", Value: item.TxID},
				sql.NamedArg{Name: "table", Value: table},
				sql.NamedArg{Name: "op", Value: item.Operation},
				sql.NamedArg{Name: "id", Value: item.TableID},
			)
			if err != nil {
				return fmt.Errorf("insert cdc_batch: %w", err)
			}
		}
	}

	tx.cdcBatch = true
	return nil
}

// ChangeItem represents a single change in CDC data
type ChangeItem struct {
	Table     string
	LSN       string
	TxID      string
	Operation Operation
	TableID   interface{}
	Data      map[string]interface{}
}

// GetChangesByTable groups changes by table name
func GetChangesByTable(changes []ChangeItem) map[string][]ChangeItem {
	result := make(map[string][]ChangeItem)
	for _, c := range changes {
		result[c.Table] = append(result[c.Table], c)
	}
	return result
}
