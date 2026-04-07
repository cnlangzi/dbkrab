package sqlplugin

import (
	"fmt"
	"strings"
)

// Mapper maps CDC data to SQL parameters
type Mapper struct{}

// NewMapper creates a new CDC parameter mapper
func NewMapper() *Mapper {
	return &Mapper{}
}

// BuildParams converts a single ChangeItem to SQL template parameters.
// Implements Task 1.2 principles:
// - Single Change processing
// - All parameters prefixed with table name (e.g., orders_order_id)
// - Data fields overwrite CDC metadata if same name
// - OpUpdateBefore returns empty params (caller should skip)
func (m *Mapper) BuildParams(change *ChangeItem) (map[string]interface{}, error) {
	params := make(map[string]interface{})

	// CDC metadata (lower priority) - also prefixed with table name
	if change.LSN != "" {
		params["cdc_lsn"] = change.LSN
	}
	if change.TxID != "" {
		params["cdc_tx_id"] = change.TxID
	}
	if change.Table != "" {
		params["cdc_table"] = change.Table
	}
	params["cdc_operation"] = int(change.Operation)

	// All Data fields prefixed with table name
	shortTable := m.shortTableName(change.Table)
	for k, v := range change.Data {
		// Prefix: {table}_{field_name}
		key := fmt.Sprintf("%s_%s", shortTable, k)
		params[key] = v
	}

	return params, nil
}

// shortTableName extracts short table name (e.g., dbo.orders -> orders)
func (m *Mapper) shortTableName(table string) string {
	parts := strings.Split(table, ".")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return table
}

// ChangeItem represents a single change in CDC data
// All Data fields use {table_name}_{column_name} format for parameter naming
type ChangeItem struct {
	Table     string                  // Table name (e.g., "orders")
	LSN       string                  // LSN of the change
	TxID      string                  // Transaction ID
	Operation Operation               // Operation type (Insert/Update/Delete)
	Data      map[string]interface{}  // Data fields, key = {table_name}_{column_name}
}

// GetChangesByTable groups changes by table name
func GetChangesByTable(changes []ChangeItem) map[string][]ChangeItem {
	result := make(map[string][]ChangeItem)
	for _, c := range changes {
		result[c.Table] = append(result[c.Table], c)
	}
	return result
}
