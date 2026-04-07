package sqlplugin

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// Engine is the main SQL Plugin engine that orchestrates the entire flow
type Engine struct {
	skill    *Skill
	executor *Executor // MSSQL executor for enrichment SQL
	writer   *Writer   // SQLite writer for sink output
}

// NewEngine creates a new SQL Plugin engine
func NewEngine(skill *Skill, mssqlDB *sql.DB, sqliteDB *sql.DB) *Engine {
	return &Engine{
		skill:    skill,
		executor: NewExecutorWithDriver(mssqlDB, DriverMSSQL),
		writer:   NewWriter(sqliteDB),
	}
}

// Handle processes a core.Transaction through the SQL Plugin
// It extracts CDC changes, executes sink SQL against MSSQL, and writes results to SQLite
func (e *Engine) Handle(tx *core.Transaction) error {
	if tx == nil || len(tx.Changes) == 0 {
		return nil
	}

	// Group changes by operation type
	groups := groupChangesByOperation(tx.Changes)

	// Process each operation group
	for op, changes := range groups {
		// Skip UpdateBefore - we only handle Insert, Update (after), Delete
		if len(changes) == 0 {
			continue
		}

		// Get corresponding sink type
		sinkType := e.operationToSinkType(op)
		if sinkType == 0 {
			continue // Unknown operation
		}

		// Get sinks for this operation
		sinks := e.skill.GetSinks(sinkType)
		if len(sinks) == 0 {
			continue
		}

		// Build batch parameters
		params, err := e.buildBatchParams(tx, changes)
		if err != nil {
			return fmt.Errorf("build batch params: %w", err)
		}

		// Get table name from first change
		tableName := changes[0].Table

		// Process each sink
		for _, sink := range sinks {
			// Filter by table if sink has 'on' filter
			if sink.On != "" && sink.On != tableName {
				continue
			}

			// Execute sink SQL against MSSQL
			ds, err := e.executor.ExecuteDriver(sink.SQL, params)
			if err != nil {
				return fmt.Errorf("execute sink %s: %w", sink.Name, err)
			}

			// Write result to SQLite
			sinkOp := &SinkOp{
				Config:  sink,
				DataSet: ds,
				OpType:  sinkType,
			}

			if err := e.writer.WriteBatch([]*SinkOp{sinkOp}); err != nil {
				return fmt.Errorf("write sink %s: %w", sink.Name, err)
			}
		}
	}

	return nil
}

// groupChangesByOperation groups changes by their operation type
func groupChangesByOperation(changes []core.Change) map[core.Operation][]core.Change {
	groups := make(map[core.Operation][]core.Change)
	for _, c := range changes {
		groups[c.Operation] = append(groups[c.Operation], c)
	}
	return groups
}

// buildBatchParams builds a parameter map from a transaction and changes
// It collects all {table}_ids arrays and CDC metadata
func (e *Engine) buildBatchParams(tx *core.Transaction, changes []core.Change) (map[string]interface{}, error) {
	params := make(map[string]interface{})

	if len(changes) == 0 {
		return params, nil
	}

	// CDC metadata from transaction
	params["cdc_tx_id"] = tx.ID
	params["cdc_lsn"] = txLsnToString(tx.Changes)
	params["cdc_table"] = changes[0].Table

	// Group data fields by table
	tableData := make(map[string][]interface{})
	for _, change := range changes {
		shortTable := shortTableName(change.Table)

		// Add all data fields with table prefix
		for k, v := range change.Data {
			key := fmt.Sprintf("%s_%s", shortTable, k)
			params[key] = v
		}

		// Collect IDs for each table
		if id, ok := change.Data["id"]; ok {
			tableData[shortTable] = append(tableData[shortTable], id)
		} else {
			// Try to find any field that could be an ID
			for fieldName, value := range change.Data {
				if isIDField(fieldName) {
					tableData[shortTable] = append(tableData[shortTable], value)
					break
				}
			}
		}
	}

	// Add {table}_ids arrays
	for table, ids := range tableData {
		params[table+"_ids"] = ids
	}

	return params, nil
}

// operationToSinkType converts core.Operation to sqlplugin.Operation
// Returns 0 for unknown operations (e.g., UpdateBefore) which indicates skip
func (e *Engine) operationToSinkType(op core.Operation) Operation {
	switch op {
	case core.OpInsert:
		return Insert
	case core.OpUpdateAfter:
		return Update
	case core.OpDelete:
		return Delete
	default:
		return 0 // 0 is not valid, indicates skip
	}
}

// txLsnToString extracts the LSN from the last change in the transaction
func txLsnToString(changes []core.Change) string {
	if len(changes) == 0 {
		return ""
	}
	// Use the last change's LSN
	return hex.EncodeToString(changes[len(changes)-1].LSN)
}

// shortTableName extracts short table name (e.g., dbo.orders -> orders)
func shortTableName(table string) string {
	// Handle common prefixes
	for _, prefix := range []string{"dbo.", "sys.", "cdc.", "alwayson."} {
		if len(table) > len(prefix) && table[:len(prefix)] == prefix {
			return table[len(prefix):]
		}
	}
	return table
}

// isIDField checks if a field name looks like an ID field
// Handles: id, order_id, customerId, product_ID, uuid, key, etc.
func isIDField(name string) bool {
	idNames := []string{"id", "uuid", "key"}
	// Check prefix (id, uuid, key)
	for _, idName := range idNames {
		if len(name) >= len(idName) && strings.ToLower(name[:len(idName)]) == idName {
			return true
		}
	}
	// Check suffix (_id, Id, ID, Id, UUID, Key)
	for _, idName := range idNames {
		if len(name) >= len(idName) {
			// Try lowercase comparison for _id style
			if strings.ToLower(name[len(name)-len(idName):]) == idName {
				return true
			}
			// Try exact match for CamelCase (Id, ID)
			if name[len(name)-len(idName):] == idName || name[len(name)-len(idName):] == strings.ToUpper(idName) {
				return true
			}
		}
	}
	return false
}
