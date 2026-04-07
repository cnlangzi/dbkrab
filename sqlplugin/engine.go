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
// Each change (row) is processed individually - if there are 3 row changes, sink is executed 3 times
func (e *Engine) Handle(tx *core.Transaction) error {
	if tx == nil || len(tx.Changes) == 0 {
		return nil
	}

	// Process each change (row) individually
	for _, change := range tx.Changes {
		// Get corresponding sink type for this operation
		sinkType := e.operationToSinkType(change.Operation)
		if sinkType == 0 {
			continue // Skip unknown operations (e.g., UpdateBefore)
		}

		// Get sinks for this operation
		sinks := e.skill.GetSinks(sinkType)
		if len(sinks) == 0 {
			continue
		}

		// Build params for this single change
		params, err := e.buildParams(&change)
		if err != nil {
			return fmt.Errorf("build params: %w", err)
		}

		// Filter sinks by table
		for _, sink := range sinks {
			if sink.On != "" && sink.On != change.Table {
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


// buildParams builds a parameter map from a single change
// It extracts CDC metadata and data fields with table prefix
func (e *Engine) buildParams(change *core.Change) (map[string]interface{}, error) {
	params := make(map[string]interface{})

	// CDC metadata
	params["cdc_tx_id"] = change.TransactionID
	params["cdc_lsn"] = hex.EncodeToString(change.LSN)
	params["cdc_table"] = change.Table

	// Add all data fields with table prefix
	shortTable := shortTableName(change.Table)
	for k, v := range change.Data {
		params[fmt.Sprintf("%s_%s", shortTable, k)] = v
	}

	// Add id field if exists
	if id, ok := change.Data["id"]; ok {
		params[shortTable+"_id"] = id
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
