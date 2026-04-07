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
// It extracts CDC changes, executes optional stages and sink SQL against MSSQL, and writes results to SQLite
// Each change (row) is processed individually - if there are 3 row changes, sink is executed 3 times
// All sink operations are collected and written in one transaction at the end
func (e *Engine) Handle(tx *core.Transaction) error {
	if tx == nil || len(tx.Changes) == 0 {
		return nil
	}

	// Collect all sink operations for batch write
	var allSinkOps []*SinkOp

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

		// Build CDC params for this single change
		cdcParams, err := e.buildCDCParams(&change)
		if err != nil {
			return fmt.Errorf("build params: %w", err)
		}

		// Execute stages in memory if defined
		var stageDataSet *DataSet
		if len(e.skill.Stages) > 0 {
			stageDataSet, err = e.executor.ExecuteStages(e.skill, cdcParams)
			if err != nil {
				return fmt.Errorf("execute stages: %w", err)
			}
		}

		// Build params for sink, including stage results if available
		sinkParams := e.buildSinkParams(cdcParams, stageDataSet)

		// Filter sinks by table and execute
		for _, sink := range sinks {
			if sink.On != "" && sink.On != change.Table {
				continue
			}

			// Execute sink SQL against MSSQL
			ds, err := e.executor.ExecuteDriver(sink.SQL, sinkParams)
			if err != nil {
				return fmt.Errorf("execute sink %s: %w", sink.Name, err)
			}

			// Collect sink operation for batch write
			sinkOp := &SinkOp{
				Config:  sink,
				DataSet: ds,
				OpType:  sinkType,
			}
			allSinkOps = append(allSinkOps, sinkOp)
		}
	}

	// Write all sink operations in one transaction
	if len(allSinkOps) > 0 {
		if err := e.writer.WriteBatch(allSinkOps); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
	}

	return nil
}

// buildCDCParams builds CDC parameters from a single change
func (e *Engine) buildCDCParams(change *core.Change) (CDCParameters, error) {
	params := CDCParameters{
		CDCLSN:       hex.EncodeToString(change.LSN),
		CDCTxID:      change.TransactionID,
		CDCTable:     change.Table,
		CDCOperation: int(change.Operation),
		Fields:       make(map[string]interface{}),
	}

	// Add all data fields with table prefix
	shortTable := shortTableName(change.Table)
	for k, v := range change.Data {
		params.Fields[fmt.Sprintf("%s_%s", shortTable, k)] = v
	}

	// Add id field if exists
	if id, ok := change.Data["id"]; ok {
		params.Fields[shortTable+"_id"] = id
	}

	return params, nil
}

// buildSinkParams builds parameters for sink execution, merging CDC params with stage results
func (e *Engine) buildSinkParams(cdcParams CDCParameters, stageDataSet *DataSet) map[string]interface{} {
	params := make(map[string]interface{})

	// CDC metadata
	params["cdc_lsn"] = cdcParams.CDCLSN
	params["cdc_tx_id"] = cdcParams.CDCTxID
	params["cdc_table"] = cdcParams.CDCTable
	params["cdc_operation"] = cdcParams.CDCOperation

	// CDC data fields
	for k, v := range cdcParams.Fields {
		params[k] = v
	}

	// Add stage results as @<stage_name>_<field> if available
	if stageDataSet != nil && len(stageDataSet.Rows) > 0 {
		// Get the last stage name
		if len(e.skill.Stages) > 0 {
			lastStage := e.skill.Stages[len(e.skill.Stages)-1]
			stageName := lastStage.Name
			if stageName == "" {
				stageName = lastStage.TempTable
			}

			if stageName != "" {
				row := stageDataSet.Rows[0]
				for i, col := range stageDataSet.Columns {
					if i < len(row) {
						params[fmt.Sprintf("%s_%s", stageName, col)] = row[i]
					}
				}
			}
		}
	}

	return params
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
