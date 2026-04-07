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
	executor *Executor // MSSQL executor for SQL execution
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
// It extracts CDC changes, executes jobs and sinks against MSSQL, and writes results to SQLite
// Each change (row) is processed individually
// All operations are collected and written in one transaction at the end
func (e *Engine) Handle(tx *core.Transaction) error {
	if tx == nil || len(tx.Changes) == 0 {
		return nil
	}

	// Collect all operations (jobs + sinks) for batch write
	var allOps []*SinkOp

	// Process each change (row) individually
	for _, change := range tx.Changes {
		// Get corresponding sink type for this operation
		sinkType := e.operationToSinkType(change.Operation)
		if sinkType == 0 {
			continue // Skip unknown operations (e.g., UpdateBefore)
		}

		// Build CDC params for this single change
		cdcParams, err := e.buildCDCParams(&change)
		if err != nil {
			return fmt.Errorf("build params: %w", err)
		}
		sinkParams := e.cdcParamsToMap(cdcParams)

		// Execute all jobs (parallel, independent)
		jobResults, err := e.executor.ExecuteJobs(e.skill, cdcParams)
		if err != nil {
			return fmt.Errorf("execute jobs: %w", err)
		}

		// Convert job results to sink operations
		for _, jr := range jobResults {
			jobSinkOp := &SinkOp{
				Config: SinkConfig{
					Name:       jr.Name,
					Output:     jr.Output,
					PrimaryKey: e.inferPrimaryKey(jr.DataSet),
					OnConflict: "overwrite", // Jobs default to overwrite
				},
				DataSet: jr.DataSet,
				OpType:  Insert, // Jobs always INSERT/upsert
			}
			allOps = append(allOps, jobSinkOp)
		}

		// Get sinks for this operation
		sinks := e.skill.GetSinks(sinkType)
		if len(sinks) > 0 {
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
				allOps = append(allOps, sinkOp)
			}
		}
	}

	// Write all operations in one transaction
	if len(allOps) > 0 {
		if err := e.writer.WriteBatch(allOps); err != nil {
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

// cdcParamsToMap converts CDCParameters to map[string]interface{} for ExecuteDriver
func (e *Engine) cdcParamsToMap(params CDCParameters) map[string]interface{} {
	m := make(map[string]interface{})
	m["cdc_lsn"] = params.CDCLSN
	m["cdc_tx_id"] = params.CDCTxID
	m["cdc_table"] = params.CDCTable
	m["cdc_operation"] = params.CDCOperation
	for k, v := range params.Fields {
		m[k] = v
	}
	return m
}

// inferPrimaryKey tries to infer the primary key from the DataSet columns
func (e *Engine) inferPrimaryKey(ds *DataSet) string {
	if ds == nil {
		return "id"
	}
	// Common PK names
	for _, col := range ds.Columns {
		if col == "id" || col == "pk" || strings.HasSuffix(strings.ToLower(col), "_id") {
			return col
		}
	}
	// Default to first column
	if len(ds.Columns) > 0 {
		return ds.Columns[0]
	}
	return "id"
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
