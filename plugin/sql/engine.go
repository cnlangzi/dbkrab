package sql

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// Engine is the SQL Plugin execution engine
// It transforms CDC changes into DataSets through Jobs and Sinks
// The caller (e.g., SQLite Sink) is responsible for writing the DataSets
type Engine struct {
	skill    *Skill
	executor *Executor // MSSQL executor for SQL execution
}

// NewEngine creates a new SQL Plugin engine
func NewEngine(skill *Skill, mssqlDB *sql.DB) *Engine {
	return &Engine{
		skill:    skill,
		executor: NewExecutorWithDriver(mssqlDB, DriverMSSQL),
	}
}

// Handle processes a core.Transaction through the SQL Plugin
// It extracts CDC changes, executes jobs and sinks against MSSQL
// Returns all operations (jobs + sinks) as []core.Sink for the caller to write
func (e *Engine) Handle(tx *core.Transaction) ([]core.Sink, error) {
	if tx == nil || len(tx.Changes) == 0 {
		return nil, nil
	}

	// Collect all operations (jobs + sinks)
	var allOps []core.Sink

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
			return nil, fmt.Errorf("build params: %w", err)
		}

		// Execute all jobs (parallel, independent)
		jobResults, err := e.executor.ExecuteJobs(e.skill, cdcParams)
		if err != nil {
			return nil, fmt.Errorf("execute jobs: %w", err)
		}

		// Convert job results to sink operations
		for _, jr := range jobResults {
			jobSinkOp := core.Sink{
				Config: core.SinkConfig{
					Name:       jr.Name,
					Output:     jr.Output,
					PrimaryKey: e.inferPrimaryKey(jr.DataSet),
					OnConflict: "overwrite", // Jobs default to overwrite
				},
				DataSet: convertDataSet(jr.DataSet),
				OpType:  core.OpInsert, // Jobs always INSERT/upsert
			}
			allOps = append(allOps, jobSinkOp)
		}

		// Get sinks for this operation
		sinks := e.skill.GetSinks(Operation(sinkType))
		if len(sinks) > 0 {
			sinkParams := e.cdcParamsToMap(cdcParams)

			// Filter sinks by table and execute
			for _, sink := range sinks {
				if sink.On != "" && sink.On != change.Table {
					continue
				}

				// Execute sink SQL against MSSQL
				ds, err := e.executor.ExecuteDriver(sink.SQL, sinkParams)
				if err != nil {
					return nil, fmt.Errorf("execute sink %s: %w", sink.Name, err)
				}

				// Collect sink operation
				sinkOp := core.Sink{
					Config: core.SinkConfig{
						Name:       sink.Name,
						Output:     sink.Output,
						PrimaryKey: sink.PrimaryKey,
						OnConflict: sink.OnConflict,
					},
					DataSet: convertDataSet(ds),
					OpType:  sinkType,
				}
				allOps = append(allOps, sinkOp)
			}
		}
	}

	return allOps, nil
}

// convertDataSet converts sqlplugin.DataSet to core.DataSet
func convertDataSet(ds *DataSet) *core.DataSet {
	if ds == nil {
		return nil
	}
	// Convert [][]interface{} to [][]any
	rows := make([][]any, len(ds.Rows))
	for i, row := range ds.Rows {
		rows[i] = make([]any, len(row))
		copy(rows[i], row)
	}
	return &core.DataSet{
		Columns: ds.Columns,
		Rows:    rows,
	}
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

// operationToSinkType converts core.Operation to core.Operation
// Returns 0 for unknown operations (e.g., UpdateBefore) which indicates skip
func (e *Engine) operationToSinkType(op core.Operation) core.Operation {
	switch op {
	case core.OpInsert:
		return core.OpInsert
	case core.OpUpdateAfter:
		return core.OpUpdateAfter
	case core.OpDelete:
		return core.OpDelete
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
