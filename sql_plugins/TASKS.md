# SQL Plugin Implementation Tasks

**Status**: Ready to Start  
**Created**: 2026-04-06  
**Last Updated**: 2026-04-06  

---

## Overview

Implement SQL Plugin as a data enrichment and fan-out engine for dbkrab. SQL Plugin uses `skill.yml` configuration to define how CDC data is transformed and routed to SQLite sinks.

**Goal**: Replace the simple WASM plugin approach with a SQL-based engine that executes user-defined SQL against MSSQL and writes results to SQLite.

---

## Final Architecture

```
core.Transaction (CDC data)
    ↓
SQL Plugin Engine
    ↓
MSSQL (SQL execution: enrichment, join, fanout)
    ↓
SQLite Sink (per skill.yml output config)
```

---

## Phase 1: MVP (Minimum Viable Product)

**Goal**: End-to-end for simplest scenario: 1 CDC table → SQL enrichment → 1 SQLite table (1:1:1)

---

### Task 1.1: skill.yml Parser

**File**: `internal/sqlplugin/skill.go`

**Description**: Parse `skill.yml` into Go structs.

```go
package sqlplugin

// Operation represents CDC operation type
type Operation int

const (
    OpDelete Operation = 1
    OpInsert Operation = 2
    OpUpdate Operation = 4
)

// Stage represents a SQL stage (temp table step)
type Stage struct {
    Name    string `yaml:"name"`
    SQLFile string `yaml:"sql_file"`
    TempTable string `yaml:"temp_table"` // output temp table name
}

// SinkConfig represents a sink configuration
type SinkConfig struct {
    Name        string `yaml:"name"`
    On          string `yaml:"on"`           // table filter, empty = all
    SQL         string `yaml:"sql"`
    SQLFile     string `yaml:"sql_file"`
    Output      string `yaml:"output"`       // target SQLite table
    PrimaryKey  string `yaml:"primary_key"`
}

// Skill represents a parsed skill.yml
type Skill struct {
    Name        string                      `yaml:"name"`
    Description string                      `yaml:"description"`
    On          []string                    `yaml:"on"`
    Stages      []Stage                     `yaml:"stages"`
    Sinks       map[Operation][]SinkConfig  `yaml:"sinks"`
}
```

**Acceptance Criteria**:
- [ ] Parse single-table config (`on: [orders]`)
- [ ] Parse `sinks.insert/update/delete` three operations
- [ ] `sql_file` path correctly joined and read
- [ ] `on` field (table filter) parsed correctly

**Deliverables**:
- `internal/sqlplugin/skill.go`
- Unit tests for parsing

---

### Task 1.2: CDC Parameter Injection

**File**: `internal/sqlplugin/params.go`

**Description**: Build parameter map from `core.Transaction` for SQL template substitution.

```go
package sqlplugin

// BuildParams builds CDC parameters from transaction for a specific operation
// Returns parameters and the set of {table}_ids collected
func BuildParams(tx *core.Transaction, op Operation) (map[string]any, error)

// CDC Parameters:
// @cdc_lsn       string  - LSN of last change in batch
// @cdc_tx_id     string  - Transaction ID
// @cdc_table     string  - Source table name (first table in batch)
// @cdc_operation int     - Operation type (1=DELETE, 2=INSERT, 4=UPDATE)
// @{table}_ids   []any   - Array of all changed IDs for that table in this op
// @{field}       any     - Direct field value (from first change of matching table)
```

**Acceptance Criteria**:
- [ ] Group changes by Operation
- [ ] Extract `{table}_ids` arrays per operation
- [ ] `@cdc_lsn` from last change in batch
- [ ] Single-table and multi-table scenarios work correctly

**Deliverables**:
- `internal/sqlplugin/params.go`
- Unit tests with mock transactions

---

### Task 1.3: SQL Template Executor

**File**: `internal/sqlplugin/executor.go`

**Description**: Execute SQL templates against MSSQL with parameter substitution.

```go
package sqlplugin

// Executor executes SQL templates with parameter substitution
type Executor struct {
    mssqlDB *sql.DB
}

// DataSet represents query results
type DataSet struct {
    Columns []string
    Rows    []map[string]any
}

// Execute executes a SQL template with params
// Handles:
// - @placeholder substitution (single values and arrays)
// - Temp table (#) creation
// - View queries
func (e *Executor) Execute(sqlTemplate string, params map[string]any) (*DataSet, error)

// ExecuteFile reads SQL from file and executes
func (e *Executor) ExecuteFile(sqlFile string, params map[string]any) (*DataSet, error)

// WriteCDCBatch writes CDC changes to #cdc_batch temp table
// Called by Engine before stage execution
func (e *Executor) WriteCDCBatch(tx *core.Transaction, op Operation) error
```

**SQL Substitution Rules**:
```go
// Single value: @order_id → 123
"WHERE order_id = @order_id"

// Array value: @order_ids → (100, 101, 102)
"WHERE order_id IN (@order_ids)"

// CDC metadata
"@cdc_lsn", "@cdc_tx_id", "@cdc_table", "@cdc_operation"
```

**Acceptance Criteria**:
- [ ] `@placeholder` substitution for single values
- [ ] `@placeholder` substitution for arrays (converts to `(v1, v2, v3)`)
- [ ] MSSQL temp table (`#`) creation and query
- [ ] MSSQL view queries
- [ ] Execution within transaction

**Deliverables**:
- `internal/sqlplugin/executor.go`
- Unit tests with MSSQL mock

---

### Task 1.4: SQLite Sink Writer

**File**: `internal/sqlplugin/writer.go`

**Description**: Write DataSet results to SQLite tables based on SinkConfig.

```go
package sqlplugin

// Writer writes DataSet to SQLite sink
type Writer struct {
    db *sql.DB
}

// SinkOp represents a single sink operation
type SinkOp struct {
    Config  SinkConfig
    DataSet *DataSet
    OpType  Operation  // INSERT, UPDATE, DELETE
}

// Write writes a SinkOp to SQLite
// Handles:
// - Upsert logic (INSERT if not exists, UPDATE if exists)
// - DELETE logic (direct delete by primary key)
func (w *Writer) Write(op *SinkOp) error

// WriteBatch writes multiple SinkOps in a single transaction
func (w *Writer) WriteBatch(ops []*SinkOp) error
```

**Upsert Logic**:
```sql
-- Check if record exists
SELECT COUNT(*) FROM {output} WHERE {primary_key} = @pk;

-- INSERT
INSERT INTO {output} (...) VALUES (...);

-- UPDATE
UPDATE {output} SET ... WHERE {primary_key} = @pk;
```

**DELETE Logic**:
```sql
-- Mode A: Direct mapping (using @ parameters directly)
DELETE FROM {output} WHERE {primary_key} IN (@order_ids);

-- Mode B: Target lookup (query target table first)
SELECT target_id FROM {output} WHERE source_order_id IN (@order_ids);
DELETE FROM {output} WHERE {primary_key} IN (...);
```

**Acceptance Criteria**:
- [ ] INSERT: insert if not exists, ignore if exists (or update based on config)
- [ ] UPDATE: update if exists
- [ ] DELETE: direct delete using primary_key
- [ ] All ops in same batch use single transaction
- [ ] Primary key column correctly identified

**Deliverables**:
- `internal/sqlplugin/writer.go`
- Unit tests with SQLite

---

### Task 1.5: SQL Plugin Engine (End-to-End)

**File**: `internal/sqlplugin/engine.go`

**Description**: Main engine that orchestrates the entire flow.

```go
package sqlplugin

// Engine is the main SQL Plugin engine
type Engine struct {
    skill    *Skill
    executor *Executor
    writer   *Writer
}

// NewEngine creates a new SQL Plugin engine
func NewEngine(skill *Skill, mssqlDB, sqliteDB *sql.DB) *Engine

// Handle processes a core.Transaction through the SQL Plugin
func (e *Engine) Handle(tx *core.Transaction) error

// Flow:
// 1. Group changes by Operation (INSERT/UPDATE/DELETE)
// 2. For each operation group:
//    a. Build CDC parameters (Task 1.2)
//    b. Execute stages if configured (Task 2.2)
//    c. For each sink in operation:
//       - Filter by sinks[].on if specified
//       - Execute sink SQL with params (Task 1.3)
//       - Write result to SQLite (Task 1.4)
```

**Acceptance Criteria**:
- [ ] `sync_orders` scenario works end-to-end: orders CDC → order_sync SQLite table
- [ ] INSERT, UPDATE, DELETE operations all work correctly
- [ ] Results written to correct SQLite table

**Deliverables**:
- `internal/sqlplugin/engine.go`
- Integration test with MSSQL + SQLite

---

## Phase 2: Multi-Table Support

**Goal**: Support monitoring multiple CDC tables with fan-out to different sinks

---

### Task 2.1: `sinks[].on` Table Filter

**File**: `internal/sqlplugin/engine.go` (modify Task 1.5)

**Description**: Route CDC changes to correct sinks based on `sinks[].on` configuration.

```go
// In Handle(), for each operation:
// 1. Group changes by source table
// 2. For each sink in operation:
//    if sink.On != "" && sink.On != table {
//        continue  // skip, this sink doesn't handle this table
//    }
//    // execute sink SQL with this table's parameters
```

**Acceptance Criteria**:
- [ ] N:N:N scenario: orders + order_items routed to separate sinks
- [ ] `sinks[].on: orders` only processes orders table changes
- [ ] `sinks[].on: order_items` only processes order_items table changes
- [ ] Single-table scenario: `on` field can be omitted (backward compatible)

**Deliverables**:
- Modified `internal/sqlplugin/engine.go`
- Test for multi-table routing

---

### Task 2.2: Stage Pipeline (Multi-Step SQL)

**File**: `internal/sqlplugin/stages.go`

**Description**: Execute multiple SQL stages in sequence, passing results between stages.

```go
package sqlplugin

// StageExecutor executes a pipeline of SQL stages
type StageExecutor struct {
    executor *Executor
}

// ExecuteStages runs stages in order, managing temp tables
// CDC batch is written to #cdc_batch before stages run
func (se *StageExecutor) ExecuteStages(stages []Stage, params map[string]any) error

// Flow:
// CDC batch → #cdc_batch (written by Engine)
//     ↓
// stage[0].sql_file → #temp1 (e.g., #customers_enriched)
//     ↓
// stage[1].sql_file → #temp2 (e.g., #products_enriched)
//     ↓
// sinks query from #temp2 or view
```

**Acceptance Criteria**:
- [ ] `#cdc_batch` temp table created and populated by Engine
- [ ] Multiple stages execute in sequence
- [ ] Each stage's `INTO #temp` creates accessible temp table for next stage
- [ ] `sync_order_flow` scenario works: orders + order_items → join → fanout

**Deliverables**:
- `internal/sqlplugin/stages.go`
- Test with multi-stage SQL

---

### Task 2.3: DELETE Sink Two Modes

**File**: `internal/sqlplugin/writer.go` (modify Task 1.4)

**Description**: Support two DELETE modes based on SQL content.

```go
// Mode A: Direct Mapping
// SQL uses @ parameters directly (no FROM clause or FROM source table)
delete:
  sql: "SELECT @cdc_lsn as cdc_lsn, @order_id as order_id"
// → Use @order_ids directly for DELETE WHERE

// Mode B: Target Lookup
// SQL queries target table to find mapped keys
delete:
  sql: "SELECT target_id FROM order_sync WHERE source_order_id IN (@order_ids)"
// → Query target first, then DELETE with found IDs
```

```go
func detectDeleteMode(sql string) DeleteMode {
    // Mode A: SELECT with only @ parameters (no table reference)
    // Mode B: SELECT with FROM target table
}
```

**Acceptance Criteria**:
- [ ] Mode A: DELETE using `@ids` directly
- [ ] Mode B: Query target table first, then DELETE
- [ ] Both modes work in N:N:N scenario

**Deliverables**:
- Modified `internal/sqlplugin/writer.go`
- Tests for both delete modes

---

## Phase 3: Poller Integration

**Goal**: Integrate SQL Plugin Engine into the CDC Poller

---

### Task 3.1: Config Extension

**File**: `internal/config/config.go`

**Description**: Add SQL Plugin configuration to main config.

```go
// Add to Config struct:
type Config struct {
    // ... existing fields ...
    SQLPlugin SQLPluginConfig `yaml:"sql_plugin"`
}

type SQLPluginConfig struct {
    Enabled     bool   `yaml:"enabled"`
    Dir         string `yaml:"dir"`          // sql_plugins/ directory path
    Skill       string `yaml:"skill"`        // skill name to load (e.g., "sync_orders")
}
```

**Config File Example**:
```yaml
mssql:
  host: localhost
  port: 1433
  user: sa
  password: password
  database: testdb

tables:
  - orders

sql_plugin:
  enabled: true
  dir: ./sql_plugins
  skill: sync_orders

sink:
  type: sqlite
  path: ./data/cdc.db
```

**Acceptance Criteria**:
- [ ] `sql_plugin` section parsed from YAML
- [ ] Invalid/missing config handled gracefully
- [ ] Backward compatible: `sql_plugin.enabled: false` or omitted → use existing behavior

**Deliverables**:
- Modified `internal/config/config.go`
- Test config parsing

---

### Task 3.2: Poller Handler Integration

**File**: `internal/core/poller.go`

**Description**: Wire SQL Plugin Engine as the poller handler.

```go
// In NewPoller or Start():
func (p *Poller) setupSQLPlugin(cfg *config.Config) error {
    if !cfg.SQLPlugin.Enabled {
        return nil  // use existing behavior
    }

    // Load skill.yml
    skillPath := filepath.Join(cfg.SQLPlugin.Dir, cfg.SQLPlugin.Skill, "skill.yml")
    skill, err := sqlplugin.LoadSkill(skillPath)
    if err != nil {
        return fmt.Errorf("load skill: %w", err)
    }

    // Create engine (reuse MSSQL db from poller)
    engine := sqlplugin.NewEngine(skill, p.db, sqliteDB)

    // Set as handler
    p.SetHandler(engine.Handle)

    return nil
}
```

**Acceptance Criteria**:
- [ ] When `sql_plugin.enabled: true`, SQL Plugin handles transactions
- [ ] When disabled, existing behavior unchanged
- [ ] MSSQL connection shared with poller
- [ ] SQLite sink created per skill config

**Deliverables**:
- Modified `internal/core/poller.go`
- Integration test

---

### Task 3.3: MSSQL Connection Management

**File**: `internal/sqlplugin/mssql.go`

**Description**: Reuse MSSQL connection from poller for SQL Plugin execution.

```go
package sqlplugin

// MSSQLDB wraps MSSQL connection for SQL Plugin
type MSSQLDB struct {
    db *sql.DB  // shared from poller
}

// NewMSSQLDB creates MSSQLDB wrapper
func NewMSSQLDB(db *sql.DB) *MSSQLDB

// Query executes a query and returns DataSet
func (m *MSSQLDB) Query(sql string, args ...any) (*DataSet, error)

// Exec executes a non-query (INSERT, UPDATE, DELETE, CREATE #temp)
func (m *MSSQLDB) Exec(sql string, args ...any) error
```

**Acceptance Criteria**:
- [ ] Reuses existing MSSQL connection from poller
- [ ] Connection pooling works correctly
- [ ] Temp tables (`#`) created in correct database

**Deliverables**:
- `internal/sqlplugin/mssql.go`
- Tests for connection reuse

---

## Phase 4: Example Verification

---

### Task 4.1: Verify `sync_orders` (1:1:1)

**Directory**: `sql_plugins/sync_orders/`

**Scenario**: Monitor `orders` table, sync to `order_sync` SQLite table.

**Steps**:
1. Create MSSQL test database with `orders` table
2. Enable CDC on `orders`
3. Configure `sql_plugin` with `skill: sync_orders`
4. Insert/Update/Delete rows in `orders`
5. Verify rows appear in `order_sync` SQLite table

**Acceptance Criteria**:
- [ ] INSERT creates new row in `order_sync`
- [ ] UPDATE modifies existing row in `order_sync`
- [ ] DELETE removes row from `order_sync`
- [ ] All CDC metadata columns populated correctly

---

### Task 4.2: Verify `sync_order_flow` (N:N:N)

**Directory**: `sql_plugins/sync_order_flow/`

**Scenario**: Monitor `orders` + `order_items`, join with `customers` + `products`, fanout to separate tables.

**Tables**:
- `orders_enriched`: orders with customer info
- `order_items_enriched`: order_items with product info

**Steps**:
1. Create MSSQL test database with 4 tables + CDC
2. Configure `sql_plugin` with `skill: sync_order_flow`
3. Insert order + order_items in same transaction
4. Verify both output tables populated correctly

**Acceptance Criteria**:
- [ ] Orders INSERT creates row in `orders_enriched`
- [ ] Order_items INSERT creates row in `order_items_enriched`
- [ ] Same transaction processes both atomically
- [ ] JOIN with `customers` and `products` works correctly
- [ ] DELETE on either table works correctly

---

## Task Dependencies

```
Phase 1 (MVP)
├── Task 1.1: skill.yml Parser
├── Task 1.2: CDC Parameter Injection
├── Task 1.3: SQL Template Executor
├── Task 1.4: SQLite Sink Writer
└── Task 1.5: Engine (End-to-End) ← depends on 1.1-1.4

Phase 2 (Multi-Table)
├── Task 2.1: sinks[].on Table Filter ← modifies 1.5
├── Task 2.2: Stage Pipeline ← new file
└── Task 2.3: DELETE Two Modes ← modifies 1.4

Phase 3 (Integration)
├── Task 3.1: Config Extension
├── Task 3.2: Poller Integration ← depends on 1.5 + 3.1
└── Task 3.3: MSSQL Connection ← new file

Phase 4 (Verification)
├── Task 4.1: Verify sync_orders ← depends on 3.2
└── Task 4.2: Verify sync_order_flow ← depends on 2.1 + 2.2 + 2.3 + 3.2
```

---

## File Structure (Target)

```
dbkrab/
├── internal/
│   ├── sqlplugin/
│   │   ├── skill.go        # Task 1.1
│   │   ├── params.go       # Task 1.2
│   │   ├── executor.go     # Task 1.3
│   │   ├── writer.go       # Task 1.4
│   │   ├── stages.go       # Task 2.2
│   │   ├── mssql.go        # Task 3.3
│   │   ├── engine.go       # Task 1.5
│   │   └── engine_test.go
│   └── config/
│       └── config.go       # Task 3.1
├── sql_plugins/
│   ├── TASKS.md            # This file
│   ├── sync_orders/        # Example: Task 4.1
│   │   └── skill.yml
│   └── sync_order_flow/    # Example: Task 4.2
│       ├── skill.yml
│       ├── join_all.sql
│       └── fanout.sql
└── internal/core/
    └── poller.go           # Task 3.2
```

---

## Notes

1. **Task Order**: Start with Task 1.1 and proceed sequentially. Each task builds on the previous.

2. **Testing Strategy**: Write unit tests for each task. Integration tests in Phase 4 verify end-to-end behavior.

3. **Backward Compatibility**: Existing poller behavior must remain when `sql_plugin.enabled: false`.

4. **MSSQL Connection**: SQL Plugin should reuse the existing MSSQL connection from the poller, not create a new one.

5. **Transaction Boundary**: All sink writes for a single CDC transaction should be atomic (same SQLite transaction).
