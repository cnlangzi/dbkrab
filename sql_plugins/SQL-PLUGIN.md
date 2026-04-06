# SQL Plugin Architecture

**Version**: 1.0  
**Status**: Draft  
**Created**: 2026-04-06

---

## Overview

SQL Plugin is a **data enrichment and fan-out engine** for dbkrab. It uses SQL as the domain-specific language for business logic, enabling developers to express data transformations and routing rules in familiar SQL syntax.

**Core Philosophy**:
- SQL is responsible for data computation (SELECT only)
- Go Engine is responsible for data transportation and storage
- Transaction boundary is the processing unit
- Convention over configuration

---

## Architecture

```
MSSQL CDC Tables
    ↓
Core Poller (Transaction-based batch collection)
    ↓
┌─────────────────────────────────────────────┐
│          SQL Plugin Engine (Go)              │
│                                              │
│  1. Input Mapper                             │
│     CDC data → SQL Parameters (@cdc_*, @*)   │
│                                              │
│  2. SQL Template Executor                    │
│     Transaction batch → SQL execution        │
│     Multiple changes per operation type       │
│                                              │
│  3. Result Router                            │
│     DataSet → Field mapping → Target Sink    │
│                                              │
│  4. Sink Writer                              │
│     Batch write with transaction boundary     │
└─────────────────────────────────────────────┘
    ↓
┌────────┐
│ SQLite │
│  Sink  │
└────────┘
```

---

## Key Concepts

### Transaction as Processing Unit

CDC changes are grouped by transaction. All changes within the same transaction are processed together:

```
Transaction T1:
  orders INSERT: [{order_id: 100}, {order_id: 101}]
  orders UPDATE: [{order_id: 200}]
  order_items INSERT: [{order_id: 100, product_id: 999}]
  
    ↓
    
All changes in T1 are processed as one batch
```

### CDC Parameters

When executing SQL, CDC captured data is injected as parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `@cdc_lsn` | string | LSN of the last change in this transaction |
| `@cdc_tx_id` | string | Transaction ID |
| `@cdc_table` | string | Source table name |
| `@cdc_operation` | int | Operation type (1=DELETE, 2=INSERT, 4=UPDATE) |
| `@{table}_ids` | []any | Array of all changed IDs for that table (per operation type within transaction) |
| `@{field}` | any | Direct field value from CDC captured data |

### Output Configuration

`output` specifies the target **SQLite table name** directly. The Engine uses `primary_key` to:
- For INSERT/UPDATE: Determine if record exists (for upsert logic)
- For DELETE: Construct the WHERE clause

---

## Directory Structure

```
sql_plugins/
└── plugin_name/
    └── skill.yml
```

Each plugin is a directory containing a `skill.yml` configuration file.

---

## skill.yml Schema

```yaml
name: plugin_name
description: "Plugin description"

# Tables to monitor (CDC source)
on:
  - table_name

# Sink configurations (one for each operation type)
sinks:
  insert:
    - name: sink_name
      sql: |
        SELECT ... FROM source_table WHERE id IN (@table_ids);
      output: target_table      # SQLite table name
      primary_key: id

  update:
    - name: sink_name
      sql: |
        SELECT ... FROM source_table WHERE id IN (@table_ids);
      output: target_table      # SQLite table name
      primary_key: id

  delete:
    - name: sink_name
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @id as id
      output: target_table      # SQLite table name
      primary_key: id
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Plugin name |
| `description` | No | Plugin description |
| `on` | Yes | List of tables to monitor |
| `sinks` | Yes | Sink configurations keyed by operation type |
| `sinks[].sql` | Yes | SQL template (SELECT only) |
| `sinks[].sql_file` | No | Path to external SQL file (alternative to inline sql) |
| `sinks[].output` | Yes | Target SQLite table name |
| `sinks[].primary_key` | Yes | Primary key column name in target table |

---

## Operations

### INSERT

CDC INSERT operation triggers the `insert` sink.

- SQL executes against **source database**
- Batch execution using `IN (@table_ids)` for efficiency
- Target behavior: **Upsert** (INSERT if not exists, UPDATE if exists)

### UPDATE

CDC UPDATE (after-image, operation=4) triggers the `update` sink.

- SQL executes against **source database**
- Batch execution using `IN (@table_ids)` for efficiency
- Target behavior: **Upsert**

### DELETE

CDC DELETE operation triggers the `delete` sink.

- SQL may or may not query database (see scenarios below)
- Batch execution using `IN (@table_ids)` for efficiency
- Target behavior: **DELETE**

---

## DELETE Sink Scenarios

### Scenario A: Direct Mapping (No SQL FROM Required)

When target table primary key matches source table field name:

```yaml
delete:
  - name: sync
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @order_id as order_id
    output: order_sync
    primary_key: order_id
```

### Scenario B: Target Database Lookup Required

When target table primary key or structure differs from source:

```yaml
delete:
  - name: sync
    sql: |
      -- Query target database to find mapped primary key
      SELECT id as target_id FROM order_sync WHERE source_order_id IN (@order_ids);
    output: order_sync
    primary_key: id
```

---

## Execution Flow

### 1. CDC Batch Collection

Poller collects CDC changes within a transaction:

```
Transaction T1 changes:
  orders INSERT: [{order_id: 100, amount: 500}, {order_id: 101, amount: 300}]
  orders UPDATE: [{order_id: 200, status: 'completed'}]
  orders DELETE: [{order_id: 300}]
```

### 2. Grouping by Operation Type

Changes are grouped:

```
INSERT group: @order_ids = [100, 101]
UPDATE group: @order_ids = [200]
DELETE group: @order_ids = [300]
```

### 3. SQL Execution (Per Operation Type)

**INSERT sink:**

```sql
-- @cdc_lsn = '0x0001A2B3'
-- @cdc_tx_id = 'T1'
-- @cdc_table = 'orders'
-- @cdc_operation = 2
-- @order_ids = [100, 101]

SELECT 
  @cdc_lsn as cdc_lsn,
  @cdc_tx_id as cdc_transaction_id,
  @cdc_table as cdc_source_table,
  @cdc_operation as cdc_operation,
  o.order_id,
  o.amount,
  o.status,
  o.created_at
FROM orders o
WHERE o.order_id IN (100, 101);
```

**UPDATE sink:**

```sql
-- @cdc_lsn = '0x0001A2C0'
-- @cdc_tx_id = 'T1'
-- @cdc_table = 'orders'
-- @cdc_operation = 4
-- @order_ids = [200]

SELECT ...
FROM orders o
WHERE o.order_id IN (200);
```

**DELETE sink:**

```sql
-- @cdc_lsn = '0x0001A2D0'
-- @cdc_tx_id = 'T1'
-- @cdc_table = 'orders'
-- @cdc_operation = 1
-- @order_ids = [300]

SELECT @cdc_lsn as cdc_lsn, @order_id as order_id
```

### 4. DataSet Result

Each SQL execution returns a DataSet:

```
INSERT DataSet:
  [{order_id: 100, amount: 500, status: 'pending', ...},
   {order_id: 101, amount: 300, status: 'pending', ...}]

UPDATE DataSet:
  [{order_id: 200, status: 'completed', ...}]

DELETE DataSet:
  [{cdc_lsn: '0x0001A2D0', order_id: 300}]
```

### 5. Sink Write (Transaction Boundary)

All sink operations within the same CDC transaction are written as one transaction to target database:

```
BEGIN TRANSACTION
  INSERT INTO order_sync (...) VALUES (...)
  INSERT INTO order_sync (...) VALUES (...)
  UPDATE order_sync SET ... WHERE order_id = 200
  DELETE FROM order_sync WHERE order_id = 300
COMMIT
```

---

## Example: 1:1:1 (Single Table CDC, No Join, Single Output)

### Scenario

Monitor `orders` table, enrich with full row data, sync to `order_sync` table.

### Directory Structure

```
sql_plugins/
└── sync_orders/
    └── skill.yml
```

### skill.yml

```yaml
name: sync_orders
description: "orders CDC sync to order_sync"

on:
  - orders

sinks:
  insert:
    - name: sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          o.order_id,
          o.amount,
          o.status,
          o.created_at
        FROM orders o
        WHERE o.order_id IN (@order_ids);
      output: order_sync        # SQLite table name
      primary_key: order_id

  update:
    - name: sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          o.order_id,
          o.amount,
          o.status,
          o.created_at
        FROM orders o
        WHERE o.order_id IN (@order_ids);
      output: order_sync        # SQLite table name
      primary_key: order_id

  delete:
    - name: sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @order_id as order_id
      output: order_sync        # SQLite table name
      primary_key: order_id
```

---

---

## Future Enhancements

- Support for external SQL files (`sql_file` field)
- Multiple sinks per operation type (fan-out)
- Filter expressions for row-level routing
- Support for CDC UPDATE_BEFORE (for audit use cases)
