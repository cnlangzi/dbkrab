# SQL Plugin Architecture

**Version**: 3.0  
**Status**: Current  

---

## Overview

SQL Plugin is a data enrichment and fan-out engine for dbkrab. It uses SQL as the domain-specific language for business logic.

**Core Philosophy**:
- SQL handles data computation (SELECT only)
- Go Engine handles data transportation and storage
- Each CDC change is processed individually
- Convention over configuration

---

## Architecture

```
MSSQL CDC Tables
    ↓
Core Poller (Transaction-based batch collection)
    ↓
┌─────────────────────────────────────────────┐
│          SQL Plugin Engine (Go)            │
│                                              │
│  1. Input Mapper                             │
│     CDC data → SQL Parameters                │
│                                              │
│  2. Sink Executor                             │
│     SQL → DataSet (with Database name)      │
└─────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────┐
│        SinkWriter Manager (Platform)        │
│                                              │
│  Routes sinks to appropriate writers        │
│  based on Database field                    │
└─────────────────────────────────────────────┘
    ↓
┌────────┐  ┌────────┐  ┌────────┐
│ SQLite │  │ DuckDB │  │ MSSQL  │
└────────┘  └────────┘  └────────┘
```

---

## Key Concepts

### Per-Change Processing

Each CDC row is processed independently:

```
Transaction T1:
  orders INSERT: [{order_id: 100}, {order_id: 101}]
  
    ↓
    
For each change:
  1. Build params for this single change
  2. Execute matching sinks based on operation type
  3. Write all results in one transaction
```

### CDC Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `@cdc_lsn` | string | LSN of this change |
| `@cdc_tx_id` | string | Transaction ID |
| `@cdc_table` | string | Source table name |
| `@cdc_operation` | int | 1=DELETE, 2=INSERT, 4=UPDATE |
| `@{table}_{field}` | any | Data field value (e.g., `@orders_order_id`) |

### Sinks

Sinks are configured under `sinks` as a flat list. Each sink has a `when` field that specifies which operations it handles:

| `when` value | Trigger | Use Case |
|-------------|---------|----------|
| `[insert, update]` | `__$operation = 2 or 4` | INSERT and UPDATE share the same SQL |
| `[delete]` | `__$operation = 1` | DELETE operations |

**Key benefit**: INSERT and UPDATE operations now share the same SQL without duplication.

---

## Skill Schema

```yaml
name: plugin_name
description: "Optional description"
database: business   # Database name (maps to platform-configured storage)

on:
  - table_name

sinks:
  - name: job_name
    when: [insert, update]        # Required: [insert, update] or [delete]
    sql: |
      SELECT ... FROM source_table WHERE id = @table_id;
    output: target_table
    primary_key: id

  - name: delete_job
    when: [delete]
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: target_table
    primary_key: id
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Skill name |
| `on` | Yes | Tables to monitor |
| `database` | No | Target database name (maps to platform-configured storage) |
| `sinks` | Yes | Output configurations |
| `sinks[].name` | Yes | Sink identifier |
| `sinks[].when` | Yes | Operation filter: `[insert, update]` or `[delete]` |
| `sinks[].on` | No | Table filter (empty = all tables) |
| `sinks[].sql` | Yes* | SQL template (*or sql_file) |
| `sinks[].sql_file` | Yes* | External SQL file (*or sql) |
| `sinks[].output` | Yes | Target table name |
| `sinks[].primary_key` | Yes | Primary key column |
| `sinks[].on_conflict` | No | Conflict strategy: skip/overwrite/error (default: skip) |

---

## Operations

| __$operation | Trigger |
|-------------|---------|
| 1 | DELETE - triggers `when: [delete]` sinks |
| 2 | INSERT - triggers `when: [insert, update]` sinks |
| 4 | UPDATE (after) - triggers `when: [insert, update]` sinks |

---

## Examples

### 1:1:1 (Single Table, No Join)

```yaml
name: sync_orders
description: "orders CDC sync to order_sync"

on:
  - orders

sinks:
  - name: sync
    when: [insert, update]
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @orders_order_id as order_id,
        o.amount,
        o.status
      FROM orders o
      WHERE o.order_id = @orders_order_id;
    output: order_sync
    primary_key: order_id

  - name: sync_delete
    when: [delete]
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: order_sync
    primary_key: order_id
```

### 1:N:1 (Enrichment with JOIN)

```yaml
name: enrich_orders
description: "orders CDC with customer enrichment"

on:
  - orders

sinks:
  - name: sync
    when: [insert, update]
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @orders_order_id as order_id,
        c.name as customer_name,
        c.email as customer_email
      FROM customers c
      WHERE c.customer_id = @orders_customer_id;
    output: enriched_orders
    primary_key: order_id

  - name: sync_delete
    when: [delete]
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: enriched_orders
    primary_key: order_id
```

### N:N:N (Multiple Tables, Fan-out)

```yaml
name: sync_order_flow
description: "orders + order_items CDC, separate outputs"

on:
  - orders
  - order_items

sinks:
  - name: orders_sync
    when: [insert, update]
    on: orders
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @orders_order_id as order_id,
        c.name as customer_name
      FROM customers c
      WHERE c.customer_id = @orders_customer_id;
    output: orders_enriched
    primary_key: order_id

  - name: orders_delete
    when: [delete]
    on: orders
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: orders_enriched
    primary_key: order_id

  - name: order_items_sync
    when: [insert, update]
    on: order_items
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @order_items_id as id,
        @orders_order_id as order_id,
        p.product_name
      FROM products p
      WHERE p.product_id = @order_items_product_id;
    output: order_items_enriched
    primary_key: id

  - name: order_items_delete
    when: [delete]
    on: order_items
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @order_items_id as id;
    output: order_items_enriched
    primary_key: id
```

---

## DELETE Handling

### Direct Mapping

```yaml
- name: sync_delete
  when: [delete]
  sql: |
    SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
  output: order_sync
  primary_key: order_id
```

### Target Lookup Required

```yaml
- name: sync_delete
  when: [delete]
  sql: |
    SELECT @orders_order_id as target_id 
    FROM order_sync 
    WHERE source_order_id = @orders_order_id;
  output: order_sync
  primary_key: id
```

---

## Directory Structure

```
skills/
├── sync_orders.yml        # Skill definition (flat structure)
├── enrich_orders.yml
└── ...
```

Skills are YAML files placed directly in `skills/` root.
