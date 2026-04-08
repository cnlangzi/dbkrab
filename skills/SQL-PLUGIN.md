# SQL Plugin Architecture

**Version**: 2.0  
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
│  2. Job Executor (optional)                  │
│     Parallel SQL → DataSet                   │
│                                              │
│  3. Sink Executor                            │
│     SQL → DataSet                            │
│                                              │
│  4. Sink Writer                              │
│     Batch upsert to SQLite                   │
└─────────────────────────────────────────────┘
    ↓
┌────────┐
│ SQLite │
└────────┘
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
  2. Execute all jobs in parallel
  3. Execute matching sinks
  4. Write all results in one transaction
```

### CDC Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `@cdc_lsn` | string | LSN of this change |
| `@cdc_tx_id` | string | Transaction ID |
| `@cdc_table` | string | Source table name |
| `@cdc_operation` | int | 1=DELETE, 2=INSERT, 4=UPDATE |
| `@{table}_{field}` | any | Data field value (e.g., `@orders_order_id`) |

### Jobs vs Sinks

| | Jobs | Sinks |
|---|---|---|
| **Execution** | Parallel for all changes | Per-change, filtered by operation |
| **Output** | Independent SQLite table | Target table |
| **Use case** | Pre-fetch, logging, cache | Final sync output |

---

## Skill Schema

```yaml
name: plugin_name
description: "Optional description"
sqlite: ./data/sinks/business.db   # Optional separate SQLite

on:
  - table_name

jobs:
  - name: job_name
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: job_output_table

sinks:
  insert:
    - name: sink_name
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table
      primary_key: id

  update:
    - name: sink_name
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table
      primary_key: id

  delete:
    - name: sink_name
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
| `sqlite` | No | Separate SQLite database path |
| `jobs` | No | Parallel SQL jobs |
| `jobs[].name` | Yes | Job identifier |
| `jobs[].sql` | Yes | SQL template |
| `jobs[].output` | Yes | Target table name |
| `sinks` | Yes | Output configurations |
| `sinks[].sql` | Yes | SQL template (SELECT only) |
| `sinks[].output` | Yes | Target table name |
| `sinks[].primary_key` | Yes | Primary key column |

---

## Operations

| __$operation | Trigger |
|-------------|---------|
| 1 | DELETE - triggers `delete` sinks |
| 2 | INSERT - triggers `insert` sinks |
| 4 | UPDATE (after) - triggers `update` sinks |

---

## Examples

### 1:1:1 (Single Table, No Join)

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
          @orders_order_id as order_id,
          o.amount,
          o.status
        FROM orders o
        WHERE o.order_id = @orders_order_id;
      output: order_sync
      primary_key: order_id

  update:
    - name: sync
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

  delete:
    - name: sync
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
  insert:
    - name: sync
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

  update:
    - name: sync
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

  delete:
    - name: sync
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
  insert:
    - name: orders_sync
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

    - name: order_items_sync
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
```

---

## DELETE Handling

### Direct Mapping

```yaml
delete:
  - name: sync
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: order_sync
    primary_key: order_id
```

### Target Lookup Required

```yaml
delete:
  - name: sync
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
