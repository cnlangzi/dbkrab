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
│  2. Job Executor                             │
│     SQL → DataSet                            │
│                                              │
│  3. Sink Writer                              │
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

Sinks are configured under `sinks` with three operation types:

| Operation | Trigger | Use Case |
|-----------|---------|----------|
| `insert` | `__$operation = 2` | New records |
| `update` | `__$operation = 4` | Updated records |
| `delete` | `__$operation = 1` | Deleted records |

---

## Skill Schema

```yaml
name: plugin_name
description: "Optional description"
sqlite: ./data/sinks/business.db   # Optional separate SQLite

on:
  - table_name

sinks:
  insert:
    - name: job_name
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table
      primary_key: id

  update:
    - name: job_name
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table
      primary_key: id

  delete:
    - name: job_name
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
| `sinks` | Yes | Output configurations |
| `sinks[insert/update/delete][].name` | Yes | Job identifier |
| `sinks[insert/update/delete][].on` | No | Table filter (empty = all tables) |
| `sinks[insert/update/delete][].sql` | Yes* | SQL template (*or sql_file) |
| `sinks[insert/update/delete][].sql_file` | Yes* | External SQL file (*or sql) |
| `sinks[insert/update/delete][].output` | Yes | Target table name |
| `sinks[insert/update/delete][].primary_key` | Yes | Primary key column |
| `sinks[insert/update/delete][].on_conflict` | No | Conflict strategy: skip/overwrite/error (default: skip) |

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
