# SQL Plugin Architecture

**Version**: 2.0  
**Status**: Draft  
**Created**: 2026-04-06  
**Updated**: 2026-04-07

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
│  2. Job Executor (optional)                  │
│     Parallel SQL jobs → DataSet             │
│     Each job independent, no cross-reference │
│                                              │
│  3. Sink Executor                            │
│     Sink SQL → DataSet                       │
│                                              │
│  4. Sink Writer                              │
│     Batch write with transaction boundary    │
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

CDC changes are grouped by transaction, but **each change (row) is processed individually**:

```
Transaction T1:
  orders INSERT: [{order_id: 100}, {order_id: 101}]
  orders UPDATE: [{order_id: 200}]
  order_items INSERT: [{order_id: 100, product_id: 999}]
  
    ↓
    
For each change (row):
  1. Build fresh params for this single change
  2. Execute all jobs in parallel (if any)
  3. Execute sink SQL for this operation type
  
Result: Jobs and sinks execute per change, all results written in one transaction
```

**Key principle**: Each change gets its own independent execution context. Parameters are NOT shared between changes.

### CDC Parameters

When executing SQL (jobs or sinks), CDC captured data is injected as parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `@cdc_lsn` | string | LSN of this specific change |
| `@cdc_tx_id` | string | Transaction ID |
| `@cdc_table` | string | Source table name |
| `@cdc_operation` | int | Operation type (1=DELETE, 2=INSERT, 4=UPDATE) |
| `@{table}_{field}` | any | Data field value, prefixed with table name (e.g., `@orders_order_id`) |
| `@{table}_id` | any | The `id` field value (if present in Data), prefixed with table name |

### Jobs vs Sinks

**Jobs** (optional):
- Execute SQL against MSSQL in parallel
- Each job produces a DataSet that is written to SQLite
- Jobs have their own `output` table name
- Jobs run for ALL operation types (not filtered by INSERT/UPDATE/DELETE)

**Sinks** (required):
- Execute SQL against MSSQL based on operation type
- Each sink produces a DataSet that is written to SQLite
- Sink `output` specifies the target table
- Sinks are filtered by `on` table and operation type

### Execution Flow

```
CDC change (e.g., INSERT on orders)
    ↓
[Job 1] SQL → DataSet → SQLite (table: job1_output)
[Job 2] SQL → DataSet → SQLite (table: job2_output)
    ↓
[Sink] SQL (filtered by INSERT) → DataSet → SQLite (table: sink_output)
    ↓
All writes happen in one transaction
```

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

# Optional parallel SQL jobs (executed before sinks, for all operation types)
jobs:
  - name: job_name          # Job identifier
    sql_file: job.sql        # Path to external SQL file
    sql: |                     # Or inline SQL
      SELECT ... FROM table WHERE id = @table_id;
    output: job_output_table   # Target table in SQLite

# Sink configurations (one for each operation type)
sinks:
  insert:
    - name: sink_name
      on: table_name         # Optional: filter by source table
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table      # SQLite table name
      primary_key: id

  update:
    - name: sink_name
      on: table_name
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table
      primary_key: id

  delete:
    - name: sink_name
      on: table_name
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
      output: target_table
      primary_key: id
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Plugin name |
| `description` | No | Plugin description |
| `on` | Yes | List of tables to monitor |
| `jobs` | No | Parallel SQL jobs (executed for all operations) |
| `jobs[].name` | Yes (if jobs used) | Job name (identifier) |
| `jobs[].sql` | Yes (if jobs used) | Inline SQL template |
| `jobs[].sql_file` | No | Path to external SQL file |
| `jobs[].output` | Yes (if jobs used) | Target SQLite table name |
| `sinks` | Yes | Sink configurations keyed by operation type |
| `sinks[].on` | No | Filter sink to only process changes from specified table |
| `sinks[].sql` | Yes | SQL template (SELECT only) |
| `sinks[].sql_file` | No | Path to external SQL file |
| `sinks[].output` | Yes | Target SQLite table name |
| `sinks[].primary_key` | Yes | Primary key column name in target table |

---

## Operations

### INSERT

CDC INSERT operation triggers the `insert` sinks.

### UPDATE

CDC UPDATE (after-image, operation=4) triggers the `update` sinks.

### DELETE

CDC DELETE operation triggers the `delete` sinks.

---

## DELETE Sink Scenarios

### Scenario A: Direct Mapping (No SQL FROM Required)

When target table primary key matches source table field name:

```yaml
delete:
  - name: sync
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
    output: order_sync
    primary_key: order_id
```

### Scenario B: Target Database Lookup Required

When target table primary key or structure differs from source:

```yaml
delete:
  - name: sync
    sql: |
      SELECT @orders_order_id as target_id FROM order_sync WHERE source_order_id = @orders_order_id;
    output: order_sync
    primary_key: id
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
        WHERE o.order_id = @orders_order_id;
      output: order_sync
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
        WHERE o.order_id = @orders_order_id;
      output: order_sync
      primary_key: order_id

  delete:
    - name: sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
      output: order_sync
      primary_key: order_id
```

---

## Example: 1:N:1 with Jobs (Single Table CDC, Join N Tables, Single Output)

### Scenario

Monitor `orders` table. Use jobs to pre-fetch related data. Join with customers and products in sink SQL.

### Directory Structure

```
sql_plugins/
└── enrich_orders/
    └── skill.yml
```

### skill.yml

```yaml
name: enrich_orders
description: "orders CDC with customer and product enrichment"

on:
  - orders

jobs:
  - name: fetch_customers
    sql: |
      SELECT 
        @orders_customer_id as customer_id,
        c.name as customer_name,
        c.email as customer_email,
        c.segment as customer_segment
      FROM customers c
      WHERE c.customer_id = @orders_customer_id;
    output: customers_cache

  - name: fetch_products
    sql: |
      SELECT 
        @orders_product_id as product_id,
        p.product_name,
        p.category as product_category,
        p.price as product_price
      FROM products p
      WHERE p.product_id = @orders_product_id;
    output: products_cache

sinks:
  insert:
    - name: sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_order_id as order_id,
          @orders_customer_id as customer_id,
          c.name as customer_name,
          c.email as customer_email,
          c.segment as customer_segment,
          p.product_name,
          p.category as product_category,
          p.price as product_price
        FROM customers c
        LEFT JOIN products p ON p.product_id = @orders_product_id
        WHERE c.customer_id = @orders_customer_id;
      output: enriched_orders
      primary_key: order_id

  update:
    - name: sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_order_id as order_id,
          @orders_customer_id as customer_id,
          c.name as customer_name,
          c.email as customer_email,
          c.segment as customer_segment,
          p.product_name,
          p.category as product_category,
          p.price as product_price
        FROM customers c
        LEFT JOIN products p ON p.product_id = @orders_product_id
        WHERE c.customer_id = @orders_customer_id;
      output: enriched_orders
      primary_key: order_id

  delete:
    - name: sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
      output: enriched_orders
      primary_key: order_id
```

### Execution Flow

```
CDC [order_id: 123, customer_id: 456, product_id: 789]
    ↓
Job: fetch_customers → customers_cache table
Job: fetch_products → products_cache table
    ↓
INSERT sink → enriched_orders table
    ↓
All tables written in one transaction
```

---

## Example: 1:N:N (Single Table CDC, Fan-out to Multiple Tables)

### Scenario

Monitor `orders` table, output to two separate tables: `customers_sync` and `products_sync`.

### skill.yml

```yaml
name: sync_orders_split
description: "orders CDC fanout to customers and products tables"

on:
  - orders

jobs:
  - name: log_order_event
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @cdc_tx_id as cdc_tx_id,
        @cdc_table as cdc_table,
        @cdc_operation as cdc_operation,
        @orders_order_id as order_id,
        @orders_customer_id as customer_id,
        @orders_product_id as product_id,
        GETDATE() as logged_at
    output: order_events

sinks:
  insert:
    - name: customers_sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_customer_id as customer_id,
          c.name as customer_name,
          c.email as customer_email,
          c.segment as customer_segment
        FROM customers c
        WHERE c.customer_id = @orders_customer_id;
      output: customers_sync
      primary_key: customer_id

    - name: products_sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_product_id as product_id,
          p.product_name,
          p.category as product_category,
          p.price as product_price
        FROM products p
        WHERE p.product_id = @orders_product_id;
      output: products_sync
      primary_key: product_id

  update:
    - name: customers_sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_customer_id as customer_id,
          c.name as customer_name,
          c.email as customer_email,
          c.segment as customer_segment
        FROM customers c
        WHERE c.customer_id = @orders_customer_id;
      output: customers_sync
      primary_key: customer_id

    - name: products_sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_product_id as product_id,
          p.product_name,
          p.category as product_category,
          p.price as product_price
        FROM products p
        WHERE p.product_id = @orders_product_id;
      output: products_sync
      primary_key: product_id

  delete:
    - name: customers_sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_customer_id as customer_id
      output: customers_sync
      primary_key: customer_id

    - name: products_sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_product_id as product_id
      output: products_sync
      primary_key: product_id
```

---

## Example: N:N:N (Multiple Tables CDC, Join N Tables, Multiple Outputs)

### Scenario

Monitor `orders` and `order_items` tables (same transaction), output to `orders_enriched` and `order_items_enriched` separately.

### skill.yml

```yaml
name: sync_order_flow
description: "orders + order_items CDC with enrichment, fanout to separate tables"

on:
  - orders
  - order_items

jobs:
  - name: log_cdc_event
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @cdc_tx_id as cdc_tx_id,
        @cdc_table as cdc_table,
        @cdc_operation as cdc_operation,
        @orders_order_id as order_id,
        GETDATE() as logged_at
    output: cdc_events

sinks:
  insert:
    - name: orders_sync
      on: orders
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_order_id as order_id,
          @orders_customer_id as customer_id,
          @orders_amount as amount,
          c.name as customer_name,
          c.email as customer_email,
          c.segment as customer_segment
        FROM customers c
        WHERE c.customer_id = @orders_customer_id;
      output: orders_enriched
      primary_key: order_id

    - name: order_items_sync
      on: order_items
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_order_id as order_id,
          @order_items_id as id,
          @order_items_product_id as product_id,
          @order_items_quantity as quantity,
          p.product_name,
          p.category as product_category,
          p.price as product_price
        FROM products p
        WHERE p.product_id = @order_items_product_id;
      output: order_items_enriched
      primary_key: id

  update:
    - name: orders_sync
      on: orders
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_order_id as order_id,
          @orders_customer_id as customer_id,
          @orders_amount as amount,
          c.name as customer_name,
          c.email as customer_email,
          c.segment as customer_segment
        FROM customers c
        WHERE c.customer_id = @orders_customer_id;
      output: orders_enriched
      primary_key: order_id

    - name: order_items_sync
      on: order_items
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @cdc_tx_id as cdc_transaction_id,
          @cdc_table as cdc_source_table,
          @cdc_operation as cdc_operation,
          @orders_order_id as order_id,
          @order_items_id as id,
          @order_items_product_id as product_id,
          @order_items_quantity as quantity,
          p.product_name,
          p.category as product_category,
          p.price as product_price
        FROM products p
        WHERE p.product_id = @order_items_product_id;
      output: order_items_enriched
      primary_key: id

  delete:
    - name: orders_sync
      on: orders
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
      output: orders_enriched
      primary_key: order_id

    - name: order_items_sync
      on: order_items
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @order_items_id as id
      output: order_items_enriched
      primary_key: id
```

---

## Future Enhancements

- Multiple sinks per operation type (fan-out)
- Filter expressions for row-level routing
- Support for CDC UPDATE_BEFORE (for audit use cases)
