# SQL Plugin Architecture

**Version**: 1.3  
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
│  2. Stage Executor (optional)                │
│     Multi-step SQL → DataSet in memory      │
│     Each stage receives previous result      │
│                                              │
│  3. Sink Executor                            │
│     Final SQL → DataSet                      │
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
  2. Execute stages (if any) in memory
  3. Execute sink SQL once with those params
  
Result: Sink executed 4 times, each with independent parameters
```

**Key principle**: Each change gets its own independent execution context. Parameters are NOT shared between changes.

### CDC Parameters

When executing SQL, CDC captured data is injected as parameters. **Parameters are built fresh for each change (row)**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `@cdc_lsn` | string | LSN of this specific change |
| `@cdc_tx_id` | string | Transaction ID |
| `@cdc_table` | string | Source table name |
| `@cdc_operation` | int | Operation type (1=DELETE, 2=INSERT, 4=UPDATE) |
| `@{table}_{field}` | any | Data field value, prefixed with table name (e.g., `@orders_order_id`) |
| `@{table}_id` | any | The `id` field value (if present in Data), prefixed with table name |

**Per-Change Execution**: Each change is processed independently. If a transaction has 3 row changes, the sink SQL executes 3 times, each with its own fresh parameter set.

### In-Memory Stage Pipeline

When `stages` are defined, they form a pipeline:

```
CDC change
    ↓
Stage 1 SQL → DataSet (in memory)
    ↓
Stage 2 SQL → DataSet (in memory)  [previous DataSet fields injected as @stage_* params]
    ↓
...
    ↓
Sink SQL → DataSet → SQLite
```

**No temp tables in MSSQL.** Data flows through stages as DataSet objects in Go memory. Each stage receives the previous stage's DataSet as parameters, allowing subsequent stages to reference computed fields.

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

# Optional multi-step SQL stages (executed in order)
stages:
  - name: stage_name          # Used for injecting results to next stage
    sql_file: stage.sql        # Path to external SQL file
    sql: |                     # Or inline SQL
      SELECT ...;

# Sink configurations (one for each operation type)
sinks:
  insert:
    - name: sink_name
      on: table_name         # Optional: filter by source table (required for multi-table plugins)
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table      # SQLite table name
      primary_key: id

  update:
    - name: sink_name
      on: table_name         # Optional: filter by source table
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table      # SQLite table name
      primary_key: id

  delete:
    - name: sink_name
      on: table_name         # Optional: filter by source table
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
      output: target_table      # SQLite table name
      primary_key: id
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Plugin name |
| `description` | No | Plugin description |
| `on` | Yes | List of tables to monitor |
| `stages` | No | Multi-step SQL pipeline |
| `stages[].name` | Yes (if stages used) | Stage name, used to inject results to next stage |
| `stages[].sql` | Yes (if stages used) | Inline SQL template |
| `stages[].sql_file` | No | Path to external SQL file (alternative to inline sql) |
| `sinks` | Yes | Sink configurations keyed by operation type |
| `sinks[].on` | No | Filter sink to only process changes from specified table. Required when `on` contains multiple tables |
| `sinks[].sql` | Yes | SQL template (SELECT only) |
| `sinks[].sql_file` | No | Path to external SQL file (alternative to inline sql) |
| `sinks[].output` | Yes | Target SQLite table name |
| `sinks[].primary_key` | Yes | Primary key column name in target table |

---

## Operations

### INSERT

CDC INSERT operation triggers the `insert` sink.

- SQL executes against **source database**
- Target behavior: **Upsert** (INSERT if not exists, UPDATE if exists)

### UPDATE

CDC UPDATE (after-image, operation=4) triggers the `update` sink.

- SQL executes against **source database**
- Target behavior: **Upsert**

### DELETE

CDC DELETE operation triggers the `delete` sink.

- SQL may or may not query database (see scenarios below)
- Target behavior: **DELETE**

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
      -- Query target database to find mapped primary key
      SELECT @orders_order_id as target_id FROM order_sync WHERE source_order_id = @orders_order_id;
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

### 2. Per-Change Processing

Each change (row) is processed **individually**. For each change:
1. Build fresh parameters from this change's data
2. Execute stages in memory (if defined), passing previous DataSet to next
3. Execute sink SQL once with the final parameters
4. Collect SinkOp for later batch write

**Change 1 - INSERT orders(order_id=100):**

```sql
-- @cdc_lsn = '0x0001A2B1'
-- @cdc_tx_id = 'T1'
-- @cdc_table = 'orders'
-- @cdc_operation = 2
-- @orders_order_id = 100
-- @orders_amount = 500

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
WHERE o.order_id = 100;
```

**Change 2 - INSERT orders(order_id=101):**

```sql
-- @cdc_lsn = '0x0001A2B2'
-- @cdc_tx_id = 'T1'
-- @cdc_table = 'orders'
-- @cdc_operation = 2
-- @orders_order_id = 101
-- @orders_amount = 300

SELECT ... WHERE o.order_id = 101;
```

**Change 3 - UPDATE orders(order_id=200):**

```sql
-- @cdc_lsn = '0x0001A2C0'
-- @cdc_tx_id = 'T1'
-- @cdc_table = 'orders'
-- @cdc_operation = 4
-- @orders_order_id = 200

SELECT ... WHERE o.order_id = 200;
```

**Change 4 - DELETE orders(order_id=300):**

```sql
-- @cdc_lsn = '0x0001A2D0'
-- @cdc_tx_id = 'T1'
-- @cdc_table = 'orders'
-- @cdc_operation = 1
-- @orders_order_id = 300

SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
```

### 3. Result Collection

Each SQL execution returns a DataSet:

```
Change 1 DataSet:
  [{order_id: 100, amount: 500, status: 'pending', ...}]

Change 2 DataSet:
  [{order_id: 101, amount: 300, status: 'pending', ...}]

Change 3 DataSet:
  [{order_id: 200, status: 'completed', ...}]

Change 4 DataSet:
  [{cdc_lsn: '0x0001A2D0', order_id: 300}]
```

### 4. Sink Write (Transaction Boundary)

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
        WHERE o.order_id = @orders_order_id;
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
        WHERE o.order_id = @orders_order_id;
      output: order_sync        # SQLite table name
      primary_key: order_id

  delete:
    - name: sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
      output: order_sync        # SQLite table name
      primary_key: order_id
```

---

## Example: 1:N:1 (Single Table CDC, Join N Tables, Single Output)

### Scenario

Monitor `orders` table, JOIN `customers` and `products` tables to enrich data, output to single table `enriched_orders`.

### Directory Structure

```
sql_plugins/
└── enrich_orders/
    ├── skill.yml
    ├── step1_join_customers.sql
    └── step2_join_products.sql
```

### skill.yml

```yaml
name: enrich_orders
description: "orders CDC with customer and product enrichment"

on:
  - orders

stages:
  - name: join_customers
    sql_file: step1_join_customers.sql
    
  - name: join_products
    sql_file: step2_join_products.sql

sinks:
  insert:
    - name: sync
      sql: |
        SELECT * FROM join_products;
      output: enriched_orders
      primary_key: order_id

  update:
    - name: sync
      sql: |
        SELECT * FROM join_products;
      output: enriched_orders
      primary_key: order_id

  delete:
    - name: sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
      output: enriched_orders
      primary_key: order_id
```

### step1_join_customers.sql

```sql
-- Enrich with customer data using CTE
-- Previous stage results are available as @stage_<name>_<field> params
WITH base AS (
  SELECT 
    @cdc_lsn as cdc_lsn,
    @cdc_tx_id as cdc_transaction_id,
    @cdc_table as cdc_source_table,
    @cdc_operation as cdc_operation,
    @orders_order_id as order_id,
    @orders_customer_id as customer_id
)
SELECT 
  b.cdc_lsn,
  b.cdc_transaction_id,
  b.cdc_source_table,
  b.cdc_operation,
  b.order_id,
  b.customer_id,
  c.name as customer_name,
  c.email as customer_email,
  c.segment as customer_segment
FROM base b
LEFT JOIN customers c ON b.customer_id = c.id;
```

### step2_join_products.sql

```sql
-- Continue with products enrichment
-- Stage results from join_customers are injected as @join_customers_<field>
WITH base AS (
  SELECT 
    @cdc_lsn as cdc_lsn,
    @cdc_tx_id as cdc_transaction_id,
    @cdc_table as cdc_source_table,
    @cdc_operation as cdc_operation,
    @orders_order_id as order_id,
    @orders_customer_id as customer_id,
    -- Fields from previous stage
    @join_customers_customer_name as customer_name,
    @join_customers_customer_email as customer_email,
    @join_customers_customer_segment as customer_segment
)
SELECT 
  b.cdc_lsn,
  b.cdc_transaction_id,
  b.cdc_source_table,
  b.cdc_operation,
  b.order_id,
  b.customer_id,
  b.customer_name,
  b.customer_email,
  b.customer_segment,
  p.product_name,
  p.category as product_category,
  p.price as product_price
FROM base b
LEFT JOIN products p ON b.order_id = p.order_id;
```

### Execution Flow

```
CDC [order_id: 123, customer_id: 456, product_id: 789]
    ↓
Stage 1 (join_customers): CTE with CDC params + JOIN customers → DataSet
    ↓
Stage 2 (join_products): CTE with CDC params + previous stage params + JOIN products → DataSet
    ↓
Sink: SELECT * FROM join_products → enriched_orders table
```

### Stage Parameter Injection

When a stage has a `name`, its resulting DataSet fields are injected into the next stage as `@<stage_name>_<field>` parameters. This allows subsequent stages to reference computed fields from previous stages.

For the example above, after Stage 1 executes, Stage 2 receives these additional parameters:
- `@join_customers_order_id`
- `@join_customers_customer_name`
- `@join_customers_customer_email`
- `@join_customers_customer_segment`

---

## Example: 1:N:N (Single Table CDC, Join N Tables, Multiple Outputs)

### Scenario

Monitor `orders` table, JOIN `customers` and `products` tables, split into two outputs: `customers_sync` and `products_sync`.

### Directory Structure

```
sql_plugins/
└── sync_orders_split/
    ├── skill.yml
    ├── enrich.sql
    └── fanout.sql
```

### skill.yml

```yaml
name: sync_orders_split
description: "orders CDC enrich and fanout to two tables"

on:
  - orders

stages:
  - name: enrich
    sql_file: enrich.sql

  - name: fanout
    sql_file: fanout.sql

sinks:
  insert:
    - name: customers_sync
      sql: |
        SELECT * FROM fanout WHERE record_type = 'customer';
      output: customers_sync
      primary_key: customer_id

    - name: products_sync
      sql: |
        SELECT * FROM fanout WHERE record_type = 'product';
      output: products_sync
      primary_key: product_id

  update:
    - name: customers_sync
      sql: |
        SELECT * FROM fanout WHERE record_type = 'customer';
      output: customers_sync
      primary_key: customer_id

    - name: products_sync
      sql: |
        SELECT * FROM fanout WHERE record_type = 'product';
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

### enrich.sql

```sql
-- Enrichment: JOIN customers and products using CTE
WITH base AS (
  SELECT 
    @cdc_lsn as cdc_lsn,
    @cdc_tx_id as cdc_transaction_id,
    @cdc_table as cdc_source_table,
    @cdc_operation as cdc_operation,
    @orders_order_id as order_id,
    @orders_customer_id as customer_id,
    @orders_product_id as product_id,
    @orders_amount as order_amount
)
SELECT 
  b.cdc_lsn,
  b.cdc_transaction_id,
  b.cdc_source_table,
  b.cdc_operation,
  b.order_id,
  b.customer_id,
  b.product_id,
  b.order_amount,
  -- Customer fields
  c.name as customer_name,
  c.email as customer_email,
  c.segment as customer_segment,
  -- Product fields
  p.product_name,
  p.category as product_category,
  p.price as product_price
FROM base b
LEFT JOIN customers c ON b.customer_id = c.id
LEFT JOIN products p ON b.product_id = p.id;
```

### fanout.sql

```sql
-- Fanout: Route to appropriate record type using CASE expressions
-- Previous stage results are injected as @enrich_<field>
WITH base AS (
  SELECT 
    @cdc_lsn as cdc_lsn,
    @cdc_tx_id as cdc_transaction_id,
    @cdc_table as cdc_source_table,
    @cdc_operation as cdc_operation,
    @orders_order_id as order_id,
    @orders_customer_id as customer_id,
    @orders_product_id as product_id,
    -- Fields from enrich stage
    @enrich_customer_name as customer_name,
    @enrich_customer_email as customer_email,
    @enrich_customer_segment as customer_segment,
    @enrich_product_name as product_name,
    @enrich_product_category as product_category,
    @enrich_product_price as product_price,
    @enrich_order_amount as order_amount
)
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  'customer' as record_type,
  customer_id,
  customer_name,
  customer_email,
  customer_segment,
  NULL as product_id,
  NULL as product_name,
  NULL as product_category,
  NULL as product_price,
  NULL as order_amount
FROM base WHERE customer_id IS NOT NULL

UNION ALL

SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  'product' as record_type,
  NULL as customer_id,
  NULL as customer_name,
  NULL as customer_email,
  NULL as customer_segment,
  product_id,
  product_name,
  product_category,
  product_price,
  order_amount
FROM base WHERE product_id IS NOT NULL;
```

### Execution Flow

```
CDC [order_id: 123, customer_id: 456, product_id: 789]
    ↓
Stage 1 (enrich): CTE with CDC params + JOIN → DataSet
    ↓
Stage 2 (fanout): CTE with CDC params + enrich stage params + UNION ALL → DataSet
    ↓
Sink:
  - SELECT * FROM fanout WHERE record_type = 'customer' → customers_sync table
  - SELECT * FROM fanout WHERE record_type = 'product' → products_sync table
```

---

## Example: N:N:N (Multiple Tables CDC, Join N Tables, Multiple Outputs)

### Scenario

Monitor `orders` and `order_items` tables (same transaction), JOIN `customers` and `products` tables, output to `orders_enriched` and `order_items_enriched` separately.

### Directory Structure

```
sql_plugins/
└── sync_order_flow/
    ├── skill.yml
    ├── join_all.sql
    └── fanout.sql
```

### skill.yml

```yaml
name: sync_order_flow
description: "orders + order_items CDC with enrichment, fanout to separate tables"

on:
  - orders
  - order_items

stages:
  - name: join
    sql_file: join_all.sql

  - name: fanout
    sql_file: fanout.sql

sinks:
  insert:
    - name: orders_sync
      on: orders               # Only process orders table CDC changes
      sql: |
        SELECT * FROM fanout WHERE record_type = 'orders';
      output: orders_enriched
      primary_key: order_id

    - name: order_items_sync
      on: order_items         # Only process order_items table CDC changes
      sql: |
        SELECT * FROM fanout WHERE record_type = 'order_items';
      output: order_items_enriched
      primary_key: id

  update:
    - name: orders_sync
      on: orders
      sql: |
        SELECT * FROM fanout WHERE record_type = 'orders';
      output: orders_enriched
      primary_key: order_id

    - name: order_items_sync
      on: order_items
      sql: |
        SELECT * FROM fanout WHERE record_type = 'order_items';
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
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id
      output: order_items_enriched
      primary_key: id
```

### join_all.sql

```sql
-- Handle CDC change with enrichment
-- All CDC fields are available as @table_field parameters
WITH base AS (
  SELECT 
    @cdc_lsn as cdc_lsn,
    @cdc_tx_id as cdc_transaction_id,
    @cdc_table as cdc_source_table,
    @cdc_operation as cdc_operation,
    @orders_order_id as order_id,
    @orders_customer_id as customer_id,
    @orders_amount as amount,
    @order_items_product_id as product_id,
    @order_items_quantity as quantity
)
SELECT 
  b.cdc_lsn,
  b.cdc_transaction_id,
  b.cdc_source_table,
  b.cdc_operation,
  b.order_id,
  -- orders fields
  b.amount,
  -- order_items fields
  b.product_id,
  b.quantity,
  -- Joined customer data
  c.name as customer_name,
  c.email as customer_email,
  c.segment as customer_segment,
  -- Joined product data
  p.product_name,
  p.category as product_category,
  p.price as product_price,
  -- Computed amount
  CASE 
    WHEN b.cdc_source_table = 'order_items' THEN p.price * b.quantity
    ELSE b.amount
  END as final_amount
FROM base b
LEFT JOIN customers c ON b.customer_id = c.id
LEFT JOIN products p ON b.product_id = p.id;
```

### fanout.sql

```sql
-- Fanout: Route to appropriate table based on cdc_source_table
-- Previous stage results injected as @join_<field>
WITH base AS (
  SELECT 
    @cdc_lsn as cdc_lsn,
    @cdc_tx_id as cdc_transaction_id,
    @cdc_table as cdc_source_table,
    @cdc_operation as cdc_operation,
    @orders_order_id as order_id,
    -- Fields from join stage
    @join_amount as amount,
    @join_quantity as quantity,
    @join_customer_name as customer_name,
    @join_customer_email as customer_email,
    @join_customer_segment as customer_segment,
    @join_product_name as product_name,
    @join_product_category as product_category,
    @join_product_price as product_price,
    @join_final_amount as final_amount
)
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  'orders' as record_type,
  order_id,
  amount as order_amount,
  customer_name,
  customer_email,
  customer_segment,
  NULL as product_id,
  NULL as product_name,
  NULL as product_category,
  NULL as product_price,
  NULL as quantity,
  final_amount
FROM base WHERE cdc_source_table = 'orders'

UNION ALL

SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  'order_items' as record_type,
  order_id,
  NULL as order_amount,
  NULL as customer_name,
  NULL as customer_email,
  NULL as customer_segment,
  @order_items_product_id as product_id,
  product_name,
  product_category,
  product_price,
  quantity,
  final_amount
FROM base WHERE cdc_source_table = 'order_items';
```

### Execution Flow

```
Transaction T1 CDC batch:
  orders: {order_id: 123, customer_id: 456, amount: 5000}
  order_items: {order_id: 123, product_id: 789, quantity: 2}
    ↓
For each change:
    ↓
Stage 1 (join): CTE with CDC params + JOIN customers/products → DataSet
    ↓
Stage 2 (fanout): CTE with CDC params + join stage params + UNION ALL → DataSet
    ↓
Sinks (filtered by `on`):
  - orders_sync → SELECT WHERE record_type = 'orders' → orders_enriched table
  - order_items_sync → SELECT WHERE record_type = 'order_items' → order_items_enriched table
```

---

## Future Enhancements

- Multiple sinks per operation type (fan-out)
- Filter expressions for row-level routing
- Support for CDC UPDATE_BEFORE (for audit use cases)
