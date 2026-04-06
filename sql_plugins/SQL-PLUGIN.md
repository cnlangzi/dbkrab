# SQL Plugin Architecture

**Version**: 1.2  
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
      on: table_name         # Optional: filter by source table (required for multi-table plugins)
      sql: |
        SELECT ... FROM source_table WHERE id IN (@table_ids);
      output: target_table      # SQLite table name
      primary_key: id

  update:
    - name: sink_name
      on: table_name         # Optional: filter by source table
      sql: |
        SELECT ... FROM source_table WHERE id IN (@table_ids);
      output: target_table      # SQLite table name
      primary_key: id

  delete:
    - name: sink_name
      on: table_name         # Optional: filter by source table
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
| `sinks[].on` | No | Filter sink to only process changes from specified table. Required when `on` contains multiple tables |
| `sinks[].sql` | Yes | SQL template (SELECT only) |
| `sinks[].sql_file` | No | Path to external SQL file (alternative to inline sql) |
| `sinks[].output` | Yes | Target SQLite table name |
| `sinks[].primary_key` | Yes | Primary key column name in target table |

### Multi-Table Note

When monitoring multiple tables via `on`, each sink should specify `on` to filter which table's CDC changes it handles. CDC parameters are table-specific:
- When `orders` changes: only `@order_id`, `@amount`, `@status` are available
- When `order_items` changes: only `@order_id`, `@product_id`, `@quantity` are available

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
    temp_table: customers_enriched
    
  - name: join_products
    sql_file: step2_join_products.sql

sinks:
  insert:
    - name: sync
      sql: |
        SELECT * FROM products_enriched;
      output: enriched_orders
      primary_key: order_id

  update:
    - name: sync
      sql: |
        SELECT * FROM products_enriched;
      output: enriched_orders
      primary_key: order_id

  delete:
    - name: sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @order_id as order_id
      output: enriched_orders
      primary_key: order_id
```

### step1_join_customers.sql

```sql
-- Enrich with customer data, store in temp table
SELECT 
  @cdc_lsn as cdc_lsn,
  @cdc_tx_id as cdc_transaction_id,
  @cdc_table as cdc_source_table,
  @cdc_operation as cdc_operation,
  @order_id as order_id,
  @customer_id as customer_id,
  c.name as customer_name,
  c.email as customer_email,
  c.segment as customer_segment
INTO #customers_enriched
FROM (SELECT @customer_id as customer_id, @order_id as order_id) o
LEFT JOIN customers c ON o.customer_id = c.id;
```

### step2_join_products.sql

```sql
-- Continue with products enrichment, final result to #products_enriched
WITH base AS (
  SELECT * FROM customers_enriched
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
INTO #products_enriched
FROM base b
LEFT JOIN products p ON b.order_id = p.order_id;
```

### Execution Flow

```
CDC [order_id: 123, customer_id: 456, product_id: 789]
    ↓
step1_join_customers.sql → #customers_enriched
    ↓
step2_join_products.sql → #products_enriched
    ↓
INSERT/UPDATE sink: SELECT * FROM products_enriched
    ↓
enriched_orders table
```

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
        SELECT * FROM customers_view;
      output: customers_sync
      primary_key: customer_id

    - name: products_sync
      sql: |
        SELECT * FROM products_view;
      output: products_sync
      primary_key: product_id

  update:
    - name: customers_sync
      sql: |
        SELECT * FROM customers_view;
      output: customers_sync
      primary_key: customer_id

    - name: products_sync
      sql: |
        SELECT * FROM products_view;
      output: products_sync
      primary_key: product_id

  delete:
    - name: customers_sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @customer_id as customer_id
      output: customers_sync
      primary_key: customer_id

    - name: products_sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @product_id as product_id
      output: products_sync
      primary_key: product_id
```

### enrich.sql

```sql
-- Enrichment: JOIN customers and products, result to #enriched
SELECT 
  @cdc_lsn as cdc_lsn,
  @cdc_tx_id as cdc_transaction_id,
  @cdc_table as cdc_source_table,
  @cdc_operation as cdc_operation,
  @order_id as order_id,
  @customer_id as customer_id,
  @product_id as product_id,
  @amount as order_amount,
  -- Customer fields
  c.name as customer_name,
  c.email as customer_email,
  c.segment as customer_segment,
  -- Product fields
  p.product_name,
  p.category as product_category,
  p.price as product_price
INTO #enriched
FROM (SELECT @customer_id as customer_id, @product_id as product_id, @amount as amount) o
LEFT JOIN customers c ON o.customer_id = c.id
LEFT JOIN products p ON o.product_id = p.id;
```

### fanout.sql

```sql
-- Fanout: Create two views from enriched result

-- View 1: customers_view
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  customer_id,
  customer_name,
  customer_email,
  customer_segment
FROM #enriched
WHERE customer_id IS NOT NULL;

-- View 2: products_view
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  product_id,
  product_name,
  product_category,
  product_price
FROM #enriched
WHERE product_id IS NOT NULL;
```

### Execution Flow

```
CDC [order_id: 123, customer_id: 456, product_id: 789]
    ↓
enrich.sql → #enriched
    ↓
fanout.sql → #customers_view + #products_view
    ↓
INSERT/UPDATE:
  - customers_sync sink → SELECT * FROM customers_view → customers_sync table
  - products_sync sink → SELECT * FROM products_view → products_sync table
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
        SELECT * FROM orders_view;
      output: orders_enriched
      primary_key: order_id

    - name: order_items_sync
      on: order_items         # Only process order_items table CDC changes
      sql: |
        SELECT * FROM order_items_view;
      output: order_items_enriched
      primary_key: id

  update:
    - name: orders_sync
      on: orders
      sql: |
        SELECT * FROM orders_view;
      output: orders_enriched
      primary_key: order_id

    - name: order_items_sync
      on: order_items
      sql: |
        SELECT * FROM order_items_view;
      output: order_items_enriched
      primary_key: id

  delete:
    - name: orders_sync
      on: orders
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @order_id as order_id
      output: orders_enriched
      primary_key: order_id

    - name: order_items_sync
      on: order_items
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @id as id
      output: order_items_enriched
      primary_key: id
```

### join_all.sql

```sql
-- Handle CDC batch with multiple tables
-- CDC batch data is written to #cdc_batch by Engine

SELECT 
  b.cdc_lsn,
  b.cdc_transaction_id,
  b.cdc_source_table,
  b.cdc_operation,
  b.order_id,
  -- orders fields (NULL for order_items)
  b.amount,
  -- order_items fields (NULL for orders)
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
INTO #enriched_flow
FROM #cdc_batch b
LEFT JOIN customers c ON b.customer_id = c.id
LEFT JOIN products p ON b.product_id = p.id;
```

### fanout.sql

```sql
-- Fanout: Route to appropriate table based on cdc_source_table

-- View 1: orders
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  order_id,
  amount as order_amount,
  customer_name,
  customer_email,
  customer_segment,
  final_amount
FROM #enriched_flow
WHERE cdc_source_table = 'orders';

-- View 2: order_items
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  order_id,
  quantity,
  product_name,
  product_category,
  product_price,
  final_amount
FROM #enriched_flow
WHERE cdc_source_table = 'order_items';
```

### CDC Batch Structure for N:N:N

```
#cdc_batch (written by Engine):

| cdc_lsn | cdc_tx_id | cdc_source_table | cdc_operation | order_id | customer_id | amount | product_id | quantity |
|---------|-----------|------------------|---------------|----------|-------------|--------|------------|----------|
| 0x001   | T1        | orders           | 2             | 123      | 456         | 5000   | NULL       | NULL    |
| 0x002   | T1        | order_items      | 2             | 123      | NULL        | NULL   | 789        | 2        |
```

### Execution Flow

```
Transaction T1 CDC batch:
  orders: {order_id: 123, customer_id: 456, amount: 5000}
  order_items: {order_id: 123, product_id: 789, quantity: 2}
    ↓
Engine writes to #cdc_batch
    ↓
join_all.sql → #enriched_flow
    ↓
fanout.sql:
  - orders_view → SELECT ... WHERE cdc_source_table = 'orders'
  - order_items_view → SELECT ... WHERE cdc_source_table = 'order_items'
    ↓
INSERT/UPDATE:
  - orders_sync sink → orders_enriched table
  - order_items_sync sink → order_items_enriched table
```

---

## Future Enhancements

- Support for external SQL files (`sql_file` field)
- Multiple sinks per operation type (fan-out)
- Filter expressions for row-level routing
- Support for CDC UPDATE_BEFORE (for audit use cases)
