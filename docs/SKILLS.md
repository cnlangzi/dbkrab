# Writing SQL Skills

SQL Skills define how CDC changes are processed and stored.

---

## Quick Example

```yaml
name: sync_orders
description: "Sync orders to SQLite"

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
```

---

## Skill Structure

```yaml
name: skill_name
description: "Optional description"
sqlite: ./data/sinks/business.db   # Optional: separate SQLite for this skill

on:
  - table_name              # Tables to monitor

jobs:                        # Optional: parallel SQL execution
  - name: job_name
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: job_output_table

sinks:                       # Required: output configuration
  insert:                    # Triggered on INSERT
    - name: sink_name
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table
      primary_key: id

  update:                    # Triggered on UPDATE
    - name: sink_name
      sql: |
        SELECT ... FROM source_table WHERE id = @table_id;
      output: target_table
      primary_key: id

  delete:                    # Triggered on DELETE
    - name: sink_name
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
      output: target_table
      primary_key: id
```

---

## CDC Parameters

Use these in your SQL:

| Parameter | Type | Description |
|-----------|------|-------------|
| `@cdc_lsn` | string | LSN of this change |
| `@cdc_tx_id` | string | Transaction ID |
| `@cdc_table` | string | Source table name |
| `@cdc_operation` | int | 1=DELETE, 2=INSERT, 4=UPDATE |
| `@orders_order_id` | any | Field value from CDC data |

---

## CDC Operation Types

| __$operation | Meaning |
|-------------|---------|
| 1 | DELETE |
| 2 | INSERT |
| 3 | UPDATE (before image - not used) |
| 4 | UPDATE (after image) |

---

## Examples

### Simple 1:1 Sync

Monitor `orders`, sync to `order_sync`:

```yaml
name: sync_orders
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

### With JOIN Enrichment

Join with related tables:

```yaml
name: enrich_orders
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

### Multiple Output Tables (Fan-out)

Output to separate tables:

```yaml
name: split_orders
on:
  - orders

sinks:
  insert:
    - name: customers_sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @orders_customer_id as customer_id,
          c.name,
          c.email
        FROM customers c
        WHERE c.customer_id = @orders_customer_id;
      output: customers_sync
      primary_key: customer_id

    - name: products_sync
      sql: |
        SELECT 
          @cdc_lsn as cdc_lsn,
          @orders_product_id as product_id,
          p.product_name,
          p.price
        FROM products p
        WHERE p.product_id = @orders_product_id;
      output: products_sync
      primary_key: product_id
```

---

## DELETE Handling

For DELETE, you often don't need a JOIN. Use direct mapping:

```yaml
delete:
  - name: sync
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: order_sync
    primary_key: order_id
```

This generates: `DELETE FROM order_sync WHERE order_id = @orders_order_id`

---

## Jobs (Pre-fetch)

Jobs run in parallel for every change (not filtered by operation):

```yaml
name: with_jobs
on:
  - orders

jobs:
  - name: log_event
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @orders_order_id as order_id,
        GETDATE() as created_at;
    output: event_log

sinks:
  insert:
    - name: sync
      sql: |
        SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
      output: order_sync
      primary_key: order_id
```

---

## Tips

1. **Use `IN (@id)` for single-value**: SQL Server requires `IN` even for single values
2. **Separate skill DBs**: Use `sqlite: ./data/sinks/myskill.db` for isolation
3. **Jobs are parallel**: All jobs run concurrently for each change
4. **Sinks are sequential**: Sinks run in order within a change

---

## File Location

Skills are YAML files in `./skills/`:

```
skills/
├── sync_orders.yml
├── enrich_orders.yml
└── ...
```
