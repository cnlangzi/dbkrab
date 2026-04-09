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
```

---

## Skill Structure

```yaml
name: skill_name
description: "Optional description"
database: business   # Database name (maps to platform-configured storage)

on:
  - table_name              # Tables to monitor

sinks:                       # Required: output configuration
  - name: sink_name         # Sink identifier
    when: [insert, update] # Required: [insert, update] or [delete]
    on: table_name        # Optional: table filter for multi-table CDC
    sql: |              # Inline SQL (or use sql_file)
      SELECT ... FROM source_table WHERE id = @table_id;
    sql_file: path/to.sql # Optional: external SQL file
    output: target_table  # Target table name
    primary_key: id    # Primary key column
    on_conflict: skip  # Optional: skip|overwrite|error (default: skip)
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
| `@{table}_{field}` | any | Field value from CDC data |

---

## CDC Operation Types

| __$operation | Meaning | Sink Trigger |
|-------------|----------|-------------|
| 1 | DELETE | `when: [delete]` |
| 2 | INSERT | `when: [insert, update]` |
| 4 | UPDATE (after image) | `when: [insert, update]` |

---

## Examples

### Simple 1:1 Sync

Monitor `orders`, sync to `order_sync`:

```yaml
name: sync_orders
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

### With JOIN Enrichment

Join with related tables:

```yaml
name: enrich_orders
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

### Multiple Output Tables (Fan-out)

Output to separate tables:

```yaml
name: split_orders
on:
  - orders

sinks:
  - name: customers_sync
    when: [insert, update]
    on: orders
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
    when: [insert, update]
    on: order_items
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @order_items_product_id as product_id,
        p.product_name,
        p.price
      FROM products p
      WHERE p.product_id = @order_items_product_id;
    output: products_sync
    primary_key: product_id
```

---

## DELETE Handling

For DELETE, you often don't need a JOIN. Use direct mapping:

```yaml
  - name: sync_delete
    when: [delete]
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: order_sync
    primary_key: order_id
```

---

## Sink Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Sink identifier |
| `when` | Yes | Operation filter: `[insert, update]` or `[delete]` |
| `on` | No | Table filter (for multi-table CDC) |
| `sql` | Yes* | Inline SQL template (*or sql_file) |
| `sql_file` | Yes* | External SQL file (*or sql) |
| `output` | Yes | Target table name |
| `primary_key` | Yes | Primary key column |
| `on_conflict` | No | Conflict strategy: skip\|overwrite\|error (default: skip) |

---

## Tips

1. **Use `IN (@id)` for single-value**: SQL Server requires `IN` even for single values
2. **Separate skill DBs**: Use `database: business` to route to platform-configured storage

---

## File Location

Skills are YAML files in `./skills/`:

```
skills/
├── sync_orders.yml
├── enrich_orders.yml
└── ...
```