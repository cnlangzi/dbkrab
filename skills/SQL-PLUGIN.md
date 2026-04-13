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

Sinks are configured as a flat list with a `when` field:

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
    on: table_name              # Optional: table filter (for multi-table)
    sql: |
      SELECT ... FROM source_table WHERE id = @table_id;
    sql_file: path/to.sql       # Optional: external SQL file
    output: target_table
    primary_key: id
    on_conflict: skip         # Optional: skip|overwrite|error

  - name: delete_job
    when: [delete]
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id;
    output: target_table
    primary_key: id
```

### Sink Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Sink identifier |
| `when` | Yes | Operation filter: `[insert, update]` or `[delete]` |
| `on` | No | Table filter (for multi-table CDC) |
| `if` | No | SQL-style condition expression for conditional sink triggering |
| `sql` | Yes* | SQL template (*or sql_file) |
| `sql_file` | Yes* | External SQL file (*or sql) |
| `output` | Yes | Target table name |
| `primary_key` | Yes | Primary key column |
| `on_conflict` | No | Conflict strategy: skip\|overwrite\|error (default: skip) |

---

## Skill Outputs Metadata

When a skill is loaded, the `Skill.Outputs` map is automatically populated by parsing the SQL from all insert/update sinks. This metadata provides a static map of output table names to their field lists, enabling other components to rely on this information without re-parsing SQL at runtime.

### Outputs Structure

```go
type Skill struct {
    // ... other fields ...
    Outputs map[string][]string `yaml:"-"` // key = output table name, value = field names
}
```

### Field Extraction Rules

The loader parses each sink's SQL (inline or from `sql_file`) and extracts column names following these rules:

| SQL Pattern | Extracted Field Name |
|-------------|----------------------|
| `SELECT @cdc_lsn as cdc_lsn` | `cdc_lsn` (uses AS alias) |
| `SELECT o.order_id` | `order_id` (strips table prefix) |
| `SELECT @variable` | `variable` (removes @ prefix) |

### Example

For a skill with:

```yaml
sinks:
  - name: customers_sync
    when: [insert, update]
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @orders_customer_id as customer_id,
        c.name as customer_name,
        c.email as customer_email
      FROM customers c
      WHERE c.customer_id = @orders_customer_id;
    output: customers_sync
    primary_key: customer_id

  - name: customers_delete
    when: [delete]
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_customer_id as customer_id
    output: customers_sync
    primary_key: customer_id
```

After loading, `Skill.Outputs` will contain:

```go
skill.Outputs = map[string][]string{
    "customers_sync": {"cdc_lsn", "customer_id", "customer_name", "customer_email"},
}
```

Note: Only insert/update sinks are analyzed. Delete-only sinks (`when: [delete]`) are ignored for Outputs extraction.

### Deterministic Field Ordering

Fields within each output are ordered by first-seen order in SQL (top to bottom, left to right). This ensures predictable ordering for testing and debugging.

### YAML Serialization

The `Outputs` field is marked with `yaml:"-"`, so it is never serialized to disk. It exists only as an in-memory field populated at load time.


## Conditional Sink Triggering (`if` field)

The optional `if` field allows sinks to be triggered conditionally based on CDC payload values. It uses SQL/WHERE-like syntax that is evaluated at runtime against each CDC event.

### Syntax Rules

| Feature | Syntax | Example |
|---------|--------|---------|
| Equality | `=` | `status = 'vip'` |
| Inequality | `!=` | `status != 'cancelled'` |
| Greater than | `>` | `amount > 100` |
| Less than | `<` | `amount < 100` |
| Greater or equal | `>=` | `amount >= 100` |
| Less or equal | `<=` | `amount <= 100` |
| Logical AND | `AND` | `status = 'vip' AND amount > 100` |
| Logical OR | `OR` | `status = 'vip' OR status = 'gold'` |
| IN operator | `IN` | `status IN ('vip', 'gold', 'silver')` |
| Negation | `!=` or NOT | `status != 'cancelled'` |
| Parentheses | `(...)` | `(status = 'vip' OR status = 'gold') AND amount > 100` |

### Case Sensitivity

- **Table and field names**: Case-insensitive. `orders.status`, `ORDERS.STATUS`, and `Orders.Status` all map to the same field.
- **Literal values**: Case-sensitive. `status = 'vip'` only matches `vip`, not `VIP` or `Vip`.

### Field References

Fields can be referenced in two ways:

1. **Direct field name** (without table prefix):
   ```yaml
   if: "status = 'vip' AND amount > 100"
   ```
   This references fields from the CDC record being processed.

2. **Table.field format** (for disambiguation in complex expressions):
   ```yaml
   if: "orders.status = 'vip' AND orders.amount > orders.min_amount"
   ```
   This is converted internally to `orders_status` and `orders_amount`.

### CDC Metadata

You can also reference CDC metadata in expressions:

| Field | Description | Example |
|-------|-------------|---------|
| `cdc_operation` | Operation type (1=delete, 2=insert, 4=update) | `cdc_operation = 2` |
| `cdc_table` | Source table name | `cdc_table = 'orders'` |
| `cdc_tx_id` | Transaction ID | `cdc_tx_id = 'tx-123'` |

### Examples

#### Simple condition with literal value

```yaml
sinks:
  - name: sync_vip
    when: [insert, update]
    if: "status = 'vip'"
    sql: |
      SELECT @orders_order_id as order_id, @orders_amount as amount
      FROM orders WHERE order_id = @orders_order_id;
    output: vip_orders
    primary_key: order_id
```

#### Field-to-field comparison

```yaml
sinks:
  - name: sync_high_amount
    when: [insert, update]
    if: "orders.amount > orders.min_amount"
    sql: |
      SELECT @orders_order_id as order_id, @orders_amount as amount
      FROM orders WHERE order_id = @orders_order_id;
    output: high_amount_orders
    primary_key: order_id
```

#### Complex expression with AND/OR

```yaml
sinks:
  - name: sync_premium
    when: [insert, update]
    if: "(status = 'vip' OR status = 'gold') AND amount > 100"
    sql: |
      SELECT @orders_order_id as order_id, @orders_amount as amount
      FROM orders WHERE order_id = @orders_order_id;
    output: premium_orders
    primary_key: order_id
```

#### IN operator

```yaml
sinks:
  - name: sync_preferred
    when: [insert, update]
    if: "status IN ('vip', 'gold', 'silver')"
    sql: |
      SELECT @orders_order_id as order_id, @orders_status as status
      FROM orders WHERE order_id = @orders_order_id;
    output: preferred_orders
    primary_key: order_id
```

### Backward Compatibility

Omitting the `if` field maintains backward compatibility - the sink will trigger for all matched events (as before).

---

## Operations

| __$operation | Sink Trigger |
|---------------|--------------|
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