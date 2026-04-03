# dbkrab

**Lightweight MSSQL Change Data Capture in Go**

> Capture database changes without the heavyweight infrastructure.

---

## Overview

dbkrab is a pure Go implementation of MSSQL CDC (Change Data Capture). It reads change data directly from SQL Server's CDC tables, providing a lightweight alternative to Debezium without requiring Java or Kafka.

## Features

- **Incremental Polling**: LSN-based incremental reading, no full table scans
- **Transaction Boundary**: Changes within the same transaction are grouped together
- **Batch + Stream (Initial Load + CDC)**: Full consistency via SNAPSHOT isolation
- **Multi-table Monitoring**: Concurrent polling with cross-table transaction correlation
- **Plugin Architecture**: Customizable Handler (business logic) and Sink (storage)
- **Minimal Dependencies**: Pure Go + MSSQL driver only

## Architecture

```
MSSQL CDC Tables
    │
    ▼
┌─────────────────────────────────────────┐
│           dbkrab Core                   │
│  • Minimum LSN Synchronization          │
│  • Transaction Grouping                 │
│  • Handler Callback                     │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│      Handler (Business Layer)           │
│  • Filter: Filter unwanted data         │
│  • Transform: Data transformation       │
│  • Enrich: Data enrichment              │
│  • Custom business logic                │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│      Sink (Storage Layer)               │
│  • KafkaSink → Kafka                    │
│  • FileSink → File                      │
│  • DBSink → Target Database             │
│  • ElasticSearchSink                   │
│  • Custom storage                       │
└─────────────────────────────────────────┘
```

## Core Concepts

### 1. LSN (Log Sequence Number)

MSSQL CDC uses LSN to track change positions. Each change record has a `__$start_lsn` field that uniquely identifies its position in the transaction log.

### 2. Minimum LSN Synchronization

To ensure cross-table transactions are captured in the same batch:

1. Query all monitored tables to get their current max LSN
2. Use the **minimum** LSN as the batch anchor
3. Poll all tables up to this minimum LSN
4. Group by `__$transaction_id` for delivery

This ensures all tables have data up to the same point.

### 3. Transaction Boundary

Same transaction across multiple tables shares the same `__$transaction_id`:

```sql
-- Transaction 12345:
--   INSERT into orders
--   INSERT into order_items
--
-- CDC tables record:
--   cdc.dbo_orders_CT:      __$transaction_id = 12345
--   cdc.dbo_order_items_CT: __$transaction_id = 12345
```

dbkrab groups changes by `__$transaction_id` and delivers them together.

### 4. Batch + Stream (Initial Load + CDC)

**Initial Load** (full snapshot):
```sql
-- 1. Get current LSN
DECLARE @fromLSN binary(10) = sys.fn_cdc_get_min_lsn('dbo_orders');

-- 2. Read in SNAPSHOT transaction (consistent view)
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
BEGIN TRANSACTION;
SELECT * FROM orders;
COMMIT;

-- 3. CDC continues from this LSN
```

**CDC Stream**: Poll CDC tables from the LSN where initial load ended.

## Usage

### Quick Start

```go
package main

import (
    "database/sql"
    _ "github.com/denisenkom/go-mssqldb"
    "github.com/cnlangzi/dbkrab"
)

func main() {
    db, _ := sql.Open("mssql", "server=localhost;user id=cdc_user;password=pwd;database=mydb")

    dbkrab.New().
        Tables([]string{"orders", "order_items"}).
        DB(db).
        Handler(MyHandler{}).
        Sink(dbkrab.KafkaSink{Broker: "localhost:9092", Topic: "cdc-events"}).
        Start()
}

type MyHandler struct{}

func (h *MyHandler) Handle(tx *dbkrab.Transaction) error {
    // Process transaction
    return nil
}
```

### Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| Tables | []string | required | Tables to monitor |
| DB | *sql.DB | required | MSSQL connection |
| Interval | time.Duration | 500ms | Polling interval |
| Handler | Handler | nil | Business logic handler |
| Sink | Sink | nil | Storage sink |
| OffsetFile | string | ./data/offset.json | LSN persistence path |

## Handler Interface

```go
type Handler interface {
    Handle(tx *Transaction) error
}

type Transaction struct {
    TransactionID string
    Changes       []Change
}

type Change struct {
    Table   string
    Op      Operation // 1=DELETE, 2=INSERT, 3=UPDATE(before), 4=UPDATE(after)
    Data    map[string]interface{}
}
```

## Sink Interface

```go
type Sink interface {
    Write(tx *Transaction) error
}

// Built-in sinks
type KafkaSink struct {
    Broker string
    Topic  string
}

type FileSink struct {
    Path string
}

type DBSink struct {
    DSN string
}
```

## MSSQL CDC Setup

```sql
-- Enable CDC on database
USE your_database;
EXEC sp_cdc_enable_db;

-- Enable CDC on table (requires db_owner)
EXEC sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'your_table',
    @role_name     = NULL;

-- Verify CDC is enabled
SELECT name, is_cdc_enabled FROM sys.tables;
```

## Change Operations

| __$operation | Meaning | Description |
|---------------|---------|--------------|
| 1 | DELETE | Record deleted |
| 2 | INSERT | Record inserted |
| 3 | UPDATE (before) | Record before update |
| 4 | UPDATE (after) | Record after update |

## Performance

- **CDC Query Overhead**: Low - reads from CDC tables, not main tables
- **Polling Interval**: 500ms default (configurable)
- **Multi-table**: Concurrent polling with transaction correlation
- **LSN Persistence**: File-based, survives restarts

## Comparison

| Feature | dbkrab | Debezium Server | Debezium Connect |
|---------|--------|-----------------|------------------|
| Dependencies | None | Java | Java + Kafka |
| API Control | No | No | Yes |
| Latency | ~500ms | Real-time | Real-time |
| Transaction Grouping | ✅ | ✅ | ✅ |
| Weight | Light | Medium | Heavy |

## License

MIT License - see LICENSE file.

---

**dbkrab** — Lightweight MSSQL CDC in Go 🦀