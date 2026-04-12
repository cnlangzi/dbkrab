# dbkrab Architecture

**Lightweight MSSQL Change Data Capture in Go**

---

## System Overview

```
MSSQL CDC Tables
    │
    ▼
┌─────────────────────────────────────────┐
│           dbkrab Core                   │
│  • Minimum LSN Synchronization          │
│  • Transaction Grouping                 │
│  • CDC Gap Detection                     │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│     SQL Plugin Engine (Hot-Reload)      │
│  • Skill Loader                         │
│  • Sink Executor (parallel SQL)          │
│  • Sink Executor (per-change SQL)       │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│      SQLite Sink (Per-Skill)            │
│  • Read/Write connection separation     │
│  • WAL mode                             │
│  • Transaction boundary                 │
└─────────────────────────────────────────┘
```

---

## Core Components

### 1. CDC Poller (`internal/core/`)

Reads changes from MSSQL CDC tables using LSN-based incremental polling.

- **LSN Tracking**: Persists last consumed LSN to SQLite
- **Transaction Grouping**: Groups changes by `__$transaction_id`
- **Gap Detection**: Monitors CDC cleanup window

### 2. SQL Plugin Engine (`plugin/sql/`)

Processes CDC changes using SQL as the domain-specific language.

- **Loader**: Reads skill YAML files from `./skills/`
- **Mapper**: Converts CDC data to SQL parameters
- **Executor**: Runs jobs and sinks in parallel
- **Writer**: Writes results to SQLite with upsert logic

### 3. Dashboard (`api/dashboard/`)

Go + HTMX dashboard for operations.

- Skills management, DLQ monitoring, CDC status, Gap detection

---

## Data Flow

```
CDC Change (single row)
    │
    └──► Sink SQL (by operation type) ──► SQLite
```

**Per-Change Processing**: Each CDC row triggers sink execution. Parameters are isolated per change.

---

## Directory Structure

```
dbkrab/
├── cmd/dbkrab/           # CLI entry point
├── internal/
│   ├── core/             # CDC poller, LSN handling
│   ├── cdc/              # CDC query functions
│   ├── config/           # YAML config loader + hot-reload
│   ├── offset/           # LSN persistence (SQLite)
│   ├── dlq/              # Dead letter queue
│   └── ...
├── plugin/sql/           # SQL plugin engine
│   ├── engine.go         # Main executor
│   ├── loader.go         # Skill YAML loader
│   ├── executor.go       # Sink executor
│   ├── writer.go         # SQLite writer (upsert)
│   └── pool.go          # SQLite connection pool
├── api/
│   ├── dashboard/        # HTMX dashboard
│   └── skills.go         # Skills CRUD API
├── skills/               # Skill definitions (YAML)
└── data/                 # Data directory
    ├── system/           # System DB (offset, DLQ)
    └── sinks/            # Per-skill SQLite databases
```

---

## Key Concepts

### CDC Parameters

CDC data is injected as SQL parameters:

| Parameter | Description |
|-----------|-------------|
| `@cdc_lsn` | LSN of this change |
| `@cdc_tx_id` | Transaction ID |
| `@cdc_table` | Source table name |
| `@cdc_operation` | Operation type (1=DELETE, 2=INSERT, 4=UPDATE) |
| `@{table}_{field}` | Data field value (e.g., `@orders_order_id`) |

### Transactions Table (SQLite)

The `transactions` table stores captured CDC changes:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Local autoincrement ID |
| `transaction_id` | TEXT | MSSQL transaction ID (GUID) |
| `table_name` | TEXT | Source table name |
| `operation` | TEXT | Operation type (INSERT/DELETE/UPDATE_AFTER) |
| `data` | TEXT | JSON-encoded row data |
| `lsn` | TEXT | CDC LSN as hex string (e.g., `0x0000002D00000A760066`), nullable |
| `changed_at` | TIMESTAMP | Transaction commit time from MSSQL |
| `pulled_at` | TIMESTAMP | When the change was pulled |

**LSN Semantics**: LSN (Log Sequence Number) is a transaction-level identifier from MSSQL CDC. Multiple row changes in the same transaction share the same LSN. Stored as a `0x`-prefixed hex string for readability and deterministic sorting.

### Sinks

| | Sinks |
|---|---|
| **Execution** | Per-change, filtered by operation |
| **Output** | Target table |
| **Use case** | Final sync output |

### Transaction Boundary

All writes (jobs + sinks) for a single CDC change are committed in one transaction. Failed changes go to DLQ.

---

## Skill Processing Flow

```
For each CDC change (row):
    1. Build params for this change
    2. Execute matching sinks → DataSets
    3. Begin transaction
    4. Write all DataSets to SQLite (upsert)
    5. Commit transaction
    6. If failed → write to DLQ
```

---

## Configuration

| Config | Description |
|--------|-------------|
| `mssql.*` | MSSQL connection settings |
| `tables` | CDC tables to monitor |
| `cdc.interval` | Poll frequency |
| `plugins.sql.path` | Skills directory |
| `app.listen` | Dashboard port |
| `app.path` | System SQLite path |
