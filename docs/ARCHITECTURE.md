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

- Skills management (create/edit/delete)
- DLQ monitoring and replay
- CDC table status
- Gap detection alerts

---

## Data Flow

```
CDC Change (single row)
    │
    ├──► Job SQL (parallel, all ops) ──► SQLite (job output)
    │
    └──► Sink SQL (by operation type) ──► SQLite (sink output)
```

**Per-Change Processing**: Each CDC row triggers its own job and sink execution. Parameters are isolated per change.

---

## Directory Structure

```
dbkrab/
├── cmd/dbkrab/           # CLI entry point
├── internal/
│   ├── core/             # CDC poller, LSN, transaction buffer
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
├── sink/sqlite/          # System SQLite (offset, DLQ)
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
| `polling_interval` | Poll frequency |
| `plugins.sql.path` | Skills directory |
| `api_port` | Dashboard port (default 3000) |
| `sink.path` | System SQLite path |
