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

## Database Layer

### Storage Engine: cnlangzi/sqlite

All SQLite databases in dbkrab use **`github.com/cnlangzi/sqlite`** — a Go wrapper that provides:

- **Read/Write separation**: dedicated reader pool and single buffered writer connection
- **Buffered writes**: writer batches changes and flushes asynchronously (100ms interval, 100-item buffer) for better TPS
- **WAL journal mode**: enabled automatically via DSN pragmas on the writer
- **Automatic PRAGMAs**: busy_timeout, synchronous, cache_size, temp_store, mmap_size all configured via DSN parameters

```go
import "github.com/cnlangzi/sqlite"

// All reads go through Reader (concurrent, connection-pooled)
rows, err := db.Reader.Query("SELECT ...")

// All writes go through Writer (buffered, auto-batched)
_, err := db.Writer.Exec("INSERT INTO ...")

// Flush ensures buffered writes are committed before reads
db.Flush()
```

**Reader DSN pragmas**: `mode=ro`, `cache=private`, `query_only`, `mmap_size`, `threads` (scales with CPU count)

**Writer DSN pragmas**: `mode=rwc`, `_journal_mode=WAL`, `_synchronous=NORMAL`, `_busy_timeout=5000`, `temp_store=MEMORY`, `cache_size`

> **Do not** use `PRAGMA journal_mode=WAL` or other PRAGMA statements manually. The wrapper handles all SQLite tuning automatically.

### Schema Migration: yaitoo/sqle/migrate

Database schemas are managed by **`github.com/yaitoo/sqle/migrate`** with semver migration directories.

**Migration directory structure:**

```
internal/store/migrations/
├── v1.0.0/
│   └── 001_initial.sql
└── v1.1.0/
    └── 001_add_column.sql
```

**SQL file format:**

```sql
-- Migration: 001_initial
-- Module: dbkrab-store
-- Description: Create initial schema

CREATE TABLE IF NOT EXISTS transactions (...);
CREATE INDEX IF NOT EXISTS idx_xxx ON transactions(...);
```

**Initialization (raw *sql.DB required for sqle/migrate):**

```go
import (
    "github.com/cnlangzi/sqlite"
    "github.com/yaitoo/sqle"
    "github.com/yaitoo/sqle/migrate"
)

// sqle/migrate needs the raw *sql.DB from the buffered writer
sqleDB := sqle.Open(db.Writer.DB)

migrator := migrate.New(sqleDB)
if err := migrator.Discover(os.DirFS("./internal/store/migrations"), migrate.WithModule("dbkrab-store")); err != nil {
    return err
}
if err := migrator.Migrate(ctx); err != nil {
    return err
}
```

> **Important**: Only use `sqle.Open(db.Writer.DB)` for migrations. After migration, use `sqlite.DB` for all application reads and writes (Writer for writes, Reader for reads).

### Unified App DB

The main application database (`./data/app/dbkrab.db`) consolidates:

| Table | Purpose |
|-------|---------|
| `transactions` | Captured CDC changes |
| `poller_state` | Polling progress and metrics |
| `offsets` | Per-table LSN positions |
| `dlq_entries` | Dead letter queue |

### Key Concepts

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
