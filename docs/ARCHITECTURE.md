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
- **Net Changes Mode**: When MSSQL CDC is enabled with `supports_net_changes = 1`, the poller uses `fn_cdc_get_net_changes_*` functions which return only the final row state, eliminating `UPDATE_BEFORE` rows from the stream.

#### CDC Capture Modes

MSSQL CDC supports two capture modes:

| Mode | Function | Returns | UPDATE_BEFORE Rows |
|------|----------|---------|-------------------|
| `all_changes` | `fn_cdc_get_all_changes_*` | All row versions | Yes (before image) |
| `net_changes` | `fn_cdc_get_net_changes_*` | Final row state only | No |

**Why Net Changes?**

The `net_changes` mode eliminates `UPDATE_BEFORE` rows, which:
- Prevents "unknown operation type: UPDATE_BEFORE" errors
- Reduces the number of CDC rows to process
- Simplifies transaction handling (no intermediate states)

**Migration for Existing Capture Instances**

If CDC was previously enabled without net_changes (`supports_net_changes = 0`), existing capture instances must be rebuilt to use net_changes:

```sql
-- Disable old capture instance
EXEC sys.sp_cdc_disable_table
    @source_schema = 'dbo',
    @source_name = 'YourTable',
    @capture_instance = 'dbo_YourTable';

-- Re-enable with net_changes support
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'YourTable',
    @role_name = NULL,
    @supports_net_changes = 1;
```

> **Note**: Rebuilding a capture instance clears all CDC history for that table. Plan accordingly for production systems.

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
└── 1.0.0/
    └── 001_initial.sql
```

**SQL file format:**

```sql
-- Migration: 001_initial
-- Module: dbkrab-store
-- Description: Create initial schema
-- Versioning Rules:
--   - Major changes (breaking schema, new tables): increment major, require Devin confirmation
--   - Schema table-structure changes: increment minor version (e.g., 1.1.0, 1.2.0)
--   - Field-only changes (defaults, constraints without structural change): increment patch (e.g., 1.0.1)
--   - All migrations live under the initial semver folder (1.0.0) per current policy decision

CREATE TABLE IF NOT EXISTS transactions (...);
CREATE INDEX IF NOT EXISTS idx_xxx ON transactions(...);
```

**Initialization (raw *sql.DB required for sqle/migrate):**

```go
import (
    "embed"
    "github.com/cnlangzi/sqlite"
    "github.com/yaitoo/sqle"
    "github.com/yaitoo/sqle/migrate"
)

//go:embed migrations
var migrationsFS embed.FS

// sqle/migrate needs the raw *sql.DB from the buffered writer
sqleDB := sqle.Open(db.Writer.DB)

migrator := migrate.New(sqleDB)
if err := migrator.Discover(migrationsFS, migrate.WithModule("dbkrab-store")); err != nil {
    return err
}
if err := migrator.Migrate(ctx); err != nil {
    return err
}
```

> **Important**: Only use `sqle.Open(db.Writer.DB)` for migrations. After migration, use `sqlite.DB` for all application reads and writes (Writer for writes, Reader for reads).

### Unified App DB

The main application database (`./data/app/dbkrab.db`) stores:

| Table | Purpose |
|-------|---------|
| `transactions` | Captured CDC changes |
| `poller_state` | Polling progress and metrics |
| `offsets` | Per-table LSN positions |

The DLQ uses its own separate database (`./data/app/dlq.db`):

| Table | Purpose |
|-------|---------|
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
| `id` | TEXT | Content-based hash (SHA256 of transaction_id+table_name+data+lsn+operation, first 16 bytes as 32-char hex). Primary key; prevents CDC record loss from LSN advancement skipping rows in same LSN group. |
| `transaction_id` | TEXT | MSSQL transaction ID (GUID); NOT NULL for transaction boundary safety and diagnostics |
| `table_name` | TEXT | Source table name |
| `operation` | TEXT | Operation type (INSERT/DELETE/UPDATE_AFTER) |
| `data` | TEXT | JSON-encoded row data |
| `lsn` | TEXT | CDC LSN as hex string (e.g., `0x0000002D00000A760066`), retained for tracing/debugging |
| `changed_at` | TIMESTAMP | Transaction commit time from MSSQL |
| `pulled_at` | TIMESTAMP | When the change was pulled |

**Content-Based ID**: The `id` column uses SHA256(transaction_id + table_name + data + lsn + operation), truncated to the first 16 bytes (32 hex characters). This is deterministic and unique per change content. Using a content-based primary key instead of an auto-increment integer prevents record loss when LSN advancement skips remaining rows in the same LSN group during CDC pulls. Insert operations use `INSERT OR IGNORE` to silently skip duplicates.

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
| `app.db` | Store/offset SQLite path |
| `app.dlq` | DLQ SQLite path |
