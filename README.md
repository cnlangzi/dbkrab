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
- **Dynamic Plugin System**: Load/unload SQL plugins without restarting
- **Minimal Dependencies**: Pure Go + MSSQL driver only
- **Graceful Degradation**: Auto-retry on MSSQL disconnection
- **CDC Gap Protection**: Detect and alert on CDC cleanup data loss
- **DLQ Support**: Handle failed transactions with dead letter queue

## Architecture

```
MSSQL CDC Tables
    │
    ▼
┌─────────────────────────────────────────┐
│           dbkrab Core                   │
│  • Minimum LSN Synchronization          │
│  • Transaction Grouping                 │
│  • Dynamic Plugin Handler               │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│     Plugin Manager (Hot-Reload)         │
│  • Load/unload SQL plugins              │
│  • REST API for plugin management       │
│  • Watch skill directory                │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│      Dashboard (Go + HTMX)              │
│  • Skills management                    │
│  • DLQ management                       │
│  • CDC status monitoring               │
│  • Gap detection                        │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│      Store (Storage Layer)              │
│  • SQLite (built-in)                    │
└─────────────────────────────────────────┘
```

## Quick Start

```bash
# Copy example config
cp config.example.yml config.yml

# Edit config with your MSSQL credentials
vim config.yml

# Build
make build

# Run
./bin/dbkrab -config config.yml
```

## Configuration

```yaml
mssql:
  host: localhost
  port: 1433
  user: sa
  password: your_password
  database: your_database

tables:
  - dbo.orders
  - dbo.order_items

cdc:
  interval: 500ms

plugins:
  sql:
    enabled: true
    path: ./skills

app:
  listen: 3000
  type: sqlite
  path: ./data/app/dbkrab.db  # Unified: transactions, poller_state, offsets, dlq_entries
```

## Dashboard

Dashboard runs on port 3000 (configurable via `api_port`):

- `/` - Overview and system status
- `/skills` - Skills management
- `/dlq` - Dead letter queue
- `/tables` - CDC tables status
- `/gap` - Gap monitoring

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
```

## Change Operations

| __$operation | Meaning |
|---------------|---------|
| 1 | DELETE |
| 2 | INSERT |
| 3 | UPDATE (before) |
| 4 | UPDATE (after) |

## License

Apache License 2.0 - see [LICENSE](LICENSE)
