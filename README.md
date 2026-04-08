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
- **Dynamic Plugin System**: Load/unload WASM and SQL plugins without restarting
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
│  • Dynamic Plugin Handler               │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│     Plugin Manager (Hot-Reload)         │
│  • Load/unload WASM plugins             │
│  • Load SQL plugins with DB access      │
│  • REST API for plugin management       │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│      Store (Storage Layer)              │
│  • SQLite (built-in)                    │
└─────────────────────────────────────────┘
```

## Dynamic Plugin System

Plugins can be **loaded, unloaded, and reloaded at runtime** without restarting dbkrab.

### Plugin Management API

```bash
# List loaded plugins
curl http://localhost:9020/api/plugins

# Load a new plugin
curl -X POST http://localhost:9020/api/plugins \
  -H "Content-Type: application/json" \
  -d '{"name": "my-plugin", "path": "./plugins/my-handler.wasm", "config": ""}'

# Reload a plugin (hot-reload)
curl -X POST http://localhost:9020/api/plugins/my-plugin/reload

# Unload a plugin
curl -X DELETE http://localhost:9020/api/plugins/my-plugin
```

### Auto-Load from Directory

Configure `plugins:` in `config.yml`. dbkrab will:
1. Scan the directory on startup and load all `.wasm` files
2. Watch for new/modified `.wasm` files and auto-load/reload

### Writing a Plugin

```go
// plugins/my_handler/main.go
package main

import (
    "github.com/cnlangzi/dbkrab/plugin"
)

//export Init
func Init(config string) int {
    return 0
}

//export Handle
func Handle(ptr *byte, len int) int {
    // Handle transaction data
    return 0
}

//export Close
func Close() int {
    return 0
}

func main() {}
```

Build: `tinygo build -target wasi -o my_handler.wasm my_handler.go`

## Usage

### Quick Start

```bash
# Copy example config
cp config.example.yml config.yml

# Edit config with your MSSQL credentials
vim config.yml

# Run
go run ./cmd/dbkrab
```

### Configuration

```yaml
# config.yml
mssql:
  host: localhost
  port: 1433
  user: sa
  password: your_password
  database: your_database

tables:
  - dbo.orders
  - dbo.order_items

polling_interval: 500ms
offset_file: ./data/offset.json

# Plugin configuration (hierarchical, both disabled by default)
plugins:
  wasm:
    enabled: true
    path: ./plugins     # WASM plugins (hot-reload enabled)
  sql:
    enabled: true
    path: ./sql_plugins # SQL plugins (data enrichment engine)

# API server for plugin management
api_port: 9020

sink:
  type: sqlite
  path: ./data/cdc.db
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

---

**dbkrab** — Lightweight MSSQL CDC in Go 🦀