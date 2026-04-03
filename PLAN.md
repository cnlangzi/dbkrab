# dbkrab Implementation Plan

## Project Overview

**dbkrab** — Lightweight MSSQL Change Data Capture in Go

A pure Go implementation of MSSQL CDC that reads change data directly from SQL Server's CDC tables. No Java, no Kafka, no dependencies.

---

## Phase 1: Core Infrastructure

### 1.1 Project Structure

```
dbkrab/
├── cmd/
│   └── dbkrab/           # CLI entry point
├── internal/
│   ├── core/             # CDC core logic
│   │   ├── poller.go     # LSN polling
│   │   ├── transaction.go # transaction grouping
│   │   └── lsn.go        # LSN management
│   ├── cdc/              # MSSQL CDC operations
│   │   ├── query.go      # CDC query functions
│   │   └── snapshot.go   # Initial load with SNAPSHOT
│   ├── config/           # Configuration
│   └── offset/           # LSN persistence
├── plugin/               # WASM plugin runtime
│   ├── runtime.go        # WASI/VM runtime
│   └── wasi.go           # WASM syscalls
├── sink/                 # Storage sinks
│   └── sqlite/
├── LICENSE
├── README.md
├── PLAN.md              # This file
└── go.mod
```

### 1.2 Core Interfaces

```go
// Config
type Config struct {
    Tables      []string
    DB          *sql.DB
    Interval    time.Duration
    OffsetFile  string
    PluginPath  string
    Sink        Sink
}

// Core
type Poller interface {
    Start(ctx context.Context) error
    Stop() error
}

type Transaction struct {
    TransactionID string
    Changes       []Change
}

type Change struct {
    Table   string
    Op      Operation // 1=DELETE, 2=INSERT, 3=UPDATE, 4=UPDATE
    Data    map[string]interface{}
}

// Plugin (WASM)
type PluginRuntime interface {
    Load(path string) error
    Execute(tx *Transaction) error
    Close() error
}

// Sink
type Sink interface {
    Write(tx *Transaction) error
}
```

### 1.3 MVP Deliverables (Week 1)

- [ ] Project skeleton with go.mod
- [ ] Config loader from YAML
- [ ] LSN persistence (file-based)
- [ ] Basic CDC polling (single table)
- [ ] Operation parsing (DELETE/INSERT/UPDATE)
- [ ] SQLite sink implementation

---

## Phase 2: Advanced Features

### 2.1 Multi-table + Transaction Grouping

- [ ] Minimum LSN synchronization across tables
- [ ] Cross-table transaction correlation via `__$transaction_id`
- [ ] Batch delivery grouped by transaction

### 2.2 Initial Load (Batch + Stream)

- [ ] SNAPSHOT isolation for consistent initial load
- [ ] Record LSN after initial load completes
- [ ] Seamless transition to CDC polling

### 2.3 Plugin System (WASM)

- [ ] TinyGo WASI compilation support
- [ ] Plugin interface definition
- [ ] Sandbox runtime (wasmtime or wasmer)
- [ ] Hot reload capability

---

## Phase 3: Observability & Operations

### 3.1 Logging & Metrics

- [ ] Structured logging (zap/zerolog)
- [ ] Prometheus metrics endpoint
  - Polls total
  - Transaction count
  - Error rate
  - Processing latency

### 3.2 Health & Monitoring

- [ ] Health check endpoint (`/health`)
- [ ] LSN gap detection (CDC cleanup warning)
- [ ] Graceful shutdown

### 3.3 CLI & Configuration

- [ ] Command-line flags
- [ ] Config file (YAML)
- [ ] Environment variable support

---

## Phase 4: Production Ready

### 4.1 Error Handling

- [ ] Retry logic with backoff
- [ ] Dead letter queue for failed transactions
- [ ] Circuit breaker

### 4.2 Performance

- [ ] Connection pooling
- [ ] Batch size configuration
- [ ] Memory limits

### 4.3 Testing

- [ ] Unit tests for core logic
- [ ] Integration tests with real MSSQL
- [ ] Mock CDC server for testing

---

## Documentation Plan

### README.md (Main)

- One-liner description
- Quick start example
- Feature list
- Architecture diagram
- Comparison with Debezium

### PLAN.md (This file)

- Implementation phases
- Milestones
- Task breakdown

### SPEC.md (Future)

- Detailed technical specification
- API documentation
- Configuration reference

---

## Milestones

| Milestone | Description | Target |
|-----------|-------------|--------|
| M1 | MVP: Single table polling + SQLite sink | Week 1 |
| M2 | Multi-table + Transaction grouping | Week 2 |
| M3 | Initial Load + CDC streaming | Week 2-3 |
| M4 | WASM Plugin runtime | Week 3 |
| M5 | Observability (logging, metrics, health) | Week 4 |
| M6 | Production hardening | Week 5 |

---

## Dependencies

### Required

```go
import (
    "database/sql"           // stdlib
    _ "github.com/denisenkom/go-mssqldb" // MSSQL driver
    "github.com/segmentio/asm" // optional: high perf asm
)
```

### Optional

```go
// Plugin runtime (choose one)
"github.com/bytecodealliance/wasmtime-go" // faster, Rust-based
"github.com/wa-lang/wasmer-go"           // pure Go

// Logging
"github.com/rs/zerolog"
"github.com/uber-go/zap"

// Metrics
"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promhttp"
```

---

## Task Breakdown

### Week 1: Core MVP

```
T1.1: Initialize project, go.mod
T1.2: Config loader (YAML)
T1.3: LSN offset storage (JSON file)
T1.4: CDC query functions (fn_cdc_get_all_changes_*)
T1.5: Basic poller with single table
T1.6: SQLite sink implementation
T1.7: CLI entry point
T1.8: Basic README
```

### Week 2: Multi-table + Transactions

```
T2.1: Multi-table polling
T2.2: Minimum LSN synchronization
T2.3: Transaction grouping by __$transaction_id
T2.4: Cross-table batch delivery
T2.5: Unit tests
```

### Week 3: Initial Load + WASM

```
T3.1: SNAPSHOT isolation for initial load
T3.2: LSN checkpoint after initial load
T3.3: WASM runtime setup (wasmtime/wasmer)
T3.4: Plugin interface
T3.5: Plugin example (tinygo)
```

### Week 4: Observability

```
T4.1: Structured logging
T4.2: Prometheus metrics
T4.3: Health endpoint
T4.4: Graceful shutdown
T4.5: LSN gap detection
```

---

## Notes

- Use **Semantic Versioning** for releases
- Keep dependencies minimal (MVP: 1 driver only)
- Prefer standard library where possible
- Test with real MSSQL CDC (need test environment)

---

**Last Updated**: 2026-04-02