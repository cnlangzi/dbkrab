## PR Title

feat: dbkrab v1 - Lightweight MSSQL CDC in Go

## PR Body

### Overview

dbkrab 是一个纯 Go 实现的 MSSQL CDC（Change Data Capture）工具，无需 Java、无需 Kafka，轻量级读取 SQL Server 的 CDC 表变更。

### Features

#### Core CDC
- ✅ LSN-based incremental polling
- ✅ Transaction grouping by `__$transaction_id`
- ✅ Multi-table monitoring
- ✅ LSN persistence (file-based JSON)

#### Storage
- ✅ SQLite sink for storing change data

#### Plugin System
- ✅ Dynamic plugin loading/unloading (no restart required)
- ✅ Hot-reload capability
- ✅ REST API for plugin management
- ✅ Auto-watch plugin directory

#### Developer Experience
- ✅ Makefile with common commands
- ✅ Git hooks (pre-commit: vet + test)
- ✅ GitHub Actions CI (PR only)

### Usage

```bash
# Build
make build

# Run
./bin/dbkrab -config config.yml

# Plugin API
curl http://localhost:9020/api/plugins
```

### Project Structure

```
dbkrab/
├── cmd/dbkrab/          # CLI entry point
├── internal/
│   ├── core/            # Poller, Transaction, LSN
│   ├── cdc/             # MSSQL CDC queries
│   ├── config/          # YAML config loader
│   └── offset/          # LSN persistence
├── plugin/              # Dynamic plugin manager
├── api/                 # HTTP API server
├── sink/sqlite/         # SQLite storage
└── .github/workflows/   # CI
```

### Testing

```bash
make test        # Run tests
make coverage    # Coverage report
make pre-commit  # Pre-commit checks
```

### Next Steps

- [ ] WASM runtime integration (wasmtime/wasmer)
- [ ] Initial Load (batch + stream with SNAPSHOT isolation)
- [ ] Metrics and health endpoints
- [ ] Integration tests with real MSSQL

### Commits

- 0c887e4 fix: only run CI on PRs to main branch
- d570766 fix: run GitHub Actions only on PRs, not on push
- 8acc3f0 fix: improve pre-commit hook
- 625f174 feat: add git hooks and unify CI scripts with Makefile
- 034866a feat: add Makefile with common development commands
- daf7743 feat: add dynamic plugin system with hot-reload support
- 9e0a9b0 feat: implement dbkrab v1 with core CDC polling and SQLite sink
- 8cadb9b docs: add implementation plan with phases and milestones
- e956c82 refactor: simplify sink to SQLite only, add WASM plugin runtime
- 89be4ff fix: update license to Apache 2.0
- 7c65866 feat: add implementation plan and documentation