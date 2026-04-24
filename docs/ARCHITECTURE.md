# dbkrab Architecture

**Lightweight MSSQL Change Data Capture in Go**

---

## System Overview

```
MSSQL CDC Tables
    │
    ▼
┌─────────────────────────────────────────┐
│         Capturer Interface              │
│  (core.Capturer)                       │
│  • ChangeCapturer  - 增量轮询           │
│  • SnapshotCapturer - 全量同步          │
│  • ReplayCapturer  - 历史重放           │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│           dbkrab Runtime                │
│  (core.Runtime)                         │
│  • Transformer 处理                    │
│  • Store 写入                         │
│  • DLQ 错误处理                       │
│  • Offset 管理                        │
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

### 1. Runtime (`internal/core/runtime.go`)

`Runtime` 是统一的数据管道编排器，负责所有捕获后的处理逻辑：

- **Capturer 管理**：维护所有 3 个 Capturer，根据 `NextCapturer` 切换
- **Transformer 处理**：调用 ETL 插件处理 CDC 变更
- **DLQ 处理**：失败数据写入死信队列

**注意**：
- Store 和 Offset 管理已移入 `ChangeCapturer` 内部
- `ChangeCapturer` 自己管理 LSN offset 和 cdc.db 写入
- 3 个 Capturer 同时初始化，串行执行（不可并行）

```go
// CapturerName represents the name of a data capturer.
type CapturerName string

const (
    CapturerCDC     CapturerName = "cdc"
    CapturerSnapshot CapturerName = "snapshot"
    CapturerReplay  CapturerName = "replay"
)

// CaptureResult carries data and metadata from a Capturer fetch operation.
type CaptureResult struct {
    Changes      []CaptureChange
    BatchID      string
    NextCapturer CapturerName   // Which capturer to use for next fetch
}

// Runtime orchestrates data capture and ETL processing.
type Runtime struct {
    capturers   map[CapturerName]Capturer
    current     CapturerName
    transformer Transformer
    dlq         *dlq.DLQ
    monitorDB   *monitor.DB
    mu          sync.RWMutex
}

// Runtime methods for Dashboard
func (r *Runtime) CDC() Capturer
func (r *Runtime) Snapshot() Capturer
func (r *Runtime) Replay() Capturer
func (r *Runtime) SwitchTo(name CapturerName)
```

### 2. Capturer 接口 (`internal/core/capturer.go`)

`Capturer` 是数据摄取接口，每种实现对应一种 CDC 模式：

```go
type Capturer interface {
    Fetch(ctx context.Context) *CaptureResult
    Stop()
}
```

#### 实现类型

| 实现 | 位置 | 说明 | NextCapturer |
|------|------|------|--------------|
| `ChangeCapturer` | `internal/cdc/capturer.go` | 增量轮询，从 MSSQL CDC 表读取变更 | `CapturerCDC` |
| `SnapshotCapturer` | `internal/snapshot/capturer.go` | 全量同步，从 MSSQL 表读取快照 | `CapturerCDC` |
| `ReplayCapturer` | `internal/replay/capturer.go` | 历史重放，从 cdc.db 读取历史 | `CapturerCDC` |

**设计说明**：
- 3 个 Capturer 全局只有一个实例，存储在 `Runtime.capturers` map 中
- Dashboard 通过 `runtime.Snapshot()` / `runtime.Replay()` 获取对应 Capturer 实例
- `runtime.SwitchTo()` 供 Dashboard 手动切换当前运行的 Capturer
- `Fetch()` 返回 `NextCapturer` 指示自动切回 CDC
- `Stop()` 仅在 Runtime 关闭时调用，非切换时调用

### 3. CDC Query (`internal/cdc/`)

直接从 MSSQL CDC 表读取变更数据：

- **Querier**：执行 CDC 查询 (`fn_cdc_get_net_changes_*`)
- **GapDetector**：CDC 间隙检测，防止数据丢失
- **OffsetManager**：LSN 偏移量管理，计算并持久化 next_lsn

### 4. SQL Plugin Engine (`plugin/sql/`)

使用 SQL 作为领域特定语言处理 CDC 变更：

- **Loader**：从 `./skills/` 读取 skill YAML 文件
- **Mapper**：将 CDC 数据转换为 SQL 参数
- **Executor**：并行执行 job 和 sink
- **Writer**：写入 SQLite（upsert 逻辑）

### 5. Dashboard (`api/dashboard/`)

Go + HTMX 仪表板用于运维：

- Skills 管理
- DLQ 监控
- CDC 状态
- Gap 检测

---

## Data Flow

```
CDC Change (single row)
    │
    └──► Capturer.Fetch() → CaptureResult
                            ├─ Changes: []CaptureChange
                            └─ NextCapturer: CapturerName

Runtime.Run() loop:
    for {
        result := capturer[current].Fetch(ctx)
        process(result.Changes)
        current = result.NextCapturer  // Auto-switch via Fetch result
    }

Dashboard:
    runtime.SwitchTo(core.CapturerSnapshot)  // Manual switch
    runtime.Snapshot().Fetch(ctx)             // Direct interaction

                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
           ChangeCapturer      SnapshotCapturer      ReplayCapturer
           ├─OffsetManager     (no store)           (no store)
           ├─Store (cdc.db)                          │
           └─NextCapturer:                          │
                   CapturerCDC (always)              │
                    │                   │                   │
                    └───────────────────┴───────────────────┘
                                        │
                                        ▼
                              Transformer
                                        │
                                        ▼
                              Sink SQL ──────► SQLite (upsert)
                                        │
                                        ▼
                                   DLQ.Write
```

**Per-Change Processing**：每行 CDC 变更触发一次 sink 执行，参数按变更隔离。

**架构说明**：
- 3 个 Capturer 同时初始化，存储在 `Runtime.capturers` map 中
- `Runtime` 根据 `NextCapturer` 切换当前使用的 Capturer
- `ChangeCapturer` 内部管理 `OffsetManager`（LSN offset）和 `Store`（cdc.db 写入）
- `SnapshotCapturer` 和 `ReplayCapturer` 只返回数据，不写 store
- `Runtime` 只负责调用 `Transformer`，不再管理 Store

---

## Directory Structure

```
dbkrab/
├── cmd/app/              # CLI entry point
├── internal/
│   ├── core/             # Runtime, Capturer interface
│   │   ├── runtime.go    # Main orchestrator
│   │   ├── capturer.go  # Capturer interface + CaptureChange
│   │   ├── transaction.go # Change struct, Operation
│   │   ├── state.go     # State management
│   │   └── ...
│   ├── cdc/              # CDC query & ChangeCapturer
│   │   ├── capturer.go  # ChangeCapturer (implements Capturer)
│   │   ├── offset_manager.go # LSN offset management
│   │   ├── query.go     # CDC query functions
│   │   ├── gap_detector.go # Gap detection
│   │   └── ...
│   ├── snapshot/         # SnapshotCapturer
│   │   └── capturer.go  # SnapshotCapturer
│   ├── replay/           # ReplayCapturer
│   │   ├── capturer.go  # ReplayCapturer
│   │   └── replay.go    # Replay logic
│   ├── config/          # YAML config loader + hot-reload
│   ├── offset/           # LSN persistence (SQLite)
│   ├── dlq/             # Dead letter queue
│   └── ...
├── plugin/sql/           # SQL plugin engine
│   ├── engine.go        # Main executor
│   ├── loader.go        # Skill YAML loader
│   ├── executor.go      # Sink executor
│   └── writer.go        # SQLite writer (upsert)
├── api/
│   └── dashboard/       # HTMX dashboard
├── skills/              # Skill definitions (YAML)
└── data/                # Data directory
    └── app/             # System DB (offset, DLQ, transactions)
```

---

## CDC Capture Modes

MSSQL CDC 支持两种捕获模式：

| Mode | Function | Returns | UPDATE_BEFORE Rows |
|------|----------|---------|-------------------|
| `all_changes` | `fn_cdc_get_all_changes_*` | All row versions | Yes (before image) |
| `net_changes` | `fn_cdc_get_net_changes_*` | Final row state only | No |

**为什么使用 Net Changes？**

`net_changes` 模式消除 `UPDATE_BEFORE` 行，这：
- 防止 "unknown operation type: UPDATE_BEFORE" 错误
- 减少处理的 CDC 行数
- 简化事务处理（无中间状态）

**迁移现有捕获实例**

如果 CDC 之前未启用 net_changes（`supports_net_changes = 0`），必须重建捕获实例才能使用 net_changes：

```sql
-- 禁用旧捕获实例
EXEC sys.sp_cdc_disable_table
    @source_schema = 'dbo',
    @source_name = 'YourTable',
    @capture_instance = 'dbo_YourTable';

-- 使用 net_changes 支持重新启用
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'YourTable',
    @role_name = NULL,
    @supports_net_changes = 1;
```

> **注意**：重建捕获实例会清除该表的所有 CDC 历史。生产系统请谨慎规划。

---

## Database Layer

### Storage Engine: cnlangzi/sqlite

All SQLite databases in dbkrab use **`github.com/cnlangzi/sqlite`** — a Go wrapper that provides:

- **Read/Write separation**: dedicated reader pool and single buffered writer connection
- **Buffered writes**: writer batches changes and flushes asynchronously (100ms interval, 100-item buffer) for better TPS
- **WAL journal mode**: enabled automatically via DSN pragmas on the writer
- **Automatic PRAGMAs**: busy_timeout, synchronous, cache_size, temp_store, mmap_size all configured via DSN parameters

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

---

## CDC Parameters

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

**Content-Based ID**: The `id` column uses SHA256(transaction_id + table_name + data + lsn + operation), truncated to the first 16 bytes (32 hex characters). This is deterministic and unique per change content.

---

## Transformer Pipeline

The `core.Transformer` interface (`Transform(ctx, changes, batchCtx)`) is the ETL processing entry point:

```go
type Transformer interface {
    Transform(ctx context.Context, changes []Change, batchCtx *BatchContext) error
}
```

```
[]core.Change
    │
    └──► plugin.Manager.Transform()
            │
            ├── SQL Plugin Engine (Skill Pipeline)
            │       │
            │       └── Execute Skill SQL with CDC parameters
            │
            └── Sinker Manager
                    │
                    └── Write to SQLite (upsert)

    │
    └──► If failed → write to DLQ
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
