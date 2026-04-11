# BatchWriter Design

**Transparent batch writing with global transaction for SQLite to improve TPS**

---

## Motivation

SQLite with WAL mode has limited TPS due to:
- Frequent transaction commits and WAL fsyncs
- Single writer connection bottleneck

BatchWriter solves this by:
1. Accumulating writes in a global transaction
2. Committing in batches when size or time threshold is reached
3. Preserving atomicity for explicit transactions (BeginTx/Commit/Rollback)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        BatchWriter                          │
│  ┌───────────┐  ┌──────────────┐  ┌─────────────────────┐ │
│  │  *sql.DB  │  │ globalTx    │  │    timer            │ │
│  │ (reader) │  │  (lazy)     │  │  (time-based flush) │ │
│  └───────────┘  └──────────────┘  └─────────────────────┘ │
│         │              ↑                    ↑              │
│         ▼              │                    │              │
│  ┌─────────────────────────────────────────────────────┐  │
│  │                 sync.Mutex                          │  │
│  │  Ensures exclusive access during BatchTx           │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Two Write Patterns

### 1. Direct Exec (batch mode)

```
Exec() → Lock → globalTx.Exec() → pendingCount++ → tryFlushLocked() → Unlock
                                                                         ↑
Timer fires → Lock → if pendingCount >= BatchSize → globalTx.Commit() ─┘
```

### 2. BeginTx + Commit/Rollback (drop-in replacement)

```
BeginTx():
  → Flush pending globalTx first (commit any uncommitted data)
  → Start new globalTx
  → Create BatchTx
  → Lock is held until Commit/Rollback
    ↓
Exec(BatchTx) → buffer to BatchTx.buf (NOT executed yet)
    ↓
Commit(BatchTx):
  → Execute BatchTx.buf in globalTx
  → globalTx.Commit() (make data visible)
  → Start new globalTx
  → Unlock
    OR
Rollback(BatchTx):
  → Discard BatchTx.buf (already discarded, never executed)
  → Unlock
```

---

## Key Design: BeginTx Flushes Pending Data

**When BeginTx is called, any pending data is immediately committed.**

This ensures:
1. **No SAVEPOINT needed** - simpler design
2. **BatchTx failures don't affect prior data** - pending data already committed
3. **BatchTx isolation** - operates on fresh globalTx
4. **Drop-in replacement** - BatchTx satisfies sql.Tx interface

---

## Drop-in Replacement for *sql.Tx

`BatchTx` implements the same interface as `*sql.Tx`:

```go
type TxExec interface {
    Exec(query string, args ...any) (sql.Result, error)
    Commit() error
    Rollback() error
}
```

Users can replace `*sql.Tx` with `*BatchTx`:

```go
// Before
tx, _ := db.BeginTx(ctx, nil)
tx.Exec("INSERT ...")
tx.Commit()

// After (with BatchWriter)
tx, _ := bw.BeginTx(ctx, nil)  // bw.BeginTx returns *BatchTx
tx.Exec("INSERT ...")
tx.Commit()
```

---

## Global Transaction Lifecycle

```
First write (Direct or BeginTx)
    ↓
globalTx = db.Begin()
    ↓
[If BeginTx]: Flush previous pending, start new globalTx
    ↓
Accumulate writes (Direct Exec or BatchTx buffer)
    ↓
size trigger (≥ BatchSize) → globalTx.Commit()
time trigger → globalTx.Commit()
    ↓
globalTx = nil (next write creates new one)
```

---

## Data Structures

```go
type BatchConfig struct {
    BatchSize     int           // Default: 100
    FlushInterval time.Duration // Default: 100ms
}

type BatchTx struct {
    writer *BatchWriter
    buf    []stmt
    done   bool
}

type BatchWriter struct {
    *sql.DB // embedded: passthrough Query/QueryRow
    mu     sync.Mutex

    globalTx     *sql.Tx // lazily created
    pendingCount  int
    lastFlush    time.Time
    timer        *time.Timer
    cfg          BatchConfig
}
```

---

## Timer Flush Behavior

```
Timer fires (every FlushInterval):
  1. TryLock() - if locked (BatchTx active), skip
  2. If pendingCount > 0 → globalTx.Commit()
  3. Reset timer
```

Timer uses `TryLock` to avoid blocking. If lock is held by BatchTx, the flush is skipped for that cycle.

---

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| BatchSize | int | 100 | Flush when pending count reaches this |
| FlushInterval | duration | 100ms | Time-based flush interval |

---

## Usage Examples

### Direct Exec (automatic batching)

```go
bw := NewBatchWriter(db, BatchConfig{
    BatchSize:     100,
    FlushInterval: 100 * time.Millisecond,
})

bw.Exec("INSERT INTO orders (id, amount) VALUES (1, 100)")
bw.Exec("INSERT INTO orders (id, amount) VALUES (2, 200)")
// When BatchSize reached or FlushInterval fires:
// All pending writes committed in single transaction
```

### Explicit Transaction (drop-in replacement)

```go
tx, _ := bw.BeginTx(ctx, nil)
tx.Exec("INSERT INTO orders (id, amount) VALUES (1, 100)")
tx.Exec("INSERT INTO items (order_id, product) VALUES (1, 'widget')")
tx.Commit() // Both statements execute atomically
```

### Rollback

```go
tx, _ := bw.BeginTx(ctx, nil)
tx.Exec("INSERT INTO orders ...")
tx.Rollback() // Changes discarded, no data written
```

---

## Implementation Details

### Commit Execution Order

1. Execute all buffered statements in globalTx
2. If any fails → rollback globalTx, start new, return error
3. If all succeed → commit globalTx, start new, return nil

### Why BeginTx Flushes Pending Data?

Without flushing pending data:
- BatchTx failure would rollback globalTx, losing prior data

With flushing:
- Pending data committed before BatchTx starts
- BatchTx operates on fresh globalTx
- BatchTx failure only affects its own buffer

### Lock Behavior

| Operation | Lock | Unlock |
|-----------|------|--------|
| Exec | Lock | defer Unlock |
| BeginTx | Lock | (held) |
| BatchTx.Commit | (held) | Unlock |
| BatchTx.Rollback | (held) | Unlock |
| Timer | TryLock (skip if busy) | Unlock |

---

## Files

| File | Description |
|------|-------------|
| `batch.go` | Implementation |
| `batch_test.go` | Unit tests |
| `BATCH.md` | This documentation |

---

## Key Design Principles

1. **BeginTx flushes pending data** - ensures BatchTx isolation
2. **No SAVEPOINT needed** - simpler design with BeginTx flush
3. **Drop-in replacement** - BatchTx satisfies sql.Tx interface
4. **Timer uses TryLock** - doesn't block BatchTx operations
5. **Immediate commit on BatchTx.Commit** - data visible immediately
