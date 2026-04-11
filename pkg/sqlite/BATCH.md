# BatchWriter Design

**Transparent batch writing with channel-based coordination for SQLite to improve TPS**

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
│  │     cmdCh (channel - commands to transaction goroutine) │  │
│  │     transactionLoop (single goroutine)                 │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Channel-based Coordination

All write operations are serialized through a single goroutine via `cmdCh`:

```
User goroutine(s):          cmdCh:                 transactionLoop:
  Exec(...) ──────────────→ Command{Exec} ──────→ handleExec()
  BeginTx() ──────────────→ Command{BeginTx} ───→ handleBeginTx()
  Tx.Commit() ───────→ Command{BatchCommit} → handleBatchCommit()

Timer goroutine:
  onTimer() ──────────────→ Command{Flush}
```

---

## Two Write Patterns

### 1. Direct Exec (automatic batching)

```
Exec() → cmdCh → transactionLoop → globalTx.Exec() → pendingCount++ → tryFlushLocked()
                                                                          ↑
Timer fires ────────────────────────────────────────────────────────────┘
```

### 2. BeginTx + Commit/Rollback (drop-in replacement)

```
BeginTx():
  → Flush pending globalTx first (commit any uncommitted data)
  → Start new globalTx
  → Create Tx
    ↓
Exec(Tx) → buffer to Tx.buf (NOT executed yet)
    ↓
Commit(Tx):
  → Send BatchCommit command
  → transactionLoop executes:
    → Execute Tx.buf in globalTx
    → globalTx.Commit() (make data visible)
    → Start new globalTx
    OR
Rollback(Tx):
  → Discard Tx.buf (already discarded, never executed)
```

---

## Key Design: BeginTx Flushes Pending Data

**When BeginTx is called, any pending data is immediately committed.**

This ensures:
1. **No SAVEPOINT needed** - simpler design
2. **Tx failures don't affect prior data** - pending data already committed
3. **Tx isolation** - operates on fresh globalTx
4. **Drop-in replacement** - Tx satisfies sql.Tx interface

---

## Drop-in Replacement for *sql.Tx

`Tx` implements the same interface as `*sql.Tx`:

```go
type TxExec interface {
    Exec(query string, args ...any) (sql.Result, error)
    Commit() error
    Rollback() error
}
```

Users can replace `*sql.Tx` with `*Tx`:

```go
// Before
tx, _ := db.BeginTx(ctx, nil)
tx.Exec("INSERT ...")
tx.Commit()

// After (with BatchWriter)
tx, _ := bw.BeginTx(ctx, nil)  // bw.BeginTx returns *Tx
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
Accumulate writes (Direct Exec or Tx buffer)
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
    TxTimeout     time.Duration // Reserved for future use
}

type Command struct {
    Type     string
    TxOptions  *sql.TxOptions
    TxResultCh chan<- *TxResult
    Query    string
    Args     []any
    Buffer   []stmt // For BatchCommit
    ResultCh chan<- Result
}

type Tx struct {
    writer    *BatchWriter
    buf       []stmt
    done      bool
}

type BatchWriter struct {
    *sql.DB // embedded: passthrough Query/QueryRow
    cfg      BatchConfig
    cmdCh    chan Command

    // Global transaction state (only accessed by transaction goroutine)
    globalTx     *sql.Tx
    pendingCount  int
    lastFlush    time.Time
    timer        *time.Timer
}
```

---

## Timer Flush Behavior

```
Timer fires (every FlushInterval):
  → Non-blocking send to cmdCh with Flush command
  → transactionLoop processes Flush when ready
  → If pendingCount > 0 → globalTx.Commit()
```

Timer uses non-blocking send to avoid blocking or racing with transaction operations.

---

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| BatchSize | int | 100 | Flush when pending count reaches this |
| FlushInterval | duration | 100ms | Time-based flush interval |
| TxTimeout | duration | 30s | Reserved for future use |

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

## Why Channel-based Design?

### Solves Multiple goroutine Problems

Without channel (mutex-based):
```
Goroutine A:                      Goroutine B:
  bw.mu.Lock()
  globalTx.Exec(work1)
  pendingCount=1
  bw.mu.Unlock() ──────────────→ bw.mu.Lock()
                                    globalTx.Commit()  ← pending cleared
                                    globalTx = nil
                                  bw.mu.Unlock()
                                    ↓
                                  Goroutine A (later):
                                  bw.mu.Lock()
                                  globalTx.Exec(work2) ← ERROR! globalTx is nil
```

With channel (single goroutine):
```
All operations handled by transactionLoop (single goroutine)
No race conditions possible
```

### Key Benefits

| Aspect | Benefit |
|-------|---------|
| Single goroutine | No mutex/race conditions |
| BeginTx flush | Tx isolated from prior data |
| BatchCommit | Atomic execution in transactionLoop |
| Non-blocking timer | Timer doesn't block or race |

---

## Implementation Details

### Tx.Commit Execution Order

1. Send BatchCommit command to cmdCh
2. transactionLoop receives command
3. Execute all buffered statements in globalTx
4. If any fails → rollback globalTx, start new, return error
5. If all succeed → commit globalTx, start new, return nil

### Why BeginTx Flushes Pending Data?

Without flushing pending data:
- Tx failure would rollback globalTx, losing prior data

With flushing:
- Pending data committed before Tx starts
- Tx operates on fresh globalTx
- Tx failure only affects its own buffer

---

## Files

| File | Description |
|------|-------------|
| `batch.go` | Implementation |
| `batch_test.go` | Unit tests |
| `BATCH.md` | This documentation |

---

## Key Design Principles

1. **Channel for serialization**: All operations through single goroutine
2. **BeginTx flushes pending data**: Ensures Tx isolation
3. **Drop-in replacement**: Tx satisfies sql.Tx interface
4. **Non-blocking timer**: Doesn't block or race with operations
5. **Immediate commit on Tx.Commit**: Data visible immediately
