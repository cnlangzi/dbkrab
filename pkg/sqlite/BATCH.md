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

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        BatchWriter                          │
│  ┌───────────┐  ┌──────────────┐  ┌─────────────────────┐ │
│  │  *sql.DB  │  │ globalTx    │  │    timer            │ │
│  │ (reader) │  │  (lazy)     │  │  (time-based flush) │ │
│  └───────────┘  └──────────────┘  └─────────────────────┘ │
│         │              ↑                    ↑              │
│         │              │                    │              │
│         ▼              │                    │              │
│  ┌─────────────────────────────────────────────────────┐  │
│  │                    mu (mutex)                        │  │
│  │  Guards: globalTx, pendingCount, active operations  │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Two Write Patterns

**1. Direct Exec (no transaction)**

```
Exec() → globalTx → immediate execution → pendingCount++
         ↓
         size threshold reached → Commit globalTx → reset
```

**2. BeginTx + Commit/Rollback (explicit transaction)**

```
BeginTx() → BatchTx created → lock held
    ↓
Exec(BatchTx) → buffer to BatchTx.buf (NOT executed yet)
    ↓
Commit(BatchTx) → execute BatchTx.buf in globalTx → pendingCount += len(buf)
                   → release lock
                   → try size-based flush
    OR
Rollback(BatchTx) → discard BatchTx.buf → release lock (no execution)
```

---

## Locking Model

SQLite writer is single-process, so only one write transaction at a time.

| Operation | Lock Point | Unlock Point |
|-----------|-----------|--------------|
| Direct Exec | Function entry | Before return |
| BeginTx | Function entry | **Not unlocked** |
| Commit | - | After all operations |
| Rollback | - | Immediately |
| Time-based | Function entry | After flush |

### Time-based Cannot Break Atomicity

When time-based timer fires:
1. Try to acquire `mu`
2. If BeginTx is in progress (lock held) → **blocked**, skip this trigger
3. If no active transaction → acquire lock → flush pending → unlock

---

## Global Transaction Lifecycle

```
First write (Direct or BeginTx)
    ↓
globalTx = db.BeginTx()
    ↓
Accumulate writes (Direct Exec + BatchTx Commit)
    ↓
size trigger (≥ BatchSize) → Flush
time trigger → Flush (if no active BatchTx)
    ↓
globalTx.Commit()
    ↓
globalTx = nil (next write creates new one)
```

---

## Data Structures

```go
type stmt struct {
    query string
    args  []any
}

// BatchTx wraps sql.Tx to defer execution until Commit.
// The embedded *sql.Tx is a placeholder - it is never used.
type BatchTx struct {
    *sql.Tx             // placeholder: satisfies *sql.Tx interface
    writer *BatchWriter // reference to BatchWriter
    buf    []stmt      // buffered statements
    done   bool         // committed or rolled back
}

// BatchWriter provides transparent batch writing.
// Embeds *sql.DB for Query/QueryRow (read operations pass through).
type BatchWriter struct {
    *sql.DB            // embedded: BatchWriter is-a sql.DB
    mu     sync.Mutex
    globalTx   *sql.Tx // lazy created
    pendingCount int  // accumulated count
    lastFlush  time.Time
    timer      *time.Timer
}
```

---

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| BatchSize | int | 100 | Flush when pending count reaches this |
| FlushInterval | duration | 100ms | Time-based flush interval |
| TxTimeout | duration | 30s | Timeout for single flush |

---

## Usage Examples

### Direct Exec

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

### Explicit Transaction

```go
tx, _ := bw.BeginTx(ctx, nil)
tx.Exec("INSERT INTO orders (id, amount) VALUES (1, 100)")
tx.Exec("INSERT INTO items (order_id, product) VALUES (1, 'widget')")
tx.Commit() // Both statements execute atomically
```

### Atomicity Guarantee

```go
tx, _ := bw.BeginTx(ctx, nil)
tx.Exec("INSERT INTO orders ...")
tx.Exec("INSERT INTO items ...")
// If items insert fails, both are rolled back
tx.Rollback()
```

---

## Anti-Double-Unlock Protection

When using `defer tx.Rollback()` after `tx.Commit()`:

```go
tx, _ := bw.BeginTx(ctx, nil)
defer func() { _ = tx.Rollback() }()
tx.Exec(...)
tx.Commit() // marks done=true, unlocks

// defer's Rollback() is safely ignored (done=true skips unlock)
```

---

## Failure Handling

On flush error:
1. Rollback globalTx
2. Reset globalTx = nil
3. Clear pendingCount
4. Return error (caller decides retry)

On BatchTx commit error:
1. Rollback globalTx
2. Reset globalTx = nil  
3. Clear pendingCount
4. Return error (caller's defer Rollback safely ignored)

---

## Files

| File | Description |
|------|-------------|
| `batch.go` | BatchWriter + BatchTx implementation |
| `batch_test.go` | Unit tests |
| `BATCH.md` | This documentation |
