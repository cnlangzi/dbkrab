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
│         │              │                    │              │
│         ▼              │                    │              │
│  ┌─────────────────────────────────────────────────────┐  │
│  │           cmdCh (channel - commands)                 │  │
│  │           transactionLoop (single goroutine)          │  │
│  └─────────────────────────────────────────────────────┘  │
│         │              │                    │              │
│         ▼              ▼                    ▼              │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────┐  │
│  │  Query/    │  │ globalTx   │  │  timer.OnTimer()   │  │
│  │  QueryRow  │  │ operations │  │  → Flush command   │  │
│  └────────────┘  └────────────┘  └────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Two Coordination Mechanisms

1. **Channel (cmdCh)**: Serializes all commands to a single goroutine
2. **No lock needed!**: SAVEPOINT is created at Commit time, not BeginTx time

---

## Two Write Patterns

**1. Direct Exec (no transaction)**

```
Exec() → cmdCh → transactionLoop → globalTx → immediate execution → pendingCount++
         ↓
         size threshold reached → Commit globalTx → reset
```

**2. BeginTx + Commit/Rollback (explicit transaction)**

```
BeginTx() → savepointMu.Lock() → savepointMu held
            → Create SAVEPOINT btx_N
            → BatchTx created with savepointName
    ↓
Exec(BatchTx) → buffer to BatchTx.buf (NOT executed yet)
    ↓
Commit(BatchTx) → Execute BatchTx.buf in globalTx
                   → RELEASE SAVEPOINT btx_N
                   → savepointMu.Unlock()
                   → try size-based flush
    OR
Rollback(BatchTx) → ROLLBACK TO SAVEPOINT btx_N (discard changes)
                    → savepointMu.Unlock()
```

---

## SAVEPOINT for Nested Transactions

SQLite supports SAVEPOINT for nested transactions within the global transaction.

### Why SAVEPOINT?

Without SAVEPOINT, if BatchTx B fails and Rollback:
- Would roll back ALL writes including BatchTx A and Direct Exec
- Data loss!

With SAVEPOINT:
- Each BatchTx has its own savepoint
- Failed BatchTx rolls back only to its savepoint
- Other writes remain intact

### SAVEPOINT Commands

```sql
-- Create savepoint for BatchTx
SAVEPOINT btx_1;

-- Execute BatchTx operations
INSERT INTO ...;  -- BatchTx B's operations

-- If BatchTx B fails:
ROLLBACK TO btx_1;  -- Discard only BatchTx B's changes

-- If BatchTx B succeeds:
RELEASE SAVEPOINT btx_1;  -- Make changes permanent
```

---

## Simplified Design: SAVEPOINT at Commit Time

**Key Insight: No lock needed!**

Previous design created SAVEPOINT at BeginTx time, requiring a lock to prevent conflicts.

Simplified design:
- BeginTx: Returns empty BatchTx wrapper (no SAVEPOINT, no lock)
- Commit: Creates SAVEPOINT, executes buffer atomically, releases/rollbacks

**Why no lock needed?**
- All operations happen inside Commit, which is atomic
- SAVEPOINT created inside Commit is naturally isolated
- Other BatchTx/Exec also go through Commit, so no conflict

---

## Global Transaction Lifecycle

```
First write (Direct or BeginTx)
    ↓
globalTx = db.Begin()
    ↓
[If BeginTx]: SAVEPOINT btx_N, set batchActive = true
    ↓
Accumulate writes (Direct Exec + BatchTx Commit)
    ↓
size trigger (≥ BatchSize) AND batchActive = false → Flush
time trigger → Flush (if batchActive = false)
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

// Command sent to transaction goroutine via cmdCh
type Command struct {
	Type string // "Exec", "BeginTx", "BatchCommit", "Flush"

	// For BeginTx
	TxOptions  *sql.TxOptions
	TxResultCh chan<- *BatchTxResult

	// For Exec / BatchCommit
	Query string
	Args  []any
	Buffer []stmt // For BatchCommit (entire buffer at once!)

	// Response channel (each operation has its own)
	ResultCh chan<- Result
}

type Result struct {
	LastResult sql.Result
	LastError  error
	Results    []Result // For batch operations
}

type BatchTxResult struct {
	Tx   *BatchTx
	Error error
}

// BatchTx buffers statements and commits atomically via savepoint
type BatchTx struct {
	writer        *BatchWriter
	savepointName string // e.g., "btx_1", "btx_2"
	buf           []stmt
	done          bool
}

// BatchWriter provides transparent batch writing
type BatchWriter struct {
	*sql.DB

	cfg           BatchConfig
	cmdCh         chan Command

	// Protected by channel ordering (not mutex):
	globalTx     *sql.Tx
	pendingStmts []stmt
	pendingCount int
	lastFlush    time.Time
	timer        *time.Timer
	txCounter    int64
}
```

---

## Batch Commit Must Send Entire Buffer at Once

**Problem if sent one-by-one:**

```
BatchTx has 3 operations in buffer:
  Send op 1 → Timer fires → Flush → globalTx replaced
  Send op 2 → globalTx is not the original one!
  Send op 3 → Data loss!
```

**Solution: Send entire buffer atomically**

```go
func (btx *BatchTx) Commit() error {
	btx.writer.cmdCh <- Command{
		Type:   "BatchCommit",
		Buffer: btx.buf, // Entire buffer at once!
	}
	result := <-btx.writer.cmdCh.ResultCh
	return result.Error
}
```

---

## Error Handling for Batch Operations

Continue executing all statements, record each error:

```go
func (bw *BatchWriter) handleBatchCommit(cmd Command) {
	results := make([]Result, 0, len(cmd.Buffer))
	var commitErr error

	// Execute all statements, record each error
	for _, s := range cmd.Buffer {
		_, err := bw.globalTx.Exec(s.query, s.args...)
		results = append(results, Result{LastError: err})
		if err != nil && commitErr == nil {
			commitErr = err // Record first error but continue
		}
	}

	// After all executed, decide Commit or Rollback
	if commitErr != nil {
		bw.globalTx.Exec("ROLLBACK TO "+cmd.SavepointName)
		cmd.ResultCh <- Result{LastError: commitErr, Results: results}
		return
	}

	bw.globalTx.Exec("RELEASE SAVEPOINT "+cmd.SavepointName)
	cmd.ResultCh <- Result{Results: results}
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

### Explicit Transaction (with SAVEPOINT)

```go
tx, _ := bw.BeginTx(ctx, nil)
tx.Exec("INSERT INTO orders (id, amount) VALUES (1, 100)")
tx.Exec("INSERT INTO items (order_id, product) VALUES (1, 'widget')")
tx.Commit() // Both statements execute atomically within SAVEPOINT
```

### Atomicity Guarantee with SAVEPOINT

```go
tx, _ := bw.BeginTx(ctx, nil)
tx.Exec("INSERT INTO orders ...")
tx.Exec("INSERT INTO items ...")
// If items insert fails, only this BatchTx is rolled back
// Other BatchTxs and Direct Exec remain intact!
tx.Rollback()
```

---

## Channel Protection Only

**Only Channel is needed for serialization:**

| Mechanism | Purpose |
|-----------|---------|
| cmdCh (Channel) | Serializes all commands to single goroutine |

- No mutex needed!
- SAVEPOINT created at Commit time, not BeginTx time
- Commit is atomic - all operations inside happen together

This dual protection ensures:
1. No race conditions on globalTx
2. No savepoint conflicts
3. Timer flush cannot break BatchTx atomicity

---

## Anti-Double-Unlock Protection

When using `defer tx.Rollback()` after `tx.Commit()`:

```go
tx, _ := bw.BeginTx(ctx, nil)
defer func() { _ = tx.Rollback() }()
tx.Exec(...)
tx.Commit() // marks done=true, releases savepoint

// defer's Rollback() is safely ignored (done=true skips unlock)
```

---

## Failure Handling

On flush error:
1. Rollback globalTx
2. Reset globalTx = nil
3. Clear pendingCount
4. Return error

On BatchTx commit error:
1. Execute `ROLLBACK TO savepoint` (only this BatchTx's changes discarded)
2. Release savepoint
3. Return error (other writes remain intact)

---

## Files

| File | Description |
|------|-------------|
| `batch.go` | Mutex-based implementation (legacy) |
| `batch_channel.go` | Channel + SAVEPOINT implementation (recommended) |
| `batch_test.go` | Unit tests |
| `BATCH.md` | This documentation |

---

## Key Design Principles

1. **Channel for serialization**: All globalTx operations go through single goroutine
2. **SAVEPOINT at Commit time**: No lock needed, Commit is atomic
3. **SAVEPOINT for isolation**: Each BatchTx has independent transaction boundary
4. **Atomic batch commit**: BatchTx sends entire buffer at once, not one-by-one
5. **Immediate rollback on error**: BatchCommit failure rolls back immediately (no need to wait for Rollback call)
