# BatchSQLiteSink Design

**Batch flushing for SQLite sink to improve TPS by reducing commit frequency**

---

## Motivation

The original SQLite sink opens a new DB transaction for every `Write()` call, causing:
- Frequent commits and WAL fsyncs
- Limited TPS due to I/O overhead

The `BatchSQLiteSink` provides transparent batching inside `pkg/sqlite`, accumulating CDC transactions and committing them in batches.

---

## Architecture

```
Caller (handler/core)
    │
    ▼
BatchSQLiteSink.Write(txOps)
    │
    ├── Group by Transaction ID → txBuffer
    │
    ├── If pendingTxCount >= batchSize
    │       └── Trigger flush
    │
    └── Timer fires (batchIntervalMs elapsed)
            └── Trigger flush (if idle)
    │
    ▼
Sinker.Write(ctx, ops)  ← Single transaction
    │
    ▼
SQLite DB
```

---

## Key Design Decisions

### CDC Transaction Atomicity

CDC transactions (grouped by `transaction_id`) must be **never split** across flushes. This is guaranteed by:

1. Operations are grouped by `transaction_id` into `txBuffer` map
2. A flush only commits when the **entire buffer** is full or timed out
3. Each CDC transaction is treated as a single unit for batch counting

### Concurrency Model

Two mutexes + state machine:

| Mutex | Purpose |
|-------|---------|
| `mu` | Protects buffer, txBuffer, state transitions |
| `flushMu` | Serializes actual flush execution |

**State Machine** (atomic int32):

| State | Value | Transitions |
|-------|-------|-------------|
| `idle` | 0 | → `writing` (on Write), → `flushing` (on timer flush) |
| `writing` | 1 | → `idle` (Write completes) |
| `flushing` | 2 | → `idle` (flush completes) |

**Why state machine?**  
Timer-driven flushes must not interrupt mid-append operations. The state machine ensures:
- Timer only flushes when `state == idle`
- Write only proceeds when `state == idle`

### Timer Coordination

- Timer resets on **every Write()** call
- When timer fires, it checks `state == idle` before flushing
- If a Write is in progress, the timer skip and will be retriggered

---

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `BatchSize` | int | 1000 | CDC transactions per flush |
| `BatchIntervalMs` | int | 500 | Time threshold to flush (ms) |
| `BatchTxTimeoutMs` | int | 30000 | Single flush execution timeout (ms) |
| `DLQ` | *dlq.DLQ | nil | Dead letter queue for failures |

---

## Failure Handling

On flush error:

1. **Rollback**: SQLite transaction rolls back automatically
2. **DLQ Enqueue**: Entire batch (all CDC transactions in that flush) is enqueued to `internal/dlq`
3. **Buffer Clear**: Buffer is cleared after DLQ enqueue

```
Flush Error
    │
    ▼
Rollback SQLite Tx
    │
    ▼
Enqueue batch to DLQ
    │
    ▼
Clear buffer
```

---

## Lifecycle

### Shutdown

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
sink.Shutdown(ctx)
```

1. Set `closed = true`
2. Stop flush timer
3. Signal flush loop to stop
4. **Final flush** of remaining buffer
5. Close underlying sinker

### Close

```go
sink.Close()
```

Forces shutdown **without** flushing remaining buffer.

---

## API

```go
// Create batch sink
sink, err := NewBatchSQLiteSink(BatchConfig{
    Sinker:           underlyingSinker,
    BatchSize:        10,
    BatchIntervalMs:  100,
    BatchTxTimeoutMs: 30000,
    DLQ:              dlqStore,
})

// Write (same interface as regular sink)
err := sink.Write(ops)

// Manual flush
err := sink.Flush()

// Graceful shutdown
err := sink.Shutdown(ctx)

// Close (no flush)
err := sink.Close()
```

---

## Usage Example

```go
// Create underlying sinker
sinker, err := sqlite.NewSinker(sqlite.Config{
    Name: "mydb",
    File: "/path/to/db.sqlite",
})

// Wrap with batch sink
batchSink, err := sqlite.NewBatchSQLiteSink(sqlite.BatchConfig{
    Sinker:          sinker,
    BatchSize:       10,        // Flush every 10 CDC transactions
    BatchIntervalMs: 100,       // Or every 100ms
    BatchTxTimeoutMs: 30000,   // 30s timeout per flush
    DLQ:             dlqStore, // Handle failures
})

// Use like normal sink
err = batchSink.Write(ops)
```

---

## Files

| File | Description |
|------|-------------|
| `internal/sinker/sqlite/batch.go` | BatchSQLiteSink implementation |
