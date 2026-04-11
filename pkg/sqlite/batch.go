package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// BatchConfig holds batch writer configuration.
type BatchConfig struct {
	// BatchSize is the number of statements to buffer before flushing.
	// Default: 100
	BatchSize int

	// FlushInterval is the maximum time to wait before flushing.
	// Default: 100ms
	FlushInterval time.Duration
}

// Validate sets defaults and validates the configuration.
func (c *BatchConfig) Validate() {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 100 * time.Millisecond
	}
}

// stmt represents a buffered statement.
type stmt struct {
	query string
	args  []any
}

// TxExec is the interface for transaction executors.
// Both *sql.Tx and *BatchTx implement this interface.
type TxExec interface {
	Exec(query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// BatchTx wraps sql.Tx to provide deferred execution until Commit.
// It is a drop-in replacement for *sql.Tx: call BeginTx to get a BatchTx,
// Exec to buffer statements, and Commit to execute them atomically.
type BatchTx struct {
	writer *BatchWriter // reference to BatchWriter
	buf    []stmt       // buffered statements
	done   bool         // committed or rolled back
}

// Exec buffers the query instead of executing immediately.
func (btx *BatchTx) Exec(query string, args ...any) (sql.Result, error) {
	if btx.done {
		return nil, errors.New("transaction already committed or rolled back")
	}
	btx.buf = append(btx.buf, stmt{query: query, args: args})
	return nil, nil
}

// Commit executes all buffered statements in the global transaction,
// commits the transaction, and starts a new one for future operations.
// On failure, the global transaction is rolled back and a new one is started.
func (btx *BatchTx) Commit() error {
	if btx.done {
		return errors.New("transaction already committed or rolled back")
	}
	btx.done = true

	bw := btx.writer

	// Execute buffered statements in global transaction
	for _, s := range btx.buf {
		if _, err := bw.globalTx.Exec(s.query, s.args...); err != nil {
			// Failed - rollback globalTx and start new one
			_ = bw.globalTx.Rollback()
			bw.globalTx, _ = bw.DB.Begin()
			bw.pendingCount = 0
			btx.buf = nil
			bw.mu.Unlock()
			return err
		}
	}
	btx.buf = nil

	// Commit the global transaction to make data visible
	if err := bw.globalTx.Commit(); err != nil {
		_ = bw.globalTx.Rollback()
		bw.globalTx, _ = bw.DB.Begin()
		bw.pendingCount = 0
		bw.mu.Unlock()
		return err
	}

	// Start new global transaction for future operations
	bw.globalTx, _ = bw.DB.Begin()
	bw.pendingCount = 0

	bw.mu.Unlock()
	return nil
}

// Rollback discards all buffered statements.
// No actual work needed since statements were never executed.
func (btx *BatchTx) Rollback() error {
	if btx.done {
		return nil
	}
	btx.done = true
	btx.buf = nil
	bw.mu.Unlock()
	return nil
}

// BatchWriter wraps *sql.DB and provides transparent batch writing.
// All writes accumulate in a global transaction and are executed together
// when size or time threshold is reached.
type BatchWriter struct {
	*sql.DB // embedded: BatchWriter is-a sql.DB for Query/QueryRow etc

	cfg BatchConfig
	mu  sync.Mutex // Mutex allows Unlock from different goroutine

	// Global transaction state
	globalTx     *sql.Tx // lazily created
	pendingCount int     // accumulated count
	lastFlush    time.Time

	// Flush control
	timer *time.Timer
}

// NewBatchWriter creates a BatchWriter wrapping the provided *sql.DB.
func NewBatchWriter(db *sql.DB, cfg BatchConfig) *BatchWriter {
	cfg.Validate()
	bw := &BatchWriter{
		DB:           db,
		cfg:          cfg,
		pendingCount: 0,
		lastFlush:    time.Now(),
	}
	bw.timer = time.AfterFunc(cfg.FlushInterval, bw.onTimer)
	return bw
}

// onTimer is called when the flush timer fires.
func (bw *BatchWriter) onTimer() {
	// Use TryLock to avoid blocking if lock is held by BeginTx
	if !bw.mu.TryLock() {
		bw.timer.Reset(bw.cfg.FlushInterval)
		return
	}
	defer bw.mu.Unlock()

	if bw.globalTx != nil && bw.pendingCount > 0 {
		if err := bw.flushLocked(); err != nil {
			slog.Error("BatchWriter.onTimer: flush failed", "error", err)
		}
	}

	bw.timer.Reset(bw.cfg.FlushInterval)
}

// flushLocked commits the global transaction and resets.
// Must be called with mu held.
func (bw *BatchWriter) flushLocked() error {
	if bw.globalTx == nil {
		bw.pendingCount = 0
		bw.lastFlush = time.Now()
		return nil
	}

	if err := bw.globalTx.Commit(); err != nil {
		_ = bw.globalTx.Rollback()
		bw.globalTx = nil
		bw.pendingCount = 0
		return err
	}

	bw.globalTx = nil
	bw.pendingCount = 0
	bw.lastFlush = time.Now()
	return nil
}

// Exec executes a query in the global transaction.
func (bw *BatchWriter) Exec(query string, args ...any) (sql.Result, error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Ensure global transaction exists
	if bw.globalTx == nil {
		tx, err := bw.Begin()
		if err != nil {
			return nil, err
		}
		bw.globalTx = tx
	}

	// Execute immediately in global transaction
	result, err := bw.globalTx.Exec(query, args...)
	if err != nil {
		_ = bw.globalTx.Rollback()
		bw.globalTx = nil
		bw.pendingCount = 0
		return nil, err
	}

	bw.pendingCount++
	bw.tryFlushLocked()
	return result, nil
}

// tryFlushLocked attempts to flush if size threshold is reached.
// Must be called with mu held.
func (bw *BatchWriter) tryFlushLocked() {
	if bw.pendingCount >= bw.cfg.BatchSize {
		if err := bw.flushLocked(); err != nil {
			slog.Error("BatchWriter.tryFlushLocked: flush failed", "error", err)
		}
	}
}

// BeginTx starts a batched transaction.
//
// IMPORTANT: Before starting the BatchTx, any pending data in the global
// transaction is committed. The BatchTx then operates on a fresh global
// transaction. This ensures that BatchTx failures do not affect prior data.
//
// The lock is held until Commit or Rollback is called, ensuring atomicity:
// time-based flush cannot interrupt the BatchTx's operations.
func (bw *BatchWriter) BeginTx(ctx context.Context, opts *sql.TxOptions) (TxExec, error) {
	bw.mu.Lock()

	// If there's pending data, flush it first before starting BatchTx
	if bw.globalTx != nil && bw.pendingCount > 0 {
		if err := bw.globalTx.Commit(); err != nil {
			_ = bw.globalTx.Rollback()
		}
		bw.globalTx = nil
		bw.pendingCount = 0
	}

	// Start fresh global transaction for BatchTx
	tx, err := bw.DB.BeginTx(ctx, opts)
	if err != nil {
		bw.mu.Unlock()
		return nil, err
	}
	bw.globalTx = tx

	// Create BatchTx - lock is held until Commit/Rollback
	btx := &BatchTx{
		writer: bw,
		buf:    make([]stmt, 0, bw.cfg.BatchSize),
	}

	return btx, nil
}

// Flush commits the global transaction if there are pending statements.
func (bw *BatchWriter) Flush() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.flushLocked()
}

// BufferLen returns the current number of pending statements.
func (bw *BatchWriter) BufferLen() int {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.pendingCount
}

// Close stops the timer.
func (bw *BatchWriter) Close() error {
	if bw.timer != nil {
		bw.timer.Stop()
	}
	return nil
}
