package sqlite

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"
)

// BatchConfig holds batch writer configuration.
type BatchConfig struct {
	// BatchSize is the number of statements to buffer before flushing.
	BatchSize int

	// FlushInterval is the maximum time to wait before flushing.
	FlushInterval time.Duration

	// TxTimeout is reserved for future use
	TxTimeout time.Duration
}

// Validate sets defaults.
func (c *BatchConfig) Validate() {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 100 * time.Millisecond
	}
}

// Tx wraps operations for batched transaction.
// It is a drop-in replacement for *sql.Tx.
type Tx struct {
	mu   sync.Mutex
	buf  []stmt
	done bool
	w    *BatchWriter
}

// Exec buffers the query instead of executing immediately.
func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	if tx.done {
		return nil, sql.ErrTxDone
	}
	tx.buf = append(tx.buf, stmt{query: query, args: args})
	return nil, nil
}

// Commit executes all buffered statements in global transaction.
func (tx *Tx) Commit() error {
	if tx.done {
		return sql.ErrTxDone
	}
	tx.done = true

	// Send commit command to BatchWriter
	resultCh := make(chan any, 1)
	tx.w.cmdCh <- Command{Type: "BatchCommit", Buffer: tx.buf, ResultCh: resultCh}
	result := <-resultCh
	tx.buf = nil

	if err, ok := result.(error); ok && err != nil {
		return err
	}
	return nil
}

// Rollback discards buffered statements.
func (tx *Tx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	tx.buf = nil
	return nil
}

// BatchWriter provides transparent batch writing.
type BatchWriter struct {
	*sql.DB
	cfg          BatchConfig
	mu           sync.Mutex
	globalTx     *sql.Tx
	pendingCount int
	cmdCh        chan Command
	timer        *time.Timer
}

// NewBatchWriter creates a BatchWriter.
func NewBatchWriter(db *sql.DB, cfg BatchConfig) *BatchWriter {
	cfg.Validate()
	bw := &BatchWriter{
		DB:    db,
		cfg:   cfg,
		cmdCh: make(chan Command, 100),
	}
	return bw
}

// Close stops the timer.
func (bw *BatchWriter) Close() error {
	if bw.timer != nil {
		bw.timer.Stop()
	}
	if bw.cmdCh != nil {
		close(bw.cmdCh)
	}
	return nil
}

// Exec executes in global transaction.
func (bw *BatchWriter) Exec(query string, args ...any) (sql.Result, error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	if bw.globalTx == nil {
		tx, err := bw.DB.Begin()
		if err != nil {
			return nil, err
		}
		bw.globalTx = tx
	}

	result, err := bw.globalTx.Exec(query, args...)
	if err != nil {
		_ = bw.globalTx.Rollback()
		bw.globalTx = nil
		bw.pendingCount = 0
		return result, err
	}

	bw.pendingCount++
	if bw.pendingCount >= bw.cfg.BatchSize {
		if err := bw.globalTx.Commit(); err != nil {
			slog.Error("BatchWriter.Exec: commit failed", "error", err)
			bw.globalTx = nil
			bw.pendingCount = 0
			return result, err
		}
		bw.globalTx, _ = bw.DB.Begin()
		bw.pendingCount = 0
	}

	return result, nil
}

// BeginTx starts a batched transaction.
func (bw *BatchWriter) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Flush pending first
	if bw.globalTx != nil && bw.pendingCount > 0 {
		if err := bw.globalTx.Commit(); err != nil {
			return nil, err
		}
	}

	// Start new global transaction
	tx, err := bw.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	bw.globalTx = tx
	bw.pendingCount = 0

	return &Tx{w: bw}, nil
}

// BufferLen returns pending count.
func (bw *BatchWriter) BufferLen() int {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.pendingCount
}
