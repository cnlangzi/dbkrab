package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
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

	// TxTimeout is reserved for future use
	// Default: 30s (not currently used)
	TxTimeout time.Duration
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

// Result of an operation.
type Result struct {
	LastResult sql.Result
	LastError  error
}

// Command sent to the transaction goroutine.
type Command struct {
	Type string // "Exec", "BeginTx", "BatchCommit", "Flush"

	// For BeginTx
	TxOptions  *sql.TxOptions
	TxResultCh chan<- *TxResult

	// For Exec / BatchCommit
	Query  string
	Args   []any
	Buffer []stmt // For BatchCommit (entire buffer at once!)

	// Response channel
	ResultCh chan<- Result
}

// TxResult is returned to the caller of BeginTx.
type TxResult struct {
	Tx    *BatchTx
	Error error
}

// TxExec is the interface for transaction executors.
// Both *sql.Tx and *BatchTx implement this interface.
type TxExec interface {
	Exec(query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// BatchTx wraps operations to provide deferred execution until Commit.
// It is a drop-in replacement for *sql.Tx.
type BatchTx struct {
	writer *BatchWriter
	buf    []stmt
	done   bool
}

// Exec buffers the query instead of executing immediately.
func (btx *BatchTx) Exec(query string, args ...any) (sql.Result, error) {
	if btx.done {
		return nil, errors.New("transaction already committed or rolled back")
	}
	btx.buf = append(btx.buf, stmt{query: query, args: args})
	return nil, nil
}

// Commit executes all buffered statements atomically in the global transaction.
func (btx *BatchTx) Commit() error {
	if btx.done {
		return errors.New("transaction already committed or rolled back")
	}

	resultCh := make(chan Result, 1)
	btx.writer.cmdCh <- Command{
		Type:     "BatchCommit",
		Buffer:   btx.buf,
		ResultCh: resultCh,
	}
	result := <-resultCh
	btx.done = true // Mark done AFTER command completes
	btx.buf = nil
	return result.LastError
}

// Rollback discards all buffered statements.
// If already committed (done=true), just return.
func (btx *BatchTx) Rollback() error {
	if btx.done {
		return nil
	}
	btx.done = true
	btx.buf = nil
	// Send empty rollback to release any lock
	doneCh := make(chan Result, 1)
	btx.writer.cmdCh <- Command{Type: "BatchRollback", ResultCh: doneCh}
	<-doneCh
	return nil
}

// BatchWriter provides transparent batch writing with channel-based coordination.
// All writes are serialized through a single goroutine via cmdCh.
type BatchWriter struct {
	*sql.DB
	cfg   BatchConfig
	cmdCh chan Command

	// Global transaction state (only accessed by transaction goroutine)
	globalTx     *sql.Tx
	pendingCount int
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
		cmdCh:        make(chan Command, 100),
		pendingCount: 0,
		lastFlush:    time.Now(),
		timer:        time.NewTimer(cfg.FlushInterval),
	}

	// Start transaction goroutine
	go bw.transactionLoop()

	// Start timer goroutine
	go bw.timerLoop()

	return bw
}

// Close stops the timer and closes the command channel.
func (bw *BatchWriter) Close() error {
	bw.timer.Stop()
	// Signal transactionLoop to exit by closing cmdCh
	close(bw.cmdCh)
	return nil
}

// timerLoop handles time-based flushing.
func (bw *BatchWriter) timerLoop() {
	for {
		select {
		case <-bw.timer.C:
			bw.timer.Reset(bw.cfg.FlushInterval)
			// Non-blocking send to trigger flush
			select {
			case bw.cmdCh <- Command{Type: "Flush"}:
			default:
			}
		case _, ok := <-bw.cmdCh:
			// Channel closed, exit
			if !ok {
				return
			}
		}
	}
}

// transactionLoop runs in a single goroutine, serializing all operations.
func (bw *BatchWriter) transactionLoop() {
	for cmd := range bw.cmdCh {
		switch cmd.Type {
		case "Exec":
			bw.handleExec(cmd)
		case "BeginTx":
			bw.handleBeginTx(cmd)
		case "BatchCommit":
			bw.handleBatchCommit(cmd)
		case "BatchRollback":
			bw.handleBatchRollback(cmd)
		case "Flush":
			bw.handleFlush(cmd)
		}
	}
}

func (bw *BatchWriter) handleExec(cmd Command) {
	// Ensure global transaction exists
	if bw.globalTx == nil {
		tx, err := bw.Begin()
		if err != nil {
			cmd.ResultCh <- Result{LastError: err}
			return
		}
		bw.globalTx = tx
	}

	// Execute immediately
	result, err := bw.globalTx.Exec(cmd.Query, cmd.Args...)
	if err != nil {
		_ = bw.globalTx.Rollback()
		bw.globalTx = nil
		bw.pendingCount = 0
		cmd.ResultCh <- Result{LastResult: result, LastError: err}
		return
	}

	bw.pendingCount++

	// Check size threshold
	if bw.pendingCount >= bw.cfg.BatchSize {
		bw.doFlush()
	}

	cmd.ResultCh <- Result{LastResult: result, LastError: nil}
}

func (bw *BatchWriter) handleBeginTx(cmd Command) {
	// If there's pending data, flush it first
	if bw.globalTx != nil && bw.pendingCount > 0 {
		bw.doFlush()
	}

	// Start fresh global transaction for BatchTx
	tx, err := bw.BeginTx(context.Background(), cmd.TxOptions)
	if err != nil {
		cmd.TxResultCh <- &TxResult{Error: err}
		return
	}
	bw.globalTx = tx

	// Create BatchTx wrapper
	btx := &BatchTx{
		writer: bw,
		buf:    make([]stmt, 0, bw.cfg.BatchSize),
	}

	cmd.TxResultCh <- &TxResult{Tx: btx}
}

func (bw *BatchWriter) handleBatchCommit(cmd Command) {
	if bw.globalTx == nil {
		cmd.ResultCh <- Result{LastError: errors.New("no active transaction")}
		return
	}

	// Execute all buffered statements
	var commitErr error
	for _, s := range cmd.Buffer {
		if _, err := bw.globalTx.Exec(s.query, s.args...); err != nil {
			commitErr = err
			break
		}
	}

	if commitErr != nil {
		// Failed - rollback globalTx and start new one
		_ = bw.globalTx.Rollback()
		bw.globalTx, _ = bw.Begin()
		bw.pendingCount = 0
		cmd.ResultCh <- Result{LastError: commitErr}
		return
	}

	// Success - commit globalTx and start new one
	if err := bw.globalTx.Commit(); err != nil {
		cmd.ResultCh <- Result{LastError: err}
		bw.globalTx, _ = bw.Begin()
		return
	}

	// Start new global transaction for future operations
	bw.globalTx, _ = bw.Begin()
	bw.pendingCount = 0

	cmd.ResultCh <- Result{LastError: nil}
}

func (bw *BatchWriter) handleBatchRollback(cmd Command) {
	// Just acknowledge and release the lock
	if cmd.ResultCh != nil {
		cmd.ResultCh <- Result{}
	}
}

func (bw *BatchWriter) handleFlush(cmd Command) {
	bw.doFlush()
	if cmd.ResultCh != nil {
		cmd.ResultCh <- Result{}
	}
}

func (bw *BatchWriter) doFlush() {
	if bw.globalTx == nil || bw.pendingCount == 0 {
		bw.lastFlush = time.Now()
		return
	}

	if err := bw.globalTx.Commit(); err != nil {
		slog.Error("BatchWriter.doFlush: commit failed", "error", err)
		_ = bw.globalTx.Rollback()
		bw.globalTx = nil
		bw.pendingCount = 0
		return
	}

	// Start new transaction for next batch
	bw.globalTx, _ = bw.Begin()
	bw.pendingCount = 0
	bw.lastFlush = time.Now()
}

// Exec executes a single statement in the global transaction.
func (bw *BatchWriter) Exec(query string, args ...any) (sql.Result, error) {
	resultCh := make(chan Result, 1)
	bw.cmdCh <- Command{
		Type:     "Exec",
		Query:    query,
		Args:     args,
		ResultCh: resultCh,
	}
	result := <-resultCh
	return result.LastResult, result.LastError
}

// BeginTx starts a batched transaction.
//
// IMPORTANT: Before starting the BatchTx, any pending data in the global
// transaction is committed. The BatchTx then operates on a fresh global
// transaction. This ensures that BatchTx failures do not affect prior data.
//
// Returns a *BatchTx that satisfies the sql.Tx interface.
func (bw *BatchWriter) BeginTx(ctx context.Context, opts *sql.TxOptions) (TxExec, error) {
	resultCh := make(chan *TxResult, 1)
	bw.cmdCh <- Command{
		Type:       "BeginTx",
		TxOptions:  opts,
		TxResultCh: resultCh,
	}
	result := <-resultCh
	return result.Tx, result.Error
}

// Flush commits the global transaction if there are pending statements.
func (bw *BatchWriter) Flush() error {
	resultCh := make(chan Result, 1)
	bw.cmdCh <- Command{
		Type:     "Flush",
		ResultCh: resultCh,
	}
	result := <-resultCh
	return result.LastError
}

// BufferLen returns the current number of pending statements.
func (bw *BatchWriter) BufferLen() int {
	// This is approximate since we're using channel-based coordination
	return bw.pendingCount
}
