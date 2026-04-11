package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// BatchConfig holds batch writer configuration.
type BatchConfig struct {
	BatchSize     int           // Default: 100
	FlushInterval time.Duration // Default: 100ms
	TxTimeout     time.Duration // Default: 30s
}

func (c *BatchConfig) Validate() {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 100 * time.Millisecond
	}
	if c.TxTimeout <= 0 {
		c.TxTimeout = 30 * time.Second
	}
}

// stmt represents a buffered statement.
type stmt struct {
	query string
	args  []any
}

// Result of a command execution.
type Result struct {
	LastResult sql.Result
	LastError  error
	Results    []Result // For batch operations
}

// Command sent to the transaction goroutine.
type Command struct {
	Type string // "Exec", "BeginTx", "BatchCommit", "Flush"

	// For BeginTx
	TxOptions  *sql.TxOptions
	TxResultCh chan<- *BatchTxResult

	// For Exec / BatchCommit
	Query string
	Args  []any
	Buffer []stmt // For BatchCommit (entire buffer at once!)

	// Response channel
	ResultCh chan<- Result
}

// BatchTxResult is returned to the caller of BeginTx.
type BatchTxResult struct {
	Tx   *BatchTx
	Error error
}

// BatchWriter provides transparent batch writing with channel-based coordination.
// All writes are serialized through a single goroutine via cmdCh.
type BatchWriter struct {
	*sql.DB
	cfg   BatchConfig
	cmdCh chan Command
	done  chan struct{} // Signal goroutines to stop

	// Protected by channel ordering (not mutex):
	globalTx     *sql.Tx
	pendingStmts []stmt
	pendingCount uint64
	lastFlush    time.Time
	timer        *time.Timer
	txCounter    int64
}

// NewBatchWriter creates a new BatchWriter.
func NewBatchWriter(db *sql.DB, cfg BatchConfig) *BatchWriter {
	cfg.Validate()
	bw := &BatchWriter{
		DB:           db,
		cfg:          cfg,
		cmdCh:        make(chan Command, 100),
		done:         make(chan struct{}),
		pendingStmts: make([]stmt, 0, cfg.BatchSize),
		timer:        time.NewTimer(cfg.FlushInterval),
		lastFlush:    time.Now(),
	}

	// Start transaction goroutine
	go bw.transactionLoop()

	// Start timer goroutine
	go bw.timerLoop()

	return bw
}

// Close stops the batch writer.
func (bw *BatchWriter) Close() error {
	bw.timer.Stop()
	close(bw.done) // Signal goroutines to stop
	close(bw.cmdCh) // Signal transaction goroutine to stop

	// Drain pending if any
	if bw.globalTx != nil && len(bw.pendingStmts) > 0 {
		if err := bw.globalTx.Commit(); err != nil {
			slog.Error("BatchWriter.Close: commit failed", "error", err)
			_ = bw.globalTx.Rollback()
		}
		bw.globalTx = nil
	}
	return nil
}

// transactionLoop runs in a single goroutine, serializing all transaction operations.
func (bw *BatchWriter) transactionLoop() {
	for cmd := range bw.cmdCh {
		switch cmd.Type {
		case "BeginTx":
			bw.handleBeginTx(cmd)
		case "Exec":
			bw.handleExec(cmd)
		case "BatchCommit":
			bw.handleBatchCommit(cmd)
		case "Flush":
			bw.handleFlush(cmd)
		}
	}
}

// BufferLen returns the number of pending statements.
func (bw *BatchWriter) BufferLen() int {
	return int(atomic.LoadUint64(&bw.pendingCount))
}

func (bw *BatchWriter) handleBeginTx(cmd Command) {
	// Ensure global transaction exists
	if bw.globalTx == nil {
		tx, err := bw.Begin()
		if err != nil {
			cmd.TxResultCh <- &BatchTxResult{Error: err}
			return
		}
		bw.globalTx = tx
	}

	// Create BatchTx wrapper (no SAVEPOINT created yet - created at Commit time)
	btx := &BatchTx{
		writer: bw,
		buf:    make([]stmt, 0),
	}

	cmd.TxResultCh <- &BatchTxResult{Tx: btx}
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
		if rbErr := bw.globalTx.Rollback(); rbErr != nil {
			slog.Error("BatchWriter.handleExec: rollback failed", "error", rbErr)
		}
		bw.globalTx = nil
		atomic.StoreUint64(&bw.pendingCount, 0)
		cmd.ResultCh <- Result{LastResult: result, LastError: err}
		return
	}

	bw.pendingStmts = append(bw.pendingStmts, stmt{query: cmd.Query, args: cmd.Args})
	atomic.AddUint64(&bw.pendingCount, 1)

	// Check size threshold
	if atomic.LoadUint64(&bw.pendingCount) >= uint64(bw.cfg.BatchSize) {
		bw.doFlush()
	}

	cmd.ResultCh <- Result{LastResult: result, LastError: nil}
}

func (bw *BatchWriter) handleBatchCommit(cmd Command) {
	if bw.globalTx == nil {
		cmd.ResultCh <- Result{LastError: errors.New("no active transaction")}
		return
	}

	// Generate unique savepoint name
	savepointName := fmt.Sprintf("btx_%d", atomic.AddInt64(&bw.txCounter, 1))

	// Create SAVEPOINT first
	_, err := bw.globalTx.Exec("SAVEPOINT " + savepointName)
	if err != nil {
		cmd.ResultCh <- Result{LastError: fmt.Errorf("create savepoint failed: %w", err)}
		return
	}

	results := make([]Result, 0, len(cmd.Buffer))
	var commitErr error

	// Execute all buffered statements
	for _, s := range cmd.Buffer {
		_, err := bw.globalTx.Exec(s.query, s.args...)
		results = append(results, Result{LastResult: nil, LastError: err})
		if err != nil && commitErr == nil {
			commitErr = err // Record first error but continue
		}
	}

	// After all executed, decide Commit or Rollback
	if commitErr != nil {
		// Immediate rollback - don't wait for user to call Rollback
		if _, rollbackErr := bw.globalTx.Exec("ROLLBACK TO " + savepointName); rollbackErr != nil {
			slog.Error("BatchWriter.handleBatchCommit: rollback failed", "error", rollbackErr)
		}
		cmd.ResultCh <- Result{LastError: commitErr, Results: results}
		return
	}

	// Success - release savepoint
	if _, releaseErr := bw.globalTx.Exec("RELEASE SAVEPOINT " + savepointName); releaseErr != nil {
		slog.Error("BatchWriter.handleBatchCommit: release savepoint failed", "error", releaseErr)
		cmd.ResultCh <- Result{LastError: releaseErr, Results: results}
		return
	}

	// Commit globalTx to make BatchTx changes permanent
	if err := bw.globalTx.Commit(); err != nil {
		slog.Error("BatchWriter.handleBatchCommit: commit failed", "error", err)
		cmd.ResultCh <- Result{LastError: err, Results: results}
		return
	}

	// Start new globalTx for next batch
	bw.globalTx, err = bw.Begin()
	if err != nil {
		slog.Error("BatchWriter.handleBatchCommit: begin failed", "error", err)
		cmd.ResultCh <- Result{LastError: err, Results: results}
		return
	}

	cmd.ResultCh <- Result{Results: results}
}

func (bw *BatchWriter) handleFlush(cmd Command) {
	bw.doFlush()
	if cmd.ResultCh != nil {
		cmd.ResultCh <- Result{}
	}
}

func (bw *BatchWriter) doFlush() {
	if bw.globalTx == nil || atomic.LoadUint64(&bw.pendingCount) == 0 {
		bw.lastFlush = time.Now()
		return
	}

	if err := bw.globalTx.Commit(); err != nil {
		slog.Error("BatchWriter.doFlush: commit failed", "error", err)
		if rbErr := bw.globalTx.Rollback(); rbErr != nil {
			slog.Error("BatchWriter.doFlush: rollback failed", "error", rbErr)
		}
		bw.globalTx = nil
		bw.pendingStmts = bw.pendingStmts[:0]
		atomic.StoreUint64(&bw.pendingCount, 0)
		return
	}

	// Start new transaction for next batch
	tx, err := bw.Begin()
	if err != nil {
		slog.Error("BatchWriter.doFlush: begin failed", "error", err)
		bw.globalTx = nil
		bw.pendingStmts = bw.pendingStmts[:0]
		atomic.StoreUint64(&bw.pendingCount, 0)
		return
	}
	bw.globalTx = tx

	bw.pendingStmts = bw.pendingStmts[:0]
	atomic.StoreUint64(&bw.pendingCount, 0)
	bw.lastFlush = time.Now()
}

// BeginTx starts a new batch transaction.
// Returns a BatchTx that buffers all Exec calls until Commit.
func (bw *BatchWriter) BeginTx(ctx context.Context, opts *sql.TxOptions) (*BatchTx, error) {
	resultCh := make(chan *BatchTxResult, 1)
	bw.cmdCh <- Command{
		Type:       "BeginTx",
		TxOptions:  opts,
		TxResultCh: resultCh,
	}

	result := <-resultCh
	return result.Tx, result.Error
}

// Flush triggers an immediate flush of pending statements.
func (bw *BatchWriter) Flush() error {
	resultCh := make(chan Result, 1)
	bw.cmdCh <- Command{
		Type:     "Flush",
		ResultCh: resultCh,
	}
	result := <-resultCh
	return result.LastError
}

// Exec executes a single statement in the global transaction.
// For batch operations, use BatchTx.
func (bw *BatchWriter) Exec(query string, args ...any) (sql.Result, error) {
	resultCh := make(chan Result, 1)
	bw.cmdCh <- Command{
		Type:    "Exec",
		Query:   query,
		Args:    args,
		ResultCh: resultCh,
	}
	result := <-resultCh
	return result.LastResult, result.LastError
}

// TxExec is the interface for batch transactions.
type TxExec interface {
	Exec(query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// BatchTx buffers statements and commits them atomically via SAVEPOINT.
// No lock needed - all operations happen inside Commit which is atomic.
type BatchTx struct {
	writer *BatchWriter
	buf    []stmt
	done   bool
}

// Exec adds a statement to the buffer.
func (btx *BatchTx) Exec(query string, args ...any) (sql.Result, error) {
	if btx.done {
		return nil, errors.New("transaction already committed or rolled back")
	}
	btx.buf = append(btx.buf, stmt{query: query, args: args})
	// Return dummy result - real result comes at Commit
	return nil, nil
}

// Commit sends all buffered statements to be committed atomically via SAVEPOINT.
// SAVEPOINT is created here, not at BeginTx time.
// On failure, immediately rolls back (no need to wait for Rollback call).
func (btx *BatchTx) Commit() error {
	if btx.done {
		return errors.New("transaction already committed or rolled back")
	}
	btx.done = true

	resultCh := make(chan Result, 1)
	btx.writer.cmdCh <- Command{
		Type:   "BatchCommit",
		Buffer: btx.buf,
		ResultCh: resultCh,
	}

	result := <-resultCh
	if result.LastError != nil {
		btx.done = false // Allow retry if user wants
		return result.LastError
	}
	return nil
}

// Rollback discards all buffered statements.
// No actual work needed since SAVEPOINT is only created at Commit time.
func (btx *BatchTx) Rollback() error {
	if btx.done {
		return nil
	}
	btx.done = true
	btx.buf = nil
	return nil
}

// timerLoop listens to the timer channel and triggers flushes.
func (bw *BatchWriter) timerLoop() {
	for {
		select {
		case <-bw.timer.C:
			bw.timer.Reset(bw.cfg.FlushInterval)

			resultCh := make(chan Result, 1)
			select {
			case bw.cmdCh <- Command{Type: "Flush", ResultCh: resultCh}:
				<-resultCh // Wait for flush to complete
			default:
				// Channel full, skip this trigger
			}
		case <-bw.done:
			return // Exit when Close is called
		}
	}
}

// OnTimer is called when the flush timer fires.
func (bw *BatchWriter) OnTimer() {
	bw.timer.Reset(bw.cfg.FlushInterval)

	resultCh := make(chan Result, 1)
	select {
	case bw.cmdCh <- Command{Type: "Flush", ResultCh: resultCh}:
		<-resultCh // Wait for flush to complete
	default:
		// Channel full, skip this trigger
	}
}


