package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/dlq"
)

// SinkWriter is the interface for the underlying SQLite sinker.
type SinkWriter interface {
	DatabaseName() string
	DatabaseType() string
	Write(ctx context.Context, ops []core.Sink) error
	Close() error
}

var (
	// ErrSinkerRequired is returned when no sinker is provided
	ErrSinkerRequired = errors.New("sinker is required")

	// ErrBufferFull is returned when buffer is full and synchronous flush fails
	ErrBufferFull = errors.New("buffer is full")

	// ErrShuttingDown is returned when the sink is shutting down
	ErrShuttingDown = errors.New("shutting down")

	// ErrInvalidBatchConfig is returned when batch configuration is invalid
	ErrInvalidBatchConfig = errors.New("invalid batch configuration")

	// State values for concurrency coordination
	stateIdle     int32 = 0
	stateWriting  int32 = 1
	stateFlushing int32 = 2
)

// BatchConfig holds batch sink configuration options.
type BatchConfig struct {
	// SinkWriter is the underlying SQLite sinker to wrap
	SinkWriter SinkWriter

	// BatchSize is the number of CDC transactions per flush.
	// Must be > 0. Default: 10
	BatchSize int

	// BatchIntervalMs is the time threshold in milliseconds to flush.
	// Must be > 0. Default: 100
	BatchIntervalMs int

	// BatchTxTimeoutMs is the maximum time in milliseconds for a single transaction flush.
	// Must be > 0. Default: 30000
	BatchTxTimeoutMs int

	// DLQ is the dead letter queue for failed operations
	DLQ *dlq.DLQ
}

// Validate validates the batch configuration.
func (c *BatchConfig) Validate() error {
	if c.SinkWriter == nil {
		return ErrSinkerRequired
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.BatchIntervalMs <= 0 {
		c.BatchIntervalMs = 500
	}
	if c.BatchTxTimeoutMs <= 0 {
		c.BatchTxTimeoutMs = 30000
	}
	return nil
}

// BatchSQLiteSink wraps a Sinker with batching functionality.
// It buffers Sink operations and flushes them based on size and time thresholds,
// while preserving CDC transaction atomicity.
type BatchSQLiteSink struct {
	config   BatchConfig
	database string

	// mu protects buffer and state
	mu sync.Mutex
	// buffer holds buffered Sink operations
	buffer []core.Sink
	// txBuffer holds CDC transactions grouped by transaction ID
	txBuffer map[string]*core.Transaction
	// pendingTxCount tracks the number of transactions in txBuffer
	pendingTxCount int
	// lastFlush is the time of the last flush
	lastFlush time.Time
	// closed indicates if the sink is closed
	closed bool

	// state atomically tracks current state: idle(0), writing(1), flushing(2)
	// Used to coordinate between Write() and timer-driven flushes
	state int32

	// flushMu serializes flush operations so only one flush executes at a time
	flushMu sync.Mutex
	// flushTimer triggers time-based flush checks
	flushTimer *time.Timer
	// stopCh signals shutdown
	stopCh chan struct{}
	// shutdownCh signals shutdown complete
	shutdownCh chan struct{}
}

// NewBatchSQLiteSink creates a new batch SQLite sink.
func NewBatchSQLiteSink(config BatchConfig) (*BatchSQLiteSink, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	s := &BatchSQLiteSink{
		config:   config,
		database: config.SinkWriter.DatabaseName(),
		buffer:   make([]core.Sink, 0, config.BatchSize),
		txBuffer: make(map[string]*core.Transaction),
		// pendingTxCount starts at 0
		pendingTxCount: 0,
		lastFlush:      time.Now(),
		stopCh:         make(chan struct{}),
		shutdownCh:     make(chan struct{}),
		flushTimer:     time.NewTimer(time.Duration(config.BatchIntervalMs) * time.Millisecond),
	}

	// Start flush goroutine
	go s.flushLoop()

	return s, nil
}

// casState performs compare-and-swap to transition from expected to desired state.
// Returns true if the transition was successful.
func (s *BatchSQLiteSink) casState(expected, desired int32) bool {
	return atomic.CompareAndSwapInt32(&s.state, expected, desired)
}

// setState sets the state directly (use with caution - prefer casState for transitions)
func (s *BatchSQLiteSink) setState(newState int32) {
	atomic.StoreInt32(&s.state, newState)
}

// getState returns the current state
func (s *BatchSQLiteSink) getState() int32 {
	return atomic.LoadInt32(&s.state)
}

// Write buffers a Sink operation for batch writing.
// CDC transaction atomicity is preserved - operations from the same
// transaction are kept together and flushed atomically.
func (s *BatchSQLiteSink) Write(ops []core.Sink) error {
	return s.WriteCtx(context.Background(), ops)
}

// WriteCtx buffers a Sink operation for batch writing with context.
// CDC transaction atomicity is preserved - operations from the same
// transaction are kept together and flushed atomically.
func (s *BatchSQLiteSink) WriteCtx(ctx context.Context, ops []core.Sink) error {
	if len(ops) == 0 {
		return nil
	}

	// Try to acquire writing state
	if !s.casState(stateIdle, stateWriting) {
		return fmt.Errorf("could not acquire write lock, current state: %d", s.getState())
	}
	defer s.setState(stateIdle)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrShuttingDown
	}

	// Group operations by transaction to preserve CDC atomicity
	for _, op := range ops {
		txID := s.extractTransactionID(op)

		if txID != "" {
			// This operation is part of a CDC transaction
			tx, exists := s.txBuffer[txID]
			if !exists {
				tx = core.NewTransaction(txID)
				s.txBuffer[txID] = tx
				s.pendingTxCount++
			}
			// Add the operation's data as changes to the transaction
			for _, row := range op.DataSet.Rows {
				change := core.Change{
					Table:         op.Config.Output,
					TransactionID: txID,
					Operation:     op.OpType,
					Data:          s.rowToMap(op.DataSet.Columns, row),
				}
				tx.AddChange(change)
			}
		} else {
			// Non-transactional operation - add directly to buffer
			s.buffer = append(s.buffer, op)
		}

		// Check if we need to trigger a flush based on size
		if s.shouldFlushLocked() {
			s.mu.Unlock()
			// Note: state is now idle, Flush() can proceed
			if err := s.FlushCtx(ctx); err != nil {
				slog.Error("BatchSQLiteSink.Write: automatic flush failed",
					"error", err)
				return err
			}
			// Re-acquire write state before re-locking mu
			if !s.casState(stateIdle, stateWriting) {
				return fmt.Errorf("could not re-acquire write lock after flush, current state: %d", s.getState())
			}
			s.mu.Lock()
		}
	}

	// Reset the flush timer on each write
	s.resetFlushTimer()

	return nil
}

// resetFlushTimer resets the flush timer to fire after batchIntervalMs
func (s *BatchSQLiteSink) resetFlushTimer() {
	interval := time.Duration(s.config.BatchIntervalMs) * time.Millisecond
	s.flushTimer.Reset(interval)
}

// extractTransactionID extracts the transaction ID from a Sink operation.
// Returns empty string if no transaction ID is found.
func (s *BatchSQLiteSink) extractTransactionID(op core.Sink) string {
	// Check if DataSet has special columns for transaction tracking
	if op.DataSet == nil || len(op.DataSet.Columns) == 0 {
		return ""
	}

	// Look for transaction_id in columns
	for i, col := range op.DataSet.Columns {
		if col == "_transaction_id" || col == "transaction_id" {
			if len(op.DataSet.Rows) > 0 && len(op.DataSet.Rows[0]) > i {
				if txID, ok := op.DataSet.Rows[0][i].(string); ok {
					return txID
				}
			}
		}
	}
	return ""
}

// rowToMap converts a row to a map using column names.
func (s *BatchSQLiteSink) rowToMap(columns []string, row []any) map[string]interface{} {
	result := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		if i < len(row) {
			result[col] = row[i]
		}
	}
	return result
}

// shouldFlushLocked returns true if the buffer should be flushed.
// Must be called with mu held.
func (s *BatchSQLiteSink) shouldFlushLocked() bool {
	return s.pendingTxCount >= s.config.BatchSize
}

// shouldFlushByTimeLocked returns true if enough time has passed since last flush.
// Must be called with mu held.
func (s *BatchSQLiteSink) shouldFlushByTimeLocked() bool {
	elapsed := time.Since(s.lastFlush)
	interval := time.Duration(s.config.BatchIntervalMs) * time.Millisecond
	return elapsed >= interval
}

// Flush flushes all buffered operations to the underlying sink.
// This is thread-safe and can be called concurrently.
// Only one flush can execute at a time due to flushMu.
func (s *BatchSQLiteSink) Flush() error {
	return s.FlushCtx(context.Background())
}

// FlushCtx flushes all buffered operations with the given context.
func (s *BatchSQLiteSink) FlushCtx(ctx context.Context) error {
	// Try to acquire flush lock - only one flush at a time
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	if s.closed && len(s.buffer) == 0 && len(s.txBuffer) == 0 {
		s.mu.Unlock()
		return nil
	}

	// Check state - only flush if idle or if we're already flushing
	currentState := s.getState()
	if currentState == stateFlushing {
		// Another flush is in progress, skip
		s.mu.Unlock()
		return nil
	}
	// If state is writing, we can't flush right now
	if currentState == stateWriting {
		s.mu.Unlock()
		return nil
	}

	// Gather all operations to flush
	ops := s.gatherOperationsLocked()
	txCount := len(s.txBuffer)
	s.mu.Unlock()

	if len(ops) == 0 {
		return nil
	}

	start := time.Now()
	slog.Debug("BatchSQLiteSink.Flush: starting flush",
		"database", s.database,
		"operations", len(ops),
		"transactions", txCount)

	flushCtx, cancel := context.WithTimeout(
		ctx,
		time.Duration(s.config.BatchTxTimeoutMs)*time.Millisecond,
	)
	defer cancel()

	// Attempt to write to the underlying sink with timeout context
	err := s.config.SinkWriter.Write(flushCtx, ops)
	if err != nil {
		slog.Error("BatchSQLiteSink.Flush: write failed",
			"database", s.database,
			"error", err,
			"operations", len(ops))

		// On failure, send to DLQ
		if s.config.DLQ != nil {
			if dlqErr := s.sendToDLQ(ops, err); dlqErr != nil {
				slog.Error("BatchSQLiteSink.Flush: failed to send to DLQ",
					"error", dlqErr)
			}
		}

		// Clear buffer after DLQ send
		s.mu.Lock()
		s.buffer = s.buffer[:0]
		s.txBuffer = make(map[string]*core.Transaction)
		s.pendingTxCount = 0
		s.mu.Unlock()

		return fmt.Errorf("flush write: %w", err)
	}

	// Success - clear the buffers
	s.mu.Lock()
	s.buffer = s.buffer[:0]
	s.txBuffer = make(map[string]*core.Transaction)
	s.pendingTxCount = 0
	s.lastFlush = time.Now()
	s.mu.Unlock()

	duration := time.Since(start).Seconds()
	slog.Info("BatchSQLiteSink.Flush: flush completed",
		"database", s.database,
		"operations", len(ops),
		"transactions", txCount,
		"duration_ms", int(duration*1000))

	return nil
}

// gatherOperationsLocked gathers all operations from buffer and txBuffer.
// Must be called with mu held.
func (s *BatchSQLiteSink) gatherOperationsLocked() []core.Sink {
	var ops []core.Sink

	// Add direct buffer operations
	ops = append(ops, s.buffer...)

	// Convert transaction buffer to sink operations
	// Each transaction becomes a single sink operation
	for _, tx := range s.txBuffer {
		if len(tx.Changes) == 0 {
			continue
		}

		// Group changes by table and operation type
		tableOps := make(map[string]map[core.Operation][]core.Change)
		for _, change := range tx.Changes {
			if tableOps[change.Table] == nil {
				tableOps[change.Table] = make(map[core.Operation][]core.Change)
			}
			tableOps[change.Table][change.Operation] = append(
				tableOps[change.Table][change.Operation],
				change,
			)
		}

		// Create sink operations for each table/op combination
		for table, opMap := range tableOps {
			for opType, changes := range opMap {
				if len(changes) == 0 {
					continue
				}

				// Convert changes back to DataSet format
				columns := s.extractColumns(changes)
				rows := s.extractRows(changes, columns)

				ops = append(ops, core.Sink{
					Config: core.SinkConfig{
						Output:     table,
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: columns,
						Rows:    rows,
					},
					OpType: opType,
				})
			}
		}
	}

	return ops
}

// extractColumns extracts unique column names from changes.
func (s *BatchSQLiteSink) extractColumns(changes []core.Change) []string {
	columnSet := make(map[string]bool)
	for _, change := range changes {
		for col := range change.Data {
			columnSet[col] = true
		}
	}

	columns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		columns = append(columns, col)
	}
	return columns
}

// extractRows converts changes to rows using the specified columns.
func (s *BatchSQLiteSink) extractRows(changes []core.Change, columns []string) [][]any {
	rows := make([][]any, len(changes))
	for i, change := range changes {
		row := make([]any, len(columns))
		for j, col := range columns {
			row[j] = change.Data[col]
		}
		rows[i] = row
	}
	return rows
}

// sendToDLQ sends failed operations to the dead letter queue.
func (s *BatchSQLiteSink) sendToDLQ(ops []core.Sink, originalErr error) error {
	var dlqErrs []error

	for _, op := range ops {
		for rowIdx, row := range op.DataSet.Rows {
			changeData, err := json.Marshal(s.rowToMap(op.DataSet.Columns, row))
			if err != nil {
				dlqErrs = append(dlqErrs, err)
				continue
			}

			entry := &dlq.DLQEntry{
				TraceID:      fmt.Sprintf("batch-%d", time.Now().UnixNano()),
				Source:       "batch_sink",
				LSN:          fmt.Sprintf("%s-%d", op.Config.Output, rowIdx),
				TableName:    op.Config.Output,
				Operation:    op.OpType.String(),
				ChangeData:   string(changeData),
				ErrorMessage: originalErr.Error(),
				RetryCount:   0,
				Status:       dlq.StatusPending,
			}

			if err := s.config.DLQ.Write(entry); err != nil {
				dlqErrs = append(dlqErrs, err)
			}
		}
	}

	if len(dlqErrs) > 0 {
		return errors.Join(dlqErrs...)
	}
	return nil
}

// flushLoop runs the periodic flush check.
func (s *BatchSQLiteSink) flushLoop() {
	for {
		select {
		case <-s.stopCh:
			// Stop signal received - flush remaining and exit
			slog.Debug("BatchSQLiteSink.flushLoop: stop signal received")
			_ = s.Flush() // Best effort, ignore error
			close(s.shutdownCh)
			return

		case <-s.flushTimer.C:
			// Timer fired - attempt flush only if state is idle and buffer non-empty
			if s.getState() == stateIdle {
				s.mu.Lock()
				shouldFlush := s.shouldFlushLocked() || s.shouldFlushByTimeLocked()
				s.mu.Unlock()

				if shouldFlush {
					if err := s.Flush(); err != nil {
						slog.Error("BatchSQLiteSink.flushLoop: timed flush failed",
							"error", err)
					}
				}
			}
			// Reset timer for next interval
			s.resetFlushTimer()
		}
	}
}

// Shutdown gracefully shuts down the batch sink.
// It flushes any remaining buffered operations and closes the underlying sink.
func (s *BatchSQLiteSink) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	// Stop the flush timer
	if !s.flushTimer.Stop() {
		// Timer already fired, drain the channel
		select {
		case <-s.flushTimer.C:
		default:
		}
	}

	// Signal the flush loop to stop
	close(s.stopCh)

	// Wait for shutdown to complete with context
	select {
	case <-s.shutdownCh:
		slog.Debug("BatchSQLiteSink.Shutdown: shutdown completed")
	case <-ctx.Done():
		slog.Warn("BatchSQLiteSink.Shutdown: context cancelled, forcing close")
	}

	// Close the underlying sink
	if s.config.SinkWriter != nil {
		return s.config.SinkWriter.Close()
	}

	return nil
}

// Close closes the batch sink without flushing.
// Use Shutdown for graceful closure.
func (s *BatchSQLiteSink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	// Stop the flush timer
	if !s.flushTimer.Stop() {
		select {
		case <-s.flushTimer.C:
		default:
		}
	}

	// Signal the flush loop to stop
	select {
	case <-s.stopCh:
		// Already stopped
	default:
		close(s.stopCh)
	}

	// Wait for shutdown
	<-s.shutdownCh

	// Close the underlying sink
	if s.config.SinkWriter != nil {
		return s.config.SinkWriter.Close()
	}

	return nil
}

// BufferSize returns the current number of buffered CDC transactions.
func (s *BatchSQLiteSink) BufferSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingTxCount
}

// DatabaseName returns the database name.
func (s *BatchSQLiteSink) DatabaseName() string {
	return s.database
}

// DatabaseType returns the database type.
func (s *BatchSQLiteSink) DatabaseType() string {
	return "sqlite_batch"
}
