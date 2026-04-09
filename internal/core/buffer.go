package core

import (
	"log/slog"
	"sync"
	"time"
)

// DeliveryReason indicates why a transaction was delivered
type DeliveryReason string

const (
	DeliveryReasonCommitTime   DeliveryReason = "commit_time"    // All involved tables indicated transaction complete
	DeliveryReasonPollInterval DeliveryReason = "poll_interval"  // Poll interval gating safety boundary
	DeliveryReasonTimeout      DeliveryReason = "timeout"        // maxWaitTime timeout fallback
	DeliveryReasonBatchLimit   DeliveryReason = "batch_limit"   // Batch size limits reached

	// pollIntervalSafetyMultiplier is the multiplier for poll-interval gating
	// 2x provides a sliding safety window to account for cross-table changes
	// that may arrive in different poll cycles
	pollIntervalSafetyMultiplier = 2.0
)

// TransactionBuffer holds pending transactions waiting for completion
type TransactionBuffer struct {
	mu                 sync.RWMutex
	pending            map[string]*pendingTransaction
	maxWaitTime        time.Duration
	pollInterval       time.Duration               // Derived from CDC poll configuration
	maxTxCount         int                         // maximum transactions per batch
	maxBatchBytes      int                         // maximum estimated bytes per batch
	estimatedBytes     int                         // current estimated batch size in bytes
	onComplete         func(*Transaction)
	onTimeout          func(*Transaction, DeliveryReason)
	cleanupTicker      *time.Ticker
	stopCh             chan struct{}
	tableMaxCommitTime map[string]time.Time         // Per-table max commit time
}

// pendingTransaction holds changes for an in-flight transaction
type pendingTransaction struct {
	transactionID string
	changes       []Change
	firstSeen     time.Time
	commitTime    time.Time              // Transaction commit time (from first change's CommitTime)
	tables        map[string]bool        // Track which tables have changes for this transaction
	complete      bool
	estimatedSize int                    // Estimated size of this transaction in bytes
}

// NewTransactionBuffer creates a new transaction buffer
// pollInterval is derived from CDC poll configuration and used for commit-time gating
func NewTransactionBuffer(maxWaitTime time.Duration, pollInterval time.Duration, maxTxCount int, maxBatchBytes int) *TransactionBuffer {
	// Normalize zero/negative values to "no limit"
	if maxTxCount <= 0 {
		maxTxCount = int(^uint(0) >> 1) // Max int ( effectively unlimited)
	}
	if maxBatchBytes <= 0 {
		maxBatchBytes = int(^uint(0) >> 1) // Max int (effectively unlimited)
	}
	// pollInterval of 0 means no poll-interval gating
	if pollInterval < 0 {
		pollInterval = 0
	}

	tb := &TransactionBuffer{
		pending:            make(map[string]*pendingTransaction),
		maxWaitTime:        maxWaitTime,
		pollInterval:       pollInterval,
		maxTxCount:         maxTxCount,
		maxBatchBytes:      maxBatchBytes,
		stopCh:             make(chan struct{}),
		tableMaxCommitTime: make(map[string]time.Time),
	}

	// Start cleanup ticker
	if maxWaitTime > 0 {
		tb.cleanupTicker = time.NewTicker(maxWaitTime / 2)
		go tb.cleanupLoop()
	}

	slog.Info("transaction buffer initialized",
		"max_wait_time", maxWaitTime,
		"poll_interval", pollInterval,
		"max_transactions_per_batch", maxTxCount,
		"max_batch_bytes", maxBatchBytes)

	return tb
}

// Add adds a change to the buffer.
// Non-transactional changes (empty TransactionID) are delivered immediately.
func (tb *TransactionBuffer) Add(change Change) {
	// Non-transactional changes are delivered immediately
	if change.TransactionID == "" {
		tb.deliverNonTransactional(change)
		return
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	txID := change.TransactionID

	pending, exists := tb.pending[txID]
	if !exists {
		// Initialize commitTime from change, or default to firstSeen if not available
		commitTime := change.CommitTime
		if commitTime.IsZero() {
			commitTime = time.Now()
		}
		pending = &pendingTransaction{
			transactionID: txID,
			changes:       make([]Change, 0),
			firstSeen:     time.Now(),
			commitTime:    commitTime, // Default to now if Change.CommitTime is zero
			tables:        make(map[string]bool),
		}
		tb.pending[txID] = pending
	}

	// Update commit time if this change is older (we want the earliest commit time)
	// since all changes in a transaction should have the same commit time
	if !change.CommitTime.IsZero() && (pending.commitTime.IsZero() || change.CommitTime.Before(pending.commitTime)) {
		pending.commitTime = change.CommitTime
	}

	// Estimate change size
	changeSize := estimateChangeSize(change)
	pending.estimatedSize += changeSize
	tb.estimatedBytes += changeSize

	// Add change
	pending.changes = append(pending.changes, change)
	pending.tables[change.Table] = true

	// Update per-table max commit time
	if !change.CommitTime.IsZero() {
		if existing, ok := tb.tableMaxCommitTime[change.Table]; !ok || change.CommitTime.After(existing) {
			tb.tableMaxCommitTime[change.Table] = change.CommitTime
		}
	}
}

// deliverNonTransactional immediately delivers a non-transactional change
func (tb *TransactionBuffer) deliverNonTransactional(change Change) {
	tx := &Transaction{
		TraceID:        generateTraceID(),
		ID:             generateTraceID(),
		Changes:        []Change{change},
		CreatedAt:      time.Now(),
		CommitTime:     change.CommitTime,
		FirstSeenTime:  time.Now(),
		InvolvedTables: []string{change.Table},
	}

	slog.Debug("delivering non-transactional change immediately",
		"table", change.Table,
		"trace_id", tx.TraceID)

	if tb.onComplete != nil {
		tb.onComplete(tx)
	}
}

// estimateChangeSize estimates the memory size of a change in bytes
func estimateChangeSize(change Change) int {
	// Rough estimation: table name + operation + basic overhead
	size := len(change.Table) + len(change.TransactionID) + 64 // base overhead
	// Add data map size (rough approximation)
	for k := range change.Data {
		size += len(k) + 64 // key + estimated value size
	}
	return size
}

// GetCompleteTransactions returns transactions that are ready for delivery
// Delivery conditions (OR):
// 1. All involved tables have max(tableCommitTime) > tx.CommitTime (commit-time gating)
// 2. now.Sub(tx.CommitTime) > pollInterval * 2 (poll-interval gating safety boundary)
// 3. Batch limits reached
func (tb *TransactionBuffer) GetCompleteTransactions() []*Transaction {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	var complete []*Transaction
	var toRemove []string

	// Track batch size to enforce per-batch limits (not global buffer size)
	batchTxCount := 0
	batchBytes := 0

	for txID, pending := range tb.pending {
		reason := tb.shouldDeliver(pending, now)

		// Check batch limits only if not already delivering for other reasons
		if reason == "" {
			// Check if adding this transaction would exceed batch limits
			// Use > (not >=) to allow exactly reaching the limit
			if batchTxCount >= tb.maxTxCount {
				reason = DeliveryReasonBatchLimit
			} else if batchBytes+pending.estimatedSize > tb.maxBatchBytes {
				reason = DeliveryReasonBatchLimit
			}
		}

		if reason != "" {
			tx := tb.buildTransaction(pending)
			if tx != nil {
				if tb.onTimeout != nil {
					tb.onTimeout(tx, reason)
				}
				complete = append(complete, tx)
				toRemove = append(toRemove, txID)
				// Update batch counters and subtract from global estimated bytes
				batchTxCount++
				batchBytes += pending.estimatedSize
				tb.estimatedBytes -= pending.estimatedSize
			}
		}
	}

	// Remove delivered transactions and clean up table tracking
	for _, txID := range toRemove {
		delete(tb.pending, txID)
	}

	return complete
}

// shouldDeliver checks if a pending transaction should be delivered
// Returns empty string if not ready, or the DeliveryReason if ready
func (tb *TransactionBuffer) shouldDeliver(pending *pendingTransaction, now time.Time) DeliveryReason {
	// Already marked complete
	if pending.complete {
		return DeliveryReasonPollInterval // Use poll-interval as generic "marked complete" reason
	}

	// NOTE: maxWaitTime timeout fallback removed - poll-interval gating is sufficient
	// Transactions are delivered either by commit-time gating (Condition 1) or
	// poll-interval gating (Condition 2), both derived from CDC poll configuration

	// Condition 2: now.Sub(tx.CommitTime) > pollInterval * safetyMultiplier (poll-interval gating)
	// This is a safety boundary: even if other tables haven't confirmed,
	// we deliver after pollInterval * 2 has passed since commit
	if tb.pollInterval > 0 {
		safetyThreshold := tb.pollInterval * pollIntervalSafetyMultiplier
		if now.Sub(pending.commitTime) > safetyThreshold {
			slog.Debug("transaction ready for delivery by poll-interval gating",
				"tx_id", pending.transactionID,
				"commit_time", pending.commitTime,
				"age_since_commit", now.Sub(pending.commitTime),
				"poll_interval", tb.pollInterval,
				"safety_threshold", safetyThreshold)
			return DeliveryReasonPollInterval
		}
	}

	// Condition 1: All involved tables have max(tableCommitTime) > tx.CommitTime
	// This means all tables that had changes in this transaction have reported
	// a commit time AFTER our transaction's commit time, indicating completeness
	if tb.checkCommitTimeComplete(pending) {
		slog.Debug("transaction ready for delivery by commit-time gating",
			"tx_id", pending.transactionID,
			"commit_time", pending.commitTime,
			"involved_tables", len(pending.tables))
		return DeliveryReasonCommitTime
	}

	return ""
}

// checkCommitTimeComplete returns true if all involved tables have reported
// a commit time greater than this transaction's commit time
func (tb *TransactionBuffer) checkCommitTimeComplete(pending *pendingTransaction) bool {
	if pending.commitTime.IsZero() {
		// No commit time information, can't use commit-time gating
		return false
	}

	// Only consider tables that actually produced changes for this transaction
	for table := range pending.tables {
		tableMaxCommit, ok := tb.tableMaxCommitTime[table]
		if !ok {
			// Table has changes in this transaction but has no max commit time yet
			// This means the table hasn't reported any commits after this transaction
			slog.Debug("table has changes but no max commit time yet",
				"tx_id", pending.transactionID,
				"table", table,
				"tx_commit_time", pending.commitTime)
			return false
		}

		// If this table's max commit time is NOT after the transaction's commit time,
		// the table hasn't confirmed the transaction yet
		if !tableMaxCommit.After(pending.commitTime) {
			slog.Debug("table max commit time not after transaction commit time",
				"tx_id", pending.transactionID,
				"table", table,
				"table_max_commit", tableMaxCommit,
				"tx_commit_time", pending.commitTime)
			return false
		}
	}

	return true
}

// SetComplete marks a transaction as complete and returns it for delivery
func (tb *TransactionBuffer) SetComplete(txID string) *Transaction {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	pending, exists := tb.pending[txID]
	if !exists {
		return nil
	}

	pending.complete = true
	tx := tb.buildTransaction(pending)
	delete(tb.pending, txID)
	tb.estimatedBytes -= pending.estimatedSize

	return tx
}

// buildTransaction creates a Transaction from pending changes
func (tb *TransactionBuffer) buildTransaction(pending *pendingTransaction) *Transaction {
	if len(pending.changes) == 0 {
		return nil
	}

	// Collect involved tables
	involvedTables := make([]string, 0, len(pending.tables))
	for table := range pending.tables {
		involvedTables = append(involvedTables, table)
	}

	tx := &Transaction{
		ID:             pending.transactionID,
		Changes:        make([]Change, len(pending.changes)),
		CreatedAt:      time.Now(),
		CommitTime:     pending.commitTime,
		FirstSeenTime:  pending.firstSeen,
		InvolvedTables: involvedTables,
	}

	copy(tx.Changes, pending.changes)
	return tx
}

// cleanupLoop is kept for potential future use (e.g., periodic memory cleanup)
// Currently just a placeholder - no timeout-based delivery
func (tb *TransactionBuffer) cleanupLoop() {
	// Reserved for future use - e.g., periodic cleanup of stale transactions
	// No-op now since timeout fallback was removed
	for {
		select {
		case <-tb.stopCh:
			return
		}
	}
}

// checkTimeouts is kept for potential future use (e.g., memory management)
// Currently no timeout logic - transactions are delivered by commit-time or poll-interval gating
func (tb *TransactionBuffer) checkTimeouts() {
	// Reserved for future use - e.g., periodic cleanup of stale transactions
	// Currently all delivery is handled by shouldDeliver() conditions
}

// Close stops the cleanup loop
func (tb *TransactionBuffer) Close() {
	close(tb.stopCh)
}

// Size returns the number of pending transactions
func (tb *TransactionBuffer) Size() int {
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	return len(tb.pending)
}

// IsEmpty returns true if there are no pending transactions
func (tb *TransactionBuffer) IsEmpty() bool {
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	return len(tb.pending) == 0
}

// Flush returns all pending transactions immediately (for shutdown/rebuild)
// Marks all as complete regardless of timeout status
func (tb *TransactionBuffer) Flush() []*Transaction {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	var all []*Transaction
	for txID, pending := range tb.pending {
		tx := tb.buildTransaction(pending)
		if tx != nil {
			all = append(all, tx)
			tb.estimatedBytes -= pending.estimatedSize
		}
		delete(tb.pending, txID)
	}

	return all
}

// SetOnComplete sets the callback for completed transactions
func (tb *TransactionBuffer) SetOnComplete(fn func(*Transaction)) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.onComplete = fn
}

// SetOnTimeout sets the callback for timed-out transactions (includes delivery reason)
func (tb *TransactionBuffer) SetOnTimeout(fn func(*Transaction, DeliveryReason)) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.onTimeout = fn
}

// UpdateTableMaxCommitTime updates the max commit time for a table
// This is called externally when CDC captures new commit times
func (tb *TransactionBuffer) UpdateTableMaxCommitTime(table string, commitTime time.Time) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	if existing, ok := tb.tableMaxCommitTime[table]; !ok || commitTime.After(existing) {
		tb.tableMaxCommitTime[table] = commitTime
		slog.Debug("updated table max commit time",
			"table", table,
			"commit_time", commitTime)
	}
}

// GetTableMaxCommitTime returns the max commit time for a table
func (tb *TransactionBuffer) GetTableMaxCommitTime(table string) (time.Time, bool) {
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	t, ok := tb.tableMaxCommitTime[table]
	return t, ok
}
