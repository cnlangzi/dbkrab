package core

import (
	"log/slog"
	"sync"
	"time"
)

// TransactionBuffer holds pending transactions waiting for completion
type TransactionBuffer struct {
	mu            sync.RWMutex
	pending       map[string]*pendingTransaction
	maxWaitTime   time.Duration
	maxTxCount    int              // maximum transactions per batch
	maxBatchBytes int              // maximum estimated bytes per batch
	estimatedBytes int              // current estimated batch size in bytes
	onComplete    func(*Transaction)
	onTimeout     func(*Transaction)
	cleanupTicker *time.Ticker
	stopCh        chan struct{}
}

type pendingTransaction struct {
	transactionID string
	changes       []Change
	firstSeen     time.Time
	tables        map[string]bool    // Track which tables have changes for this transaction
	complete      bool
	estimatedSize int                // Estimated size of this transaction in bytes
}

// NewTransactionBuffer creates a new transaction buffer
func NewTransactionBuffer(maxWaitTime time.Duration, maxTxCount int, maxBatchBytes int) *TransactionBuffer {
	// Normalize zero/negative values to "no limit"
	if maxTxCount <= 0 {
		maxTxCount = int(^uint(0) >> 1) // Max int ( effectively unlimited)
	}
	if maxBatchBytes <= 0 {
		maxBatchBytes = int(^uint(0) >> 1) // Max int (effectively unlimited)
	}

	tb := &TransactionBuffer{
		pending:        make(map[string]*pendingTransaction),
		maxWaitTime:    maxWaitTime,
		maxTxCount:    maxTxCount,
		maxBatchBytes: maxBatchBytes,
		stopCh:         make(chan struct{}),
	}

	// Start cleanup ticker
	tb.cleanupTicker = time.NewTicker(maxWaitTime / 2)
	go tb.cleanupLoop()

	slog.Info("transaction buffer initialized",
		"max_wait_time", maxWaitTime,
		"max_transactions_per_batch", maxTxCount,
		"max_batch_bytes", maxBatchBytes)

	return tb
}

// Add adds a change to the buffer
func (tb *TransactionBuffer) Add(change Change) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	txID := change.TransactionID

	pending, exists := tb.pending[txID]
	if !exists {
		pending = &pendingTransaction{
			transactionID: txID,
			changes:       make([]Change, 0),
			firstSeen:     time.Now(),
			tables:        make(map[string]bool),
		}
		tb.pending[txID] = pending
	}

	// Estimate change size
	changeSize := estimateChangeSize(change)
	pending.estimatedSize += changeSize
	tb.estimatedBytes += changeSize

	// Add change
	pending.changes = append(pending.changes, change)
	pending.tables[change.Table] = true
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

// GetCompleteTransactions returns transactions that are ready for delivery (timed out, batch full, or marked complete)
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
		// Check if transaction should be delivered:
		// 1. Timed out
		// 2. Marked complete
		// 3. Batch size limits reached
		shouldDeliver := pending.complete || now.Sub(pending.firstSeen) > tb.maxWaitTime

		// Check batch limits only if not already delivering for other reasons
		if !shouldDeliver {
			// Check if adding this transaction would exceed batch limits
			// Use > (not >=) to allow exactly reaching the limit
			if batchTxCount >= tb.maxTxCount {
				shouldDeliver = true
			}
			if batchBytes+pending.estimatedSize > tb.maxBatchBytes {
				shouldDeliver = true
			}
		}

		if shouldDeliver {
			tx := tb.buildTransaction(pending)
			if tx != nil {
				if tb.onTimeout != nil {
					tb.onTimeout(tx)
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

	// Remove delivered transactions
	for _, txID := range toRemove {
		delete(tb.pending, txID)
	}

	return complete
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

	tx := &Transaction{
		ID:        pending.transactionID,
		Changes:   make([]Change, len(pending.changes)),
		CreatedAt: time.Now(),
	}

	copy(tx.Changes, pending.changes)
	return tx
}

// cleanupLoop periodically checks for timed-out transactions
func (tb *TransactionBuffer) cleanupLoop() {
	for {
		select {
		case <-tb.cleanupTicker.C:
			// Just check for timeouts, don't deliver
			// Delivery happens when GetCompleteTransactions is called
			tb.checkTimeouts()
		case <-tb.stopCh:
			tb.cleanupTicker.Stop()
			return
		}
	}
}

// checkTimeouts marks timed-out transactions as complete (internal use)
func (tb *TransactionBuffer) checkTimeouts() {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	for _, pending := range tb.pending {
		if now.Sub(pending.firstSeen) > tb.maxWaitTime {
			pending.complete = true
		}
	}
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

// SetOnTimeout sets the callback for timed-out transactions
func (tb *TransactionBuffer) SetOnTimeout(fn func(*Transaction)) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.onTimeout = fn
}
