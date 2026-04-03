package core

import (
	"sync"
	"time"
)

// TransactionBuffer holds pending transactions waiting for completion
type TransactionBuffer struct {
	mu            sync.RWMutex
	pending       map[string]*pendingTransaction
	maxWaitTime   time.Duration
	onComplete    func(*Transaction)
	onTimeout     func(*Transaction)
	cleanupTicker *time.Ticker
	stopCh        chan struct{}
}

type pendingTransaction struct {
	transactionID string
	changes       []Change
	firstSeen     time.Time
	tables        map[string]bool // Track which tables have changes for this transaction
	complete      bool
}

// NewTransactionBuffer creates a new transaction buffer
func NewTransactionBuffer(maxWaitTime time.Duration) *TransactionBuffer {
	tb := &TransactionBuffer{
		pending:     make(map[string]*pendingTransaction),
		maxWaitTime: maxWaitTime,
		stopCh:      make(chan struct{}),
	}

	// Start cleanup ticker
	tb.cleanupTicker = time.NewTicker(maxWaitTime / 2)
	go tb.cleanupLoop()

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

	// Add change
	pending.changes = append(pending.changes, change)
	pending.tables[change.Table] = true
}

// GetCompleteTransactions returns transactions that are ready for delivery (timed out or marked complete)
func (tb *TransactionBuffer) GetCompleteTransactions() []*Transaction {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	var complete []*Transaction
	var toRemove []string

	for txID, pending := range tb.pending {
		// Check if transaction has timed out or is marked complete
		if now.Sub(pending.firstSeen) > tb.maxWaitTime || pending.complete {
			// Force deliver incomplete transaction
			tx := tb.buildTransaction(pending)
			if tx != nil {
				if tb.onTimeout != nil {
					tb.onTimeout(tx)
				}
				complete = append(complete, tx)
				toRemove = append(toRemove, txID)
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
