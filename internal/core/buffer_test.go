package core

import (
	"sync"
	"testing"
	"time"
)

// TestTransactionBufferBasicAddAndGet tests basic add and retrieval
func TestTransactionBufferBasicAddAndGet(t *testing.T) {
	buffer := NewTransactionBuffer(100*time.Millisecond, 50*time.Millisecond, 1000, 10*1024*1024)
	defer buffer.Close()

	// Add a change
	buffer.Add(Change{
		Table:         "dbo.Test",
		TransactionID: "tx-1",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
	})

	// Check buffer size
	if buffer.Size() != 1 {
		t.Errorf("Expected buffer size 1, got %d", buffer.Size())
	}
}

// TestTransactionBufferMultipleChangesSameTransaction tests multiple changes in same transaction
func TestTransactionBufferMultipleChangesSameTransaction(t *testing.T) {
	buffer := NewTransactionBuffer(100*time.Millisecond, 50*time.Millisecond, 1000, 10*1024*1024)
	defer buffer.Close()

	// Add multiple changes to same transaction
	for i := 0; i < 5; i++ {
		buffer.Add(Change{
			Table:         "dbo.Test",
			TransactionID: "tx-1",
			Operation:     OpInsert,
			Data:          map[string]interface{}{"id": i},
		})
	}

	// Should still be 1 pending transaction
	if buffer.Size() != 1 {
		t.Errorf("Expected 1 pending transaction, got %d", buffer.Size())
	}

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	// Get complete transactions
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 1 {
		t.Fatalf("Expected 1 complete transaction, got %d", len(complete))
	}

	// Verify all 5 changes are present
	if len(complete[0].Changes) != 5 {
		t.Errorf("Expected 5 changes, got %d", len(complete[0].Changes))
	}
}

// TestTransactionBufferMultipleTransactions tests multiple concurrent transactions
func TestTransactionBufferMultipleTransactions(t *testing.T) {
	buffer := NewTransactionBuffer(100*time.Millisecond, 50*time.Millisecond, 1000, 10*1024*1024)
	defer buffer.Close()

	// Add changes to 3 different transactions
	for txNum := 1; txNum <= 3; txNum++ {
		for i := 0; i < 3; i++ {
			buffer.Add(Change{
				Table:         "dbo.Test",
				TransactionID: string(rune(txNum)),
				Operation:     OpInsert,
				Data:          map[string]interface{}{"id": i},
			})
		}
	}

	// Should have 3 pending transactions
	if buffer.Size() != 3 {
		t.Errorf("Expected 3 pending transactions, got %d", buffer.Size())
	}

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	// Get complete transactions
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 3 {
		t.Errorf("Expected 3 complete transactions, got %d", len(complete))
	}

	// Verify buffer is now empty
	if buffer.Size() != 0 {
		t.Errorf("Expected empty buffer, got size %d", buffer.Size())
	}
}

// TestTransactionBufferCallback tests timeout callback with delivery reason
func TestTransactionBufferCallback(t *testing.T) {
	buffer := NewTransactionBuffer(50*time.Millisecond, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	var timeoutCalled bool
	var deliveryReason DeliveryReason
	var mu sync.Mutex

	buffer.SetOnTimeout(func(tx *Transaction, reason DeliveryReason) {
		mu.Lock()
		defer mu.Unlock()
		timeoutCalled = true
		deliveryReason = reason
	})

	// Add a change
	buffer.Add(Change{
		Table:         "dbo.Test",
		TransactionID: "tx-1",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
	})

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Trigger cleanup
	buffer.GetCompleteTransactions()

	// Verify callback was called
	mu.Lock()
	called := timeoutCalled
	reason := deliveryReason
	mu.Unlock()

	if !called {
		t.Error("Expected timeout callback to be called")
	}
	if reason != DeliveryReasonTimeout {
		t.Errorf("Expected delivery reason %s, got %s", DeliveryReasonTimeout, reason)
	}
}

// TestTransactionBufferClose tests proper cleanup
func TestTransactionBufferClose(t *testing.T) {
	buffer := NewTransactionBuffer(1*time.Second, 0, 1000, 10*1024*1024)

	// Add some changes
	buffer.Add(Change{
		Table:         "dbo.Test",
		TransactionID: "tx-1",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
	})

	// Close the buffer
	buffer.Close()

	// Wait a bit to ensure cleanup loop stopped
	time.Sleep(50 * time.Millisecond)

	// Should not panic - buffer is closed
	buffer.Add(Change{
		Table:         "dbo.Test",
		TransactionID: "tx-2",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 2},
	})
}

// TestTransactionBufferTableTracking tests that tables are tracked correctly
func TestTransactionBufferTableTracking(t *testing.T) {
	buffer := NewTransactionBuffer(100*time.Millisecond, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	// Add changes from different tables for same transaction
	tables := []string{"dbo.Orders", "dbo.OrderItems", "dbo.Inventory"}
	for _, table := range tables {
		buffer.Add(Change{
			Table:         table,
			TransactionID: "tx-1",
			Operation:     OpInsert,
			Data:          map[string]interface{}{"id": 1},
		})
	}

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	// Get complete transaction
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 1 {
		t.Fatalf("Expected 1 complete transaction, got %d", len(complete))
	}

	// Verify changes from all tables
	tableSet := make(map[string]bool)
	for _, change := range complete[0].Changes {
		tableSet[change.Table] = true
	}

	for _, expected := range tables {
		if !tableSet[expected] {
			t.Errorf("Missing change from table %s", expected)
		}
	}

	// Verify InvolvedTables
	for _, expected := range tables {
		found := false
		for _, table := range complete[0].InvolvedTables {
			if table == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Missing table %s from InvolvedTables", expected)
		}
	}
}

// TestTransactionBufferConcurrentAdd tests concurrent add operations
func TestTransactionBufferConcurrentAdd(t *testing.T) {
	buffer := NewTransactionBuffer(1*time.Second, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	changesPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < changesPerGoroutine; j++ {
				buffer.Add(Change{
					Table:         "dbo.Test",
					TransactionID: string(rune(id)),
					Operation:     OpInsert,
					Data:          map[string]interface{}{"id": j},
				})
			}
		}(i)
	}

	wg.Wait()

	// Should have numGoroutines pending transactions
	size := buffer.Size()
	if size != numGoroutines {
		t.Errorf("Expected %d pending transactions, got %d", numGoroutines, size)
	}
}

// TestTransactionBufferEmptyTransaction tests handling of empty transactions
func TestTransactionBufferEmptyTransaction(t *testing.T) {
	buffer := NewTransactionBuffer(50*time.Millisecond, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	// Don't add any changes

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Get complete transactions - should be empty
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 0 {
		t.Errorf("Expected no complete transactions, got %d", len(complete))
	}
}

// TestTransactionBufferSize tests buffer size tracking
func TestTransactionBufferSize(t *testing.T) {
	buffer := NewTransactionBuffer(1*time.Second, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	// Initial size should be 0
	if buffer.Size() != 0 {
		t.Errorf("Expected initial size 0, got %d", buffer.Size())
	}

	// Add 5 transactions
	for i := 0; i < 5; i++ {
		buffer.Add(Change{
			Table:         "dbo.Test",
			TransactionID: string(rune(i)),
			Operation:     OpInsert,
			Data:          map[string]interface{}{"id": i},
		})
	}

	// Size should be 5
	if buffer.Size() != 5 {
		t.Errorf("Expected size 5, got %d", buffer.Size())
	}
}

// TestNonTransactionalChangesDeliveredImmediately tests that changes without TransactionID are delivered immediately
func TestNonTransactionalChangesDeliveredImmediately(t *testing.T) {
	buffer := NewTransactionBuffer(10*time.Second, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	var deliveredTx *Transaction
	var mu sync.Mutex

	buffer.SetOnComplete(func(tx *Transaction) {
		mu.Lock()
		defer mu.Unlock()
		deliveredTx = tx
	})

	// Add a non-transactional change (empty TransactionID)
	change := Change{
		Table:         "dbo.Test",
		TransactionID: "", // Empty - non-transactional
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
	}
	buffer.Add(change)

	// Give it a moment to process
	time.Sleep(10 * time.Millisecond)

	// Buffer should be empty (change was delivered immediately)
	if buffer.Size() != 0 {
		t.Errorf("Expected buffer size 0 for non-transactional change, got %d", buffer.Size())
	}

	// Verify the transaction was delivered
	mu.Lock()
	tx := deliveredTx
	mu.Unlock()

	if tx == nil {
		t.Fatal("Expected non-transactional change to be delivered immediately")
	}

	if len(tx.Changes) != 1 {
		t.Errorf("Expected 1 change in delivered transaction, got %d", len(tx.Changes))
	}

	if tx.Changes[0].Table != "dbo.Test" {
		t.Errorf("Expected table 'dbo.Test', got '%s'", tx.Changes[0].Table)
	}
}

// TestCommitTimeGating tests that transactions are delivered when all involved tables confirm commit time
func TestCommitTimeGating(t *testing.T) {
	buffer := NewTransactionBuffer(10*time.Second, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	commitTime := time.Now().Add(-1 * time.Second) // Transaction committed 1 second ago

	// Add changes from table A and table B in the same transaction
	buffer.Add(Change{
		Table:         "dbo.TableA",
		TransactionID: "tx-cross-table",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
		CommitTime:    commitTime,
	})
	buffer.Add(Change{
		Table:         "dbo.TableB",
		TransactionID: "tx-cross-table",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 2},
		CommitTime:    commitTime,
	})

	// Should not be delivered yet (pollInterval gating not met, timeout not met)
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 0 {
		t.Errorf("Expected 0 complete transactions before commit-time confirmation, got %d", len(complete))
	}

	// Now table A reports a newer commit time (after our transaction)
	buffer.UpdateTableMaxCommitTime("dbo.TableA", commitTime.Add(1*time.Millisecond))

	// Should still not be delivered (table B hasn't confirmed)
	complete = buffer.GetCompleteTransactions()
	if len(complete) != 0 {
		t.Errorf("Expected 0 complete transactions when only one table confirmed, got %d", len(complete))
	}

	// Now table B also reports a newer commit time
	buffer.UpdateTableMaxCommitTime("dbo.TableB", commitTime.Add(2*time.Millisecond))

	// Should now be delivered (both tables have confirmed)
	complete = buffer.GetCompleteTransactions()
	if len(complete) != 1 {
		t.Fatalf("Expected 1 complete transaction after all tables confirmed, got %d", len(complete))
	}

	// Verify delivery reason is commit_time
	var deliveryReason DeliveryReason
	buffer.SetOnTimeout(func(tx *Transaction, reason DeliveryReason) {
		deliveryReason = reason
	})

	// Re-add for delivery reason check
	buffer.Add(Change{
		Table:         "dbo.TableA",
		TransactionID: "tx-verify-reason",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
		CommitTime:    commitTime,
	})
	buffer.UpdateTableMaxCommitTime("dbo.TableA", commitTime.Add(1*time.Millisecond))

	complete = buffer.GetCompleteTransactions()
	if len(complete) == 1 && deliveryReason != DeliveryReasonCommitTime {
		t.Errorf("Expected delivery reason %s, got %s", DeliveryReasonCommitTime, deliveryReason)
	}
}

// TestPollIntervalGating tests that transactions are delivered after pollInterval since commit
func TestPollIntervalGating(t *testing.T) {
	pollInterval := 50 * time.Millisecond
	buffer := NewTransactionBuffer(10*time.Second, pollInterval, 1000, 10*1024*1024)
	defer buffer.Close()

	commitTime := time.Now().Add(-100 * time.Millisecond) // Transaction committed 100ms ago (before pollInterval)

	// Add a change
	buffer.Add(Change{
		Table:         "dbo.Test",
		TransactionID: "tx-poll-gate",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
		CommitTime:    commitTime,
	})

	// Should not be delivered yet (pollInterval is 50ms, but age since commit is 100ms, wait - actually it should be delivered because 100ms > 50ms)
	// Actually wait - the condition is now.Sub(tx.CommitTime) > pollInterval
	// If commitTime is 100ms ago and pollInterval is 50ms, then 100ms > 50ms is true, so it should deliver
	time.Sleep(10 * time.Millisecond)
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 1 {
		t.Errorf("Expected transaction to be delivered by poll-interval gating, got %d", len(complete))
	}
}

// TestPollIntervalGatingNotYet tests that transactions are NOT delivered before pollInterval
func TestPollIntervalGatingNotYet(t *testing.T) {
	pollInterval := 200 * time.Millisecond
	buffer := NewTransactionBuffer(10*time.Second, pollInterval, 1000, 10*1024*1024)
	defer buffer.Close()

	commitTime := time.Now().Add(-50 * time.Millisecond) // Transaction committed 50ms ago (less than pollInterval)

	// Add a change
	buffer.Add(Change{
		Table:         "dbo.Test",
		TransactionID: "tx-poll-gate-not-yet",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
		CommitTime:    commitTime,
	})

	// Should NOT be delivered yet (pollInterval is 200ms, age since commit is 50ms < 200ms)
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 0 {
		t.Errorf("Expected 0 complete transactions before pollInterval elapsed, got %d", len(complete))
	}
}

// TestSingleTableTransactionWithCommitTimeGating tests single-table transaction delivery
func TestSingleTableTransactionWithCommitTimeGating(t *testing.T) {
	buffer := NewTransactionBuffer(10*time.Second, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	commitTime := time.Now().Add(-1 * time.Second)

	// Add change from single table
	buffer.Add(Change{
		Table:         "dbo.SingleTable",
		TransactionID: "tx-single",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
		CommitTime:    commitTime,
	})

	// Should not be delivered yet
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 0 {
		t.Errorf("Expected 0 complete transactions before commit confirmation, got %d", len(complete))
	}

	// Table confirms with newer commit time
	buffer.UpdateTableMaxCommitTime("dbo.SingleTable", commitTime.Add(1*time.Millisecond))

	// Should now be delivered
	complete = buffer.GetCompleteTransactions()
	if len(complete) != 1 {
		t.Errorf("Expected 1 complete transaction after single table confirmed, got %d", len(complete))
	}
}

// TestTransactionWithTablesNeverParticipating tests edge case where some tables never report
func TestTransactionWithTablesNeverParticipating(t *testing.T) {
	buffer := NewTransactionBuffer(10*time.Second, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	commitTime := time.Now().Add(-1 * time.Second)

	// Add changes from table A only
	buffer.Add(Change{
		Table:         "dbo.TableA",
		TransactionID: "tx-never-b",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
		CommitTime:    commitTime,
	})

	// Table B never participated in this transaction - it's not in the involved tables set
	// So commit-time gating should only consider table A
	buffer.UpdateTableMaxCommitTime("dbo.TableA", commitTime.Add(1*time.Millisecond))

	// Should be delivered because table A confirmed
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 1 {
		t.Errorf("Expected 1 complete transaction when involved table confirmed, got %d", len(complete))
	}
}

// TestTimeoutFallback tests that timeout still works as fallback
func TestTimeoutFallback(t *testing.T) {
	buffer := NewTransactionBuffer(100*time.Millisecond, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	commitTime := time.Now().Add(-1 * time.Hour) // Very old commit time

	// Add a change but don't confirm
	buffer.Add(Change{
		Table:         "dbo.Test",
		TransactionID: "tx-timeout-fallback",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
		CommitTime:    commitTime,
	})

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	// Should be delivered by timeout
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 1 {
		t.Errorf("Expected 1 complete transaction by timeout, got %d", len(complete))
	}
}

// TestDeliveryReasonMetrics tests that delivery reasons are tracked
func TestDeliveryReasonMetrics(t *testing.T) {
	var deliveryReasons []DeliveryReason
	var mu sync.Mutex

	buffer := NewTransactionBuffer(100*time.Millisecond, 0, 1000, 10*1024*1024)
	defer buffer.Close()

	buffer.SetOnTimeout(func(tx *Transaction, reason DeliveryReason) {
		mu.Lock()
		defer mu.Unlock()
		deliveryReasons = append(deliveryReasons, reason)
	})

	// Add a change and wait for timeout
	buffer.Add(Change{
		Table:         "dbo.Test",
		TransactionID: "tx-reason-test",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
	})

	time.Sleep(150 * time.Millisecond)
	buffer.GetCompleteTransactions()

	mu.Lock()
	reasons := deliveryReasons
	mu.Unlock()

	if len(reasons) != 1 {
		t.Fatalf("Expected 1 delivery reason, got %d", len(reasons))
	}
	if reasons[0] != DeliveryReasonTimeout {
		t.Errorf("Expected delivery reason %s, got %s", DeliveryReasonTimeout, reasons[0])
	}
}
