package core

import (
	"sync"
	"testing"
	"time"
)

// TestTransactionBufferBasicAddAndGet tests basic add and retrieval
func TestTransactionBufferBasicAddAndGet(t *testing.T) {
	buffer := NewTransactionBuffer(100 * time.Millisecond)
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
	buffer := NewTransactionBuffer(100 * time.Millisecond)
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
	buffer := NewTransactionBuffer(100 * time.Millisecond)
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

// TestTransactionBufferCallback tests timeout callback
func TestTransactionBufferCallback(t *testing.T) {
	buffer := NewTransactionBuffer(50 * time.Millisecond)
	defer buffer.Close()

	var timeoutCalled bool
	var mu sync.Mutex

	buffer.SetOnTimeout(func(tx *Transaction) {
		mu.Lock()
		defer mu.Unlock()
		timeoutCalled = true
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
	mu.Unlock()

	if !called {
		t.Error("Expected timeout callback to be called")
	}
}

// TestTransactionBufferClose tests proper cleanup
func TestTransactionBufferClose(t *testing.T) {
	buffer := NewTransactionBuffer(1 * time.Second)

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
	buffer := NewTransactionBuffer(100 * time.Millisecond)
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
}

// TestTransactionBufferConcurrentAdd tests concurrent add operations
func TestTransactionBufferConcurrentAdd(t *testing.T) {
	buffer := NewTransactionBuffer(1 * time.Second)
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
	buffer := NewTransactionBuffer(50 * time.Millisecond)
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
	buffer := NewTransactionBuffer(1 * time.Second)
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
