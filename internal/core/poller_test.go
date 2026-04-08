package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cnlangzi/dbkrab/internal/offset"
)

// TestExactlyOnceSinkFailure verifies that offset is not advanced when sink fails
func TestExactlyOnceSinkFailure(t *testing.T) {
	// Create mock sink that fails
	failSink := &mockSink{
		fail: true,
	}

	// Create mock offset store
	offsetStore := &mockOffsetStore{
		data: make(map[string]offset.Offset),
	}

	// Create poller with failing store
	poller := &Poller{
		store:   failSink,
		offsets: offsetStore,
	}

	// Create test transaction
	tx := &Transaction{
		ID: "test-tx-1",
		Changes: []Change{
			{
				Table:         "dbo.Test",
				TransactionID: "test-tx-1",
				Operation:     OpInsert,
				Data:          map[string]interface{}{"id": 1},
			},
		},
	}

	// Process transaction - should fail
	results := []tablePollResult{
		{table: "dbo.Test", changes: tx.Changes, lastLSN: LSN{1, 2, 3, 4}, err: nil},
	}

	err := poller.processDirect(context.TODO(), tx.Changes, results)
	if err == nil {
		t.Fatal("Expected error from failing sink, got nil")
	}

	// Verify offset was NOT advanced (Exactly-Once guarantee)
	if offsetStore.setCalled {
		t.Error("Offset should not be advanced when sink fails")
	}
}

// TestExactlyOnceHandlerFailure verifies that handler failures don't block offset advancement
func TestExactlyOnceHandlerFailure(t *testing.T) {
	// Create mock handler that fails
	failHandler := &mockHandler{
		fail: true,
	}

	// Create mock sink that succeeds
	successSink := &mockSink{
		fail: false,
	}

	// Create mock offset store
	offsetStore := &mockOffsetStore{
		data: make(map[string]offset.Offset),
	}

	// Create poller
	poller := &Poller{
		store:   successSink,
		offsets: offsetStore,
		handler: failHandler,
	}

	// Create test transaction
	tx := &Transaction{
		ID: "test-tx-1",
		Changes: []Change{
			{
				Table:         "dbo.Test",
				TransactionID: "test-tx-1",
				Operation:     OpInsert,
				Data:          map[string]interface{}{"id": 1},
			},
		},
	}

	// Process transaction - handler fails but sink succeeds
	results := []tablePollResult{
		{table: "dbo.Test", changes: tx.Changes, lastLSN: LSN{1, 2, 3, 4}, err: nil},
	}

	err := poller.processDirect(context.TODO(), tx.Changes, results)
	if err != nil {
		t.Fatalf("Expected no error (handler failures don't block), got: %v", err)
	}

	// Verify offset WAS advanced (handler failure doesn't block)
	if !offsetStore.setCalled {
		t.Error("Offset should be advanced even when handler fails")
	}
}

// TestCrossTableTransactionIntegrity verifies that cross-table transactions are delivered completely
func TestCrossTableTransactionIntegrity(t *testing.T) {
	// Enable transaction buffer
	maxWaitTime := 100 * time.Millisecond
	buffer := NewTransactionBuffer(maxWaitTime, 1000, 10*1024*1024)
	defer buffer.Close()

	// Simulate a transaction spanning 3 tables
	txID := "cross-table-tx-1"

	// Add changes from table 1
	buffer.Add(Change{
		Table:         "dbo.Orders",
		TransactionID: txID,
		Operation:     OpInsert,
		Data:          map[string]interface{}{"order_id": 1},
	})

	// Add changes from table 2
	buffer.Add(Change{
		Table:         "dbo.OrderItems",
		TransactionID: txID,
		Operation:     OpInsert,
		Data:          map[string]interface{}{"item_id": 1},
	})

	// Add changes from table 3
	buffer.Add(Change{
		Table:         "dbo.Inventory",
		TransactionID: txID,
		Operation:     OpUpdateAfter,
		Data:          map[string]interface{}{"quantity": 10},
	})

	// Wait for timeout (transaction should be delivered as incomplete)
	time.Sleep(maxWaitTime * 2)

	// Get complete (timed out) transactions
	completeTxs := buffer.GetCompleteTransactions()

	if len(completeTxs) != 1 {
		t.Fatalf("Expected 1 complete transaction, got %d", len(completeTxs))
	}

	tx := completeTxs[0]

	// Verify all 3 changes are present
	if len(tx.Changes) != 3 {
		t.Errorf("Expected 3 changes in transaction, got %d", len(tx.Changes))
	}

	// Verify changes from all 3 tables
	tables := make(map[string]bool)
	for _, change := range tx.Changes {
		tables[change.Table] = true
	}

	expectedTables := []string{"dbo.Orders", "dbo.OrderItems", "dbo.Inventory"}
	for _, expected := range expectedTables {
		if !tables[expected] {
			t.Errorf("Missing change from table %s", expected)
		}
	}
}

// TestTransactionBufferTimeout verifies that incomplete transactions are delivered after timeout
func TestTransactionBufferTimeout(t *testing.T) {
	maxWaitTime := 50 * time.Millisecond
	buffer := NewTransactionBuffer(maxWaitTime, 1000, 10*1024*1024)
	defer buffer.Close()

	// Add a single change (incomplete transaction)
	buffer.Add(Change{
		Table:         "dbo.Test",
		TransactionID: "incomplete-tx",
		Operation:     OpInsert,
		Data:          map[string]interface{}{"id": 1},
	})

	// Initially, no complete transactions
	complete := buffer.GetCompleteTransactions()
	if len(complete) != 0 {
		t.Errorf("Expected no complete transactions initially, got %d", len(complete))
	}

	// Wait for timeout
	time.Sleep(maxWaitTime * 2)

	// Now the transaction should be delivered (timed out)
	complete = buffer.GetCompleteTransactions()
	if len(complete) != 1 {
		t.Errorf("Expected 1 timed-out transaction, got %d", len(complete))
	}
}

// TestConcurrentTransactionBufferAccess verifies thread safety of transaction buffer
func TestConcurrentTransactionBufferAccess(t *testing.T) {
	maxWaitTime := 1 * time.Second
	buffer := NewTransactionBuffer(maxWaitTime, 1000, 10*1024*1024)
	defer buffer.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	changesPerGoroutine := 100

	// Concurrently add changes
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

	// Verify buffer size
	size := buffer.Size()
	if size == 0 {
		t.Error("Expected non-zero buffer size after concurrent adds")
	}
}

// Mock implementations

type mockSink struct {
	fail bool
	mu   sync.Mutex
}

func (s *mockSink) Write(tx *Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("simulated sink failure")
	}
	return nil
}

func (s *mockSink) WriteOps(ops []Sink) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("simulated sink failure")
	}
	return nil
}

func (s *mockSink) Close() error {
	return nil
}

type mockOffsetStore struct {
	data      map[string]offset.Offset
	setCalled bool
	mu        sync.Mutex
}

func (s *mockOffsetStore) Load() error {
	return nil
}

func (s *mockOffsetStore) Save() error {
	return nil
}

func (s *mockOffsetStore) Get(table string) (offset.Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	offsetVal, ok := s.data[table]
	if !ok {
		return offset.Offset{}, nil
	}
	return offsetVal, nil
}

func (s *mockOffsetStore) Set(table string, lsn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setCalled = true
	s.data[table] = offset.Offset{LSN: lsn, UpdatedAt: time.Now()}
	return nil
}

func (s *mockOffsetStore) GetAll() (map[string]offset.Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]offset.Offset)
	for k, v := range s.data {
		result[k] = v
	}
	return result, nil
}

type mockHandler struct {
	fail bool
	mu   sync.Mutex
}

func (h *mockHandler) Handle(tx *Transaction) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fail {
		return errors.New("simulated handler failure")
	}
	return nil
}
