package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cnlangzi/dbkrab/internal/cdc"
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
		store:         failSink,
		offsets:       offsetStore,
		metricsWindow: newPollMetricsWindow(60),
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

	fetchTime := time.Now()
	err := poller.processDirect(context.TODO(), tx.Changes, results, fetchTime)
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
		store:         successSink,
		offsets:       offsetStore,
		handler:       failHandler,
		metricsWindow: newPollMetricsWindow(60),
		querier:       &mockQuerier{},
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

	fetchTime := time.Now()
	err := poller.processDirect(context.TODO(), tx.Changes, results, fetchTime)
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
	// Enable transaction buffer with poll-interval gating (no maxWaitTime)
	maxWaitTime := 0 * time.Millisecond // No timeout
	pollInterval := 50 * time.Millisecond
	buffer := NewTransactionBuffer(maxWaitTime, pollInterval, 1000, 10*1024*1024)
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

	// Wait for poll-interval gating (2x safety multiplier)
	time.Sleep(pollInterval * pollIntervalSafetyMultiplier * 2)

	// Get complete (poll-interval gated) transactions
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
	// No timeout - use poll-interval gating instead
	maxWaitTime := 0 * time.Millisecond
	pollInterval := 50 * time.Millisecond
	buffer := NewTransactionBuffer(maxWaitTime, pollInterval, 1000, 10*1024*1024)
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

	// Wait for poll-interval gating (2x safety multiplier)
	time.Sleep(pollInterval * pollIntervalSafetyMultiplier * 2)

	// Now the transaction should be delivered (poll-interval gated)
	complete = buffer.GetCompleteTransactions()
	if len(complete) != 1 {
		t.Errorf("Expected 1 poll-interval-gated transaction, got %d", len(complete))
	}
}

// TestConcurrentTransactionBufferAccess verifies thread safety of transaction buffer
func TestConcurrentTransactionBufferAccess(t *testing.T) {
	maxWaitTime := 1 * time.Second
	buffer := NewTransactionBuffer(maxWaitTime, 0, 1000, 10*1024*1024)
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

func (s *mockOffsetStore) Set(table string, lsn string, hasNewData bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setCalled = true
	s.data[table] = offset.Offset{LSN: lsn, HasNewData: hasNewData, UpdatedAt: time.Now()}
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

func (h *mockHandler) Handle(ctx context.Context, tx *Transaction) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fail {
		return errors.New("simulated handler failure")
	}
	return nil
}

type mockQuerier struct{}

func (q *mockQuerier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error) {
	// Simple mock: just increment the last byte
	if len(lsn) == 0 {
		return lsn, nil
	}
	result := make([]byte, len(lsn))
	copy(result, lsn)
	result[len(result)-1]++
	return result, nil
}

func (q *mockQuerier) GetMaxLSN(ctx context.Context) ([]byte, error) {
	return []byte{0, 0, 0, 0, 0, 0, 0, 0}, nil
}

func (q *mockQuerier) GetMinLSN(ctx context.Context, captureInstance string) ([]byte, error) {
	return []byte{0, 0, 0, 0, 0, 0, 0, 0}, nil
}

func (q *mockQuerier) GetChanges(ctx context.Context, captureInstance string, tableName string, fromLSN []byte, toLSN []byte) ([]cdc.Change, error) {
	return nil, nil
}

// TestPollMetricsWindow tests the sliding window functionality
func TestPollMetricsWindow(t *testing.T) {
	window := newPollMetricsWindow(5)

	// Add 3 samples
	window.add(PollMetrics{SyncTPS: 100.0, EndToEndLatencyMs: 100})
	window.add(PollMetrics{SyncTPS: 200.0, EndToEndLatencyMs: 200})
	window.add(PollMetrics{SyncTPS: 300.0, EndToEndLatencyMs: 300})

	// Check average TPS
	avgTPS := window.avgTPS()
	if avgTPS != 200.0 {
		t.Errorf("Expected avg TPS 200.0, got %v", avgTPS)
	}

	// Check average latency
	avgLatency := window.avgLatencyMs()
	if avgLatency != 200 {
		t.Errorf("Expected avg latency 200ms, got %v", avgLatency)
	}

	// Add 3 more samples to exceed window size (5)
	window.add(PollMetrics{SyncTPS: 400.0, EndToEndLatencyMs: 400})
	window.add(PollMetrics{SyncTPS: 500.0, EndToEndLatencyMs: 500})

	// Window should have rolled, now containing last 5 samples (100,200,300,400,500)
	avgTPS = window.avgTPS()
	expectedAvgTPS := 300.0 // (100+200+300+400+500)/5
	if avgTPS != expectedAvgTPS {
		t.Errorf("Expected avg TPS %v, got %v", expectedAvgTPS, avgTPS)
	}
}

// TestPollMetricsWindowEmpty tests empty window behavior
func TestPollMetricsWindowEmpty(t *testing.T) {
	window := newPollMetricsWindow(5)

	avgTPS := window.avgTPS()
	if avgTPS != 0 {
		t.Errorf("Expected 0 TPS for empty window, got %v", avgTPS)
	}

	avgLatency := window.avgLatencyMs()
	if avgLatency != 0 {
		t.Errorf("Expected 0 latency for empty window, got %v", avgLatency)
	}
}

// TestGetMetrics tests the GetMetrics method
func TestGetMetrics(t *testing.T) {
	poller := &Poller{
		metricsWindow: newPollMetricsWindow(60),
		metrics: PollMetrics{
			FetchedChanges:    50,
			ProcessedTx:       12,
			SyncDurationMs:    42,
			DLQCount:          1,
			LastFetchTime:     time.Now().Add(-1 * time.Second),
			LastSyncTime:      time.Now(),
			LastLSN:           "0x00123:00004567",
			SyncTPS:           285.7,
			EndToEndLatencyMs: 100,
		},
	}

	metrics := poller.GetMetrics()

	if metrics["last_fetched"] != 50 {
		t.Errorf("Expected last_fetched=50, got %v", metrics["last_fetched"])
	}
	if metrics["last_processed_tx"] != 12 {
		t.Errorf("Expected last_processed_tx=12, got %v", metrics["last_processed_tx"])
	}
	if metrics["last_sync_tps"] != 285.7 {
		t.Errorf("Expected last_sync_tps=285.7, got %v", metrics["last_sync_tps"])
	}
	if metrics["last_sync_duration_ms"] != int64(42) {
		t.Errorf("Expected last_sync_duration_ms=42, got %v", metrics["last_sync_duration_ms"])
	}
	if metrics["last_dlq_count"] != 1 {
		t.Errorf("Expected last_dlq_count=1, got %v", metrics["last_dlq_count"])
	}
	if metrics["last_lsn"] != "0x00123:00004567" {
		t.Errorf("Expected last_lsn=0x00123:00004567, got %v", metrics["last_lsn"])
	}
}

// TestEmptyPollMetrics tests that empty polls have zero metrics
func TestEmptyPollMetrics(t *testing.T) {
	poller := &Poller{
		metricsWindow: newPollMetricsWindow(60),
	}

	metrics := poller.GetMetrics()

	// Empty metrics should be zero values
	if metrics["last_fetched"] != 0 {
		t.Errorf("Expected last_fetched=0 for empty metrics, got %v", metrics["last_fetched"])
	}
	if metrics["avg_tps_1m"] != 0.0 {
		t.Errorf("Expected avg_tps_1m=0 for empty window, got %v", metrics["avg_tps_1m"])
	}
}

// TestGroupByTransactionFiltersUpdateBefore tests that groupByTransaction
// defensively filters out UPDATE_BEFORE changes to prevent DLQ errors.
func TestGroupByTransactionFiltersUpdateBefore(t *testing.T) {
	poller := &Poller{}

	// Create changes including UPDATE_BEFORE and UPDATE_AFTER
	changes := []Change{
		{
			Table:         "users",
			TransactionID: "tx-1",
			Operation:     OpUpdateBefore, // Should be filtered
			Data:          map[string]interface{}{"id": 1, "name": "old-name"},
			LSN:           []byte{0x01},
		},
		{
			Table:         "users",
			TransactionID: "tx-1",
			Operation:     OpUpdateAfter, // Should remain
			Data:          map[string]interface{}{"id": 1, "name": "new-name"},
			LSN:           []byte{0x02},
		},
		{
			Table:         "users",
			TransactionID: "tx-1",
			Operation:     OpInsert, // Should remain
			Data:          map[string]interface{}{"id": 2, "name": "bob"},
			LSN:           []byte{0x03},
		},
		{
			Table:         "users",
			TransactionID: "tx-1",
			Operation:     OpDelete, // Should remain
			Data:          map[string]interface{}{"id": 3},
			LSN:           []byte{0x04},
		},
	}

	txs := poller.groupByTransaction(changes)

	// Should have 1 transaction
	if len(txs) != 1 {
		t.Fatalf("Expected 1 transaction, got %d", len(txs))
	}

	tx := txs[0]

	// Should have 3 changes (UPDATE_BEFORE filtered out)
	if len(tx.Changes) != 3 {
		t.Errorf("Expected 3 changes after filtering UPDATE_BEFORE, got %d", len(tx.Changes))
	}

	// Verify UPDATE_BEFORE is not present
	for _, c := range tx.Changes {
		if c.Operation == OpUpdateBefore {
			t.Error("UPDATE_BEFORE should have been filtered but was present")
		}
	}

	// Verify remaining operations are correct
	expectedOps := []Operation{OpUpdateAfter, OpInsert, OpDelete}
	for i, expected := range expectedOps {
		if tx.Changes[i].Operation != expected {
			t.Errorf("Change[%d] expected %v, got %v", i, expected, tx.Changes[i].Operation)
		}
	}
}
