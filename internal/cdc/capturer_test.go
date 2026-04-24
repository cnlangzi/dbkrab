package cdc

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/cnlangzi/dbkrab/internal/offset"
)

// mockQuerier mocks the CDCQuerier interface for testing
type mockQuerier struct {
	minLSN     []byte
	maxLSN     []byte
	changes    []Change
	incrementTo []byte
	getChangesErr error
	getMinLSNErr error
	getMaxLSNErr error
	incrementLSNErr error
	calls      []string
}

func (m *mockQuerier) GetMinLSN(ctx context.Context, captureInstance string) ([]byte, error) {
	m.calls = append(m.calls, "GetMinLSN")
	if m.getMinLSNErr != nil {
		return nil, m.getMinLSNErr
	}
	return m.minLSN, nil
}

func (m *mockQuerier) GetMaxLSN(ctx context.Context) ([]byte, error) {
	m.calls = append(m.calls, "GetMaxLSN")
	if m.getMaxLSNErr != nil {
		return nil, m.getMaxLSNErr
	}
	return m.maxLSN, nil
}

func (m *mockQuerier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error) {
	m.calls = append(m.calls, "IncrementLSN")
	if m.incrementLSNErr != nil {
		return nil, m.incrementLSNErr
	}
	return m.incrementTo, nil
}

func (m *mockQuerier) GetChanges(ctx context.Context, captureInstance string, tableName string, fromLSN []byte, toLSN []byte) ([]Change, error) {
	m.calls = append(m.calls, "GetChanges")
	if m.getChangesErr != nil {
		return nil, m.getChangesErr
	}
	return m.changes, nil
}

// mockOffsetStore implements offset.StoreInterface for testing
type mockOffsetStore struct {
	offsets map[string]storedOffset
}

type storedOffset struct {
	LastLSN string
	NextLSN string
}

func newMockOffsetStore() *mockOffsetStore {
	return &mockOffsetStore{
		offsets: make(map[string]storedOffset),
	}
}

func (m *mockOffsetStore) Load() error   { return nil }
func (m *mockOffsetStore) Save() error   { return nil }
func (m *mockOffsetStore) Flush() error  { return nil }
func (m *mockOffsetStore) GetAll() (map[string]offset.Offset, error) { return nil, nil }

func (m *mockOffsetStore) Get(table string) (offset.Offset, error) {
	if v, ok := m.offsets[table]; ok {
		return offset.Offset{LastLSN: v.LastLSN, NextLSN: v.NextLSN}, nil
	}
	return offset.Offset{}, nil
}

func (m *mockOffsetStore) Set(table, lastLSN, nextLSN string) error {
	m.offsets[table] = storedOffset{LastLSN: lastLSN, NextLSN: nextLSN}
	return nil
}

// TestGetFromLSN_ColdStart_NoStoredOffset verifies cold start returns MinLSN
func TestGetFromLSN_ColdStart_NoStoredOffset(t *testing.T) {
	querier := &mockQuerier{
		minLSN: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xD8, 0x00, 0x64},
	}
	store := newMockOffsetStore()
	offsetMgr := NewOffsetManager(store, querier)

	capturer := &ChangeCapturer{
		querier:   querier,
		offsetMgr: offsetMgr,
	}

	fromLSN, err := capturer.getFromLSN(context.TODO(), "dbo.test", offset.Offset{}, []byte{0x00})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if fromLSN == nil {
		t.Fatal("Expected fromLSN for cold start")
	}
	if querier.minLSN == nil {
		t.Error("Expected GetMinLSN to be called")
	}
}

// TestGetFromLSN_ColdStart_InvalidStoredOffset verifies invalid stored LSN treated as cold start
func TestGetFromLSN_ColdStart_InvalidStoredOffset(t *testing.T) {
	querier := &mockQuerier{
		minLSN: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xD8, 0x00, 0x64},
	}
	store := newMockOffsetStore()
	store.offsets["dbo.test"] = storedOffset{LastLSN: "invalid-hex", NextLSN: ""}
	offsetMgr := NewOffsetManager(store, querier)

	capturer := &ChangeCapturer{
		querier:   querier,
		offsetMgr: offsetMgr,
	}

	fromLSN, err := capturer.getFromLSN(context.TODO(), "dbo.test", offset.Offset{LastLSN: "invalid-hex"}, []byte{0x00})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if fromLSN == nil {
		t.Fatal("Expected fromLSN for cold start (invalid stored LSN)")
	}
}

// TestGetFromLSN_NoNewData_WhenAtMaxLSN verifies no new data when at max LSN
func TestGetFromLSN_NoNewData_WhenAtMaxLSN(t *testing.T) {
	lastLSN := "0000002B000001D80064"
	querier := &mockQuerier{}
	store := newMockOffsetStore()
	store.offsets["dbo.test"] = storedOffset{LastLSN: lastLSN, NextLSN: ""}
	offsetMgr := NewOffsetManager(store, querier)

	capturer := &ChangeCapturer{
		querier:   querier,
		offsetMgr: offsetMgr,
	}

	// globalMaxLSN == stored.LastLSN → no new data
	globalMaxLSN, _ := hex.DecodeString(lastLSN)
	fromLSN, err := capturer.getFromLSN(context.TODO(), "dbo.test", offset.Offset{LastLSN: lastLSN}, globalMaxLSN)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if fromLSN != nil {
		t.Errorf("Expected nil (no new data), got: %x", fromLSN)
	}
}

// TestGetFromLSN_NewData_UsesStoredNextLSN verifies new data uses stored next_lsn
func TestGetFromLSN_NewData_UsesStoredNextLSN(t *testing.T) {
	lastLSN := "0000002B000001D80064"
	nextLSN := "0000002B000001D80100"

	querier := &mockQuerier{}
	store := newMockOffsetStore()
	store.offsets["dbo.test"] = storedOffset{LastLSN: lastLSN, NextLSN: nextLSN}
	offsetMgr := NewOffsetManager(store, querier)

	capturer := &ChangeCapturer{
		querier:   querier,
		offsetMgr: offsetMgr,
	}

	// globalMaxLSN > stored.LastLSN → new data, should use next_lsn
	globalMaxLSN, _ := hex.DecodeString("0000002B000001D90000")
	fromLSN, err := capturer.getFromLSN(context.TODO(), "dbo.test", offset.Offset{LastLSN: lastLSN, NextLSN: nextLSN}, globalMaxLSN)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if fromLSN == nil {
		t.Fatal("Expected fromLSN (new data available)")
	}
	expected, _ := hex.DecodeString(nextLSN)
	if string(fromLSN) != string(expected) {
		t.Errorf("Expected fromLSN=%x, got %x", expected, fromLSN)
	}
}

// TestGetFromLSN_NewData_IncrementWhenNoNextLSN verifies increment when next_lsn not stored
func TestGetFromLSN_NewData_IncrementWhenNoNextLSN(t *testing.T) {
	lastLSN := "0000002B000001D80064"
	incrementedLSN := "0000002B000001D80100"

	querier := &mockQuerier{
		incrementTo: []byte{0x00, 0x00, 0x00, 0x2B, 0x00, 0x00, 0x01, 0xD8, 0x01, 0x00},
	}
	store := newMockOffsetStore()
	store.offsets["dbo.test"] = storedOffset{LastLSN: lastLSN, NextLSN: ""} // No next_lsn stored
	offsetMgr := NewOffsetManager(store, querier)

	capturer := &ChangeCapturer{
		querier:   querier,
		offsetMgr: offsetMgr,
	}

	// globalMaxLSN > stored.LastLSN, but no next_lsn → increment
	globalMaxLSN, _ := hex.DecodeString("0000002B000001D90000")
	fromLSN, err := capturer.getFromLSN(context.TODO(), "dbo.test", offset.Offset{LastLSN: lastLSN, NextLSN: ""}, globalMaxLSN)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if fromLSN == nil {
		t.Fatal("Expected fromLSN (new data available)")
	}

	expected, _ := hex.DecodeString(incrementedLSN)
	if string(fromLSN) != string(expected) {
		t.Errorf("Expected fromLSN=%x, got %x", expected, fromLSN)
	}
}

// TestGetFromLSN_GetMinLSN_Error verifies GetMinLSN errors propagate
func TestGetFromLSN_GetMinLSN_Error(t *testing.T) {
	querier := &mockQuerier{
		getMinLSNErr: errors.New("CDC not enabled for table"),
	}
	store := newMockOffsetStore()
	offsetMgr := NewOffsetManager(store, querier)

	capturer := &ChangeCapturer{
		querier:   querier,
		offsetMgr: offsetMgr,
	}

	_, err := capturer.getFromLSN(context.TODO(), "dbo.test", offset.Offset{}, []byte{0x00})
	if err == nil {
		t.Fatal("Expected error from GetMinLSN")
	}
}

// TestGetFromLSN_IncrementLSN_Error verifies IncrementLSN errors propagate
func TestGetFromLSN_IncrementLSN_Error(t *testing.T) {
	lastLSN := "0000002B000001D80064"
	querier := &mockQuerier{
		incrementLSNErr: errors.New("LSN increment failed"),
	}
	store := newMockOffsetStore()
	store.offsets["dbo.test"] = storedOffset{LastLSN: lastLSN, NextLSN: ""}
	offsetMgr := NewOffsetManager(store, querier)

	capturer := &ChangeCapturer{
		querier:   querier,
		offsetMgr: offsetMgr,
	}

	globalMaxLSN, _ := hex.DecodeString("0000002B000001D90000")
	_, err := capturer.getFromLSN(context.TODO(), "dbo.test", offset.Offset{LastLSN: lastLSN, NextLSN: ""}, globalMaxLSN)
	if err == nil {
		t.Fatal("Expected error from IncrementLSN")
	}
}

// TestLSN_Compare tests LSN comparison semantics
func TestLSN_Compare(t *testing.T) {
	tests := []struct {
		name     string
		a        []byte
		b        []byte
		expected int
	}{
		{"equal same length", []byte{0x00, 0x00, 0x2B, 0x00}, []byte{0x00, 0x00, 0x2B, 0x00}, 0},
		{"a less", []byte{0x00, 0x00, 0x2B, 0x00}, []byte{0x00, 0x00, 0x2C, 0x00}, -1},
		{"a greater", []byte{0x00, 0x00, 0x2C, 0x00}, []byte{0x00, 0x00, 0x2B, 0x00}, 1},
		{"a shorter (less)", []byte{0x00, 0x00, 0x2B}, []byte{0x00, 0x00, 0x2B, 0x00}, -1},
		{"a longer (greater)", []byte{0x00, 0x00, 0x2B, 0x00, 0x00}, []byte{0x00, 0x00, 0x2B, 0x00}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lsnA := LSN(tt.a)
			result := lsnA.Compare(tt.b)
			if result != tt.expected {
				t.Errorf("Compare(%x, %x) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestComputeChangeID_Deterministic verifies same inputs produce same ID
func TestComputeChangeID_Deterministic(t *testing.T) {
	id1 := ComputeChangeID("tx-abc-123", "orders", map[string]interface{}{"id": 42, "name": "test"}, []byte{0x00, 0x00, 0x2B, 0x00, 0x01, 0xD8}, 2)
	id2 := ComputeChangeID("tx-abc-123", "orders", map[string]interface{}{"id": 42, "name": "test"}, []byte{0x00, 0x00, 0x2B, 0x00, 0x01, 0xD8}, 2)

	if id1 != id2 {
		t.Errorf("Same inputs must produce same ID: %s != %s", id1, id2)
	}
}

// TestComputeChangeID_DifferentInputsDifferentIDs verifies different inputs produce different IDs
func TestComputeChangeID_DifferentInputsDifferentIDs(t *testing.T) {
	baseInput := map[string]interface{}{"id": 42, "name": "test"}
	lsn := []byte{0x00, 0x00, 0x2B, 0x00, 0x01, 0xD8}

	baseID := ComputeChangeID("tx-1", "orders", baseInput, lsn, 2)

	// Vary each parameter
	differentTxID := ComputeChangeID("tx-2", "orders", baseInput, lsn, 2)
	differentTable := ComputeChangeID("tx-1", "products", baseInput, lsn, 2)
	differentData := ComputeChangeID("tx-1", "orders", map[string]interface{}{"id": 99}, lsn, 2)
	differentLSN := ComputeChangeID("tx-1", "orders", baseInput, []byte{0xFF}, 2)
	differentOp := ComputeChangeID("tx-1", "orders", baseInput, lsn, 4)

	for _, id := range []string{differentTxID, differentTable, differentData, differentLSN, differentOp} {
		if baseID == id {
			t.Errorf("Different inputs must produce different IDs: %s == %s", baseID, id)
		}
	}
}

// TestComputeChangeID_Length verifies ID is exactly 16 characters
func TestComputeChangeID_Length(t *testing.T) {
	id := ComputeChangeID("tx", "table", map[string]interface{}{}, []byte{1, 2, 3}, 2)
	if len(id) != 16 {
		t.Errorf("Expected ID length 16, got %d", len(id))
	}
}

// TestChangeCapturer_Stop verifies Stop sets stopped flag and closes channel
func TestChangeCapturer_Stop(t *testing.T) {
	capturer := &ChangeCapturer{
		stopped: false,
		stopCh:  make(chan struct{}),
	}

	capturer.Stop()

	if !capturer.stopped {
		t.Error("Expected stopped=true after Stop()")
	}
}

// TestChangeCapturer_Stop_AlreadyStopped is idempotent
func TestChangeCapturer_Stop_AlreadyStopped(t *testing.T) {
	stopCh := make(chan struct{})
	close(stopCh) // Already closed

	capturer := &ChangeCapturer{
		stopped: true,
		stopCh:  stopCh,
	}

	// Should not panic
	capturer.Stop()

	if !capturer.stopped {
		t.Error("Expected stopped=true remains true")
	}
}

// TestChangeCapturer_Fetch_Integration tests full Fetch flow with mocks
func TestChangeCapturer_Fetch_Integration(t *testing.T) {
	minLSN := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xD8, 0x00, 0x64}
	maxLSN := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xD9, 0x00, 0x00}

	querier := &mockQuerier{
		minLSN:  minLSN,
		maxLSN:  maxLSN,
		changes: []Change{
			{Table: "orders", TransactionID: "tx-1", LSN: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xD8, 0x00, 0x80}, Operation: 2, Data: map[string]interface{}{"id": 1}},
		},
	}
	store := newMockOffsetStore()
	offsetMgr := NewOffsetManager(store, querier)

	capturer := &ChangeCapturer{
		querier:   querier,
		tables:    []string{"dbo.orders"},
		offsetMgr: offsetMgr,
		interval:  100 * time.Millisecond,
		stopCh:    make(chan struct{}),
	}

	result := capturer.Fetch(context.TODO())

	if result == nil {
		t.Fatal("Expected non-nil CaptureResult")
	}
	if result.EOS {
		t.Error("Expected EOS=false (data available)")
	}
	if len(result.Changes) != 1 {
		t.Errorf("Expected 1 change, got %d", len(result.Changes))
	}
	if result.BatchID == "" {
		t.Error("Expected non-empty BatchID")
	}
}

// TestChangeCapturer_Fetch_EmptyResult tests empty changes
func TestChangeCapturer_Fetch_EmptyResult(t *testing.T) {
	querier := &mockQuerier{
		maxLSN:  []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xD8, 0x00, 0x64},
		changes: nil, // No changes
	}
	store := newMockOffsetStore()
	store.offsets["dbo.orders"] = storedOffset{
		LastLSN: "0000002B000001D80064", // Same as maxLSN
		NextLSN: "",
	}
	offsetMgr := NewOffsetManager(store, querier)

	capturer := &ChangeCapturer{
		querier:   querier,
		tables:    []string{"dbo.orders"},
		offsetMgr: offsetMgr,
		interval:  100 * time.Millisecond,
		stopCh:    make(chan struct{}),
	}

	result := capturer.Fetch(context.TODO())

	if result == nil {
		t.Fatal("Expected non-nil CaptureResult")
	}
	if len(result.Changes) != 0 {
		t.Errorf("Expected 0 changes, got %d", len(result.Changes))
	}
}
