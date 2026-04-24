package snapshot

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKeyInfo_BuildOrderBy(t *testing.T) {
	tests := []struct {
		name     string
		pkInfo   *PrimaryKeyInfo
		expected string
	}{
		{
			name:     "single column PK",
			pkInfo:   &PrimaryKeyInfo{Columns: []string{"id"}},
			expected: "id",
		},
		{
			name:     "composite PK two columns",
			pkInfo:   &PrimaryKeyInfo{Columns: []string{"tenant_id", "id"}},
			expected: "tenant_id, id",
		},
		{
			name:     "composite PK three columns",
			pkInfo:   &PrimaryKeyInfo{Columns: []string{"org_id", "table_name", "id"}},
			expected: "org_id, table_name, id",
		},
		{
			name:     "empty PK - edge case",
			pkInfo:   &PrimaryKeyInfo{Columns: []string{}},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.pkInfo.BuildOrderBy()
			if got != tt.expected {
				t.Errorf("BuildOrderBy() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestPrimaryKeyInfo_BuildPagedQuery(t *testing.T) {
	tests := []struct {
		name    string
		pkInfo *PrimaryKeyInfo
		schema string
		table string
		off   int
		batch int
	}{
		{
			name:    "single PK, first batch",
			pkInfo:  &PrimaryKeyInfo{Columns: []string{"id"}},
			schema:  "dbo",
			table:  "orders",
			off:    0,
			batch:  1000,
		},
		{
			name:    "single PK, middle batch",
			pkInfo:  &PrimaryKeyInfo{Columns: []string{"id"}},
			schema:  "dbo",
			table:  "orders",
			off:    5000,
			batch:  1000,
		},
		{
			name:    "composite PK",
			pkInfo:  &PrimaryKeyInfo{Columns: []string{"tenant_id", "id"}},
			schema:  "dbo",
			table:  "items",
			off:    0,
			batch:  500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.pkInfo.BuildPagedQuery(tt.schema, tt.table, tt.batch, tt.off)
			if len(got) == 0 {
				t.Error("BuildPagedQuery returned empty string")
			}
		})
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.BatchSize != 10000 {
		t.Errorf("DefaultConfig().BatchSize = %d, want 10000", cfg.BatchSize)
	}
}

func TestConfig_CustomBatchSize(t *testing.T) {
	cfg := &Config{BatchSize: 5000}
	if cfg.BatchSize != 5000 {
		t.Errorf("Config.BatchSize = %d, want 5000", cfg.BatchSize)
	}
}

func TestQuerier_LSNCapture(t *testing.T) {
	startLSN := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}

	// Test hex encoding - should be 20 hex chars for 10 bytes
	lsnHex := hex.EncodeToString(startLSN)
	expected := "00000000000000000001"
	if lsnHex != expected {
		t.Errorf("hex.EncodeToString(startLSN) = %s, want %s", lsnHex, expected)
	}
}

func TestQuerier_IncrementLSN(t *testing.T) {
	startLSN := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}

	if len(startLSN) != 10 {
		t.Errorf("startLSN should be 10 bytes for MSSQL LSN")
	}

	nextLSN := make([]byte, len(startLSN))
	copy(nextLSN, startLSN)
	nextLSN[9] = 0x02

	if string(nextLSN) == string(startLSN) {
		t.Errorf("increment LSN should differ from start LSN")
	}
}

// testHandler collects changes for testing
type testHandler struct {
	changes [][]core.Change
	err     error
}

func (h *testHandler) HandleTable(ctx context.Context, changes []core.Change) error {
	if h.err != nil {
		return h.err
	}
	h.changes = append(h.changes, changes)
	return nil
}

// testOffsetStore implements OffsetUpdater for testing
type testOffsetStore struct {
	offsets map[string]struct{ last, next string }
	flushes int
}

func newTestOffsetStore() *testOffsetStore {
	return &testOffsetStore{
		offsets: make(map[string]struct{ last, next string }),
	}
}

func (s *testOffsetStore) Set(table string, lastLSN, nextLSN string) error {
	s.offsets[table] = struct{ last, next string }{lastLSN, nextLSN}
	return nil
}

func (s *testOffsetStore) Flush() error {
	s.flushes++
	return nil
}

func (s *testOffsetStore) Get(table string) (string, string, bool) {
	if o, ok := s.offsets[table]; ok {
		return o.last, o.next, true
	}
	return "", "", false
}

func TestNewQuerier_WithDefaults(t *testing.T) {
	config := DefaultConfig()

	if config.BatchSize != 10000 {
		t.Errorf("Default BatchSize = %d, want 10000", config.BatchSize)
	}
}

func TestNewQuerier_Timezone(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err, "Failed to load timezone")
	q := NewQuerier(nil, loc, nil)

	if q.timezone != loc {
		t.Errorf("timezone = %v, want %v", q.timezone, loc)
	}
}

func TestHandlerFunc(t *testing.T) {
	var called bool
	var captured []core.Change

	handler := TableHandlerFunc(func(ctx context.Context, changes []core.Change) error {
		called = true
		captured = changes
		return nil
	})

	ctx := context.Background()
	testChanges := []core.Change{
		{Table: "test", Operation: core.OpInsert, Data: map[string]interface{}{"id": 1}},
	}

	err := handler.HandleTable(ctx, testChanges)
	if err != nil {
		t.Errorf("HandleTable error = %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
	if len(captured) != 1 {
		t.Errorf("captured changes count = %d, want 1", len(captured))
	}
}

func TestRunner_OffsetUpdate(t *testing.T) {
	store := newTestOffsetStore()

	err := store.Set("dbo.test_table", "000000000000000001", "000000000000000002")
	if err != nil {
		t.Errorf("Set error = %v", err)
	}

	last, next, ok := store.Get("dbo.test_table")
	if !ok {
		t.Error("offset not found")
	}
	if last != "000000000000000001" {
		t.Errorf("last LSN = %s, want 000000000000000001", last)
	}
	if next != "000000000000000002" {
		t.Errorf("next LSN = %s, want 000000000000000002", next)
	}

	err = store.Flush()
	if err != nil {
		t.Errorf("Flush error = %v", err)
	}
	if store.flushes != 1 {
		t.Errorf("flush count = %d, want 1", store.flushes)
	}
}

func TestRunner_GetNonExistentOffset(t *testing.T) {
	store := newTestOffsetStore()

	_, _, ok := store.Get("dbo.non_existent")
	if ok {
		t.Error("should not find non-existent offset")
	}
}

// mockDBForTxOptions tests that BeginTx is called with correct options.
// SQL Server rejects snapshot isolation + ReadOnly:true combination,
// so the code must use ReadOnly:false (or omit it) when using LevelSnapshot.
type mockDBForTxOptions struct {
	beginTxCalled    bool
	beginTxOptions   *sql.TxOptions
	beginTxErr       error
}

func (m *mockDBForTxOptions) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	m.beginTxCalled = true
	m.beginTxOptions = opts
	if m.beginTxErr != nil {
		return nil, m.beginTxErr
	}
	// Simulate SQL Server behavior: snapshot isolation + ReadOnly:true is rejected.
	if opts != nil && opts.Isolation == sql.LevelSnapshot && opts.ReadOnly {
		return nil, fmt.Errorf("read-only transactions are not supported")
	}
	// Return a minimal mock tx
	return &sql.Tx{}, nil
}

func (m *mockDBForTxOptions) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}

func (m *mockDBForTxOptions) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return &sql.Row{}
}

func (m *mockDBForTxOptions) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}

// TestBeginTxWithSnapshotIsolation_NoReadOnly verifies that:
// 1. The production snapshotTxOptions() returns correct options (ReadOnly:false).
// 2. Those options do NOT trigger the SQL Server rejection error when passed to BeginTx.
// This guards against a regression where snapshotTxOptions() is changed to ReadOnly:true.
func TestBeginTxWithSnapshotIsolation_NoReadOnly(t *testing.T) {
	// First: verify production snapshotTxOptions() returns the correct format.
	opts := snapshotTxOptions()
	if opts.Isolation != sql.LevelSnapshot {
		t.Errorf("Isolation = %v, want sql.LevelSnapshot", opts.Isolation)
	}
	if opts.ReadOnly != false {
		t.Errorf("ReadOnly = %v, want false (must NOT be true for SQL Server snapshot)", opts.ReadOnly)
	}

	// Second: verify those options don't trigger the SQL Server rejection.
	mock := &mockDBForTxOptions{}
	_, err := mock.BeginTx(context.Background(), opts)
	if err != nil {
		t.Fatalf("BeginTx with production snapshotTxOptions() should not fail: %v", err)
	}
}

// TestBeginTxWithSnapshotIsolation_ErrorsOnReadOnly verifies that the mock
// correctly rejects the ReadOnly:true + LevelSnapshot combination.
// The mock now checks opts in BeginTx (not a static error), so this test
// proves the conditional rejection logic works: ReadOnly:true → error.
func TestBeginTxWithSnapshotIsolation_ErrorsOnReadOnly(t *testing.T) {
	mock := &mockDBForTxOptions{}

	// Pass ReadOnly:true + LevelSnapshot — the buggy combination that SQL Server rejects.
	_, err := mock.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSnapshot,
		ReadOnly:  true,
	})

	if err == nil {
		t.Fatal("expected error when using ReadOnly:true with snapshot, got nil")
	}

	if !strings.Contains(err.Error(), "read-only transactions are not supported") {
		t.Errorf("error = %q, want containing %q", err.Error(), "read-only transactions are not supported")
	}
}

// TestSnapshotTransactionOptions_CorrectFormat verifies the production
// snapshotTxOptions() function returns the correct TxOptions format for
// snapshot isolation. This guards against a future regression where
// snapshotTxOptions() is accidentally changed.
func TestSnapshotTransactionOptions_CorrectFormat(t *testing.T) {
	opts := snapshotTxOptions()

	if opts.Isolation != sql.LevelSnapshot {
		t.Errorf("Isolation = %v, want sql.LevelSnapshot", opts.Isolation)
	}
	if opts.ReadOnly != false {
		t.Errorf("ReadOnly = %v, want false (SQL Server rejects ReadOnly:true with snapshot)", opts.ReadOnly)
	}
}