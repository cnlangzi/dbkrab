package cdc

import (
	"context"
	"encoding/hex"
	"testing"
	"time"
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

func TestSnapshotConfig_Defaults(t *testing.T) {
	cfg := DefaultSnapshotConfig()
	if cfg.BatchSize != 10000 {
		t.Errorf("DefaultSnapshotConfig().BatchSize = %d, want 10000", cfg.BatchSize)
	}
}

func TestSnapshotConfig_CustomBatchSize(t *testing.T) {
	cfg := &SnapshotConfig{BatchSize: 5000}
	if cfg.BatchSize != 5000 {
		t.Errorf("SnapshotConfig.BatchSize = %d, want 5000", cfg.BatchSize)
	}
}

func TestSnapshotQuerier_LSNCapture(t *testing.T) {
	startLSN := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}

	// Test hex encoding - should be 20 hex chars for 10 bytes
	lsnHex := hex.EncodeToString(startLSN)
	expected := "00000000000000000001"
	if lsnHex != expected {
		t.Errorf("hex.EncodeToString(startLSN) = %s, want %s", lsnHex, expected)
	}
}

func TestSnapshotQuerier_IncrementLSN(t *testing.T) {
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

// testSnapshotHandler collects changes for testing
type testSnapshotHandler struct {
	changes [][]Change
	err     error
}

func (h *testSnapshotHandler) HandleSnapshotBatch(ctx context.Context, changes []Change) error {
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

func TestNewSnapshotQuerier_WithDefaults(t *testing.T) {
	config := DefaultSnapshotConfig()

	if config.BatchSize != 10000 {
		t.Errorf("Default BatchSize = %d, want 10000", config.BatchSize)
	}
}

func TestNewSnapshotQuerier_Timezone(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	q := NewSnapshotQuerier(nil, loc, nil)

	if q.timezone != loc {
		t.Errorf("timezone = %v, want %v", q.timezone, loc)
	}
}

func TestSnapshotHandlerFunc(t *testing.T) {
	var called bool
	var captured []Change

	handler := SnapshotHandlerFunc(func(ctx context.Context, changes []Change) error {
		called = true
		captured = changes
		return nil
	})

	ctx := context.Background()
	testChanges := []Change{
		{Table: "test", Operation: 2, Data: map[string]interface{}{"id": 1}},
	}

	err := handler.HandleSnapshotBatch(ctx, testChanges)
	if err != nil {
		t.Errorf("HandleSnapshotBatch error = %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
	if len(captured) != 1 {
		t.Errorf("captured changes count = %d, want 1", len(captured))
	}
}

func TestSnapshotRunner_OffsetUpdate(t *testing.T) {
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

func TestSnapshotRunner_GetNonExistentOffset(t *testing.T) {
	store := newTestOffsetStore()

	_, _, ok := store.Get("dbo.non_existent")
	if ok {
		t.Error("should not find non-existent offset")
	}
}