package offset

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStore(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "dbkrab-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("os.RemoveAll error: %v", err)
		}
	}()

	path := filepath.Join(tmpDir, "offset.json")
	store := NewStore(path)

	// Load empty store
	if err := store.Load(); err != nil {
		t.Errorf("Load() error = %v", err)
	}

	// Get non-existent offset
	_, ok := store.Get("nonexistent")
	if ok {
		t.Error("Expected false for non-existent offset")
	}

	// Set offset
	if err := store.Set("dbo_orders", "01020304"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// Get offset
	offset, ok := store.Get("dbo_orders")
	if !ok {
		t.Fatal("Expected to find offset")
	}
	if offset.LSN != "01020304" {
		t.Errorf("LSN = %v, want 01020304", offset.LSN)
	}
	if offset.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should not be zero")
	}

	// Reload from file
	store2 := NewStore(path)
	if err := store2.Load(); err != nil {
		t.Errorf("Load() error = %v", err)
	}

	offset2, ok := store2.Get("dbo_orders")
	if !ok {
		t.Fatal("Expected to find offset after reload")
	}
	if offset2.LSN != "01020304" {
		t.Errorf("LSN after reload = %v, want 01020304", offset2.LSN)
	}
}

func TestStoreGetAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbkrab-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("os.RemoveAll error: %v", err)
		}
	}()

	store := NewStore(filepath.Join(tmpDir, "offset.json"))

	// Set multiple offsets
	if err := store.Set("table1", "01020304"); err != nil {
		t.Fatalf("Set table1 failed: %v", err)
	}
	if err := store.Set("table2", "05060708"); err != nil {
		t.Fatalf("Set table2 failed: %v", err)
	}

	// GetAll
	all := store.GetAll()
	if len(all) != 2 {
		t.Errorf("GetAll() returned %d items, want 2", len(all))
	}

	if all["table1"].LSN != "01020304" {
		t.Errorf("table1 LSN = %v, want 01020304", all["table1"].LSN)
	}

	// Verify GetAll returns a copy (mutating it doesn't affect internal state)
	delete(all, "table1")
	all["table2"] = Offset{LSN: "modified"}

	// Verify internal state is unchanged
	offset1, ok := store.Get("table1")
	if !ok {
		t.Error("table1 should still exist after deleting from GetAll result")
	}
	if offset1.LSN != "01020304" {
		t.Errorf("table1 LSN = %v, want 01020304 (should be unchanged)", offset1.LSN)
	}

	offset2, ok := store.Get("table2")
	if !ok {
		t.Error("table2 should still exist")
	}
	if offset2.LSN != "05060708" {
		t.Errorf("table2 LSN = %v, want 05060708 (should be unchanged)", offset2.LSN)
	}
}

func TestStoreSetMultiple(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbkrab-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("os.RemoveAll error: %v", err)
		}
	}()

	path := filepath.Join(tmpDir, "offset.json")
	store := NewStore(path)

	// SetMultiple: update 3 tables at once with same barrier LSN
	updates := map[string]string{
		"dbo.orders":     "0a0b0c0d",
		"dbo.order_items": "0a0b0c0d",
		"dbo.customers":  "0a0b0c0d",
	}
	if err := store.SetMultiple(updates); err != nil {
		t.Fatalf("SetMultiple() error = %v", err)
	}

	// Verify all persisted
	for table, expectedLSN := range updates {
		offset, ok := store.Get(table)
		if !ok {
			t.Errorf("Expected to find offset for %s", table)
			continue
		}
		if offset.LSN != expectedLSN {
			t.Errorf("%s LSN = %v, want %v", table, offset.LSN, expectedLSN)
		}
	}

	// Verify file persistence: reload and check
	store2 := NewStore(path)
	if err := store2.Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	for table, expectedLSN := range updates {
		offset, ok := store2.Get(table)
		if !ok {
			t.Errorf("After reload: offset for %s not found", table)
			continue
		}
		if offset.LSN != expectedLSN {
			t.Errorf("After reload: %s LSN = %v, want %v", table, offset.LSN, expectedLSN)
		}
	}

	// Verify UpdatedAt is not zero after SetMultiple
	for _, table := range []string{"dbo.orders", "dbo.order_items", "dbo.customers"} {
		offset, _ := store.Get(table)
		if offset.UpdatedAt.IsZero() {
			t.Errorf("%s UpdatedAt is zero after SetMultiple", table)
		}
	}

	// Verify SetMultiple only writes once (no partial state)
	// Write 3 tables, then overwrite with 2 tables - all 5 should be in file
	updates2 := map[string]string{
		"dbo.orders":     "deadbeef",
		"dbo.order_items": "deadbeef",
	}
	if err := store.SetMultiple(updates2); err != nil {
		t.Fatalf("SetMultiple() second call error = %v", err)
	}

	// Verify first 3 tables still exist (not wiped by second SetMultiple)
	store3 := NewStore(path)
	if err := store3.Load(); err != nil {
		t.Fatalf("store3.Load() error = %v", err)
	}
	if _, ok := store3.Get("dbo.customers"); !ok {
		t.Error("dbo.customers should still exist after partial SetMultiple update")
	}
	customersOffset, _ := store3.Get("dbo.customers")
	if customersOffset.LSN != "0a0b0c0d" {
		t.Error("dbo.customers LSN should be unchanged after partial update")
	}
}