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
	defer os.RemoveAll(tmpDir)

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
	defer os.RemoveAll(tmpDir)

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
}