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
	offset, err := store.Get("nonexistent")
	if err != nil {
		t.Errorf("Get() unexpected error = %v", err)
	}
	if offset.LSN != "" {
		t.Error("Expected empty LSN for non-existent offset")
	}

	// Set offset
	if err := store.Set("dbo_orders", "01020304"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// Get offset
	offset, err = store.Get("dbo_orders")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
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

	offset2, err := store2.Get("dbo_orders")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if offset2.LSN != "01020304" {
		t.Errorf("LSN after reload = %v, want 01020304", offset2.LSN)
	}
}

func TestStoreGetAll(t *testing.T) {
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

	// Set multiple offsets
	if err := store.Set("dbo_orders", "01020304"); err != nil {
		t.Errorf("Set() error = %v", err)
	}
	if err := store.Set("dbo_customers", "02030405"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// GetAll
	all, err := store.GetAll()
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}

	if len(all) != 2 {
		t.Errorf("GetAll() returned %d items, want 2", len(all))
	}

	if all["dbo_orders"].LSN != "01020304" {
		t.Errorf("dbo_orders LSN = %v, want 01020304", all["dbo_orders"].LSN)
	}

	if all["dbo_customers"].LSN != "02030405" {
		t.Errorf("dbo_customers LSN = %v, want 02030405", all["dbo_customers"].LSN)
	}
}
