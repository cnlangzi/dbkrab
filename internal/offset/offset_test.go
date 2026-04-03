package offset

import (
	"errors"
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

// TestJSONStore_CorruptedFile tests that corrupted JSON files are handled gracefully
func TestJSONStore_CorruptedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	// Write invalid JSON to the file
	if err := os.WriteFile(path, []byte("{ not valid json "), 0o600); err != nil {
		t.Fatalf("failed to write corrupt JSON file: %v", err)
	}

	store := NewStore(path)

	// Load should fail with corrupted file
	if err := store.Load(); err == nil {
		t.Fatal("Load() on corrupted JSON store: expected error, got nil")
	}
}

// TestSQLiteStore_ErrStoreClosed tests that operations after close return ErrStoreClosed
func TestSQLiteStore_ErrStoreClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "offsets.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}

	// Close the store
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Get after Close
	if _, err := store.Get("some-key"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Get() after Close: expected ErrStoreClosed, got %v", err)
	}

	// GetAll after Close
	if _, err := store.GetAll(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("GetAll() after Close: expected ErrStoreClosed, got %v", err)
	}

	// Set after Close
	if err := store.Set("some-key", "01020304"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Set() after Close: expected ErrStoreClosed, got %v", err)
	}
}

// TestStoreInterface_NonExistentKey tests consistent behavior for non-existent keys across backends
func TestStoreInterface_NonExistentKey(t *testing.T) {
	type storeFactory struct {
		name string
		new  func(t *testing.T) (StoreInterface, func())
	}

	factories := []storeFactory{
		{
			name: "json",
			new: func(t *testing.T) (StoreInterface, func()) {
				t.Helper()
				path := filepath.Join(t.TempDir(), "offsets.json")
				store := NewStore(path)
				return store, func() {}
			},
		},
		{
			name: "sqlite",
			new: func(t *testing.T) (StoreInterface, func()) {
				t.Helper()
				path := filepath.Join(t.TempDir(), "offsets.db")
				store, err := NewSQLiteStore(path)
				if err != nil {
					t.Fatalf("NewSQLiteStore() error = %v", err)
				}
				return store, func() { store.Close() }
			},
		},
	}

	for _, factory := range factories {
		t.Run(factory.name, func(t *testing.T) {
			store, cleanup := factory.new(t)
			defer cleanup()

			// Load for JSON store (SQLite doesn't need explicit load)
			if err := store.Load(); err != nil {
				t.Fatalf("Load() error = %v", err)
			}

			offset, err := store.Get("nonexistent")
			if err != nil {
				t.Fatalf("Get() unexpected error for non-existent key: %v", err)
			}

			// Expect zero Offset for non-existent keys
			if offset.LSN != "" {
				t.Fatalf("Get() for non-existent key: expected empty LSN, got %+v", offset)
			}
		})
	}
}
