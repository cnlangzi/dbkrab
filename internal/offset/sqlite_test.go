package offset

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// newStandaloneTestStore creates a standalone offset store for testing
func newStandaloneTestStore(t *testing.T) *SQLiteStore {
	t.Helper()

	ctx := context.Background()
	store, err := New(ctx, ":memory:")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store
}

func TestSQLiteStore_GetSet(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Load should be a no-op
	if err := store.Load(); err != nil {
		t.Errorf("Load() error = %v", err)
	}

	// Save should be a no-op
	if err := store.Save(); err != nil {
		t.Errorf("Save() error = %v", err)
	}

	// Flush should work (for interface compatibility)
	if err := store.Flush(); err != nil {
		t.Errorf("Flush() error = %v", err)
	}

	// Get non-existent offset
	offset, err := store.Get("nonexistent")
	if err != nil {
		t.Errorf("Get() unexpected error = %v", err)
	}
	if offset.LastLSN != "" {
		t.Error("Expected empty LastLSN for non-existent offset")
	}

	// Set offset
	if err := store.Set("dbo_orders", "01020304", "01020305"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// Get offset
	offset, err = store.Get("dbo_orders")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if offset.LastLSN != "01020304" {
		t.Errorf("LastLSN = %v, want 01020304", offset.LastLSN)
	}
	if offset.NextLSN != "01020305" {
		t.Errorf("NextLSN = %v, want 01020305", offset.NextLSN)
	}
	if offset.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should not be zero")
	}

	// Update offset
	if err := store.Set("dbo_orders", "02030405", "02030406"); err != nil {
		t.Errorf("Set() update error = %v", err)
	}

	offset, err = store.Get("dbo_orders")
	if err != nil {
		t.Fatalf("Get() after update error = %v", err)
	}
	if offset.LastLSN != "02030405" {
		t.Errorf("LastLSN after update = %v, want 02030405", offset.LastLSN)
	}
	if offset.NextLSN != "02030406" {
		t.Errorf("NextLSN after update = %v, want 02030406", offset.NextLSN)
	}
}

func TestSQLiteStore_GetAll(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Set multiple offsets
	if err := store.Set("dbo_orders", "01020304", "01020305"); err != nil {
		t.Errorf("Set() orders error = %v", err)
	}
	if err := store.Set("dbo_customers", "02030405", "02030406"); err != nil {
		t.Errorf("Set() customers error = %v", err)
	}

	// GetAll
	all, err := store.GetAll()
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}

	if len(all) != 2 {
		t.Errorf("GetAll() returned %d items, want 2", len(all))
	}

	if all["dbo_orders"].LastLSN != "01020304" {
		t.Errorf("dbo_orders LastLSN = %v, want 01020304", all["dbo_orders"].LastLSN)
	}

	if all["dbo_customers"].LastLSN != "02030405" {
		t.Errorf("dbo_customers LastLSN = %v, want 02030405", all["dbo_customers"].LastLSN)
	}
}

func TestSQLiteStore_Close(t *testing.T) {
	store := newStandaloneTestStore(t)

	// Close should work for standalone store
	if err := store.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Second close should return nil (already closed)
	if err := store.Close(); err != nil {
		t.Errorf("Second Close() error = %v", err)
	}

	// Operations on closed store should return error
	if err := store.Set("test", "lsn", "next"); err != ErrStoreClosed {
		t.Errorf("Set() on closed store should return ErrStoreClosed, got %v", err)
	}
}

func TestSQLiteStore_Interface(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Verify SQLiteStore implements StoreInterface
	var _ StoreInterface = store
}

func TestSQLiteStore_NonExistentKey(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Load (no-op for SQLite)
	if err := store.Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	offset, err := store.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get() unexpected error for non-existent key: %v", err)
	}

	// Expect zero Offset for non-existent keys
	if offset.LastLSN != "" {
		t.Fatalf("Get() for non-existent key: expected empty LastLSN, got %+v", offset)
	}
}

func TestSQLiteStore_Persistence(t *testing.T) {
	// For standalone store, persistence is tested via file-based DB
	// Use temp file to test that data persists across store instances
	testDB, err := os.MkdirTemp("", "dbkrab-offset-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	defer func() {
		if err := os.RemoveAll(testDB); err != nil {
			t.Logf("RemoveAll() error = %v", err)
		}
	}()
	dbPath := filepath.Join(testDB, "offset.db")

	ctx := context.Background()

	// Create first store instance
	store1, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Set offset with first store instance
	if err := store1.Set("dbo_orders", "01020304", "01020305"); err != nil {
		t.Errorf("Set() error = %v", err)
	}
	if err := store1.Close(); err != nil {
		t.Errorf("store1.Close() error = %v", err)
	}

	// Create second store instance with the same DB file
	store2, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("New() for store2 error = %v", err)
	}
	defer func() {
		if err := store2.Close(); err != nil {
			t.Logf("store2.Close() error = %v", err)
		}
	}()

	// Verify offset is persisted
	offset, err := store2.Get("dbo_orders")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if offset.LastLSN != "01020304" {
		t.Errorf("LastLSN from second store = %v, want 01020304", offset.LastLSN)
	}
}

func TestSQLiteStore_EmptyGetAll(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// GetAll on empty store
	all, err := store.GetAll()
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}

	if len(all) != 0 {
		t.Errorf("GetAll() on empty store returned %d items, want 0", len(all))
	}
}

func TestSQLiteStore_UpdateTimestamp(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Set initial offset
	if err := store.Set("dbo_orders", "01020304", "01020305"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	offset1, err := store.Get("dbo_orders")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	firstUpdatedAt := offset1.UpdatedAt

	// Wait a bit and update
	time.Sleep(10 * time.Millisecond)
	if err := store.Set("dbo_orders", "02030405", "02030406"); err != nil {
		t.Errorf("Set() update error = %v", err)
	}

	offset2, err := store.Get("dbo_orders")
	if err != nil {
		t.Fatalf("Get() after update error = %v", err)
	}

	// UpdatedAt should be newer
	if !offset2.UpdatedAt.After(firstUpdatedAt) {
		t.Errorf("UpdatedAt should be updated, first=%v, second=%v", firstUpdatedAt, offset2.UpdatedAt)
	}
}

// TestSQLiteStore_DirectDBAccess tests that we can verify data directly in the DB
func TestSQLiteStore_DirectDBAccess(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Set offset via store
	if err := store.Set("dbo_orders", "01020304", "01020305"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// Flush to ensure data is written
	if err := store.Flush(); err != nil {
		t.Errorf("Flush() error = %v", err)
	}

	// Verify directly in DB via the underlying db.Reader
	var lastLSN, nextLSN string
	err := store.db.Reader.QueryRow(
		"SELECT last_lsn, next_lsn FROM offsets WHERE table_name = ?",
		"dbo_orders",
	).Scan(&lastLSN, &nextLSN)

	if err == sql.ErrNoRows {
		t.Fatal("offset not found in DB")
	}
	if err != nil {
		t.Fatalf("direct DB query error = %v", err)
	}

	if lastLSN != "01020304" {
		t.Errorf("direct DB lastLSN = %v, want 01020304", lastLSN)
	}
	if nextLSN != "01020305" {
		t.Errorf("direct DB nextLSN = %v, want 01020305", nextLSN)
	}
}

// TestSQLiteStore_GetFromLSN_ColdStart tests GetFromLSN with empty last_lsn (cold start)
func TestSQLiteStore_GetFromLSN_ColdStart(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Get offset for non-existent table (cold start case)
	offset, err := store.Get("new_table")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	// Should return empty offset for cold start
	if offset.LastLSN != "" {
		t.Errorf("LastLSN should be empty for cold start, got %v", offset.LastLSN)
	}
	if offset.NextLSN != "" {
		t.Errorf("NextLSN should be empty for cold start, got %v", offset.NextLSN)
	}
}

// TestSQLiteStore_SetWithEmptyValues tests setting offsets with empty values
func TestSQLiteStore_SetWithEmptyValues(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Set offset with some empty values (e.g., only last_lsn after poll with no changes)
	if err := store.Set("dbo_orders", "01020304", ""); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	offset, err := store.Get("dbo_orders")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if offset.LastLSN != "01020304" {
		t.Errorf("LastLSN = %v, want 01020304", offset.LastLSN)
	}
	if offset.NextLSN != "" {
		t.Errorf("NextLSN should be empty, got %v", offset.NextLSN)
	}
}

// TestSQLiteStore_NewWithMigrations tests that New creates the DB with migrations
func TestSQLiteStore_NewWithMigrations(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Verify the offsets table was created by migrations
	var tableName string
	err := store.db.Reader.QueryRow(
		"SELECT name FROM sqlite_master WHERE type='table' AND name='offsets'",
	).Scan(&tableName)

	if err == sql.ErrNoRows {
		t.Fatal("offsets table was not created by migrations")
	}
	if err != nil {
		t.Fatalf("query sqlite_master error = %v", err)
	}

	if tableName != "offsets" {
		t.Errorf("table name = %v, want 'offsets'", tableName)
	}
}

// TestSQLiteStore_Flush tests the Flush method
func TestSQLiteStore_Flush(t *testing.T) {
	store := newStandaloneTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error = %v", err)
		}
	}()

	// Set offset
	if err := store.Set("dbo_orders", "01020304", "01020305"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// Flush should succeed
	if err := store.Flush(); err != nil {
		t.Errorf("Flush() error = %v", err)
	}

	// Verify data is still there after flush
	offset, err := store.Get("dbo_orders")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if offset.LastLSN != "01020304" {
		t.Errorf("LastLSN after flush = %v, want 01020304", offset.LastLSN)
	}
}

// TestSQLiteStore_OperationsAfterClose tests that operations fail after close
func TestSQLiteStore_OperationsAfterClose(t *testing.T) {
	store := newStandaloneTestStore(t)

	// Close the store
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// All operations should return ErrStoreClosed
	if _, err := store.Get("test"); err != ErrStoreClosed {
		t.Errorf("Get() should return ErrStoreClosed, got %v", err)
	}

	if err := store.Set("test", "lsn", "next"); err != ErrStoreClosed {
		t.Errorf("Set() should return ErrStoreClosed, got %v", err)
	}

	if _, err := store.GetAll(); err != ErrStoreClosed {
		t.Errorf("GetAll() should return ErrStoreClosed, got %v", err)
	}

	if err := store.Flush(); err != ErrStoreClosed {
		t.Errorf("Flush() should return ErrStoreClosed, got %v", err)
	}
}
