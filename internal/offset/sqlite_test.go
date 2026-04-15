package offset

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite"
)

// newTestDB creates an in-memory SQLite DB for testing
func newTestDB(t *testing.T) *sqlite.DB {
	t.Helper()

	ctx := context.Background()
	db, err := sqlite.New(ctx, ":memory:")
	if err != nil {
		t.Fatalf("sqlite.New() error = %v", err)
	}

	// Create the offsets table with three-value LSN schema
	_, err = db.Writer.Exec(`
		CREATE TABLE IF NOT EXISTS offsets (
			table_name TEXT PRIMARY KEY,
			last_lsn TEXT,
			next_lsn TEXT,

			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("create table error = %v", err)
	}

	return db
}

func TestSQLiteStore_GetSet(t *testing.T) {
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store := NewSQLiteStore(db)

	// Load should be a no-op
	if err := store.Load(); err != nil {
		t.Errorf("Load() error = %v", err)
	}

	// Save should be a no-op
	if err := store.Save(); err != nil {
		t.Errorf("Save() error = %v", err)
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
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store := NewSQLiteStore(db)

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
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store := NewSQLiteStore(db)

	// Close should be a no-op for SQLiteStore (DB lifecycle managed externally)
	if err := store.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestSQLiteStore_Interface(t *testing.T) {
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	// Verify SQLiteStore implements StoreInterface
	var _ StoreInterface = NewSQLiteStore(db)
}

func TestSQLiteStore_NonExistentKey(t *testing.T) {
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store := NewSQLiteStore(db)

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
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store1 := NewSQLiteStore(db)

	// Set offset with first store instance
	if err := store1.Set("dbo_orders", "01020304", "01020305"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// Create another store instance with the same DB
	store2 := NewSQLiteStore(db)

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
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store := NewSQLiteStore(db)

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
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store := NewSQLiteStore(db)

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
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store := NewSQLiteStore(db)

	// Set offset via store
	if err := store.Set("dbo_orders", "01020304", "01020305"); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// Verify directly in DB
	var lastLSN, nextLSN string
	err := db.Reader.QueryRow(
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
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store := NewSQLiteStore(db)

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
	db := newTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("db.Close error = %v", err)
		}
	}()

	store := NewSQLiteStore(db)

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