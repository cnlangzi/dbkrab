package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cnlangzi/dbkrab/internal/core"
)

func TestNewStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbkrab-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("os.RemoveAll error: %v", err)
		}
	}()

	dbPath := filepath.Join(tmpDir, "test.db")
	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error: %v", err)
		}
	}()

	// Check file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file not created")
	}
}

func TestWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbkrab-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("os.RemoveAll error: %v", err)
		}
	}()

	store, err := NewStore(filepath.Join(tmpDir, "test.db"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error: %v", err)
		}
	}()

	// Create a transaction
	tx := core.NewTransaction("test-tx-123")
	tx.AddChange(core.Change{
		Table:     "dbo.orders",
		Operation: core.OpInsert,
		Data:      map[string]interface{}{"id": 1, "name": "test"},
	})
	tx.AddChange(core.Change{
		Table:     "dbo.order_items",
		Operation: core.OpInsert,
		Data:      map[string]interface{}{"id": 2, "order_id": 1},
	})

	// Write
	if err := store.Write(tx); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Read back
	changes, err := store.GetChanges(10)
	if err != nil {
		t.Fatalf("GetChanges() error = %v", err)
	}

	if len(changes) != 2 {
		t.Errorf("GetChanges() returned %d items, want 2", len(changes))
	}

	// Check first change
	first := changes[0]
	if first["transaction_id"] != "test-tx-123" {
		t.Errorf("transaction_id = %v, want test-tx-123", first["transaction_id"])
	}
	if first["operation"] != "INSERT" {
		t.Errorf("operation = %v, want INSERT", first["operation"])
	}
}

func TestWriteMultipleTransactions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbkrab-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("os.RemoveAll error: %v", err)
		}
	}()

	store, err := NewStore(filepath.Join(tmpDir, "test.db"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Logf("store.Close error: %v", err)
		}
	}()

	// Write multiple transactions
	for i := 1; i <= 3; i++ {
		tx := core.NewTransaction(string(rune('A' + i)))
		tx.AddChange(core.Change{
			Table:     "dbo.test",
			Operation: core.OpInsert,
			Data:      map[string]interface{}{"id": i},
		})
		if err := store.Write(tx); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	changes, err := store.GetChanges(0)
	if err != nil {
		t.Fatalf("GetChanges() error = %v", err)
	}

	if len(changes) != 3 {
		t.Errorf("GetChanges() returned %d items, want 3", len(changes))
	}
}