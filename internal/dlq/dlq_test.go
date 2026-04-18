package dlq

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func setupTestDLQ(t *testing.T) (*DLQ, func()) {
	t.Helper()

	// Create temp file
	tmpFile, err := os.CreateTemp("", "dlq_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	dlq, err := New(context.Background(), tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		t.Fatalf("Failed to create DLQ: %v", err)
	}

	cleanup := func() {
		_ = dlq.CloseAndDB()
		_ = os.Remove(tmpPath)
	}

	return dlq, cleanup
}

func createTestEntry(t *testing.T, lsn, tableName, operation string) *DLQEntry {
	t.Helper()

	changeData := map[string]interface{}{
		"id":    123,
		"name":  "test",
		"value": 42,
	}
	changeJSON, err := json.Marshal(changeData)
	if err != nil {
		t.Fatalf("Failed to marshal change data: %v", err)
	}

	return &DLQEntry{
		LSN:          lsn,
		TableName:    tableName,
		Operation:    operation,
		ChangeData:   string(changeJSON),
		ErrorMessage: "test error",
		RetryCount:   0,
		Status:       StatusPending,
	}
}

func TestDLQ_New(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "dlq_new_test_*.db")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		tmpPath := tmpFile.Name()
		if err := tmpFile.Close(); err != nil {
			t.Fatalf("Failed to close temp file: %v", err)
		}
		defer func() { _ = os.Remove(tmpPath) }()

		dlq, err := New(context.Background(), tmpPath)
		if err != nil {
			t.Fatalf("Failed to create DLQ: %v", err)
		}
		defer func() { _ = dlq.CloseAndDB() }()

		if dlq == nil {
			t.Fatal("Expected DLQ instance")
		}
		if dlq.db == nil {
			t.Fatal("Expected database connection")
		}
	})

	t.Run("invalid path", func(t *testing.T) {
		_, err := New(context.Background(), "/nonexistent/path/to/db.db")
		if err == nil {
			t.Fatal("Expected error for invalid path")
		}
	})
}

func TestDLQ_Write(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
		err := dlq.Write(entry)
		if err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}

		if entry.ID == 0 {
			t.Fatal("Expected entry ID to be set")
		}
		if entry.Status != StatusPending {
			t.Fatalf("Expected status pending, got %s", entry.Status)
		}
	})

	t.Run("closed dlq", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		_ = dlq.Close()

		entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
		err := dlq.Write(entry)
		if err != ErrDLQClosed {
			t.Fatalf("Expected ErrDLQClosed, got %v", err)
		}
	})

	t.Run("multiple entries", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		for i := 0; i < 5; i++ {
			entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
			err := dlq.Write(entry)
			if err != nil {
				t.Fatalf("Failed to write entry %d: %v", i, err)
			}
			if entry.ID != int64(i+1) {
				t.Fatalf("Expected ID %d, got %d", i+1, entry.ID)
			}
		}
	})
}

func TestDLQ_List(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		entries, err := dlq.List("")
		if err != nil {
			t.Fatalf("Failed to list entries: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("Expected 0 entries, got %d", len(entries))
		}
	})

	t.Run("list all entries", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		// Write 3 entries
		for i := 0; i < 3; i++ {
			entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
			if err := dlq.Write(entry); err != nil {
				t.Fatalf("Failed to write entry: %v", err)
			}
		}
		_ = dlq.Flush()

		entries, err := dlq.List("")
		if err != nil {
			t.Fatalf("Failed to list entries: %v", err)
		}
		if len(entries) != 3 {
			t.Fatalf("Expected 3 entries, got %d", len(entries))
		}
	})

	t.Run("filter by status", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		// Write entries with different statuses
		entry1 := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
		entry1.Status = StatusPending
		if err := dlq.Write(entry1); err != nil {
			t.Fatalf("Failed to write entry 1: %v", err)
		}

		entry2 := createTestEntry(t, "0x1A2B3D", "users", "UPDATE")
		entry2.Status = StatusResolved
		if err := dlq.Write(entry2); err != nil {
			t.Fatalf("Failed to write entry 2: %v", err)
		}
		_ = dlq.Flush()

		// List pending entries
		entries, err := dlq.List(string(StatusPending))
		if err != nil {
			t.Fatalf("Failed to list pending entries: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("Expected 1 pending entry, got %d", len(entries))
		}
		if entries[0].Status != StatusPending {
			t.Fatalf("Expected pending status, got %s", entries[0].Status)
		}
	})

	t.Run("closed dlq", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()
		_ = dlq.Close()

		_, err := dlq.List("")
		if err != ErrDLQClosed {
			t.Fatalf("Expected ErrDLQClosed, got %v", err)
		}
	})
}

func TestDLQ_Get(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
		if err := dlq.Write(entry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
		_ = dlq.Flush()

		retrieved, err := dlq.Get(entry.ID)
		if err != nil {
			t.Fatalf("Failed to get entry: %v", err)
		}

		if retrieved.ID != entry.ID {
			t.Fatalf("Expected ID %d, got %d", entry.ID, retrieved.ID)
		}
		if retrieved.LSN != entry.LSN {
			t.Fatalf("Expected LSN %s, got %s", entry.LSN, retrieved.LSN)
		}
	})

	t.Run("not found", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		_, err := dlq.Get(999)
		if err != ErrEntryNotFound {
			t.Fatalf("Expected ErrEntryNotFound, got %v", err)
		}
	})

	t.Run("closed dlq", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()
		_ = dlq.Close()

		_, err := dlq.Get(1)
		if err != ErrDLQClosed {
			t.Fatalf("Expected ErrDLQClosed, got %v", err)
		}
	})
}

func TestDLQ_Replay(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
		if err := dlq.Write(entry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
		_ = dlq.Flush()

		handlerCalled := false
		handler := func(e *DLQEntry) error {
			handlerCalled = true
			if e.ID != entry.ID {
				t.Errorf("Expected entry ID %d, got %d", entry.ID, e.ID)
			}
			return nil
		}

		err := dlq.Replay(context.Background(), entry.ID, handler)
		if err != nil {
			t.Fatalf("Replay failed: %v", err)
		}
		_ = dlq.Flush()

		if !handlerCalled {
			t.Fatal("Expected handler to be called")
		}

		// Verify status changed to resolved
		retrieved, err := dlq.Get(entry.ID)
		if err != nil {
			t.Fatalf("Failed to get entry: %v", err)
		}
		if retrieved.Status != StatusResolved {
			t.Fatalf("Expected status resolved, got %s", retrieved.Status)
		}
	})

	t.Run("handler failure", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
		if err := dlq.Write(entry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
		_ = dlq.Flush()

		handler := func(e *DLQEntry) error {
			return &TestError{Message: "handler failed"}
		}

		err := dlq.Replay(context.Background(), entry.ID, handler)
		if err == nil {
			t.Fatal("Expected replay to fail")
		}
		_ = dlq.Flush()

		// Verify status reverted to pending
		retrieved, err := dlq.Get(entry.ID)
		if err != nil {
			t.Fatalf("Failed to get entry: %v", err)
		}
		if retrieved.Status != StatusPending {
			t.Fatalf("Expected status pending, got %s", retrieved.Status)
		}
	})

	t.Run("not found", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		handler := func(e *DLQEntry) error { return nil }
		err := dlq.Replay(context.Background(), 999, handler)
		if err != ErrEntryNotFound {
			t.Fatalf("Expected ErrEntryNotFound, got %v", err)
		}
	})

	t.Run("closed dlq", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()
		_ = dlq.Close()

		handler := func(e *DLQEntry) error { return nil }
		err := dlq.Replay(context.Background(), 1, handler)
		if err != ErrDLQClosed {
			t.Fatalf("Expected ErrDLQClosed, got %v", err)
		}
	})
}

func TestDLQ_Ignore(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
		if err := dlq.Write(entry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}

		err := dlq.Ignore(entry.ID, "test ignore note")
		if err != nil {
			t.Fatalf("Ignore failed: %v", err)
		}
		_ = dlq.Flush()

		retrieved, err := dlq.Get(entry.ID)
		if err != nil {
			t.Fatalf("Failed to get entry: %v", err)
		}
		if retrieved.Status != StatusIgnored {
			t.Fatalf("Expected status ignored, got %s", retrieved.Status)
		}
		if retrieved.ResolvedNote != "test ignore note" {
			t.Fatalf("Expected note 'test ignore note', got %s", retrieved.ResolvedNote)
		}
	})

	t.Run("not found", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		err := dlq.Ignore(999, "test note")
		if err != ErrEntryNotFound {
			t.Fatalf("Expected ErrEntryNotFound, got %v", err)
		}
	})

	t.Run("closed dlq", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()
		_ = dlq.Close()

		err := dlq.Ignore(1, "test note")
		if err != ErrDLQClosed {
			t.Fatalf("Expected ErrDLQClosed, got %v", err)
		}
	})
}

func TestDLQ_Delete(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
		if err := dlq.Write(entry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}

		err := dlq.Delete(entry.ID)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = dlq.Get(entry.ID)
		if err != ErrEntryNotFound {
			t.Fatalf("Expected ErrEntryNotFound after delete, got %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		err := dlq.Delete(999)
		if err != ErrEntryNotFound {
			t.Fatalf("Expected ErrEntryNotFound, got %v", err)
		}
	})

	t.Run("closed dlq", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()
		_ = dlq.Close()

		err := dlq.Delete(1)
		if err != ErrDLQClosed {
			t.Fatalf("Expected ErrDLQClosed, got %v", err)
		}
	})
}

func TestDLQ_Stats(t *testing.T) {
	t.Run("empty stats", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		stats, err := dlq.Stats()
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}
		if len(stats) != 0 {
			t.Fatalf("Expected empty stats, got %d entries", len(stats))
		}
	})

	t.Run("stats with entries", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		// Write entries with different statuses
		for i := 0; i < 3; i++ {
			entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
			entry.Status = StatusPending
			if err := dlq.Write(entry); err != nil {
				t.Fatalf("Failed to write entry: %v", err)
			}
		}

		for i := 0; i < 2; i++ {
			entry := createTestEntry(t, "0x1A2B3D", "users", "UPDATE")
			entry.Status = StatusResolved
			if err := dlq.Write(entry); err != nil {
				t.Fatalf("Failed to write entry: %v", err)
			}
		}
		_ = dlq.Flush()

		stats, err := dlq.Stats()
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}

		if stats[StatusPending] != 3 {
			t.Fatalf("Expected 3 pending, got %d", stats[StatusPending])
		}
		if stats[StatusResolved] != 2 {
			t.Fatalf("Expected 2 resolved, got %d", stats[StatusResolved])
		}
	})

	t.Run("closed dlq", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()
		_ = dlq.Close()

		_, err := dlq.Stats()
		if err != ErrDLQClosed {
			t.Fatalf("Expected ErrDLQClosed, got %v", err)
		}
	})
}

func TestDLQ_Count(t *testing.T) {
	t.Run("count pending", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		for i := 0; i < 5; i++ {
			entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
			if err := dlq.Write(entry); err != nil {
				t.Fatalf("Failed to write entry: %v", err)
			}
		}
		_ = dlq.Flush()

		count, err := dlq.Count(StatusPending)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 5 {
			t.Fatalf("Expected 5 pending, got %d", count)
		}
	})

	t.Run("count resolved", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
		entry.Status = StatusResolved
		if err := dlq.Write(entry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
		_ = dlq.Flush()

		count, err := dlq.Count(StatusResolved)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 1 {
			t.Fatalf("Expected 1 resolved, got %d", count)
		}
	})

	t.Run("closed dlq", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()
		_ = dlq.Close()

		_, err := dlq.Count(StatusPending)
		if err != ErrDLQClosed {
			t.Fatalf("Expected ErrDLQClosed, got %v", err)
		}
	})
}

func TestDLQ_CountAll(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		count, err := dlq.CountAll()
		if err != nil {
			t.Fatalf("CountAll failed: %v", err)
		}
		if count != 0 {
			t.Fatalf("Expected 0 entries, got %d", count)
		}
	})

	t.Run("multiple entries", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()

		for i := 0; i < 10; i++ {
			entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
			if err := dlq.Write(entry); err != nil {
				t.Fatalf("Failed to write entry: %v", err)
			}
		}
		_ = dlq.db.Flush()

		count, err := dlq.CountAll()
		if err != nil {
			t.Fatalf("CountAll failed: %v", err)
		}
		if count != 10 {
			t.Fatalf("Expected 10 entries, got %d", count)
		}
	})

	t.Run("closed dlq", func(t *testing.T) {
		dlq, cleanup := setupTestDLQ(t)
		defer cleanup()
		_ = dlq.Close()

		_, err := dlq.CountAll()
		if err != ErrDLQClosed {
			t.Fatalf("Expected ErrDLQClosed, got %v", err)
		}
	})
}

func TestDLQ_EncodeDecodeChangeData(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		data := map[string]interface{}{
			"id":     123,
			"name":   "test",
			"value":  42.5,
			"active": true,
		}

		encoded, err := EncodeChangeData(data)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := DecodeChangeData(encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded["id"] != float64(123) {
			t.Fatalf("Expected id 123, got %v", decoded["id"])
		}
		if decoded["name"] != "test" {
			t.Fatalf("Expected name 'test', got %v", decoded["name"])
		}
	})

	t.Run("nil data", func(t *testing.T) {
		encoded, err := EncodeChangeData(nil)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		if encoded != "" {
			t.Fatalf("Expected empty string for nil data, got %s", encoded)
		}

		decoded, err := DecodeChangeData("")
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		if decoded != nil {
			t.Fatalf("Expected nil for empty string, got %v", decoded)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		_, err := DecodeChangeData("invalid json")
		if err == nil {
			t.Fatal("Expected error for invalid JSON")
		}
	})
}

func TestDLQ_CloseAndDB(t *testing.T) {
	dlq, cleanup := setupTestDLQ(t)
	defer cleanup()

	// Write an entry to verify DB is working
	entry := createTestEntry(t, "0x1A2B3C", "users", "INSERT")
	if err := dlq.Write(entry); err != nil {
		t.Fatalf("Failed to write entry before close: %v", err)
	}

	// Close the DB
	if err := dlq.CloseAndDB(); err != nil {
		t.Fatalf("CloseAndDB failed: %v", err)
	}

	// Verify operations fail after close
	err := dlq.Write(entry)
	if err != ErrDLQClosed {
		t.Fatalf("Expected ErrDLQClosed after close, got %v", err)
	}
}

// TestError is a simple error type for testing
type TestError struct {
	Message string
}

func (e *TestError) Error() string {
	return e.Message
}
