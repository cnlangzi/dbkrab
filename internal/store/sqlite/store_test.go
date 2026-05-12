package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDB(t *testing.T) (*store.DB, string) {
	// Create temp dir for test DB
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// For testing, we use a fake/no-op migration path since we create tables inline
	// The actual migration logic is tested in integration tests
	db, err := store.NewFile(context.Background(), dbPath, "dbkrab-store")
	require.NoError(t, err)

	// Create the required tables inline for testing (migrations are tested separately)
	_, err = db.Writer.Exec(`
		CREATE TABLE IF NOT EXISTS transactions (
			id TEXT PRIMARY KEY,
			transaction_id TEXT NOT NULL,
			table_name TEXT NOT NULL,
			operation TEXT NOT NULL,
			data TEXT,
			lsn TEXT,
			changed_at TIMESTAMP,
			pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	require.NoError(t, err)

	_, err = db.Writer.Exec(`CREATE INDEX IF NOT EXISTS idx_transaction_id ON transactions(transaction_id)`)
	require.NoError(t, err)
	_, err = db.Writer.Exec(`CREATE INDEX IF NOT EXISTS idx_table_name ON transactions(table_name)`)
	require.NoError(t, err)
	_, err = db.Writer.Exec(`CREATE INDEX IF NOT EXISTS idx_changed_at ON transactions(changed_at)`)
	require.NoError(t, err)
	_, err = db.Writer.Exec(`CREATE INDEX IF NOT EXISTS idx_lsn ON transactions(lsn)`)
	require.NoError(t, err)

	_, err = db.Writer.Exec(`
		CREATE TABLE IF NOT EXISTS poller_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			last_poll_time TIMESTAMP,
			last_lsn TEXT,
			total_changes INTEGER DEFAULT 0,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	require.NoError(t, err)

	// Create users table for WriteOps tests (since EnsureTable was removed)
	_, err = db.Writer.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY,
			name TEXT
		)
	`)
	require.NoError(t, err)

	// Initialize poller state row
	_, err = db.Writer.Exec(`
		INSERT OR IGNORE INTO poller_state (id, last_poll_time, last_lsn, total_changes)
		VALUES (1, NULL, NULL, 0)
	`)
	require.NoError(t, err)

	err = db.Flush()
	require.NoError(t, err)

	return db, tmpDir
}

func TestNew(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	assert.NotNil(t, store)

	err = store.Close()
	assert.NoError(t, err)
}

func TestStore_Write(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	tx := &core.Transaction{
		ID: "tx-001",
		Changes: []core.Change{
			{
				Table:         "users",
				TransactionID: "tx-001",
				Operation:     core.OpInsert,
				Data: map[string]interface{}{
					"id":   1,
					"name": "alice",
				},
				ID: "000000000000000a:000000000000000b:2",
			},
		},
	}

	_, err = store.Write(tx.Changes)
	assert.NoError(t, err)
}

func TestStore_WriteOps(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	ops := []core.Sink{
		{
			Config: core.SinkConfig{
				Name:       "test",
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]any{{1, "alice"}, {2, "bob"}},
			},
			OpType: core.OpInsert,
		},
	}

	err = store.WriteOps(ops)
	assert.NoError(t, err)
}

func TestStore_GetChanges(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Write a transaction first
	tx := &core.Transaction{
		ID: "tx-001",
		Changes: []core.Change{
			{
				Table:         "users",
				TransactionID: "tx-001",
				Operation:     core.OpInsert,
				Data:          map[string]interface{}{"id": 1, "name": "alice"},
				ID:            "0000000000000001:0000000000000001:2",
			},
		},
	}
	_, err = store.Write(tx.Changes)
	require.NoError(t, err)

	// Get changes
	changes, err := store.GetChanges(10)
	assert.NoError(t, err)
	assert.Len(t, changes, 1)
	assert.Equal(t, "tx-001", changes[0]["transaction_id"])
}

func TestStore_GetChangesWithFilter(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Write transactions
	tx1 := &core.Transaction{
		ID: "tx-001",
		Changes: []core.Change{
			{Table: "users", TransactionID: "tx-001", Operation: core.OpInsert, Data: map[string]interface{}{"id": 1}, ID: "0000000000000001:0000000000000001:2"},
		},
	}
	tx2 := &core.Transaction{
		ID: "tx-002",
		Changes: []core.Change{
			{Table: "orders", TransactionID: "tx-002", Operation: core.OpInsert, Data: map[string]interface{}{"id": 1}, ID: "0000000000000002:0000000000000001:2"},
		},
	}
	_, err = store.Write(tx1.Changes)
	require.NoError(t, err)
	_, err = store.Write(tx2.Changes)
	require.NoError(t, err)

	// Filter by table name
	changes, err := store.GetChangesWithFilter(10, "users", "", "", "")
	assert.NoError(t, err)
	assert.Len(t, changes, 1)
	assert.Equal(t, "tx-001", changes[0]["transaction_id"])

	// Filter by operation
	changes, err = store.GetChangesWithFilter(10, "", "INSERT", "", "")
	assert.NoError(t, err)
	assert.Len(t, changes, 2)

	// Filter by txID
	changes, err = store.GetChangesWithFilter(10, "", "", "tx-002", "")
	assert.NoError(t, err)
	assert.Len(t, changes, 1)
	assert.Equal(t, "tx-002", changes[0]["transaction_id"])
}

func TestStore_UpdatePollerState(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	err = store.UpdatePollerState("lsn-123", 5, 5)
	assert.NoError(t, err)

	// Force commit to make data visible to reader
	err = db.Flush()
	assert.NoError(t, err)

	state, err := store.GetPollerState()
	assert.NoError(t, err)
	assert.Equal(t, 5, state["total_changes"])
	assert.Equal(t, "lsn-123", state["last_lsn"])
}

func TestStore_GetPollerState(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Initial state
	state, err := store.GetPollerState()
	assert.NoError(t, err)
	assert.Equal(t, 0, state["total_changes"])
	assert.Equal(t, 0, state["total_inserted"])

	// Update and get again
	err = store.UpdatePollerState("lsn-456", 10, 8)
	require.NoError(t, err)

	// Force commit to make data visible to reader
	err = db.Flush()
	assert.NoError(t, err)

	state, err = store.GetPollerState()
	assert.NoError(t, err)
	assert.Equal(t, 10, state["total_changes"])
	assert.Equal(t, 8, state["total_inserted"])
	assert.Equal(t, "lsn-456", state["last_lsn"])
}

func TestStore_WriteOps_Update(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Insert data first
	ops := []core.Sink{
		{
			Config: core.SinkConfig{
				Name:       "test",
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]any{{1, "alice"}},
			},
			OpType: core.OpInsert,
		},
	}
	err = store.WriteOps(ops)
	require.NoError(t, err)

	// Update data
	ops = []core.Sink{
		{
			Config: core.SinkConfig{
				Name:       "test",
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]any{{1, "alice-updated"}},
			},
			OpType: core.OpUpdateAfter,
		},
	}
	err = store.WriteOps(ops)
	assert.NoError(t, err)
}

func TestStore_WriteOps_Delete(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Insert data first
	ops := []core.Sink{
		{
			Config: core.SinkConfig{
				Name:       "test",
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]any{{1, "alice"}, {2, "bob"}},
			},
			OpType: core.OpInsert,
		},
	}
	err = store.WriteOps(ops)
	require.NoError(t, err)

	// Delete one row
	ops = []core.Sink{
		{
			Config: core.SinkConfig{
				Name:       "test",
				Output:     "users",
				PrimaryKey: "id",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]any{{1}},
			},
			OpType: core.OpDelete,
		},
	}
	err = store.WriteOps(ops)
	assert.NoError(t, err)
}

func TestStore_Write_DuplicateIgnored(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Write the same transaction twice
	tx := &core.Transaction{
		ID: "tx-dup-001",
		Changes: []core.Change{
			{
				Table:         "users",
				TransactionID: "tx-dup-001",
				LSN:           []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10},
				Operation:     core.OpInsert,
				Data: map[string]interface{}{
					"id":   1,
					"name": "alice",
				},
				ID: "dup-test-fixed-id", // Set ID to avoid fallback hash with unordered map
			},
		},
	}

	_, err = store.Write(tx.Changes)
	require.NoError(t, err)

	// Write the same transaction again - should be ignored (not an error)
	_, err = store.Write(tx.Changes)
	require.NoError(t, err) // INSERT OR IGNORE should prevent duplicate

	// Force commit to make data visible to reader
	err = db.Flush()
	require.NoError(t, err)

	// Only one record should exist
	changes, err := store.GetChanges(10)
	require.NoError(t, err)
	assert.Len(t, changes, 1)
	assert.Equal(t, "tx-dup-001", changes[0]["transaction_id"])
}

func TestStore_Write_SameLSNDifferentContent(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Two different rows in the same transaction (same LSN, different content)
	tx := &core.Transaction{
		ID: "tx-lsn-001",
		Changes: []core.Change{
			{
				Table:         "users",
				TransactionID: "tx-lsn-001",
				LSN:           []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10},
				Operation:     core.OpInsert,
				Data: map[string]interface{}{
					"id":   1,
					"name": "alice",
				},
				ID: "0000000000000010:0000000000000001:2",
			},
			{
				Table:         "users",
				TransactionID: "tx-lsn-001",
				LSN:           []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10}, // same LSN
				Operation:     core.OpInsert,
				Data: map[string]interface{}{
					"id":   2,
					"name": "bob",
				}, // different content
				ID: "0000000000000010:0000000000000002:2",
			},
		},
	}

	_, err = store.Write(tx.Changes)
	require.NoError(t, err)

	// Force commit
	err = db.Flush()
	require.NoError(t, err)

	// Both rows should be stored (different content = different hash)
	changes, err := store.GetChanges(10)
	require.NoError(t, err)
	assert.Len(t, changes, 2)
}

func TestStore_Write_ContentBasedId(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Same content but different LSN -> different id
	tx1 := &core.Transaction{
		ID: "tx-lsn-002",
		Changes: []core.Change{
			{
				Table:         "users",
				TransactionID: "tx-lsn-002",
				LSN:           []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10},
				Operation:     core.OpInsert,
				Data:          map[string]interface{}{"id": 1, "name": "alice"},
				ID:            "0000000000000010:0000000000000001:2",
			},
		},
	}
	tx2 := &core.Transaction{
		ID: "tx-lsn-002",
		Changes: []core.Change{
			{
				Table:         "users",
				TransactionID: "tx-lsn-002",
				LSN:           []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11}, // different LSN
				Operation:     core.OpInsert,
				Data:          map[string]interface{}{"id": 1, "name": "alice"}, // same content
				ID:            "0000000000000011:0000000000000001:2",
			},
		},
	}

	_, err = store.Write(tx1.Changes)
	require.NoError(t, err)
	_, err = store.Write(tx2.Changes)
	require.NoError(t, err) // different LSN -> different hash, should both be stored

	err = db.Flush()
	require.NoError(t, err)

	// Both should exist since LSN differs -> different hash ids
	changes, err := store.GetChanges(10)
	require.NoError(t, err)
	assert.Len(t, changes, 2)

	// Verify both ids are different (32-char hex strings)
	ids := []string{}
	for _, c := range changes {
		ids = append(ids, c["id"].(string))
	}
	assert.NotEqual(t, ids[0], ids[1])
	assert.Greater(t, len(ids[0]), 16, "ID should be longer than 16 chars (native LSN tuple format)")
}


// TestStore_Write_DuplicateID_IsIgnoredAndLogged verifies duplicate IDs are deduped
func TestStore_Write_DuplicateID_IsIgnoredAndLogged(t *testing.T) {
	db, tmpDir := newTestDB(t)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	store, err := New(db)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Two changes with identical ID (same LSN:table:pk:op) but different data
	duplicateID := "0000000000000010:users:1:2"
	changes := []core.Change{
		{
			Table:         "users",
			TransactionID: "tx-dup",
			LSN:           []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10},
			Operation:     core.OpInsert,
			Data:          map[string]interface{}{"id": 1, "name": "alice"},
			ID:            duplicateID,
		},
		{
			Table:         "users",
			TransactionID: "tx-dup",
			LSN:           []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10},
			Operation:     core.OpInsert,
			Data:          map[string]interface{}{"id": 2, "name": "bob"},
			ID:            duplicateID, // same ID - should be ignored
		},
	}

	// Write should succeed, but only 1 row inserted (second ignored)
	count, err := store.Write(changes)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "only 1 change should be inserted (duplicate ignored)")

	// Verify only 1 row exists
	retrieved, err := store.GetChanges(10)
	require.NoError(t, err)
	assert.Len(t, retrieved, 1)
	dataMap, ok := retrieved[0]["data"].(map[string]interface{})
	require.True(t, ok, "data should be a map")
	assert.Equal(t, "alice", dataMap["name"])
}
