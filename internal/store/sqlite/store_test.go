package sqlite

import (
	"context"
	"testing"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/pkg/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDB(t *testing.T) *sqlite.DB {
	db, err := sqlite.NewInMemory(context.Background(), "test", nil)
	require.NoError(t, err)
	return db
}

func TestNewStore(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	store, err := NewStore(db)
	require.NoError(t, err)
	assert.NotNil(t, store)

	err = store.Close()
	assert.NoError(t, err)
}

func TestStore_Write(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	store, err := NewStore(db)
	require.NoError(t, err)
	defer store.Close()

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
			},
		},
	}

	err = store.Write(tx)
	assert.NoError(t, err)
}

func TestStore_WriteOps(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	store, err := NewStore(db)
	require.NoError(t, err)
	defer store.Close()

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
	db := newTestDB(t)
	defer db.Close()

	store, err := NewStore(db)
	require.NoError(t, err)
	defer store.Close()

	// Write a transaction first
	tx := &core.Transaction{
		ID: "tx-001",
		Changes: []core.Change{
			{
				Table:         "users",
				TransactionID: "tx-001",
				Operation:     core.OpInsert,
				Data:          map[string]interface{}{"id": 1, "name": "alice"},
			},
		},
	}
	err = store.Write(tx)
	require.NoError(t, err)

	// Get changes
	changes, err := store.GetChanges(10)
	assert.NoError(t, err)
	assert.Len(t, changes, 1)
	assert.Equal(t, "tx-001", changes[0]["transaction_id"])
}

func TestStore_GetChangesWithFilter(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	store, err := NewStore(db)
	require.NoError(t, err)
	defer store.Close()

	// Write transactions
	tx1 := &core.Transaction{
		ID: "tx-001",
		Changes: []core.Change{
			{Table: "users", TransactionID: "tx-001", Operation: core.OpInsert, Data: map[string]interface{}{"id": 1}},
		},
	}
	tx2 := &core.Transaction{
		ID: "tx-002",
		Changes: []core.Change{
			{Table: "orders", TransactionID: "tx-002", Operation: core.OpInsert, Data: map[string]interface{}{"id": 1}},
		},
	}
	err = store.Write(tx1)
	require.NoError(t, err)
	err = store.Write(tx2)
	require.NoError(t, err)

	// Filter by table name
	changes, err := store.GetChangesWithFilter(10, "users", "", "")
	assert.NoError(t, err)
	assert.Len(t, changes, 1)
	assert.Equal(t, "tx-001", changes[0]["transaction_id"])

	// Filter by operation
	changes, err = store.GetChangesWithFilter(10, "", "INSERT", "")
	assert.NoError(t, err)
	assert.Len(t, changes, 2)

	// Filter by txID
	changes, err = store.GetChangesWithFilter(10, "", "", "tx-002")
	assert.NoError(t, err)
	assert.Len(t, changes, 1)
	assert.Equal(t, "tx-002", changes[0]["transaction_id"])
}

func TestStore_UpdatePollerState(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	store, err := NewStore(db)
	require.NoError(t, err)
	defer store.Close()

	err = store.UpdatePollerState("lsn-123", 5)
	assert.NoError(t, err)

	state, err := store.GetPollerState()
	assert.NoError(t, err)
	assert.Equal(t, 5, state["total_changes"])
	assert.Equal(t, "lsn-123", state["last_lsn"])
}

func TestStore_GetPollerState(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	store, err := NewStore(db)
	require.NoError(t, err)
	defer store.Close()

	// Initial state
	state, err := store.GetPollerState()
	assert.NoError(t, err)
	assert.Equal(t, 0, state["total_changes"])

	// Update and get again
	err = store.UpdatePollerState("lsn-456", 10)
	require.NoError(t, err)

	state, err = store.GetPollerState()
	assert.NoError(t, err)
	assert.Equal(t, 10, state["total_changes"])
	assert.Equal(t, "lsn-456", state["last_lsn"])
}

func TestStore_WriteOps_Update(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	store, err := NewStore(db)
	require.NoError(t, err)
	defer store.Close()

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
	db := newTestDB(t)
	defer db.Close()

	store, err := NewStore(db)
	require.NoError(t, err)
	defer store.Close()

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
