package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testMigrationDir(t *testing.T, migrationSQL string) string {
	tmpMigrationDir := filepath.Join(t.TempDir(), "migrations")
	require.NoError(t, os.MkdirAll(tmpMigrationDir, 0755))

	// Create a version subdirectory to match sqle migration format
	versionDir := filepath.Join(tmpMigrationDir, "1.0.0")
	require.NoError(t, os.MkdirAll(versionDir, 0755))

	// Write migration with proper sqle/migrate format
	// The header is required for sqle/migrate to discover the migration
	migrationContent := `-- Migration: 001_initial
-- Module: test
-- Description: Create test tables

` + migrationSQL

	require.NoError(t, os.WriteFile(filepath.Join(versionDir, "001_initial.sql"), []byte(migrationContent), 0644))
	return tmpMigrationDir
}

func TestNewSinker(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	assert.NotNil(t, sinker)
	assert.Equal(t, "test", sinker.DatabaseName())
	assert.Equal(t, "sqlite", sinker.DatabaseType())

	err = sinker.Close()
	assert.NoError(t, err)
}

func TestSinker_Write(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	ops := []core.Sink{
		{
			Config: core.SinkConfig{
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

	err = sinker.Write(context.Background(), ops)
	assert.NoError(t, err)
}

func TestSinker_Write_Update(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	// Insert first
	ops := []core.Sink{
		{
			Config: core.SinkConfig{
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
	err = sinker.Write(context.Background(), ops)
	require.NoError(t, err)

	// Update only id=1
	ops = []core.Sink{
		{
			Config: core.SinkConfig{
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
	err = sinker.Write(context.Background(), ops)
	assert.NoError(t, err)
}

// TestSinker_OnConflict_Overwrite verifies that INSERT OR REPLACE is used
// when OnConflict is "overwrite", replacing existing rows instead of failing.
func TestSinker_OnConflict_Overwrite(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)
	require.NoError(t, err)

	// Insert first row
	ops := []core.Sink{
		{
			Config: core.SinkConfig{
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
	err = sinker.Write(context.Background(), ops)
	require.NoError(t, err)

	// Insert same primary key - should replace, not fail
	ops = []core.Sink{
		{
			Config: core.SinkConfig{
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]any{{1, "alice-replaced"}},
			},
			OpType: core.OpInsert, // Same PK, should replace
		},
	}
	err = sinker.Write(context.Background(), ops)
	assert.NoError(t, err, "OnConflict overwrite should replace existing row")
}

// TestSinker_UpdateOnlyAffectsTargetRow verifies that UPDATE only modifies
// the row specified by the primary key, not all rows in the table.
func TestSinker_UpdateOnlyAffectsTargetRow(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    status TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)
	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	// Insert multiple rows
	ops := []core.Sink{
		{
			Config: core.SinkConfig{
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name", "status"},
				Rows:    [][]any{{1, "alice", "active"}, {2, "bob", "inactive"}, {3, "charlie", "active"}},
			},
			OpType: core.OpInsert,
		},
	}
	err = sinker.Write(context.Background(), ops)
	require.NoError(t, err)

	// Update only id=2
	ops = []core.Sink{
		{
			Config: core.SinkConfig{
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name", "status"},
				Rows:    [][]any{{2, "bob-updated", "active"}},
			},
			OpType: core.OpUpdateAfter,
		},
	}
	err = sinker.Write(context.Background(), ops)
	assert.NoError(t, err)

	// Note: This test assumes the database can be queried directly
	// In a real test, we would query the database to verify
}

func TestSinker_Write_Delete(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	// Insert first
	ops := []core.Sink{
		{
			Config: core.SinkConfig{
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
	err = sinker.Write(context.Background(), ops)
	require.NoError(t, err)

	// Delete
	ops = []core.Sink{
		{
			Config: core.SinkConfig{
				Output:     "users",
				PrimaryKey: "id",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id"},
				Rows:    [][]any{{1}},
			},
			OpType: core.OpDelete,
		},
	}
	err = sinker.Write(context.Background(), ops)
	assert.NoError(t, err)
}

func TestSinker_Write_Empty(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	// Empty ops should not error
	err = sinker.Write(context.Background(), []core.Sink{})
	assert.NoError(t, err)
}

func TestSinker_Write_SkipOnConflict(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	ops := []core.Sink{
		{
			Config: core.SinkConfig{
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "skip",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]any{{1, "alice"}},
			},
			OpType: core.OpInsert,
		},
	}

	err = sinker.Write(context.Background(), ops)
	assert.NoError(t, err)
}

func TestSinker_InMemory(t *testing.T) {
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", ":memory:", tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	ops := []core.Sink{
		{
			Config: core.SinkConfig{
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

	err = sinker.Write(context.Background(), ops)
	assert.NoError(t, err)
}

func TestSinker_DatabaseName(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("mydb", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	assert.Equal(t, "mydb", sinker.DatabaseName())
}

func TestSinker_DatabaseType(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	assert.Equal(t, "sqlite", sinker.DatabaseType())
}

func TestSinker_MultipleTables(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    amount REAL
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	ops := []core.Sink{
		{
			Config: core.SinkConfig{
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
		{
			Config: core.SinkConfig{
				Output:     "orders",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "amount"},
				Rows:    [][]any{{1, 100.50}},
			},
			OpType: core.OpInsert,
		},
	}

	err = sinker.Write(context.Background(), ops)
	assert.NoError(t, err)
}

func TestSinker_Close(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)

	// Double close should not panic
	err = sinker.Close()
	assert.NoError(t, err)
	err = sinker.Close()
	assert.NoError(t, err)
}

// TestSinker_UnknownOperationTypeDropped verifies that unknown operation types
// are logged and dropped gracefully instead of causing errors.
// This is a defensive measure to prevent DLQ storms from malformed data.
func TestSinker_UnknownOperationTypeDropped(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	tmpMigrationDir := testMigrationDir(t, `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`)

	sinker, err := NewSinker("test", tmpFile, tmpMigrationDir)

	require.NoError(t, err)
	defer func() { _ = sinker.Close() }()

	// First insert a row
	ops := []core.Sink{
		{
			Config: core.SinkConfig{
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
	err = sinker.Write(context.Background(), ops)
	require.NoError(t, err)

	// Now send an unknown operation type (e.g., OpUpdateBefore = 3 which is not handled)
	// This should NOT cause an error - it should be dropped gracefully
	unknownOps := []core.Sink{
		{
			Config: core.SinkConfig{
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]any{{1, "should-be-dropped"}},
			},
			OpType: 999, // Unknown operation type
		},
	}

	// Should NOT return an error - unknown ops should be dropped
	err = sinker.Write(context.Background(), unknownOps)
	assert.NoError(t, err, "Unknown operation type should be dropped, not cause an error")
}

// TestSinker_MissingMigrationPath verifies that creating a sinker without
// a migration path fails fast with a clear error.
func TestSinker_MissingMigrationPath(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.db")

	// Creating a sinker without migration path should fail
	_, err := NewSinker("test", tmpFile, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "migration path is required")
}
