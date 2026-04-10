package sqlite

import (
	"context"
	"os"
	"testing"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSinker(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	assert.NotNil(t, sinker)
	assert.Equal(t, "test", sinker.DatabaseName())
	assert.Equal(t, "sqlite", sinker.DatabaseType())

	err = sinker.Close()
	assert.NoError(t, err)
}

func TestSinker_Write(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

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

	err = sinker.Write(ops)
	assert.NoError(t, err)
}

func TestSinker_Write_Update(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

	// Insert first
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
	err = sinker.Write(ops)
	require.NoError(t, err)

	// Update
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
	err = sinker.Write(ops)
	assert.NoError(t, err)
}

func TestSinker_Write_Delete(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

	// Insert first
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
	err = sinker.Write(ops)
	require.NoError(t, err)

	// Delete
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
	err = sinker.Write(ops)
	assert.NoError(t, err)
}

func TestSinker_Write_Empty(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

	// Empty ops should not error
	err = sinker.Write([]core.Sink{})
	assert.NoError(t, err)
}

func TestSinker_Write_SkipOnConflict(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

	ops := []core.Sink{
		{
			Config: core.SinkConfig{
				Name:       "test",
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

	err = sinker.Write(ops)
	assert.NoError(t, err)
}

func TestSinker_InMemory(t *testing.T) {
	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       ":memory:",
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

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

	err = sinker.Write(ops)
	assert.NoError(t, err)
}

func TestSinker_RunMigrations_NoMigrations(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

	// No migrations configured, should be nil
	err = sinker.RunMigrations()
	assert.NoError(t, err)
}

func TestSinker_RunMigrations_WithDir(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"
	migrationsDir := t.TempDir()

	// Create migration file
	migrationSQL := `
CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`
	err := os.WriteFile(migrationsDir+"/001_create_test.sql", []byte(migrationSQL), 0644)
	require.NoError(t, err)

	sinker, err := NewSinker(Config{
		Name:          "test",
		File:          tmpFile,
		ModuleName:    "test",
		MigrationsDir: migrationsDir,
	})
	require.NoError(t, err)
	defer sinker.Close()

	err = sinker.RunMigrations()
	assert.NoError(t, err)
}

func TestSinker_DatabaseName(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "mydb",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

	assert.Equal(t, "mydb", sinker.DatabaseName())
}

func TestSinker_DatabaseType(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

	assert.Equal(t, "sqlite", sinker.DatabaseType())
}

func TestSinker_MultipleTables(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)
	defer sinker.Close()

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
		{
			Config: core.SinkConfig{
				Name:       "test",
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

	err = sinker.Write(ops)
	assert.NoError(t, err)
}

func TestSinker_Close(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	sinker, err := NewSinker(Config{
		Name:       "test",
		File:       tmpFile,
		ModuleName: "test",
	})
	require.NoError(t, err)

	// Double close should not panic
	err = sinker.Close()
	assert.NoError(t, err)
	err = sinker.Close()
	assert.NoError(t, err)
}
