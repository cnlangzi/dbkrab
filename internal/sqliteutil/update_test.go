package sqliteutil_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/cnlangzi/dbkrab/internal/sqliteutil"
	"github.com/cnlangzi/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateInTx_Overwrite_PartialUpdate verifies that overwrite strategy
// only updates provided columns without affecting other columns.
func TestUpdateInTx_Overwrite_PartialUpdate(t *testing.T) {
	ctx := context.Background()

	// Create in-memory database
	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table
	createSQL := `CREATE TABLE test_items (
		id INTEGER PRIMARY KEY,
		name TEXT,
		amount REAL,
		extra_field TEXT
	)`
	_, err = db.Writer.Exec(createSQL)
	require.NoError(t, err)

	// Insert initial record with all fields
	insertSQL := `INSERT INTO test_items (id, name, amount, extra_field) VALUES (?, ?, ?, ?)`
	_, err = db.Writer.Exec(insertSQL, 1, "initial_name", 100.0, "should_preserve")
	require.NoError(t, err)

	// Now use UpdateInTx with overwrite strategy, only updating name
	config := sqliteutil.TableConfig{
		Output:      "test_items",
		PrimaryKey:  "id",
		OnConflict: "overwrite",
	}
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "updated_name"},
	}

	// Execute UpdateInTx within a transaction
	tx, err := db.Writer.BeginTx(ctx, nil)
	require.NoError(t, err)

	err = sqliteutil.UpdateInTx(tx, config, columns, rows)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify: only name should be updated, extra_field should be preserved
	var name string
	var amount float64
	var extraField string
	selectSQL := `SELECT name, amount, extra_field FROM test_items WHERE id = ?`
	err = db.Writer.QueryRow(selectSQL, 1).Scan(&name, &amount, &extraField)
	require.NoError(t, err)

	assert.Equal(t, "updated_name", name, "name should be updated")
	assert.Equal(t, 100.0, amount, "amount should be preserved")
	assert.Equal(t, "should_preserve", extraField, "extra_field should be preserved")
}

// TestUpdateInTx_Overwrite_InsertIfNotExists verifies that overwrite strategy
// inserts a new record if it doesn't exist.
func TestUpdateInTx_Overwrite_InsertIfNotExists(t *testing.T) {
	ctx := context.Background()

	// Create in-memory database
	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table
	createSQL := `CREATE TABLE test_items (
		id INTEGER PRIMARY KEY,
		name TEXT,
		amount REAL
	)`
	_, err = db.Writer.Exec(createSQL)
	require.NoError(t, err)

	// Use UpdateInTx with overwrite strategy for non-existing record
	config := sqliteutil.TableConfig{
		Output:      "test_items",
		PrimaryKey:  "id",
		OnConflict: "overwrite",
	}
	columns := []string{"id", "name", "amount"}
	rows := [][]interface{}{
		{1, "new_record", 50.0},
	}

	// Execute UpdateInTx within a transaction
	tx, err := db.Writer.BeginTx(ctx, nil)
	require.NoError(t, err)

	err = sqliteutil.UpdateInTx(tx, config, columns, rows)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify: record should be inserted
	var name string
	var amount float64
	selectSQL := `SELECT name, amount FROM test_items WHERE id = ?`
	err = db.Writer.QueryRow(selectSQL, 1).Scan(&name, &amount)
	require.NoError(t, err)

	assert.Equal(t, "new_record", name)
	assert.Equal(t, 50.0, amount)
}

// TestUpdateInTx_Overwrite_MultipleColumns verifies updating multiple columns at once.
func TestUpdateInTx_Overwrite_MultipleColumns(t *testing.T) {
	ctx := context.Background()

	// Create in-memory database
	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table
	createSQL := `CREATE TABLE test_items (
		id INTEGER PRIMARY KEY,
		field1 TEXT,
		field2 TEXT,
		field3 TEXT,
		preserved TEXT
	)`
	_, err = db.Writer.Exec(createSQL)
	require.NoError(t, err)

	// Insert initial record
	_, err = db.Writer.Exec(`INSERT INTO test_items (id, field1, field2, field3, preserved) VALUES (?, ?, ?, ?, ?)`,
		1, "old1", "old2", "old3", "preserved_value")
	require.NoError(t, err)

	// Update only field1 and field2
	config := sqliteutil.TableConfig{
		Output:      "test_items",
		PrimaryKey:  "id",
		OnConflict: "overwrite",
	}
	columns := []string{"id", "field1", "field2"}
	rows := [][]interface{}{
		{1, "new1", "new2"},
	}

	tx, err := db.Writer.BeginTx(ctx, nil)
	require.NoError(t, err)

	err = sqliteutil.UpdateInTx(tx, config, columns, rows)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Verify: field1 and field2 updated, field3 and preserved unchanged
	var field1, field2, field3, preserved string
	err = db.Writer.QueryRow(`SELECT field1, field2, field3, preserved FROM test_items WHERE id = 1`).
		Scan(&field1, &field2, &field3, &preserved)
	require.NoError(t, err)

	assert.Equal(t, "new1", field1)
	assert.Equal(t, "new2", field2)
	assert.Equal(t, "old3", field3, "field3 should be preserved")
	assert.Equal(t, "preserved_value", preserved, "preserved should be preserved")
}

// TestUpdateInTx_Skip_ExistingRecord verifies that skip strategy does nothing
// if record already exists.
func TestUpdateInTx_Skip_ExistingRecord(t *testing.T) {
	ctx := context.Background()

	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table
	_, err = db.Writer.Exec(`CREATE TABLE test_items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	// Insert initial record
	_, err = db.Writer.Exec(`INSERT INTO test_items (id, name) VALUES (?, ?)`, 1, "existing")
	require.NoError(t, err)

	// Try to insert with skip (should do nothing)
	config := sqliteutil.TableConfig{
		Output:      "test_items",
		PrimaryKey:  "id",
		OnConflict: "skip",
	}
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "new_name"},
	}

	tx, err := db.Writer.BeginTx(ctx, nil)
	require.NoError(t, err)

	err = sqliteutil.UpdateInTx(tx, config, columns, rows)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Verify: original name preserved
	var name string
	err = db.Writer.QueryRow(`SELECT name FROM test_items WHERE id = 1`).Scan(&name)
	require.NoError(t, err)

	assert.Equal(t, "existing", name, "skip should preserve existing record")
}

// TestUpdateInTx_EmptyOnConflict_DefaultsToOverwrite verifies that empty OnConflict
// defaults to overwrite strategy (which inserts if not exists).
func TestUpdateInTx_EmptyOnConflict_DefaultsToOverwrite(t *testing.T) {
	ctx := context.Background()

	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table
	_, err = db.Writer.Exec(`CREATE TABLE test_items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	// Use empty OnConflict - should default to overwrite (insert if not exists)
	config := sqliteutil.TableConfig{
		Output:      "test_items",
		PrimaryKey:  "id",
		OnConflict: "", // empty defaults to overwrite
	}
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "new_name"},
	}

	tx, err := db.Writer.BeginTx(ctx, nil)
	require.NoError(t, err)

	err = sqliteutil.UpdateInTx(tx, config, columns, rows)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Verify: empty OnConflict defaults to overwrite, so record should be inserted
	var count int
	err = db.Writer.QueryRow(`SELECT COUNT(*) FROM test_items`).Scan(&count)
	require.NoError(t, err)

	// Empty OnConflict defaults to overwrite, which inserts if not exists
	assert.Equal(t, 1, count, "empty OnConflict should default to overwrite and insert record")
}

// TestInsertInTx_Overwrite_PartialUpdate verifies that InsertInTx with overwrite
// strategy also does partial update when called (not just UpdateInTx).
func TestInsertInTx_Overwrite_PartialUpdate(t *testing.T) {
	ctx := context.Background()

	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table
	_, err = db.Writer.Exec(`CREATE TABLE test_items (
		id INTEGER PRIMARY KEY,
		name TEXT,
		amount REAL,
		extra_field TEXT
	)`)
	require.NoError(t, err)

	// Insert initial record
	_, err = db.Writer.Exec(`INSERT INTO test_items (id, name, amount, extra_field) VALUES (?, ?, ?, ?)`,
		1, "initial", 100.0, "preserve_me")
	require.NoError(t, err)

	// Use InsertInTx with overwrite - only updating name
	config := sqliteutil.TableConfig{
		Output:      "test_items",
		PrimaryKey:  "id",
		OnConflict: "overwrite",
	}
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "updated_via_insert"},
	}

	tx, err := db.Writer.BeginTx(ctx, nil)
	require.NoError(t, err)

	err = sqliteutil.InsertInTx(tx, config, columns, rows)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Verify: name updated, amount and extra_field preserved
	var name string
	var amount float64
	var extraField string
	err = db.Writer.QueryRow(`SELECT name, amount, extra_field FROM test_items WHERE id = 1`).
		Scan(&name, &amount, &extraField)
	require.NoError(t, err)

	assert.Equal(t, "updated_via_insert", name)
	assert.Equal(t, 100.0, amount, "amount should be preserved")
	assert.Equal(t, "preserve_me", extraField, "extra_field should be preserved")
}

// mockTx implements sqliteutil.TxExec for testing
type mockTx struct {
	*sql.Tx
	execCalls   []struct {
		query string
		args  []any
	}
}

func (m *mockTx) Exec(query string, args ...any) (sql.Result, error) {
	m.execCalls = append(m.execCalls, struct {
		query string
		args  []any
	}{query, args})
	return &mockResult{}, nil
}

func (m *mockTx) Commit() error   { return nil }
func (m *mockTx) Rollback() error { return nil }

type mockResult struct{}

func (m *mockResult) LastInsertId() (int64, error)   { return 0, nil }
func (m *mockResult) RowsAffected() (int64, error) { return 1, nil }