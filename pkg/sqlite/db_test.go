package sqlite

import (
	"context"
	"testing"
	"testing/fstest"
)

func TestNewInMemory(t *testing.T) {
	db, err := NewInMemory(context.Background(), "test", nil)
	if err != nil {
		t.Fatalf("NewInMemory failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Test write
	_, err = db.Writer.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	_, err = db.Writer.Exec("INSERT INTO test (name) VALUES (?)", "hello")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Test read
	row := db.Reader.QueryRow("SELECT name FROM test WHERE id = 1")
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if name != "hello" {
		t.Errorf("expected 'hello', got '%s'", name)
	}
}

func TestNewFile(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	db, err := NewFile(context.Background(), tmpFile, "test", "")
	if err != nil {
		t.Fatalf("NewFile failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Test write
	_, err = db.Writer.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// Test read after write
	rows, err := db.Reader.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var tableName string
	found := false
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if tableName == "test" {
			found = true
		}
	}

	if !found {
		t.Error("expected to find 'test' table")
	}
}

func TestExecContext(t *testing.T) {
	db, err := NewInMemory(context.Background(), "test", nil)
	if err != nil {
		t.Fatalf("NewInMemory failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}
}

func TestQueryContext(t *testing.T) {
	db, err := NewInMemory(context.Background(), "test", nil)
	if err != nil {
		t.Fatalf("NewInMemory failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Create and insert data using Writer
	_, _ = db.Writer.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Writer.Exec("INSERT INTO test (name) VALUES (?)", "test")

	// Query using Reader
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT name FROM test")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	_ = rows.Close()

	if name != "test" {
		t.Errorf("expected 'test', got '%s'", name)
	}
}

func TestQueryRowContext(t *testing.T) {
	db, err := NewInMemory(context.Background(), "test", nil)
	if err != nil {
		t.Fatalf("NewInMemory failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Create and insert data
	_, _ = db.Writer.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Writer.Exec("INSERT INTO test (name) VALUES (?)", "row")

	// Query single row
	ctx := context.Background()
	row := db.QueryRowContext(ctx, "SELECT name FROM test WHERE id = 1")

	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("QueryRowContext failed: %v", err)
	}

	if name != "row" {
		t.Errorf("expected 'row', got '%s'", name)
	}
}

// TestMigration tests that migrations are executed on startup
func TestMigration(t *testing.T) {
	// Create a simple in-memory fs with migration file
	// Migration files must be in versioned subdirectories
	migrations := fstest.MapFS{
		"1.0.0":             &fstest.MapDir{},
		"1.0.0/001_create_test.sql": &fstest.MapFile{
			Data: []byte(`
CREATE TABLE IF NOT EXISTS migration_test (
    id INTEGER PRIMARY KEY,
    name TEXT
);
`),
		},
	}

	db, err := NewInMemory(context.Background(), "test", migrations)
	if err != nil {
		t.Fatalf("NewInMemory with migrations failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Verify table was created via migration
	row := db.Reader.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='migration_test'")
	var tableName string
	if err := row.Scan(&tableName); err != nil {
		t.Fatalf("Migration table not found: %v", err)
	}

	if tableName != "migration_test" {
		t.Errorf("expected 'migration_test', got '%s'", tableName)
	}
}
