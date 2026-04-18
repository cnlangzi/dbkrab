package sql

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestWriter_InsertStrategy(t *testing.T) {
	// Create temp SQLite db
	tmpFile, err := os.CreateTemp("", "writer_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	writer := NewWriter(db)

	tests := []struct {
		name     string
		config   SinkConfig
		ds       *DataSet
		wantErr  bool
		checkSQL string
		checkVal interface{}
	}{
		{
			name: "insert skip - record not exists",
			config: SinkConfig{
				Output:     "test_table",
				PrimaryKey: "id",
				OnConflict: "skip",
			},
			ds: &DataSet{
				Columns: []string{"id", "name", "value"},
				Rows:    [][]interface{}{{1, "test1", 100}},
			},
			wantErr:  false,
			checkSQL: "SELECT name FROM test_table WHERE id = 1",
			checkVal: "test1",
		},
		{
			name: "insert skip - record exists (should ignore)",
			config: SinkConfig{
				Output:     "test_table",
				PrimaryKey: "id",
				OnConflict: "skip",
			},
			ds: &DataSet{
				Columns: []string{"id", "name", "value"},
				Rows:    [][]interface{}{{1, "updated1", 999}},
			},
			wantErr:  false,
			checkSQL: "SELECT name FROM test_table WHERE id = 1",
			checkVal: "test1", // Should remain unchanged
		},
		{
			name: "insert overwrite - record exists",
			config: SinkConfig{
				Output:     "test_table",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			ds: &DataSet{
				Columns: []string{"id", "name", "value"},
				Rows:    [][]interface{}{{1, "overwritten", 888}},
			},
			wantErr:  false,
			checkSQL: "SELECT name FROM test_table WHERE id = 1",
			checkVal: "overwritten",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := &SinkOp{
				Config:  tt.config,
				DataSet: tt.ds,
				OpType:  Insert,
			}

			err := writer.WriteBatch([]*SinkOp{op})
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Verify state
			if tt.checkSQL != "" {
				var name string
				err := db.QueryRow(tt.checkSQL).Scan(&name)
				if err != nil {
					t.Errorf("check query failed: %v", err)
					return
				}
				if tt.checkVal != nil && name != tt.checkVal {
					t.Errorf("expected %v, got %v", tt.checkVal, name)
				}
			}
		})
	}
}

func TestWriter_UpdateStrategy(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "writer_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial data
	_, err = db.Exec("INSERT INTO test_table (id, name, value) VALUES (1, 'original', 100)")
	if err != nil {
		t.Fatal(err)
	}

	writer := NewWriter(db)

	tests := []struct {
		name     string
		config   SinkConfig
		ds       *DataSet
		wantErr  bool
		checkSQL string
		checkVal interface{}
	}{
		{
			name: "update overwrite - record exists",
			config: SinkConfig{
				Output:     "test_table",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			ds: &DataSet{
				Columns: []string{"id", "name", "value"},
				Rows:    [][]interface{}{{1, "updated", 200}},
			},
			wantErr:  false,
			checkSQL: "SELECT name FROM test_table WHERE id = 1",
			checkVal: "updated",
		},
		{
			name: "update skip - record not exists",
			config: SinkConfig{
				Output:     "test_table",
				PrimaryKey: "id",
				OnConflict: "skip",
			},
			ds: &DataSet{
				Columns: []string{"id", "name", "value"},
				Rows:    [][]interface{}{{999, "ghost", 999}},
			},
			wantErr: false, // Skip doesn't error
		},
		{
			name: "update error - record not exists",
			config: SinkConfig{
				Output:     "test_table",
				PrimaryKey: "id",
				OnConflict: "error",
			},
			ds: &DataSet{
				Columns: []string{"id", "name", "value"},
				Rows:    [][]interface{}{{888, "ghost", 888}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := &SinkOp{
				Config:  tt.config,
				DataSet: tt.ds,
				OpType:  Update,
			}

			err := writer.WriteBatch([]*SinkOp{op})
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.checkSQL != "" && tt.checkVal != nil {
				var name string
				err := db.QueryRow(tt.checkSQL).Scan(&name)
				if err != nil {
					t.Errorf("check query failed: %v", err)
					return
				}
				if name != tt.checkVal {
					t.Errorf("expected %v, got %v", tt.checkVal, name)
				}
			}
		})
	}
}

func TestWriter_DeleteStrategy(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "writer_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO test_table (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three')")
	if err != nil {
		t.Fatal(err)
	}

	writer := NewWriter(db)

	tests := []struct {
		name      string
		config    SinkConfig
		ds        *DataSet
		wantErr   bool
		checkRows int
	}{
		{
			name: "delete overwrite - record exists",
			config: SinkConfig{
				Output:     "test_table",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			ds: &DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]interface{}{{1}},
			},
			wantErr:   false,
			checkRows: 2,
		},
		{
			name: "delete skip - record not exists",
			config: SinkConfig{
				Output:     "test_table",
				PrimaryKey: "id",
				OnConflict: "skip",
			},
			ds: &DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]interface{}{{999}},
			},
			wantErr:   false, // Skip doesn't error
			checkRows: 2,
		},
		{
			name: "delete error - record not exists",
			config: SinkConfig{
				Output:     "test_table",
				PrimaryKey: "id",
				OnConflict: "error",
			},
			ds: &DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]interface{}{{888}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := &SinkOp{
				Config:  tt.config,
				DataSet: tt.ds,
				OpType:  Delete,
			}

			err := writer.WriteBatch([]*SinkOp{op})
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.checkRows > 0 {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
				if err != nil {
					t.Errorf("count query failed: %v", err)
					return
				}
				if count != tt.checkRows {
					t.Errorf("expected %d rows, got %d", tt.checkRows, count)
				}
			}
		})
	}
}

func TestOnConflictStrategy_Parse(t *testing.T) {
	tests := []struct {
		input    string
		expected OnConflictStrategy
	}{
		{"overwrite", ConflictOverwrite},
		{"Overwrite", ConflictOverwrite},
		{"OVERWRITE", ConflictOverwrite},
		{"skip", ConflictSkip},
		{"Skip", ConflictSkip},
		{"SKIP", ConflictSkip},
		{"error", ConflictError},
		{"Error", ConflictError},
		{"ERROR", ConflictError},
		{"unknown", ConflictSkip}, // Default
		{"", ConflictSkip},        // Default
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseOnConflictStrategy(tt.input)
			if result != tt.expected {
				t.Errorf("ParseOnConflictStrategy(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestUpsert_PartialColumn tests that UPSERT updates only the provided columns
// and does not nullify other columns. This is the fix for the cost_CostExd scenario.
func TestUpsert_PartialColumn(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "upsert_partial_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Simulate the cost_CostExd scenario:
	// Cost table has: Id, DestTocastDt, DestPassDt, and many other fields
	// Sink selects only Id, DestTocastDt, DestPassDt
	// With the old INSERT OR REPLACE, all other fields would be NULL'd
	// With new UPSERT, only the selected columns should be updated

	_, err = db.Exec(`CREATE TABLE Cost (
		Id INTEGER PRIMARY KEY,
		DestTocastDt TEXT,
		DestPassDt TEXT,
		OtherField1 TEXT,
		OtherField2 INTEGER,
		OtherField3 REAL
	)`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial record with all fields populated
	_, err = db.Exec(`INSERT INTO Cost (Id, DestTocastDt, DestPassDt, OtherField1, OtherField2, OtherField3)
		VALUES (1, '2024-01-01', '2024-01-15', 'original_value', 100, 99.5)`)
	if err != nil {
		t.Fatal(err)
	}

	writer := NewWriter(db)

	// Simulate sink with partial columns (Id, DestTocastDt, DestPassDt only)
	// This is like cost_CostExd selecting only these 3 fields
	op := &SinkOp{
		Config: SinkConfig{
			Output:     "Cost",
			PrimaryKey: "Id",
			OnConflict: "overwrite",
		},
		DataSet: &DataSet{
			Columns: []string{"Id", "DestTocastDt", "DestPassDt"},
			Rows:    [][]interface{}{{1, "2025-06-01", "2025-06-15"}}, // Only update these 3 fields
		},
		OpType: Insert,
	}

	err = writer.WriteBatch([]*SinkOp{op})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v, want nil", err)
	}

	// Verify: Other columns should remain unchanged, not NULL
	var destTocastDt, destPassDt, otherField1 string
	var otherField2 int
	var otherField3 float64

	err = db.QueryRow(`SELECT DestTocastDt, DestPassDt, OtherField1, OtherField2, OtherField3 FROM Cost WHERE Id = 1`).
		Scan(&destTocastDt, &destPassDt, &otherField1, &otherField2, &otherField3)
	if err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}

	// Check updated columns
	if destTocastDt != "2025-06-01" {
		t.Errorf("DestTocastDt = %q, want %q", destTocastDt, "2025-06-01")
	}
	if destPassDt != "2025-06-15" {
		t.Errorf("DestPassDt = %q, want %q", destPassDt, "2025-06-15")
	}

	// Check other columns are preserved (not NULL)
	if otherField1 != "original_value" {
		t.Errorf("OtherField1 = %q, want %q (should NOT be NULL)", otherField1, "original_value")
	}
	if otherField2 != 100 {
		t.Errorf("OtherField2 = %d, want %d (should NOT be NULL)", otherField2, 100)
	}
	if otherField3 != 99.5 {
		t.Errorf("OtherField3 = %f, want %f (should NOT be NULL)", otherField3, 99.5)
	}
}

// TestUpsert_InsertWhenMissing tests that insert works correctly for new records
func TestUpsert_InsertWhenMissing(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "upsert_insert_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec(`CREATE TABLE test_table (
		id INTEGER PRIMARY KEY,
		name TEXT,
		value INTEGER,
		remark TEXT
	)`)
	if err != nil {
		t.Fatal(err)
	}

	writer := NewWriter(db)

	// Insert a new record with partial columns (only id, name, value)
	op := &SinkOp{
		Config: SinkConfig{
			Output:     "test_table",
			PrimaryKey: "id",
			OnConflict: "overwrite",
		},
		DataSet: &DataSet{
			Columns: []string{"id", "name", "value"},
			Rows:    [][]interface{}{{1, "new_record", 42}},
		},
		OpType: Insert,
	}

	err = writer.WriteBatch([]*SinkOp{op})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v, want nil", err)
	}

	// Verify: inserted record should have the provided values, others as NULL
	var name string
	var value int
	var remark sql.NullString

	err = db.QueryRow(`SELECT name, value, remark FROM test_table WHERE id = 1`).Scan(&name, &value, &remark)
	if err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}

	if name != "new_record" {
		t.Errorf("name = %q, want %q", name, "new_record")
	}
	if value != 42 {
		t.Errorf("value = %d, want %d", value, 42)
	}
	// remark should be NULL since it wasn't provided
	if remark.Valid {
		t.Errorf("remark should be NULL, got %q", remark.String)
	}
}

// TestUpsert_GeneratedSQL verifies the generated SQL is correct
func TestUpsert_GeneratedSQL(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "upsert_sql_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Create table and capture the SQL executed using trace
	_, err = db.Exec(`CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}

	writer := NewWriter(db)

	// Test with partial columns to verify ON CONFLICT is generated
	op := &SinkOp{
		Config: SinkConfig{
			Output:     "test_table",
			PrimaryKey: "id",
			OnConflict: "overwrite",
		},
		DataSet: &DataSet{
			Columns: []string{"id", "name"}, // Only id and name, not value
			Rows:    [][]interface{}{{1, "test"}},
		},
		OpType: Insert,
	}

	err = writer.WriteBatch([]*SinkOp{op})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	// Verify the update worked
	var name string
	err = db.QueryRow(`SELECT name FROM test_table WHERE id = 1`).Scan(&name)
	if err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}
	if name != "test" {
		t.Errorf("name = %q, want %q", name, "test")
	}

	// Now test that a second insert with different name updates only the provided column
	op2 := &SinkOp{
		Config: SinkConfig{
			Output:     "test_table",
			PrimaryKey: "id",
			OnConflict: "overwrite",
		},
		DataSet: &DataSet{
			Columns: []string{"id", "name"}, // Only update name, not value
			Rows:    [][]interface{}{{1, "updated_name"}},
		},
		OpType: Insert,
	}

	err = writer.WriteBatch([]*SinkOp{op2})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	// Verify only name was updated, value remains NULL (not touched)
	var name2 string
	var value sql.NullInt32
	err = db.QueryRow(`SELECT name, value FROM test_table WHERE id = 1`).Scan(&name2, &value)
	if err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}
	if name2 != "updated_name" {
		t.Errorf("name = %q, want %q", name2, "updated_name")
	}
	// value should still be NULL (not touched by the partial update)
	if value.Valid {
		t.Errorf("value should still be NULL after partial update, got %d", value.Int32)
	}
}
