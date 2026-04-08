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
				PrimaryKey:  "id",
				OnConflict:  "skip",
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
				PrimaryKey:  "id",
				OnConflict:  "skip",
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
				PrimaryKey:  "id",
				OnConflict:  "overwrite",
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
				PrimaryKey:  "id",
				OnConflict:  "overwrite",
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
				PrimaryKey:  "id",
				OnConflict:  "skip",
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
				PrimaryKey:  "id",
				OnConflict:  "error",
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
				PrimaryKey:  "id",
				OnConflict:  "overwrite",
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
				PrimaryKey:  "id",
				OnConflict:  "skip",
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
				PrimaryKey:  "id",
				OnConflict:  "error",
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
		{"", ConflictSkip},       // Default
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
