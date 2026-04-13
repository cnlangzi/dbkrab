package sql

import (
	"reflect"
	"testing"

	"github.com/cnlangzi/dbkrab/internal/core"
)

func TestMergeSinks(t *testing.T) {
	tests := []struct {
		name   string
		sinks  []core.Sink
		expect []core.Sink
	}{
		{
			name:   "nil sinks",
			sinks:  nil,
			expect: nil,
		},
		{
			name:   "empty sinks",
			sinks:  []core.Sink{},
			expect: []core.Sink{},
		},
		{
			name: "single sink - no merging needed",
			sinks: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name", "email"},
						Rows:    [][]any{{1, "Alice", "alice@example.com"}},
					},
					OpType: core.OpInsert,
				},
			},
			expect: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name", "email"},
						Rows:    [][]any{{1, "Alice", "alice@example.com"}},
					},
					OpType: core.OpInsert,
				},
			},
		},
		{
			name: "two sinks with different tables - no merging",
			sinks: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name"},
						Rows:    [][]any{{1, "Alice"}},
					},
					OpType: core.OpInsert,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink2",
						Output:     "orders",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "amount"},
						Rows:    [][]any{{1, 100.0}},
					},
					OpType: core.OpInsert,
				},
			},
			expect: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink2",
						Output:     "orders",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "amount"},
						Rows:    [][]any{{1, 100.0}},
					},
					OpType: core.OpInsert,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name"},
						Rows:    [][]any{{1, "Alice"}},
					},
					OpType: core.OpInsert,
				},
			},
		},
		{
			name: "two sinks with same table and same pk - should merge",
			sinks: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name", "email"},
						Rows:    [][]any{{1, "Alice", "alice@example.com"}},
					},
					OpType: core.OpInsert,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink2",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "phone", "address"},
						Rows:    [][]any{{1, "123-456-7890", "123 Main St"}},
					},
					OpType: core.OpInsert,
				},
			},
			expect: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"address", "email", "id", "name", "phone"},
						Rows:    [][]any{{"123 Main St", "alice@example.com", 1, "Alice", "123-456-7890"}},
					},
					OpType: core.OpInsert,
				},
			},
		},
		{
			name: "two sinks with same table but different pk - no merge",
			sinks: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name"},
						Rows:    [][]any{{1, "Alice"}},
					},
					OpType: core.OpInsert,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink2",
						Output:     "users",
						PrimaryKey: "external_id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"external_id", "phone"},
						Rows:    [][]any{{"EXT-001", "123-456-7890"}},
					},
					OpType: core.OpInsert,
				},
			},
			expect: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink2",
						Output:     "users",
						PrimaryKey: "external_id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"external_id", "phone"},
						Rows:    [][]any{{"EXT-001", "123-456-7890"}},
					},
					OpType: core.OpInsert,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name"},
						Rows:    [][]any{{1, "Alice"}},
					},
					OpType: core.OpInsert,
				},
			},
		},
		{
			name: "two sinks with same table, same pk, different ops - no merge",
			sinks: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink2",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "phone"},
						Rows:    [][]any{{1, "123-456-7890"}},
					},
					OpType: core.OpUpdateAfter,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name"},
						Rows:    [][]any{{1, "Alice"}},
					},
					OpType: core.OpInsert,
				},
			},
			expect: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name"},
						Rows:    [][]any{{1, "Alice"}},
					},
					OpType: core.OpInsert,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink2",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "phone"},
						Rows:    [][]any{{1, "123-456-7890"}},
					},
					OpType: core.OpUpdateAfter,
				},
			},
		},
		{
			name: "three sinks with same table, same pk - merge all",
			sinks: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "skip",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "name"},
						Rows:    [][]any{{1, "Alice"}},
					},
					OpType: core.OpInsert,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink2",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "skip",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "email"},
						Rows:    [][]any{{1, "alice@example.com"}},
					},
					OpType: core.OpInsert,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink3",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "skip",
					},
					DataSet: &core.DataSet{
						Columns: []string{"id", "phone"},
						Rows:    [][]any{{1, "123-456-7890"}},
					},
					OpType: core.OpInsert,
				},
			},
			expect: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "skip",
					},
					DataSet: &core.DataSet{
						Columns: []string{"email", "id", "name", "phone"},
						Rows:    [][]any{{"alice@example.com", 1, "Alice", "123-456-7890"}},
					},
					OpType: core.OpInsert,
				},
			},
		},
		{
			name: "two sinks with same table, same pk, different column order - should merge by name",
			sinks: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						// id is at index 0
						Columns: []string{"id", "name", "email"},
						Rows:    [][]any{{1, "Alice", "alice@example.com"}},
					},
					OpType: core.OpInsert,
				},
				{
					Config: core.SinkConfig{
						Name:       "sink2",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						// id is at index 1 (different order!)
						Columns: []string{"phone", "id", "address"},
						Rows:    [][]any{{"123-456-7890", 1, "123 Main St"}},
					},
					OpType: core.OpInsert,
				},
			},
			expect: []core.Sink{
				{
					Config: core.SinkConfig{
						Name:       "sink1",
						Output:     "users",
						PrimaryKey: "id",
						OnConflict: "overwrite",
					},
					DataSet: &core.DataSet{
						Columns: []string{"address", "email", "id", "name", "phone"},
						Rows:    [][]any{{"123 Main St", "alice@example.com", 1, "Alice", "123-456-7890"}},
					},
					OpType: core.OpInsert,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeSinks(tt.sinks)
			
			// Compare lengths
			if len(result) != len(tt.expect) {
				t.Errorf("mergeSinks() got %d sinks, want %d", len(result), len(tt.expect))
				for i, s := range result {
					t.Logf("  result[%d]: table=%s, pk=%s, op=%v, cols=%v", i, s.Config.Output, s.Config.PrimaryKey, s.OpType, s.DataSet.Columns)
				}
				for i, s := range tt.expect {
					t.Logf("  expect[%d]: table=%s, pk=%s, op=%v, cols=%v", i, s.Config.Output, s.Config.PrimaryKey, s.OpType, s.DataSet.Columns)
				}
				return
			}
			
			// Compare each sink
			for i := range result {
				if result[i].Config.Output != tt.expect[i].Config.Output {
					t.Errorf("sink[%d].Config.Output = %v, want %v", i, result[i].Config.Output, tt.expect[i].Config.Output)
				}
				if result[i].Config.PrimaryKey != tt.expect[i].Config.PrimaryKey {
					t.Errorf("sink[%d].Config.PrimaryKey = %v, want %v", i, result[i].Config.PrimaryKey, tt.expect[i].Config.PrimaryKey)
				}
				if result[i].Config.OnConflict != tt.expect[i].Config.OnConflict {
					t.Errorf("sink[%d].Config.OnConflict = %v, want %v", i, result[i].Config.OnConflict, tt.expect[i].Config.OnConflict)
				}
				if result[i].OpType != tt.expect[i].OpType {
					t.Errorf("sink[%d].OpType = %v, want %v", i, result[i].OpType, tt.expect[i].OpType)
				}
				
				// Compare datasets
				if !reflect.DeepEqual(result[i].DataSet.Columns, tt.expect[i].DataSet.Columns) {
					t.Errorf("sink[%d].DataSet.Columns = %v, want %v", i, result[i].DataSet.Columns, tt.expect[i].DataSet.Columns)
				}
				if len(result[i].DataSet.Rows) != len(tt.expect[i].DataSet.Rows) {
					t.Errorf("sink[%d].DataSet.Rows length = %v, want %v", i, len(result[i].DataSet.Rows), len(tt.expect[i].DataSet.Rows))
				}
				for j := range result[i].DataSet.Rows {
					if !reflect.DeepEqual(result[i].DataSet.Rows[j], tt.expect[i].DataSet.Rows[j]) {
						t.Errorf("sink[%d].DataSet.Rows[%d] = %v, want %v", i, j, result[i].DataSet.Rows[j], tt.expect[i].DataSet.Rows[j])
					}
				}
			}
})
	}
}

func TestMergeSinksOnConflictInconsistent(t *testing.T) {
	// When OnConflict is inconsistent, mergeSinks logs a warning but still merges
	// using the first OnConflict value
	sinks := []core.Sink{
		{
			Config: core.SinkConfig{
				Name:       "sink1",
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "overwrite",
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "name"},
				Rows:    [][]any{{1, "Alice"}},
			},
			OpType: core.OpInsert,
		},
		{
			Config: core.SinkConfig{
				Name:       "sink2",
				Output:     "users",
				PrimaryKey: "id",
				OnConflict: "skip", // Different!
			},
			DataSet: &core.DataSet{
				Columns: []string{"id", "email"},
				Rows:    [][]any{{1, "alice@example.com"}},
			},
			OpType: core.OpInsert,
		},
	}
	result := mergeSinks(sinks)
	// Should merge and use first OnConflict ("overwrite"), not skip
	if len(result) != 1 {
		t.Errorf("mergeSinks() with inconsistent OnConflict got %d sinks, want 1", len(result))
	}
	if result[0].Config.OnConflict != "overwrite" {
		t.Errorf("OnConflict = %v, want overwrite", result[0].Config.OnConflict)
	}
	// Should merge columns from both sinks (order may vary by processing)
	if len(result[0].DataSet.Columns) != 3 {
		t.Errorf("Columns length = %d, want 3", len(result[0].DataSet.Columns))
	}
	// Check that columns from both sinks are present
	hasID := false
	hasName := false
	hasEmail := false
	for _, col := range result[0].DataSet.Columns {
		if col == "id" {
			hasID = true
		}
		if col == "name" {
			hasName = true
		}
		if col == "email" {
			hasEmail = true
		}
	}
	if !hasID || !hasName || !hasEmail {
		t.Errorf("Columns = %v, missing some columns (hasID=%v, hasName=%v, hasEmail=%v)",
			result[0].DataSet.Columns, hasID, hasName, hasEmail)
	}
	// Should have merged the single row with values from both sinks
	if len(result[0].DataSet.Rows) != 1 {
		t.Errorf("Rows length = %d, want 1", len(result[0].DataSet.Rows))
	}
}
