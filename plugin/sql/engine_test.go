package sql

import (
	"database/sql"
	"os"
	"testing"

	"github.com/cnlangzi/dbkrab/internal/core"
	_ "github.com/mattn/go-sqlite3"
)

func TestEngine_Handle(t *testing.T) {
	// Create temp SQLite db
	tmpFile, err := os.CreateTemp("", "engine_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	sqliteDB, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sqliteDB.Close() }()

	// Create a mock skill
	skill := &Skill{
		Name: "test_skill",
		On:   []string{"orders"},
		Sinks: []Sink{
			{
				SinkConfig: SinkConfig{
					Name:       "sync_insert",
					Output:     "order_sync",
					PrimaryKey: "order_id",
					OnConflict: "skip",
				},
				When: []string{"insert", "update"},
			},
		},
	}

	t.Run("buildParams", func(t *testing.T) {
		engine := &Engine{
			skill: skill,
		}

		change := &core.Change{
			Table:         "orders",
			TransactionID: "tx-123",
			LSN:           []byte{0x01, 0x02, 0x03},
			Operation:     core.OpInsert,
			Data: map[string]interface{}{
				"order_id": 100,
				"amount":   500,
				"status":   "pending",
			},
		}

		params, err := engine.buildCDCParams(change)
		if err != nil {
			t.Fatalf("buildParams failed: %v", err)
		}

		// Verify CDC metadata
		if params.CDCTxID != "tx-123" {
			t.Errorf("expected cdc_tx_id = tx-123, got %v", params.CDCTxID)
		}

		if params.CDCLSN != "010203" {
			t.Errorf("expected cdc_lsn = 010203, got %v", params.CDCLSN)
		}

		if params.CDCTable != "orders" {
			t.Errorf("expected cdc_table = orders, got %v", params.CDCTable)
		}

		// Verify data fields with prefix
		if params.Fields["orders_order_id"] != 100 {
			t.Errorf("expected orders_order_id = 100, got %v", params.Fields["orders_order_id"])
		}

		if params.Fields["orders_amount"] != 500 {
			t.Errorf("expected orders_amount = 500, got %v", params.Fields["orders_amount"])
		}

		// Verify orders_id is NOT set (order_id is not id)
		if params.Fields["orders_id"] != nil {
			t.Errorf("expected orders_id = nil (order_id is not id), got %v", params.Fields["orders_id"])
		}
	})

	t.Run("operationToSinkType", func(t *testing.T) {
		engine := &Engine{skill: skill}

		tests := []struct {
			op   core.Operation
			want Operation
		}{
			{core.OpInsert, Insert},           // 1 → 1
			{core.OpUpdateAfter, Update},      // 4 → 2 (key fix for sink filtering)
			{core.OpDelete, Delete},           // 3 → 3
			{core.OpUpdateBefore, 0},          // Should be skipped
		}

		for _, tt := range tests {
			t.Run(tt.op.String(), func(t *testing.T) {
				got := engine.operationToSinkType(tt.op)
				if got != tt.want {
					t.Errorf("operationToSinkType(%v) = %v, want %v", tt.op, got, tt.want)
				}
			})
		}
	})
}

func TestEngine_ShortTableName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"orders", "orders"},
		{"dbo.orders", "orders"},
		{"sys.orders", "orders"},
		{"cdc.orders", "orders"},
		{"dbo.my_table", "my_table"},
		// Underscore prefix support (for CDC tables like dbo_Cost)
		{"dbo_Cost", "Cost"},
		{"cdc.dbo_Cost_CT", "dbo_Cost_CT"}, // Only removes first prefix
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := shortTableName(tt.input)
			if got != tt.want {
				t.Errorf("shortTableName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsIDField(t *testing.T) {
	tests := []struct {
		name string
		isID bool
	}{
		{"id", true},
		{"order_id", true},
		{"customerId", true},
		{"product_ID", true},
		{"uuid", true},
		{"name", false},
		{"amount", false},
		{"created_at", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isIDField(tt.name)
			if got != tt.isID {
				t.Errorf("isIDField(%q) = %v, want %v", tt.name, got, tt.isID)
			}
		})
	}
}

// TestEngine_BuildCDCParams_FieldNaming tests the field naming fix for case-sensitive SQL parameters
// This covers the bug where CDC lowercase fields (id) didn't match SQL parameters (@Cost_Id)
func TestEngine_BuildCDCParams_FieldNaming(t *testing.T) {
	skill := &Skill{
		Name: "test_skill",
		On:   []string{"Cost"},
	}
	engine := &Engine{skill: skill}

	tests := []struct {
		name          string
		table         string
		data          map[string]interface{}
		wantFields    map[string]interface{}
		wantShortTable string
	}{
		{
			name:  "MSSQL dbo.Cost table with id field",
			table: "dbo.Cost",
			data: map[string]interface{}{
				"id":          123,
				"cost_name":   "test",
				"amount":      100.50,
			},
			wantFields: map[string]interface{}{
				"Cost_Id":       123,      // Special case: id → {table}_Id
				"Cost_cost_name": "test",  // Regular fields: {table}_{field}
				"Cost_amount":   100.50,
			},
			wantShortTable: "Cost",
		},
		{
			name:  "CDC underscore prefix table dbo_Cost",
			table: "dbo_Cost",
			data: map[string]interface{}{
				"id":     456,
				"status": "active",
			},
			wantFields: map[string]interface{}{
				"Cost_Id":    456,
				"Cost_status": "active",
			},
			wantShortTable: "Cost",
		},
		{
			name:  "Table without prefix",
			table: "orders",
			data: map[string]interface{}{
				"id":         789,
				"order_date": "2024-01-01",
			},
			wantFields: map[string]interface{}{
				"orders_Id":      789,
				"orders_order_date": "2024-01-01",
			},
			wantShortTable: "orders",
		},
		{
			name:  "Non-id fields should not create {table}_Id",
			table: "items",
			data: map[string]interface{}{
				"item_id":  100,  // This is NOT "id", should be items_item_id
				"quantity": 5,
			},
			wantFields: map[string]interface{}{
				"items_item_id": 100,
				"items_quantity": 5,
				// items_Id should NOT exist
			},
			wantShortTable: "items",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			change := &core.Change{
				Table:         tt.table,
				TransactionID: "tx-test",
				LSN:           []byte{0xAA, 0xBB},
				Operation:     core.OpUpdateAfter,
				Data:          tt.data,
			}

			params, err := engine.buildCDCParams(change)
			if err != nil {
				t.Fatalf("buildCDCParams failed: %v", err)
			}

			// Verify short table name
			gotShortTable := shortTableName(tt.table)
			if gotShortTable != tt.wantShortTable {
				t.Errorf("shortTableName(%q) = %q, want %q", tt.table, gotShortTable, tt.wantShortTable)
			}

			// Verify all expected fields exist with correct values
			for wantKey, wantVal := range tt.wantFields {
				gotVal, exists := params.Fields[wantKey]
				if !exists {
					t.Errorf("expected field %q not found in params.Fields", wantKey)
					continue
				}
				if gotVal != wantVal {
					t.Errorf("field %q = %v, want %v", wantKey, gotVal, wantVal)
				}
			}

			// Verify no unexpected fields
			for gotKey := range params.Fields {
				if _, expected := tt.wantFields[gotKey]; !expected {
					t.Errorf("unexpected field %q in params.Fields", gotKey)
				}
			}
		})
	}
}

// TestEngine_OperationToSinkType_Comprehensive tests all operation mappings
// This covers the critical bug where OpUpdateAfter (4) was not mapped to Update (2)
func TestEngine_OperationToSinkType_Comprehensive(t *testing.T) {
	skill := &Skill{Name: "test"}
	engine := &Engine{skill: skill}

	tests := []struct {
		name      string
		op        core.Operation
		want      Operation
		wantSkip  bool  // true if should return 0 (skip)
	}{
		{"OpInsert maps to Insert", core.OpInsert, Insert, false},
		{"OpUpdateAfter maps to Update (CRITICAL FIX)", core.OpUpdateAfter, Update, false},
		{"OpDelete maps to Delete", core.OpDelete, Delete, false},
		{"OpUpdateBefore returns 0 (skip)", core.OpUpdateBefore, 0, true},
		{"Unknown operation returns 0", core.Operation(99), 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.operationToSinkType(tt.op)
			if tt.wantSkip {
				if got != 0 {
					t.Errorf("operationToSinkType(%v) = %v, want 0 (skip)", tt.op, got)
				}
			} else {
				if got != tt.want {
					t.Errorf("operationToSinkType(%v) = %v, want %v", tt.op, got, tt.want)
				}
			}
		})
	}
}

// TestSkill_FilterByOperation_WithLogging tests sink filtering with when field
// Added logging in the fix, this verifies the filtering logic works correctly
func TestSkill_FilterByOperation_WithLogging(t *testing.T) {
	skill := &Skill{
		Name: "test_skill",
		Sinks: []Sink{
			{
				SinkConfig: SinkConfig{Name: "insert_update_sink"},
				When:       []string{"insert", "update"},
			},
			{
				SinkConfig: SinkConfig{Name: "delete_sink"},
				When:       []string{"delete"},
			},
			{
				SinkConfig: SinkConfig{Name: "insert_only_sink"},
				When:       []string{"insert"},
			},
		},
	}

	tests := []struct {
		name          string
		op            Operation
		wantCount     int
		wantSinkNames []string
	}{
		{"Insert matches insert_update_sink and insert_only_sink", Insert, 2, []string{"insert_update_sink", "insert_only_sink"}},
		{"Update matches insert_update_sink only", Update, 1, []string{"insert_update_sink"}},
		{"Delete matches delete_sink only", Delete, 1, []string{"delete_sink"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := skill.FilterByOperation(tt.op)
			if len(result) != tt.wantCount {
				t.Errorf("FilterByOperation(%v) returned %d sinks, want %d", tt.op, len(result), tt.wantCount)
			}

			// Verify sink names
			gotNames := make([]string, len(result))
			for i, sink := range result {
				gotNames[i] = sink.Name
			}
			for _, wantName := range tt.wantSinkNames {
				found := false
				for _, gotName := range gotNames {
					if gotName == wantName {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected sink %q in result, got %v", wantName, gotNames)
				}
			}
		})
	}
}

// TestEngine_ShortTableName_EdgeCases tests edge cases for table name parsing
func TestEngine_ShortTableName_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"no prefix", "users", "users"},
		{"dbo dot prefix", "dbo.users", "users"},
		{"dbo underscore prefix", "dbo_users", "users"},
		{"cdc dot prefix", "cdc.users", "users"},
		{"cdc underscore prefix", "cdc_users", "users"},
		{"sys dot prefix", "sys.configurations", "configurations"},
		{"alwayson prefix", "alwayson.availability_groups", "availability_groups"},
		{"mixed case table", "dbo.MyTable", "MyTable"},
		{"underscore in table name", "dbo.user_profiles", "user_profiles"},
		{"cdc generated table", "cdc.dbo_Users_CT", "dbo_Users_CT"}, // Only removes first prefix
		{"double underscore", "dbo__test", "_test"},                   // Edge case
		{"prefix as table name", "dbo", "dbo"},                        // Exact match, no change
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shortTableName(tt.input)
			if got != tt.want {
				t.Errorf("shortTableName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
