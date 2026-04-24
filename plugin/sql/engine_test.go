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
			{core.OpInsert, Insert},      // 1 → 1
			{core.OpUpdateAfter, Update}, // 4 → 2 (key fix for sink filtering)
			{core.OpDelete, Delete},      // 3 → 3
			{core.OpUpdateBefore, 0},     // Should be skipped
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

// TestEngine_MultipleSkills_SinkIsolation verifies that switching between skills
// correctly isolates sink configurations. This is a regression test for the bug
// where sinkOpCache was shared across skills, causing incorrect sink selection.
func TestEngine_MultipleSkills_SinkIsolation(t *testing.T) {
	// Create two skills with different sink configurations
	skillA := &Skill{
		Name:     "skill_a",
		On:       []string{"table_a"},
		Database: "db_a",
		Sinks: []Sink{
			{
				SinkConfig: SinkConfig{
					Name:       "sink_a1",
					Output:     "output_a1",
					PrimaryKey: "id",
					OnConflict: "overwrite",
				},
				When: []string{"insert", "update"},
			},
			{
				SinkConfig: SinkConfig{
					Name:       "sink_a2",
					Output:     "output_a2",
					PrimaryKey: "id",
					OnConflict: "skip",
				},
				When: []string{"delete"},
			},
		},
	}

	skillB := &Skill{
		Name:     "skill_b",
		On:       []string{"table_b"},
		Database: "db_b",
		Sinks: []Sink{
			{
				SinkConfig: SinkConfig{
					Name:       "sink_b1",
					Output:     "output_b1",
					PrimaryKey: "pk",
					OnConflict: "error",
				},
				When: []string{"insert"},
			},
		},
	}

	// Create engine with skillA (engine is not needed for this test,
	// as sink cache is now per-skill)
	_ = skillA
	engine := &Engine{skill: skillA}
	_ = engine

	// Test 1: skillA should have 2 insert/update sinks
	inSinksA := skillA.FilterByOperation(Insert)
	if len(inSinksA) != 1 {
		t.Errorf("skillA insert sinks: expected 1, got %d", len(inSinksA))
	}
	if len(inSinksA) > 0 && inSinksA[0].Name != "sink_a1" {
		t.Errorf("skillA insert sink: expected sink_a1, got %s", inSinksA[0].Name)
	}

	// Test 2: skillB should have 1 insert sink
	inSinksB := skillB.FilterByOperation(Insert)
	if len(inSinksB) != 1 {
		t.Errorf("skillB insert sinks: expected 1, got %d", len(inSinksB))
	}
	if len(inSinksB) > 0 && inSinksB[0].Name != "sink_b1" {
		t.Errorf("skillB insert sink: expected sink_b1, got %s", inSinksB[0].Name)
	}

	// Test 3: After filtering skillA, cache should be populated
	_ = skillA.FilterByOperation(Insert)
	if skillA.sinkOpCache == nil {
		t.Error("skillA sinkOpCache should be initialized after FilterByOperation")
	}

	// Test 4: skillB should have its own separate cache
	_ = skillB.FilterByOperation(Insert)
	if skillB.sinkOpCache == nil {
		t.Error("skillB sinkOpCache should be initialized after FilterByOperation")
	}

	// Test 5: Cache should be independent - skillA cache doesn't affect skillB
	inSinksA2 := skillA.FilterByOperation(Insert)
	inSinksB2 := skillB.FilterByOperation(Insert)

	if len(inSinksA2) != 1 || inSinksA2[0].Name != "sink_a1" {
		t.Errorf("skillA cached result incorrect: got %v", inSinksA2)
	}
	if len(inSinksB2) != 1 || inSinksB2[0].Name != "sink_b1" {
		t.Errorf("skillB cached result incorrect: got %v", inSinksB2)
	}

	// Test 6: Delete sinks
	delSinksA := skillA.FilterByOperation(Delete)
	delSinksB := skillB.FilterByOperation(Delete)

	if len(delSinksA) != 1 || delSinksA[0].Name != "sink_a2" {
		t.Errorf("skillA delete sink: expected sink_a2, got %v", delSinksA)
	}
	if len(delSinksB) != 0 {
		t.Errorf("skillB delete sink: expected 0, got %d", len(delSinksB))
	}

	// Test 7: Verify cache isolation with repeated filtering
	for i := 0; i < 3; i++ {
		resultA := skillA.FilterByOperation(Insert)
		resultB := skillB.FilterByOperation(Insert)

		if len(resultA) != 1 || resultA[0].Name != "sink_a1" {
			t.Errorf("Iteration %d: skillA result incorrect: %v", i, resultA)
		}
		if len(resultB) != 1 || resultB[0].Name != "sink_b1" {
			t.Errorf("Iteration %d: skillB result incorrect: %v", i, resultB)
		}
	}
}
