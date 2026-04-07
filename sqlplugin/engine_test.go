package sqlplugin

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
		Sinks: SinksConfig{
			Insert: []SinkConfig{
				{
					Name:       "sync_insert",
					Output:     "order_sync",
					PrimaryKey: "order_id",
					OnConflict: "skip",
					// SQL will be set by test
				},
			},
		},
	}

	// Note: We can't fully test without MSSQL, but we can test the parameter building

	t.Run("buildBatchParams", func(t *testing.T) {
		engine := &Engine{
			skill: skill,
		}

		tx := &core.Transaction{
			ID: "tx-123",
			Changes: []core.Change{
				{
					Table:         "orders",
					TransactionID: "tx-123",
					LSN:           []byte{0x01, 0x02, 0x03},
					Operation:     core.OpInsert,
					Data: map[string]interface{}{
						"order_id": 100,
						"amount":   500,
						"status":   "pending",
					},
				},
				{
					Table:         "orders",
					TransactionID: "tx-123",
					LSN:           []byte{0x04, 0x05, 0x06},
					Operation:     core.OpInsert,
					Data: map[string]interface{}{
						"order_id": 101,
						"amount":   300,
						"status":   "pending",
					},
				},
			},
		}

		params, err := engine.buildBatchParams(tx, tx.Changes)
		if err != nil {
			t.Fatalf("buildBatchParams failed: %v", err)
		}

		// Verify params
		if params["cdc_tx_id"] != "tx-123" {
			t.Errorf("expected cdc_tx_id = tx-123, got %v", params["cdc_tx_id"])
		}

		if params["orders_order_id"] != 101 {
			t.Errorf("expected orders_order_id = 101 (last change), got %v", params["orders_order_id"])
		}

		if params["orders_amount"] != 300 {
			t.Errorf("expected orders_amount = 300 (last change), got %v", params["orders_amount"])
		}

		ids, ok := params["orders_ids"].([]interface{})
		if !ok {
			t.Fatalf("expected orders_ids to be []interface{}, got %T", params["orders_ids"])
		}
		if len(ids) != 2 {
			t.Errorf("expected 2 ids, got %d", len(ids))
		}
	})

	t.Run("operationToSinkType", func(t *testing.T) {
		engine := &Engine{skill: skill}

		tests := []struct {
			op      core.Operation
			want    Operation
			wantErr bool
		}{
			{core.OpInsert, Insert, false},
			{core.OpUpdateAfter, Update, false},
			{core.OpDelete, Delete, false},
			{core.OpUpdateBefore, 0, true}, // Should be skipped
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

	t.Run("groupChangesByOperation", func(t *testing.T) {
		changes := []core.Change{
			{Operation: core.OpInsert, Data: map[string]interface{}{"id": 1}},
			{Operation: core.OpDelete, Data: map[string]interface{}{"id": 2}},
			{Operation: core.OpInsert, Data: map[string]interface{}{"id": 3}},
			{Operation: core.OpUpdateAfter, Data: map[string]interface{}{"id": 4}},
		}

		groups := groupChangesByOperation(changes)

		if len(groups[core.OpInsert]) != 2 {
			t.Errorf("expected 2 inserts, got %d", len(groups[core.OpInsert]))
		}
		if len(groups[core.OpDelete]) != 1 {
			t.Errorf("expected 1 delete, got %d", len(groups[core.OpDelete]))
		}
		if len(groups[core.OpUpdateAfter]) != 1 {
			t.Errorf("expected 1 update, got %d", len(groups[core.OpUpdateAfter]))
		}
	})
}

func TestEngine_ShortTableName(t *testing.T) {
	tests := []struct {
		input string
		want string
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

func TestIsIDField(t *testing.T) {
	tests := []struct {
		name  string
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
