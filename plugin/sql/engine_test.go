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
		Sinks: SinksConfig{
			Insert: []SinkConfig{
				{
					Name:       "sync_insert",
					Output:     "order_sync",
					PrimaryKey: "order_id",
					OnConflict: "skip",
				},
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

	t.Run("operationToJobType", func(t *testing.T) {
		engine := &Engine{skill: skill}

		tests := []struct {
			op   core.Operation
			want core.Operation
		}{
			{core.OpInsert, core.OpInsert},
			{core.OpUpdateAfter, core.OpUpdateAfter},
			{core.OpDelete, core.OpDelete},
			{core.OpUpdateBefore, 0}, // Should be skipped
		}

		for _, tt := range tests {
			t.Run(tt.op.String(), func(t *testing.T) {
				got := engine.operationToJobType(tt.op)
				if got != tt.want {
					t.Errorf("operationToJobType(%v) = %v, want %v", tt.op, got, tt.want)
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
