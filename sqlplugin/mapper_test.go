package sqlplugin

import (
	"testing"
)

func TestNewMapper(t *testing.T) {
	mapper := NewMapper()
	if mapper == nil {
		t.Error("NewMapper() returned nil")
	}
}

func TestShortTableName(t *testing.T) {
	mapper := NewMapper()

	tests := []struct {
		input    string
		expected string
	}{
		{"dbo.orders", "orders"},
		{"dbo.order_items", "order_items"},
		{"orders", "orders"},
		{"inventory.dbo.items", "items"},
		{"sys.tables", "tables"},
	}

	for _, tt := range tests {
		result := mapper.shortTableName(tt.input)
		if result != tt.expected {
			t.Errorf("shortTableName(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestGetChangesByTable(t *testing.T) {
	changes := []ChangeItem{
		{Table: "dbo.orders", LSN: "1", TxID: "tx1", Operation: Insert, TableID: 1},
		{Table: "dbo.orders", LSN: "2", TxID: "tx1", Operation: Insert, TableID: 2},
		{Table: "dbo.order_items", LSN: "1", TxID: "tx1", Operation: Insert, TableID: 10},
		{Table: "dbo.order_items", LSN: "2", TxID: "tx1", Operation: Insert, TableID: 11},
	}

	result := GetChangesByTable(changes)

	if len(result) != 2 {
		t.Errorf("expected 2 tables, got %d", len(result))
	}
	if len(result["dbo.orders"]) != 2 {
		t.Errorf("expected 2 order changes, got %d", len(result["dbo.orders"]))
	}
	if len(result["dbo.order_items"]) != 2 {
		t.Errorf("expected 2 order_items changes, got %d", len(result["dbo.order_items"]))
	}
}

func TestBuildParams_Basic(t *testing.T) {
	mapper := NewMapper()

	change := &ChangeItem{
		Table:     "dbo.orders",
		LSN:       "0x00001abc",
		TxID:      "tx_12345",
		Operation: Insert,
		TableID:   100,
		Data: map[string]interface{}{
			"order_id": 100,
			"amount":   50.00,
			"status":   "pending",
		},
	}

	params, err := mapper.BuildParams(change)
	if err != nil {
		t.Fatalf("BuildParams error: %v", err)
	}

	// CDC metadata
	if params["cdc_lsn"] != "0x00001abc" {
		t.Errorf("cdc_lsn = %v, want 0x00001abc", params["cdc_lsn"])
	}
	if params["cdc_tx_id"] != "tx_12345" {
		t.Errorf("cdc_tx_id = %v, want tx_12345", params["cdc_tx_id"])
	}
	if params["cdc_table"] != "dbo.orders" {
		t.Errorf("cdc_table = %v, want dbo.orders", params["cdc_table"])
	}
	if params["cdc_operation"] != int(Insert) {
		t.Errorf("cdc_operation = %v, want %d", params["cdc_operation"], Insert)
	}

	// Auto-generated table ID
	if params["orders_id"] != 100 {
		t.Errorf("orders_id = %v, want 100", params["orders_id"])
	}

	// Data fields
	if params["order_id"] != 100 {
		t.Errorf("order_id = %v, want 100", params["order_id"])
	}
	if params["amount"] != 50.00 {
		t.Errorf("amount = %v, want 50.00", params["amount"])
	}
	if params["status"] != "pending" {
		t.Errorf("status = %v, want pending", params["status"])
	}
}

func TestBuildParams_DataOverwritesMetadata(t *testing.T) {
	mapper := NewMapper()

	// Data has same name as CDC metadata
	change := &ChangeItem{
		Table:     "dbo.orders",
		LSN:       "0x00001abc",
		TxID:      "tx_12345",
		Operation: Insert,
		TableID:   100,
		Data: map[string]interface{}{
			"cdc_lsn": "user-defined-lsn", // This should overwrite cdc_lsn
			"order_id": 100,
		},
	}

	params, err := mapper.BuildParams(change)
	if err != nil {
		t.Fatalf("BuildParams error: %v", err)
	}

	// Data overwrites CDC metadata
	if params["cdc_lsn"] != "user-defined-lsn" {
		t.Errorf("cdc_lsn = %v, want user-defined-lsn (overwritten by Data)", params["cdc_lsn"])
	}
}

func TestBuildParams_AutoGenerateTableID(t *testing.T) {
	mapper := NewMapper()

	tests := []struct {
		table        string
		expectedKey  string
	}{
		{"dbo.orders", "orders_id"},
		{"dbo.order_items", "order_items_id"},
		{"customers", "customers_id"},
		{"sys.tables", "tables_id"},
	}

	for _, tt := range tests {
		change := &ChangeItem{
			Table:     tt.table,
			LSN:       "0x001",
			TxID:      "tx1",
			Operation: Insert,
			TableID:   1,
			Data:      map[string]interface{}{},
		}

		params, _ := mapper.BuildParams(change)

		if params[tt.expectedKey] != 1 {
			t.Errorf("table=%s: %s = %v, want 1", tt.table, tt.expectedKey, params[tt.expectedKey])
		}
	}
}


