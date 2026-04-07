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
		{Table: "dbo.orders", LSN: "1", TxID: "tx1", Operation: Insert, Data: map[string]interface{}{"order_id": 1}},
		{Table: "dbo.orders", LSN: "2", TxID: "tx1", Operation: Insert, Data: map[string]interface{}{"order_id": 2}},
		{Table: "dbo.order_items", LSN: "1", TxID: "tx1", Operation: Insert, Data: map[string]interface{}{"order_id": 10}},
		{Table: "dbo.order_items", LSN: "2", TxID: "tx1", Operation: Insert, Data: map[string]interface{}{"order_id": 11}},
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

	// CDC metadata (not prefixed)
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

	// Data fields prefixed with table name: {table_name}_{column_name}
	if params["orders_order_id"] != 100 {
		t.Errorf("orders_order_id = %v, want 100", params["orders_order_id"])
	}
	if params["orders_amount"] != 50.00 {
		t.Errorf("orders_amount = %v, want 50.00", params["orders_amount"])
	}
	if params["orders_status"] != "pending" {
		t.Errorf("orders_status = %v, want pending", params["orders_status"])
	}
}

func TestBuildParams_MultiTableWithSameFieldName(t *testing.T) {
	mapper := NewMapper()

	// Two changes from different tables with same field name "order_id"
	changeOrders := &ChangeItem{
		Table:     "dbo.orders",
		LSN:       "0x001",
		TxID:      "tx1",
		Operation: Insert,
		Data: map[string]interface{}{
			"order_id": 100,
		},
	}

	changeItems := &ChangeItem{
		Table:     "dbo.order_items",
		LSN:       "0x002",
		TxID:      "tx1",
		Operation: Insert,
		Data: map[string]interface{}{
			"order_id": 200, // Same field name, different value
		},
	}

	paramsOrders, _ := mapper.BuildParams(changeOrders)
	paramsItems, _ := mapper.BuildParams(changeItems)

	// Different table prefixes prevent name collision
	if paramsOrders["orders_order_id"] != 100 {
		t.Errorf("orders_order_id = %v, want 100", paramsOrders["orders_order_id"])
	}
	if paramsItems["order_items_order_id"] != 200 {
		t.Errorf("order_items_order_id = %v, want 200", paramsItems["order_items_order_id"])
	}

	// Verify they are distinct
	if paramsOrders["orders_order_id"] == paramsItems["order_items_order_id"] {
		t.Error("expected different values for same field from different tables")
	}
}

func TestBuildParams_ComplexSchema(t *testing.T) {
	mapper := NewMapper()

	change := &ChangeItem{
		Table:     "inventory.dbo.products",
		LSN:       "0x001",
		TxID:      "tx1",
		Operation: Update,
		Data: map[string]interface{}{
			"product_id":    500,
			"name":          "Widget",
			"price":         29.99,
			"category_id":   10,
			"updated_at":    "2026-04-07 08:00:00",
		},
	}

	params, err := mapper.BuildParams(change)
	if err != nil {
		t.Fatalf("BuildParams error: %v", err)
	}

	// Verify all fields are prefixed with "products" (short table name)
	expectedParams := map[string]interface{}{
		"cdc_lsn":               "0x001",
		"cdc_tx_id":             "tx1",
		"cdc_table":             "inventory.dbo.products",
		"cdc_operation":         int(Update),
		"products_product_id":    500,
		"products_name":          "Widget",
		"products_price":        29.99,
		"products_category_id":   10,
		"products_updated_at":    "2026-04-07 08:00:00",
	}

	for key, expectedValue := range expectedParams {
		if params[key] != expectedValue {
			t.Errorf("%s = %v, want %v", key, params[key], expectedValue)
		}
	}
}
