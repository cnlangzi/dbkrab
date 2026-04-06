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

func TestFieldToParamName(t *testing.T) {
	mapper := NewMapper()

	tests := []struct {
		input    string
		expected string
	}{
		{"order_id", "order_id"},
		{"ORDER_ID", "order_id"},
		{"OrderID", "orderid"},
		{"customer_name", "customer_name"},
		{"Amount", "amount"},
	}

	for _, tt := range tests {
		result := mapper.fieldToParamName(tt.input)
		if result != tt.expected {
			t.Errorf("fieldToParamName(%q) = %q, want %q", tt.input, result, tt.expected)
		}
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

func TestNewSQLTransaction(t *testing.T) {
	tx := NewSQLTransaction("tx123", "0x0001")
	if tx == nil {
		t.Error("NewSQLTransaction() returned nil")
	}
	if tx.TxID != "tx123" {
		t.Errorf("expected TxID 'tx123', got '%s'", tx.TxID)
	}
	if tx.LSN != "0x0001" {
		t.Errorf("expected LSN '0x0001', got '%s'", tx.LSN)
	}
	if tx.Fields == nil {
		t.Error("Fields should be initialized")
	}
}

func TestMapperOperationString(t *testing.T) {
	if Operation(Insert).String() != "Insert" {
		t.Errorf("Operation(Insert).String() = %s, want Insert", Operation(Insert).String())
	}
	if Operation(Update).String() != "Update" {
		t.Errorf("Operation(Update).String() = %s, want Update", Operation(Update).String())
	}
	if Operation(Delete).String() != "Delete" {
		t.Errorf("Operation(Delete).String() = %s, want Delete", Operation(Delete).String())
	}
}