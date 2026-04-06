package sqlplugin

import (
	"testing"
)

func TestExecutorReplaceParameters(t *testing.T) {
	executor := NewExecutor(nil)
	params := CDCParameters{
		CDCLSN:       "0x00001abc",
		CDCTxID:      "tx_12345",
		CDCTable:     "dbo.orders",
		CDCOperation:  int(Insert),
		TableIDs:     []interface{}{1, 2, 3},
		Fields: map[string]interface{}{
			"order_id":    123,
			"amount":     100.50,
			"customer_id": 456,
		},
	}

	tests := []struct {
		name     string
		template string
		expected string
	}{
		{
			name:     "cdc_lsn",
			template: "SELECT * FROM orders WHERE cdc_lsn = '@cdc_lsn'",
			expected: "SELECT * FROM orders WHERE cdc_lsn = '0x00001abc'",
		},
		{
			name:     "cdc_tx_id",
			template: "SELECT * FROM orders WHERE cdc_tx_id = '@cdc_tx_id'",
			expected: "SELECT * FROM orders WHERE cdc_tx_id = 'tx_12345'",
		},
		{
			name:     "cdc_table",
			template: "SELECT * FROM orders WHERE table_name = '@cdc_table'",
			expected: "SELECT * FROM orders WHERE table_name = 'dbo.orders'",
		},
		{
			name:     "cdc_operation",
			template: "SELECT * FROM orders WHERE operation = @cdc_operation",
			expected: "SELECT * FROM orders WHERE operation = 1",
		},
		{
			name:     "field parameter",
			template: "SELECT * FROM orders WHERE order_id = @order_id",
			expected: "SELECT * FROM orders WHERE order_id = 123",
		},
		{
			name:     "multiple parameters",
			template: "SELECT * FROM orders WHERE order_id = @order_id AND customer_id = @customer_id",
			expected: "SELECT * FROM orders WHERE order_id = 123 AND customer_id = 456",
		},
		{
			name:     "float parameter",
			template: "SELECT * FROM orders WHERE amount > @amount",
			expected: "SELECT * FROM orders WHERE amount > 100.5", // Go truncates floats
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.ReplaceParameters(tt.template, params)
			if err != nil {
				t.Fatalf("ReplaceParameters error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("ReplaceParameters() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestExecutorParameterReplacement(t *testing.T) {
	executor := NewExecutor(nil)

	params := CDCParameters{
		CDCLSN:      "0xabc123",
		CDCTxID:     "tx001",
		CDCTable:    "dbo.test",
		CDCOperation: 1,
		TableIDs:    []interface{}{1, 2},
		Fields: map[string]interface{}{
			"id":   100,
			"name": "test",
		},
	}

	template := "SELECT * FROM test WHERE cdc_lsn = '@cdc_lsn' AND id = @id AND name = '@name'"
	result, err := executor.ReplaceParameters(template, params)

	expected := "SELECT * FROM test WHERE cdc_lsn = '0xabc123' AND id = 100 AND name = 'test'"
	if err != nil {
		t.Fatalf("ReplaceParameters error: %v", err)
	}
	if result != expected {
		t.Errorf("ReplaceParameters() = %q, want %q", result, expected)
	}
}

func TestExecutorMultipleTableIDs(t *testing.T) {
	executor := NewExecutor(nil)

	params := CDCParameters{
		CDCLSN:   "0x001",
		CDCTable: "dbo.orders",
		Fields:  map[string]interface{}{
			"order_ids": []interface{}{1, 2, 3, 4, 5},
		},
	}

	template := "SELECT * FROM orders WHERE @order_ids"
	result, err := executor.ReplaceParameters(template, params)

	// Expected: order_ids should be replaced with (1, 2, 3, 4, 5)
	expected := "SELECT * FROM orders WHERE (1, 2, 3, 4, 5)"
	if err != nil {
		t.Fatalf("ReplaceParameters error: %v", err)
	}
	if result != expected {
		t.Errorf("ReplaceParameters() = %q, want %q", result, expected)
	}
}

func TestExecutorMultipleReplacements(t *testing.T) {
	executor := NewExecutor(nil)

	params := CDCParameters{
		CDCLSN: "0x001",
		Fields: map[string]interface{}{
			"amount":    100,
			"qty":     5,
			"product_id": 999,
		},
	}

	template := `
		UPDATE inventory 
		SET quantity = quantity - @qty, 
		    amount = amount - @amount
		WHERE id = @product_id
	`
	result, err := executor.ReplaceParameters(template, params)

	expected := `
		UPDATE inventory 
		SET quantity = quantity - 5, 
		    amount = amount - 100
		WHERE id = 999
	`
	if err != nil {
		t.Fatalf("ReplaceParameters error: %v", err)
	}
	if result != expected {
		t.Errorf("ReplaceParameters() = %q, want %q", result, expected)
	}
}