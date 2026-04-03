package core

import (
	"testing"
)

func TestOperationString(t *testing.T) {
	tests := []struct {
		op       Operation
		expected string
	}{
		{OpDelete, "DELETE"},
		{OpInsert, "INSERT"},
		{OpUpdateBefore, "UPDATE_BEFORE"},
		{OpUpdateAfter, "UPDATE_AFTER"},
		{Operation(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.op.String(); got != tt.expected {
				t.Errorf("Operation.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTransaction(t *testing.T) {
	tx := NewTransaction("test-tx-123")

	if tx.ID != "test-tx-123" {
		t.Errorf("Transaction.ID = %v, want test-tx-123", tx.ID)
	}

	if len(tx.Changes) != 0 {
		t.Errorf("Expected empty changes, got %d", len(tx.Changes))
	}

	// Add a change
	c := Change{
		Table:     "orders",
		Operation: OpInsert,
		Data:      map[string]interface{}{"id": 1, "name": "test"},
	}
	tx.AddChange(c)

	if len(tx.Changes) != 1 {
		t.Errorf("Expected 1 change, got %d", len(tx.Changes))
	}

	if tx.Changes[0].Table != "orders" {
		t.Errorf("Change.Table = %v, want orders", tx.Changes[0].Table)
	}
}