package sql

import (
	"testing"
)

func TestNewRouter(t *testing.T) {
	router := NewRouter()
	if router == nil {
		t.Error("NewRouter() returned nil")
	}
}

func TestRouterGetSinkByName(t *testing.T) {
	router := NewRouter()

	sinks := []SinkConfig{
		{Name: "orders_sink", On: "dbo.orders", Output: "orders_output"},
		{Name: "items_sink", On: "dbo.order_items", Output: "items_output"},
	}

	sink := router.GetSinkByName(sinks, "orders_sink")
	if sink == nil {
		t.Error("expected to find sink orders_sink")
	}
	if sink != nil && sink.Output != "orders_output" {
		t.Errorf("expected output 'orders_output', got '%s'", sink.Output)
	}

	// Test non-existent
	sink = router.GetSinkByName(sinks, "nonexistent")
	if sink != nil {
		t.Error("expected nil for non-existent sink")
	}
}

func TestRouterSplitDataSet(t *testing.T) {
	router := NewRouter()

	sinks := []SinkConfig{
		{Name: "orders_sink", On: "dbo.orders"},
		{Name: "items_sink", On: "dbo.order_items"},
	}

	ds := &DataSet{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{1, "order1"},
			{2, "item1"},
		},
	}

	result := router.SplitDataSet(ds, sinks)
	// This tests that SplitDataSet doesn't panic
	if result == nil {
		t.Error("SplitDataSet returned nil")
	}
}

func TestRouterValidateSinks(t *testing.T) {
	router := NewRouter()

	// Test valid sinks
	validSinks := []SinkConfig{
		{Name: "sink1", On: "dbo.table1", Output: "out1", PrimaryKey: "id"},
	}
	err := router.ValidateSinks(validSinks)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Test invalid: missing output
	invalidSinks := []SinkConfig{
		{Name: "sink1", On: "dbo.table1"},
	}
	err = router.ValidateSinks(invalidSinks)
	if err == nil {
		t.Error("expected error for missing output")
	}
}