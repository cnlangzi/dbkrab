package sql

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSkillParse(t *testing.T) {
	yamlContent := `
name: test_plugin
description: Test SQL plugin
on:
  - dbo.orders
  - dbo.order_items
sinks:
  - name: orders_sink
    when: [insert, update]
    on: dbo.orders
    sql: SELECT * FROM orders
    output: orders
    primary_key: order_id
  - name: orders_delete
    when: [delete]
    on: dbo.orders
    sql: SELECT order_id FROM deleted_orders
    output: deleted_orders
    primary_key: order_id
`
	var skill Skill
	err := yaml.Unmarshal([]byte(yamlContent), &skill)
	if err != nil {
		t.Fatalf("failed to parse skill: %v", err)
	}

	if skill.Name != "test_plugin" {
		t.Errorf("expected name 'test_plugin', got '%s'", skill.Name)
	}
	if skill.Description != "Test SQL plugin" {
		t.Errorf("expected description 'Test SQL plugin', got '%s'", skill.Description)
	}
	if len(skill.On) != 2 {
		t.Errorf("expected 2 tables, got %d", len(skill.On))
	}
	if len(skill.Sinks) != 2 {
		t.Errorf("expected 2 sinks, got %d", len(skill.Sinks))
	}

	// Test FilterByOperation
	insertSinks := skill.FilterByOperation(Insert)
	if len(insertSinks) != 1 {
		t.Errorf("expected 1 insert sink, got %d", len(insertSinks))
	}
	if insertSinks[0].Name != "orders_sink" {
		t.Errorf("expected insert sink name 'orders_sink', got '%s'", insertSinks[0].Name)
	}

	deleteSinks := skill.FilterByOperation(Delete)
	if len(deleteSinks) != 1 {
		t.Errorf("expected 1 delete sink, got %d", len(deleteSinks))
	}
	if deleteSinks[0].Name != "orders_delete" {
		t.Errorf("expected delete sink name 'orders_delete', got '%s'", deleteSinks[0].Name)
	}

	updateSinks := skill.FilterByOperation(Update)
	if len(updateSinks) != 1 {
		t.Errorf("expected 1 update sink, got %d", len(updateSinks))
	}
}

func TestSinkConfigInlineSQL(t *testing.T) {
	yamlContent := `
name: inline_test
on:
  - dbo.test
sinks:
  - name: test_sink
    when: [insert, update]
    on: dbo.test
    sql: SELECT * FROM test_table WHERE id = @id
    output: test_output
    primary_key: id
`
	var skill Skill
	err := yaml.Unmarshal([]byte(yamlContent), &skill)
	if err != nil {
		t.Fatalf("failed to parse skill: %v", err)
	}

	if skill.Sinks[0].SQL != "SELECT * FROM test_table WHERE id = @id" {
		t.Errorf("expected inline SQL, got '%s'", skill.Sinks[0].SQL)
	}
}

func TestSinkConfigSQLFile(t *testing.T) {
	yamlContent := `
name: file_test
on:
  - dbo.test
sinks:
  - name: test_sink
    when: [insert, update]
    on: dbo.test
    sql_file: ./sql/test.sql
    output: test_output
    primary_key: id
`
	var skill Skill
	err := yaml.Unmarshal([]byte(yamlContent), &skill)
	if err != nil {
		t.Fatalf("failed to parse skill: %v", err)
	}

	if skill.Sinks[0].SQLFile != "./sql/test.sql" {
		t.Errorf("expected sql_file './sql/test.sql', got '%s'", skill.Sinks[0].SQLFile)
	}
}

func TestCDCParameters(t *testing.T) {
	params := CDCParameters{
		CDCLSN:       "0x00001",
		CDCTxID:      "tx123",
		CDCTable:     "dbo.orders",
		CDCOperation: 1,
		Fields:       map[string]interface{}{"order_id": 1, "amount": 100.50},
	}

	if params.CDCLSN != "0x00001" {
		t.Errorf("expected CDCLSN '0x00001', got '%s'", params.CDCLSN)
	}
	if params.CDCTable != "dbo.orders" {
		t.Errorf("expected CDCTable 'dbo.orders', got '%s'", params.CDCTable)
	}
	if params.CDCOperation != 1 {
		t.Errorf("expected CDCOperation 1, got %d", params.CDCOperation)
	}
}

func TestDataSet(t *testing.T) {
	ds := DataSet{
		Columns: []string{"id", "name", "amount"},
		Rows: [][]interface{}{
			{1, "item1", 100.0},
			{2, "item2", 200.0},
		},
	}

	if len(ds.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(ds.Columns))
	}
	if len(ds.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(ds.Rows))
	}
	if ds.Rows[0][0].(int) != 1 {
		t.Errorf("expected first row id 1, got %v", ds.Rows[0][0])
	}
}

func TestOperationString(t *testing.T) {
	if Insert.String() != "Insert" {
		t.Errorf("Insert.String() = %s, want Insert", Insert.String())
	}
	if Update.String() != "Update" {
		t.Errorf("Update.String() = %s, want Update", Update.String())
	}
	if Delete.String() != "Delete" {
		t.Errorf("Delete.String() = %s, want Delete", Delete.String())
	}
}

func TestOperationToSinkType(t *testing.T) {
	if OperationToSinkType(Insert) != "insert" {
		t.Errorf("OperationToSinkType(Insert) = %s, want insert", OperationToSinkType(Insert))
	}
	if OperationToSinkType(Update) != "update" {
		t.Errorf("OperationToSinkType(Update) = %s, want update", OperationToSinkType(Update))
	}
	if OperationToSinkType(Delete) != "delete" {
		t.Errorf("OperationToSinkType(Delete) = %s, want delete", OperationToSinkType(Delete))
	}
}

func TestSkillFilterByOperation(t *testing.T) {
	skill := &Skill{
		Name: "test",
		On:   []string{"dbo.orders"},
		Sinks: []Sink{
			{SinkConfig: SinkConfig{Name: "insert_update_sink"}, When: []string{"insert", "update"}},
			{SinkConfig: SinkConfig{Name: "delete_sink"}, When: []string{"delete"}},
		},
	}

	insertSinks := skill.FilterByOperation(Insert)
	if len(insertSinks) != 1 {
		t.Errorf("expected 1 insert sink, got %d", len(insertSinks))
	}
	if insertSinks[0].Name != "insert_update_sink" {
		t.Errorf("expected insert sink name 'insert_update_sink', got '%s'", insertSinks[0].Name)
	}

	updateSinks := skill.FilterByOperation(Update)
	if len(updateSinks) != 1 {
		t.Errorf("expected 1 update sink, got %d", len(updateSinks))
	}

	deleteSinks := skill.FilterByOperation(Delete)
	if len(deleteSinks) != 1 {
		t.Errorf("expected 1 delete sink, got %d", len(deleteSinks))
	}
	if deleteSinks[0].Name != "delete_sink" {
		t.Errorf("expected delete sink name 'delete_sink', got '%s'", deleteSinks[0].Name)
	}
}

func TestSkillFilterByOperation_InsertOnly(t *testing.T) {
	skill := &Skill{
		Name: "test",
		On:   []string{"dbo.orders"},
		Sinks: []Sink{
			{SinkConfig: SinkConfig{Name: "insert_only"}, When: []string{"insert"}},
			{SinkConfig: SinkConfig{Name: "update_only"}, When: []string{"update"}},
			{SinkConfig: SinkConfig{Name: "delete_only"}, When: []string{"delete"}},
		},
	}

	insertSinks := skill.FilterByOperation(Insert)
	if len(insertSinks) != 1 {
		t.Errorf("expected 1 insert sink, got %d", len(insertSinks))
	}
	if insertSinks[0].Name != "insert_only" {
		t.Errorf("expected insert sink name 'insert_only', got '%s'", insertSinks[0].Name)
	}

	updateSinks := skill.FilterByOperation(Update)
	if len(updateSinks) != 1 {
		t.Errorf("expected 1 update sink, got %d", len(updateSinks))
	}
	if updateSinks[0].Name != "update_only" {
		t.Errorf("expected update sink name 'update_only', got '%s'", updateSinks[0].Name)
	}

	deleteSinks := skill.FilterByOperation(Delete)
	if len(deleteSinks) != 1 {
		t.Errorf("expected 1 delete sink, got %d", len(deleteSinks))
	}
	if deleteSinks[0].Name != "delete_only" {
		t.Errorf("expected delete sink name 'delete_only', got '%s'", deleteSinks[0].Name)
	}
}

func TestFilterSinks(t *testing.T) {
	sinks := []SinkConfig{
		{Name: "sink1", On: "dbo.orders"},
		{Name: "sink2", On: "dbo.order_items"},
		{Name: "sink_all", On: ""},
	}

	// Filter for specific table (exact match or empty = all tables)
	// Empty On means "all tables", so it should be included in results
	filtered := FilterSinks(sinks, "dbo.orders")
	if len(filtered) != 2 {
		t.Errorf("expected 2 sinks for dbo.orders (exact + wildcard), got %d", len(filtered))
	}

	// Filter for another table
	filtered = FilterSinks(sinks, "dbo.order_items")
	if len(filtered) != 2 {
		t.Errorf("expected 2 sinks for dbo.order_items (exact + wildcard), got %d", len(filtered))
	}
}

func TestDataSetToMap(t *testing.T) {
	ds := &DataSet{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{1, "test1"},
			{2, "test2"},
		},
	}

	result := DataSetToMap(ds)
	if len(result) != 2 {
		t.Errorf("expected 2 maps, got %d", len(result))
	}
	if result[0]["id"].(int) != 1 {
		t.Errorf("expected id 1, got %v", result[0]["id"])
	}
}

func TestValidateSinks(t *testing.T) {
	tests := []struct {
		name    string
		skill   Skill
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid insert_update sink",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid delete sink",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{"delete"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid insert only sink",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{"insert"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid update only sink",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{"update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "missing when field",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{}},
				},
			},
			wantErr: true,
			errMsg:  "sink[0].when is required",
		},
		{
			name: "invalid when value",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{"invalid"}},
				},
			},
			wantErr: true,
			errMsg:  "invalid value 'invalid'",
		},
		{
			name: "mixed insert and delete",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{"insert", "delete"}},
				},
			},
			wantErr: true,
			errMsg:  "cannot mix delete with insert/update",
		},
		{
			name: "mixed update and delete",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{"update", "delete"}},
				},
			},
			wantErr: true,
			errMsg:  "cannot mix delete with insert/update",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.skill.ValidateSinks()
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSinks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err != nil {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateSinks() error = %v, should contain %v", err, tt.errMsg)
				}
			}
		})
	}
}

func TestValidateSinksWithIfCondition(t *testing.T) {
	tests := []struct {
		name    string
		skill   Skill
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid if expression - simple equality",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "status = 'vip'"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid if expression - comparison",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "amount > 100"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid if expression - logical operators",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "status = 'vip' AND amount > 100"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid if expression - field to field comparison",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "orders.amount > orders.min_amount"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid if expression - OR operator",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "status = 'vip' OR status = 'gold'"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid if expression - NOT operator",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "status != 'cancelled'"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid if expression - IN operator",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "status IN ('vip', 'gold', 'silver')"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid if expression - case insensitive table/field names",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "ORDERS.STATUS = 'vip'"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid if expression - syntax error",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "status = 'vip' AND"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: true,
			errMsg:  "invalid expression",
		},
		{
			name: "invalid if expression - unbalanced parentheses",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: "(status = 'vip'"}, When: []string{"insert", "update"}},
				},
			},
			wantErr: true,
			errMsg:  "invalid expression",
		},
		{
			name: "no if - always valid",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: ""}, When: []string{"insert", "update"}},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.skill.ValidateSinks()
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSinks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err != nil {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateSinks() error = %v, should contain %v", err, tt.errMsg)
				}
			}
		})
	}
}

func TestSinkConfigEvalIf(t *testing.T) {
	tests := []struct {
		name       string
		ifExpr     string
		cdcParams  CDCParameters
		wantResult bool
	}{
		{
			name:       "no if - always true",
			ifExpr:     "",
			cdcParams:  CDCParameters{Fields: map[string]interface{}{}},
			wantResult: true,
		},
		{
			name:   "simple equality - true",
			ifExpr: "status = 'vip'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "vip"},
			},
			wantResult: true,
		},
		{
			name:   "simple equality - false",
			ifExpr: "status = 'vip'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "regular"},
			},
			wantResult: false,
		},
		{
			name:   "literal value case sensitive - match",
			ifExpr: "status = 'VIP'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "VIP"},
			},
			wantResult: true,
		},
		{
			name:   "literal value case sensitive - no match",
			ifExpr: "status = 'VIP'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "vip"},
			},
			wantResult: false,
		},
		{
			name:   "comparison - greater than true",
			ifExpr: "amount > 100",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_amount": 150},
			},
			wantResult: true,
		},
		{
			name:   "comparison - greater than false",
			ifExpr: "amount > 100",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_amount": 50},
			},
			wantResult: false,
		},
		{
			name:   "AND operator - both true",
			ifExpr: "status = 'vip' AND amount > 100",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "vip", "orders_amount": 150},
			},
			wantResult: true,
		},
		{
			name:   "AND operator - one false",
			ifExpr: "status = 'vip' AND amount > 100",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "vip", "orders_amount": 50},
			},
			wantResult: false,
		},
		{
			name:   "OR operator - one true",
			ifExpr: "status = 'vip' OR status = 'gold'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "gold"},
			},
			wantResult: true,
		},
		{
			name:   "OR operator - both false",
			ifExpr: "status = 'vip' OR status = 'gold'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "regular"},
			},
			wantResult: false,
		},
		{
			name:   "NOT operator",
			ifExpr: "status != 'cancelled'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "pending"},
			},
			wantResult: true,
		},
		{
			name:   "IN operator - match",
			ifExpr: "status IN ('vip', 'gold', 'silver')",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "gold"},
			},
			wantResult: true,
		},
		{
			name:   "IN operator - no match",
			ifExpr: "status IN ('vip', 'gold', 'silver')",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "regular"},
			},
			wantResult: false,
		},
		{
			name:   "field to field comparison - true",
			ifExpr: "orders.amount > orders.min_amount",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_amount": 150, "orders_min_amount": 100},
			},
			wantResult: true,
		},
		{
			name:   "field to field comparison - false",
			ifExpr: "orders.amount > orders.min_amount",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_amount": 50, "orders_min_amount": 100},
			},
			wantResult: false,
		},
		{
			name:   "case insensitive table/field names - uppercase",
			ifExpr: "ORDERS.STATUS = 'vip'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "vip"},
			},
			wantResult: true,
		},
		{
			name:   "case insensitive table/field names - mixed case",
			ifExpr: "Orders.Status = 'vip'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "vip"},
			},
			wantResult: true,
		},
		{
			name:   ">= comparison",
			ifExpr: "amount >= 100",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_amount": 100},
			},
			wantResult: true,
		},
		{
			name:   "<= comparison",
			ifExpr: "amount <= 100",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_amount": 100},
			},
			wantResult: true,
		},
		{
			name:   "< comparison",
			ifExpr: "amount < 100",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_amount": 50},
			},
			wantResult: true,
		},
		{
			name:   "!= comparison",
			ifExpr: "amount != 0",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_amount": 100},
			},
			wantResult: true,
		},
		{
			name:   "complex expression with parentheses",
			ifExpr: "(status = 'vip' OR status = 'gold') AND amount > 100",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "vip", "orders_amount": 150},
			},
			wantResult: true,
		},
		{
			name:   "complex expression with parentheses - second condition false",
			ifExpr: "(status = 'vip' OR status = 'gold') AND amount > 100",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{"orders_status": "vip", "orders_amount": 50},
			},
			wantResult: false,
		},
		{
			name:   "CDC metadata - cdc_operation",
			ifExpr: "cdc_operation = 2",
			cdcParams: CDCParameters{
				CDCOperation: 2,
				Fields:       map[string]interface{}{},
			},
			wantResult: true,
		},
		{
			name:   "CDC metadata - cdc_table",
			ifExpr: "cdc_table = 'orders'",
			cdcParams: CDCParameters{
				CDCTable: "orders",
				Fields:   map[string]interface{}{},
			},
			wantResult: true,
		},
		{
			name:   "CDC metadata - cdc_tx_id",
			ifExpr: "cdc_tx_id = 'tx-123'",
			cdcParams: CDCParameters{
				CDCTxID: "tx-123",
				Fields:  map[string]interface{}{},
			},
			wantResult: true,
		},
		{
			name:   "missing field returns false",
			ifExpr: "status = 'vip'",
			cdcParams: CDCParameters{
				Fields: map[string]interface{}{},
			},
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			skill := &Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test", If: tt.ifExpr}, When: []string{"insert", "update"}},
				},
			}

			// Validate first to compile the expression
			err := skill.ValidateSinks()
			if err != nil {
				t.Fatalf("ValidateSinks() failed: %v", err)
			}

			sinkCfg := &skill.Sinks[0].SinkConfig
			result := sinkCfg.EvalIf(tt.cdcParams)

			if result != tt.wantResult {
				t.Errorf("EvalIf() = %v, want %v", result, tt.wantResult)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
