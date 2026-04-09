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
			name: "only insert without update",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{"insert"}},
				},
			},
			wantErr: true,
			errMsg:  "insert requires update",
		},
		{
			name: "only update without insert",
			skill: Skill{
				Sinks: []Sink{
					{SinkConfig: SinkConfig{Name: "test"}, When: []string{"update"}},
				},
			},
			wantErr: true,
			errMsg:  "update requires insert",
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
