package sqlplugin

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
stages:
  - name: stage1
    sql: SELECT * FROM dbo.orders
sinks:
  insert:
    - name: orders_sink
      on: dbo.orders
      sql: SELECT * FROM orders
      output: orders
      primary_key: order_id
  delete:
    - name: orders_delete
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
	if len(skill.Stages) != 1 {
		t.Errorf("expected 1 stage, got %d", len(skill.Stages))
	}
	if len(skill.Sinks.Insert) != 1 {
		t.Errorf("expected 1 insert sink, got %d", len(skill.Sinks.Insert))
	}
	if len(skill.Sinks.Delete) != 1 {
		t.Errorf("expected 1 delete sink, got %d", len(skill.Sinks.Delete))
	}
}

func TestSinkConfigInlineSQL(t *testing.T) {
	yamlContent := `
name: inline_test
on:
  - dbo.test
sinks:
  insert:
    - name: test_sink
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

	if skill.Sinks.Insert[0].SQL != "SELECT * FROM test_table WHERE id = @id" {
		t.Errorf("expected inline SQL, got '%s'", skill.Sinks.Insert[0].SQL)
	}
}

func TestSinkConfigSQLFile(t *testing.T) {
	yamlContent := `
name: file_test
on:
  - dbo.test
sinks:
  insert:
    - name: test_sink
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

	if skill.Sinks.Insert[0].SQLFile != "./sql/test.sql" {
		t.Errorf("expected sql_file './sql/test.sql', got '%s'", skill.Sinks.Insert[0].SQLFile)
	}
}

func TestCDCParameters(t *testing.T) {
	params := CDCParameters{
		CDCLSN:       "0x00001",
		CDCTxID:      "tx123",
		CDCTable:     "dbo.orders",
		CDCOperation: 1,
		TableIDs:     []interface{}{1, 2, 3},
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
	if len(params.TableIDs) != 3 {
		t.Errorf("expected 3 TableIDs, got %d", len(params.TableIDs))
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

func TestSkillGetSinks(t *testing.T) {
	skill := &Skill{
		Name: "test",
		On:   []string{"dbo.orders"},
		Sinks: SinksConfig{
			Insert: []SinkConfig{{Name: "insert_sink"}},
			Update: []SinkConfig{{Name: "update_sink"}},
			Delete: []SinkConfig{{Name: "delete_sink"}},
		},
	}

	if len(skill.GetSinks(Insert)) != 1 {
		t.Errorf("expected 1 insert sink, got %d", len(skill.GetSinks(Insert)))
	}
	if len(skill.GetSinks(Update)) != 1 {
		t.Errorf("expected 1 update sink, got %d", len(skill.GetSinks(Update)))
	}
	if len(skill.GetSinks(Delete)) != 1 {
		t.Errorf("expected 1 delete sink, got %d", len(skill.GetSinks(Delete)))
	}
}

func TestFilterSinks(t *testing.T) {
	sinks := []SinkConfig{
		{Name: "sink1", On: "dbo.orders"},
		{Name: "sink2", On: "dbo.order_items"},
		{Name: "sink_all", On: "*"},
	}

	// Filter for specific table (exact match or wildcard *)
	filtered := FilterSinks(sinks, "dbo.orders")
	if len(filtered) != 1 {
		t.Errorf("expected 1 sink for dbo.orders, got %d", len(filtered))
	}

	// Filter for another table
	filtered = FilterSinks(sinks, "dbo.order_items")
	if len(filtered) != 1 {
		t.Errorf("expected 1 sink for dbo.order_items, got %d", len(filtered))
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