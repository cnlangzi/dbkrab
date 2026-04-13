package sql

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLoadSkill(t *testing.T) {
	tmpDir := t.TempDir()
	// Create flat structure: pluginsDir/test_load.yml
	skillFile := filepath.Join(tmpDir, "test_load.yml")
	skillContent := `name: test_load
description: Test skill loading
on:
  - dbo.orders
  - dbo.customers
sinks:
  - name: orders_enriched
    when: [insert, update]
    on: dbo.orders
    sql: SELECT * FROM orders
    output: orders_enriched
    primary_key: order_id
`
	writeErr := os.WriteFile(skillFile, []byte(skillContent), 0644)
	if writeErr != nil {
		t.Fatalf("failed to write skill file: %v", writeErr)
	}

	loader := NewLoader(tmpDir)
	skill, loadErr := loader.Load("test_load.yml")
	if loadErr != nil {
		t.Fatalf("failed to load skill: %v", loadErr)
	}

	if skill.Name != "test_load" {
		t.Errorf("expected name 'test_load', got '%s'", skill.Name)
	}
	if len(skill.On) != 2 {
		t.Errorf("expected 2 tables, got %d", len(skill.On))
	}
}

func TestLoadAllSkills(t *testing.T) {
	tmpDir := t.TempDir()

	skills := map[string]string{
		"skill1": "name: skill1\ndescription: First skill\non:\n  - dbo.table1\nsinks:\n  - name: sink1\n    when: [insert, update]\n    sql: SELECT 1",
		"skill2": "name: skill2\ndescription: Second skill\non:\n  - dbo.table2\nsinks:\n  - name: sink2\n    when: [insert, update]\n    sql: SELECT 1",
	}

	// Flat structure: pluginsDir/{name}.yml
	for name, content := range skills {
		writeErr := os.WriteFile(filepath.Join(tmpDir, name+".yml"), []byte(content), 0644)
		if writeErr != nil {
			t.Fatalf("failed to write skill file: %v", writeErr)
		}
	}

	loader := NewLoader(tmpDir)
	loaded, loadErr := loader.LoadAll()
	if loadErr != nil {
		t.Fatalf("failed to load all skills: %v", loadErr)
	}

	if len(loaded) != 2 {
		t.Errorf("expected 2 skills, got %d", len(loaded))
	}
}

func TestLoadSkillWithSQLFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create flat structure: pluginsDir/skill_with_file.yml
	skillFile := filepath.Join(tmpDir, "skill_with_file.yml")
	skillContent := `name: skill_with_file
description: Test skill with external sql_file
on:
  - dbo.orders
sinks:
  - name: sink1
    when: [insert, update]
    sql_file: fetch.sql
    output: out
    primary_key: id
`
	if err := os.WriteFile(skillFile, []byte(skillContent), 0644); err != nil {
		t.Fatalf("failed to write skill file: %v", err)
	}

	// Create external SQL file at pluginsDir/fetch.sql (flat structure)
	sqlContent := "SELECT * FROM orders WHERE order_id = @orders_order_id"
	if err := os.WriteFile(filepath.Join(tmpDir, "fetch.sql"), []byte(sqlContent), 0644); err != nil {
		t.Fatalf("failed to write sql file: %v", err)
	}

	loader := NewLoader(tmpDir)
	skill, err := loader.Load("skill_with_file.yml")
	if err != nil {
		t.Fatalf("failed to load skill: %v", err)
	}

	if len(skill.Sinks) == 0 {
		t.Fatal("expected at least one sink")
	}
	if skill.Sinks[0].SQL != sqlContent {
		t.Errorf("expected SQL to be loaded from external file, got: %s", skill.Sinks[0].SQL)
	}
}

func TestLoadNonExistentSkill(t *testing.T) {
	tmpDir := t.TempDir()
	loader := NewLoader(tmpDir)

	_, err := loader.Load("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent skill")
	}
}

func TestNormalizeSQLParams(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "mixed case parameters",
			input: "SELECT * FROM Cost WHERE Id = @Cost_Id AND name = @Cost_Name",
			want:  "SELECT * FROM Cost WHERE Id = @cost_id AND name = @cost_name",
		},
		{
			name:  "uppercase parameters",
			input: "INSERT INTO Orders (ID, Amount) VALUES (@ORDER_ID, @AMOUNT)",
			want:  "INSERT INTO Orders (ID, Amount) VALUES (@order_id, @amount)",
		},
		{
			name:  "already lowercase",
			input: "UPDATE items SET qty = @qty WHERE id = @item_id",
			want:  "UPDATE items SET qty = @qty WHERE id = @item_id",
		},
		{
			name:  "no parameters",
			input: "SELECT COUNT(*) FROM table",
			want:  "SELECT COUNT(*) FROM table",
		},
		{
			name:  "complex SQL with multiple params",
			input: "SELECT @CDC_LSN, @cdc_tx_id, @Cost_Id, @Cost_item_id FROM cdc.dbo_Cost_CT",
			want:  "SELECT @cdc_lsn, @cdc_tx_id, @cost_id, @cost_item_id FROM cdc.dbo_Cost_CT",
		},
		{
			name:  "parameters with underscores",
			input: "@User_First_Name, @User_Last_Name, @created_at",
			want:  "@user_first_name, @user_last_name, @created_at",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeSQLForTest(tt.input)
			if got != tt.want {
				t.Errorf("normalizeSQLParams() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractOutputFields(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		want   []string
		wantErr bool
	}{
		{
			name: "simple columns with aliases",
			sql:  "SELECT a.order_id as order_id, a.amount as amount FROM orders a",
			want: []string{"order_id", "amount"},
		},
		{
			name: "table-prefixed columns without alias",
			sql:  "SELECT o.order_id, o.amount, o.status FROM orders o",
			want: []string{"order_id", "amount", "status"},
		},
		{
			name: "@variable tokens",
			sql:  "SELECT @cdc_lsn as cdc_lsn, @cdc_tx_id as cdc_transaction_id FROM orders",
			want: []string{"cdc_lsn", "cdc_transaction_id"},
		},
		{
			name: "mixed aliases and variables",
			sql:  "SELECT @orders_customer_id as customer_id, c.name as customer_name, c.email FROM customers c",
			want: []string{"customer_id", "customer_name", "email"},
		},
		{
			name: "complex query with joins",
			sql:  `SELECT 
				@cdc_lsn as cdc_lsn,
				@cdc_tx_id as cdc_transaction_id,
				@orders_customer_id as customer_id,
				c.name as customer_name,
				c.email as customer_email,
				c.segment as customer_segment
			  FROM customers c
			  WHERE c.customer_id = @orders_customer_id`,
			want: []string{"cdc_lsn", "cdc_transaction_id", "customer_id", "customer_name", "customer_email", "customer_segment"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractOutputFields(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractOutputFields() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !stringSliceEqual(got, tt.want) {
				t.Errorf("extractOutputFields() = %v, want %v", got, tt.want)
			}
		})
	}
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestPopulateSkillOutputs_InlineSQL(t *testing.T) {
	tests := []struct {
		name     string
		skill    *Skill
		wantOutputs map[string][]string
	}{
		{
			name: "basic inline SQL with aliases",
			skill: &Skill{
				Name: "test_skill",
				Sinks: []Sink{
					{SinkConfig: SinkConfig{
						Name:   "sink1",
						SQL:    "SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id, o.amount as amount FROM orders o",
						Output: "orders_sync",
					}, When: []string{"insert", "update"}},
				},
			},
			wantOutputs: map[string][]string{
				"orders_sync": {"cdc_lsn", "order_id", "amount"},
			},
		},
		{
			name: "table-prefixed columns without alias",
			skill: &Skill{
				Name: "test_skill",
				Sinks: []Sink{
					{SinkConfig: SinkConfig{
						Name:   "sink1",
						SQL:    "SELECT o.order_id, o.amount, o.status FROM orders o",
						Output: "orders_sync",
					}, When: []string{"insert", "update"}},
				},
			},
			wantOutputs: map[string][]string{
				"orders_sync": {"order_id", "amount", "status"},
			},
		},
		{
			name: "delete-only sink ignored",
			skill: &Skill{
				Name: "test_skill",
				Sinks: []Sink{
					{SinkConfig: SinkConfig{
						Name:   "sink_delete",
						SQL:    "SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id",
						Output: "orders_sync",
					}, When: []string{"delete"}},
				},
			},
			wantOutputs: map[string][]string{},
		},
		{
			name: "mixed insert/update and delete sinks",
			skill: &Skill{
				Name: "test_skill",
				Sinks: []Sink{
					{SinkConfig: SinkConfig{
						Name:   "sink_insert_update",
						SQL:    "SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id FROM orders",
						Output: "orders_sync",
					}, When: []string{"insert", "update"}},
					{SinkConfig: SinkConfig{
						Name:   "sink_delete",
						SQL:    "SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id",
						Output: "orders_sync",
					}, When: []string{"delete"}},
				},
			},
			wantOutputs: map[string][]string{
				"orders_sync": {"cdc_lsn", "order_id"},
			},
		},
		{
			name: "multiple sinks to same output - merged fields",
			skill: &Skill{
				Name: "test_skill",
				Sinks: []Sink{
					{SinkConfig: SinkConfig{
						Name:   "sink1",
						SQL:    "SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id FROM orders",
						Output: "orders_sync",
					}, When: []string{"insert", "update"}},
					{SinkConfig: SinkConfig{
						Name:   "sink2",
						SQL:    "SELECT @cdc_tx_id as cdc_transaction_id, @orders_amount as amount FROM orders",
						Output: "orders_sync",
					}, When: []string{"insert", "update"}},
				},
			},
			wantOutputs: map[string][]string{
				"orders_sync": {"cdc_lsn", "order_id", "cdc_transaction_id", "amount"},
			},
		},
		{
			name: "different outputs for different sinks",
			skill: &Skill{
				Name: "test_skill",
				Sinks: []Sink{
					{SinkConfig: SinkConfig{
						Name:   "customers_sync",
						SQL:    "SELECT @cdc_lsn as cdc_lsn, @orders_customer_id as customer_id, c.name as customer_name FROM customers c",
						Output: "customers_sync",
					}, When: []string{"insert", "update"}},
					{SinkConfig: SinkConfig{
						Name:   "products_sync",
						SQL:    "SELECT @cdc_lsn as cdc_lsn, @orders_product_id as product_id, p.product_name FROM products p",
						Output: "products_sync",
					}, When: []string{"insert", "update"}},
				},
			},
			wantOutputs: map[string][]string{
				"customers_sync": {"cdc_lsn", "customer_id", "customer_name"},
				"products_sync":  {"cdc_lsn", "product_id", "product_name"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			populateSkillOutputs(tt.skill)
			if tt.skill.Outputs == nil {
				t.Fatal("expected Outputs to be populated, got nil")
			}
			if len(tt.skill.Outputs) != len(tt.wantOutputs) {
				t.Errorf("expected %d outputs, got %d", len(tt.wantOutputs), len(tt.skill.Outputs))
			}
			for output, wantFields := range tt.wantOutputs {
				gotFields, ok := tt.skill.Outputs[output]
				if !ok {
					t.Errorf("expected output %q, got nil", output)
					continue
				}
				if !stringSliceEqual(gotFields, wantFields) {
					t.Errorf("output %q fields = %v, want %v", output, gotFields, wantFields)
				}
			}
		})
	}
}

func TestPopulateSkillOutputs_SQLFile(t *testing.T) {
	tmpDir := t.TempDir()

	skillFile := filepath.Join(tmpDir, "skill_with_file.yml")
	skillContent := `name: skill_with_file
description: Test skill with external sql_file
on:
  - dbo.orders
sinks:
  - name: sink1
    when: [insert, update]
    sql_file: fetch.sql
    output: out
    primary_key: id
`
	if err := os.WriteFile(skillFile, []byte(skillContent), 0644); err != nil {
		t.Fatalf("failed to write skill file: %v", err)
	}

	sqlContent := `SELECT 
		@cdc_lsn as cdc_lsn,
		@orders_order_id as order_id,
		@orders_amount as amount,
		o.status as status
	  FROM orders o
	  WHERE o.order_id = @orders_order_id`
	if err := os.WriteFile(filepath.Join(tmpDir, "fetch.sql"), []byte(sqlContent), 0644); err != nil {
		t.Fatalf("failed to write sql file: %v", err)
	}

	loader := NewLoader(tmpDir)
	skill, err := loader.Load("skill_with_file.yml")
	if err != nil {
		t.Fatalf("failed to load skill: %v", err)
	}

	expectedFields := []string{"cdc_lsn", "order_id", "amount", "status"}
	if skill.Outputs == nil {
		t.Fatal("expected Outputs to be populated")
	}
	fields, ok := skill.Outputs["out"]
	if !ok {
		t.Fatalf("expected output 'out', got nil")
	}
	if !stringSliceEqual(fields, expectedFields) {
		t.Errorf("output 'out' fields = %v, want %v", fields, expectedFields)
	}
}

func TestPopulateSkillOutputs_SyncOrdersSplit(t *testing.T) {
	// Test with sync_orders_split.yml - the integration fixture
	tmpDir := t.TempDir()

	skillFile := filepath.Join(tmpDir, "sync_orders_split.yml")
	skillContent := `name: sync_orders_split
description: "orders CDC enrich and fanout to two tables"

on:
  - orders

sinks:
  - name: customers_sync
    when: [insert, update]
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @cdc_tx_id as cdc_transaction_id,
        @cdc_table as cdc_source_table,
        @cdc_operation as cdc_operation,
        @orders_customer_id as customer_id,
        c.name as customer_name,
        c.email as customer_email,
        c.segment as customer_segment
      FROM customers c
      WHERE c.customer_id = @orders_customer_id;
    output: customers_sync
    primary_key: customer_id

  - name: customers_delete
    when: [delete]
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_customer_id as customer_id
    output: customers_sync
    primary_key: customer_id

  - name: products_sync
    when: [insert, update]
    sql: |
      SELECT 
        @cdc_lsn as cdc_lsn,
        @cdc_tx_id as cdc_transaction_id,
        @cdc_table as cdc_source_table,
        @cdc_operation as cdc_operation,
        @orders_product_id as product_id,
        p.product_name,
        p.category as product_category,
        p.price as product_price
      FROM products p
      WHERE p.product_id = @orders_product_id;
    output: products_sync
    primary_key: product_id

  - name: products_delete
    when: [delete]
    sql: |
      SELECT @cdc_lsn as cdc_lsn, @orders_product_id as product_id
    output: products_sync
    primary_key: product_id
`
	if err := os.WriteFile(skillFile, []byte(skillContent), 0644); err != nil {
		t.Fatalf("failed to write skill file: %v", err)
	}

	loader := NewLoader(tmpDir)
	skill, err := loader.Load("sync_orders_split.yml")
	if err != nil {
		t.Fatalf("failed to load skill: %v", err)
	}

	// Verify Outputs is populated
	if skill.Outputs == nil {
		t.Fatal("expected Outputs to be populated")
	}

	// Verify customers_sync output (from insert/update sink only)
	customersFields, ok := skill.Outputs["customers_sync"]
	if !ok {
		t.Fatal("expected 'customers_sync' output")
	}
	expectedCustomers := []string{"cdc_lsn", "cdc_transaction_id", "cdc_source_table", "cdc_operation", "customer_id", "customer_name", "customer_email", "customer_segment"}
	if !stringSliceEqual(customersFields, expectedCustomers) {
		t.Errorf("customers_sync fields = %v, want %v", customersFields, expectedCustomers)
	}

	// Verify products_sync output (from insert/update sink only)
	productsFields, ok := skill.Outputs["products_sync"]
	if !ok {
		t.Fatal("expected 'products_sync' output")
	}
	expectedProducts := []string{"cdc_lsn", "cdc_transaction_id", "cdc_source_table", "cdc_operation", "product_id", "product_name", "product_category", "product_price"}
	if !stringSliceEqual(productsFields, expectedProducts) {
		t.Errorf("products_sync fields = %v, want %v", productsFields, expectedProducts)
	}
}

func TestSkillOutputs_YAMLSerialization(t *testing.T) {
	// Verify that Outputs field is NOT serialized to YAML
	skill := &Skill{
		Name:        "test_skill",
		Description: "Test skill",
		Sinks: []Sink{
			{SinkConfig: SinkConfig{
				Name:   "sink1",
				SQL:    "SELECT @cdc_lsn as cdc_lsn, @orders_order_id as order_id FROM orders",
				Output: "orders_sync",
			}, When: []string{"insert", "update"}},
		},
	}

	populateSkillOutputs(skill)

	// Marshal to YAML and verify outputs is not present
	data, err := yaml.Marshal(skill)
	if err != nil {
		t.Fatalf("failed to marshal skill: %v", err)
	}

	// Check that the YAML does not contain "outputs:"
	yamlStr := string(data)
	if strings.Contains(yamlStr, "outputs:") {
		t.Error("expected Outputs field to be excluded from YAML serialization")
	}
}
