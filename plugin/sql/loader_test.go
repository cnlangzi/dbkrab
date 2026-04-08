package sql

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSkill(t *testing.T) {
	tmpDir := t.TempDir()
	// Create plugin directory structure: pluginsDir/plugin_name/skill.yml
	skillDir := filepath.Join(tmpDir, "test_load")
	err := os.MkdirAll(skillDir, 0755)
	if err != nil {
		t.Fatalf("failed to create skill directory: %v", err)
	}
	skillFile := filepath.Join(skillDir, "skill.yml")
	skillContent := `name: test_load
description: Test skill loading
on:
  - dbo.orders
  - dbo.customers
sinks:
  insert:
    - name: orders_enriched
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
	skill, loadErr := loader.Load("test_load")
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
		"skill1": "name: skill1\ndescription: First skill\non:\n  - dbo.table1\nsinks:\n  insert: []",
		"skill2": "name: skill2\ndescription: Second skill\non:\n  - dbo.table2\nsinks:\n  insert: []",
	}

	for name, content := range skills {
		skillDir := filepath.Join(tmpDir, name)
		mkErr := os.MkdirAll(skillDir, 0755)
		if mkErr != nil {
			t.Fatalf("failed to create skill directory: %v", mkErr)
		}
		writeErr := os.WriteFile(filepath.Join(skillDir, "skill.yml"), []byte(content), 0644)
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

func TestLoadNonExistentSkill(t *testing.T) {
	tmpDir := t.TempDir()
	loader := NewLoader(tmpDir)

	_, err := loader.Load("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent skill")
	}
}

func TestValidateRequiredFields(t *testing.T) {
	loader := NewLoader("")

	// Test 1: valid skill
	skill1 := Skill{
		Name: "test",
		On:   []string{"dbo.table1"},
		Sinks: SinksConfig{
			Insert: []SinkConfig{{Name: "sink", On: "dbo.table1", Output: "out", PrimaryKey: "id"}},
		},
	}
	err := loader.validate(&skill1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Test 2: missing name
	skill2 := Skill{
		On: []string{"dbo.table1"},
		Sinks: SinksConfig{Insert: []SinkConfig{{Name: "sink"}}},
	}
	err = loader.validate(&skill2)
	if err == nil {
		t.Error("expected error for missing name")
	}

	// Test 3: missing tables
	skill3 := Skill{
		Name: "test",
		Sinks: SinksConfig{Insert: []SinkConfig{{Name: "sink"}}},
	}
	err = loader.validate(&skill3)
	if err == nil {
		t.Error("expected error for missing tables")
	}
}