package sql

import (
	"os"
	"path/filepath"
	"testing"
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
