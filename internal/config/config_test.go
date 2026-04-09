package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	// Create temp config file
	tmpDir, err := os.MkdirTemp("", "dbkrab-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("os.RemoveAll error: %v", err)
		}
	}()

	configContent := `
mssql:
  host: localhost
  port: 1433
  user: sa
  password: test123
  database: testdb

tables:
  - dbo.orders
  - dbo.users

cdc:
  interval: 1s
  offset:
    type: json
    json_path: ./data/test-offset.json

app:
  type: sqlite
  path: ./data/test-cdc.db
`
	configPath := filepath.Join(tmpDir, "config.yml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Check MSSQL config
	if cfg.MSSQL.Host != "localhost" {
		t.Errorf("MSSQL.Host = %v, want localhost", cfg.MSSQL.Host)
	}
	if cfg.MSSQL.Port != 1433 {
		t.Errorf("MSSQL.Port = %v, want 1433", cfg.MSSQL.Port)
	}
	if cfg.MSSQL.User != "sa" {
		t.Errorf("MSSQL.User = %v, want sa", cfg.MSSQL.User)
	}
	if cfg.MSSQL.Database != "testdb" {
		t.Errorf("MSSQL.Database = %v, want testdb", cfg.MSSQL.Database)
	}

	// Check tables
	if len(cfg.Tables) != 2 {
		t.Errorf("Tables length = %v, want 2", len(cfg.Tables))
	}
	if cfg.Tables[0] != "dbo.orders" {
		t.Errorf("Tables[0] = %v, want dbo.orders", cfg.Tables[0])
	}

	// Check defaults
	if cfg.CDC.Interval != "1s" {
		t.Errorf("Interval = %v, want 1s", cfg.CDC.Interval)
	}

	// Check sink
	if cfg.App.Type != "sqlite" {
		t.Errorf("App.Type = %v, want sqlite", cfg.App.Type)
	}
}

func TestDefaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbkrab-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("os.RemoveAll error: %v", err)
		}
	}()

	// Minimal config
	configContent := `
mssql:
  host: localhost
  user: sa
  password: test
  database: test
tables:
  - dbo.test
`
	configPath := filepath.Join(tmpDir, "config.yml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Check defaults
	if cfg.CDC.Interval != "500ms" {
		t.Errorf("Interval default = %v, want 500ms", cfg.CDC.Interval)
	}
	if cfg.CDC.Offset.Type != "json" {
		t.Errorf("Offset.Type default = %v, want json", cfg.CDC.Offset.Type)
	}
	if cfg.CDC.Offset.JSONPath != "./data/offset.json" {
		t.Errorf("Offset.JSONPath default = %v, want ./data/offset.json", cfg.CDC.Offset.JSONPath)
	}
	if cfg.App.Type != "sqlite" {
		t.Errorf("App.Type default = %v, want sqlite", cfg.App.Type)
	}
}

func TestInterval(t *testing.T) {
	cfg := &Config{CDC: CDCConfig{Interval: "1s"}}
	d, err := cfg.Interval()
	if err != nil {
		t.Fatalf("Interval() error = %v", err)
	}
	if d.Seconds() != 1 {
		t.Errorf("Interval() = %v, want 1s", d)
	}

	// Invalid duration
	cfg = &Config{CDC: CDCConfig{Interval: "invalid"}}
	_, err = cfg.Interval()
	if err == nil {
		t.Error("Expected error for invalid duration")
	}
}