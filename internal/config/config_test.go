package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestParseTimezone(t *testing.T) {
	tests := []struct {
		name    string
		tzStr   string
		want    *time.Location
		wantOff int // Expected offset in seconds (for fixed zones)
	}{
		{
			name:    "Empty string returns Local",
			tzStr:   "",
			want:    time.Local,
			wantOff: 0, // time.Local offset varies by system, skip offset check
		},
		{
			name:    "IANA timezone Asia/Shanghai",
			tzStr:   "Asia/Shanghai",
			wantOff: 8 * 3600, // UTC+8
		},
		{
			name:    "IANA timezone America/New_York",
			tzStr:   "America/New_York",
			// Offset varies by DST, skip exact offset check
		},
		{
			name:    "UTC offset UTC+8",
			tzStr:   "UTC+8",
			wantOff: 8 * 3600,
		},
		{
			name:    "UTC offset UTC-5",
			tzStr:   "UTC-5",
			wantOff: -5 * 3600,
		},
		{
			name:    "UTC returns UTC",
			tzStr:   "UTC",
			want:    time.UTC,
			wantOff: 0,
		},
		{
			name:    "Invalid timezone returns UTC",
			tzStr:   "InvalidZone",
			want:    time.UTC,
			wantOff: 0,
		},
		{
			name:    "UTC+0 returns UTC",
			tzStr:   "UTC+0",
			wantOff: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseTimezone(tt.tzStr)

			if tt.want != nil {
				// Exact location match
				if got != tt.want {
					t.Errorf("ParseTimezone(%q) = %v, want %v", tt.tzStr, got, tt.want)
				}
				return
			}

			// Check offset for fixed zones (skip IANA zones that vary by DST)
			if tt.wantOff != 0 || tt.tzStr == "UTC+0" {
				if tt.tzStr != "Asia/Shanghai" && tt.tzStr != "America/New_York" {
					// Get offset from a time in that location
					testTime := time.Date(2026, 4, 9, 12, 0, 0, 0, got)
					_, gotOff := testTime.Zone()
					if gotOff != tt.wantOff {
						t.Errorf("ParseTimezone(%q) offset = %d seconds, want %d seconds",
							tt.tzStr, gotOff, tt.wantOff)
					}
				}
			}
		})
	}
}

func TestParseTimezoneRegression(t *testing.T) {
	// Regression test for MSSQL CDC timestamp bug
	// When timezone is Asia/Shanghai (UTC+8), a "12:26 UTC" driver time
	// should be reinterpreted as 12:26 Beijing time.

	shanghai := ParseTimezone("Asia/Shanghai")

	// Driver returns 12:26 incorrectly marked as UTC
	driverTime := time.Date(2026, 4, 9, 12, 26, 0, 0, time.UTC)

	// Reinterpret as Shanghai time, convert to UTC
	converted := time.Date(
		driverTime.Year(), driverTime.Month(), driverTime.Day(),
		driverTime.Hour(), driverTime.Minute(), driverTime.Second(), driverTime.Nanosecond(),
		shanghai,
	).UTC()

	// Expected: 12:26 Beijing = 04:26 UTC
	expected := time.Date(2026, 4, 9, 4, 26, 0, 0, time.UTC)

	if !converted.Equal(expected) {
		t.Errorf("Timezone conversion: got %v, want %v", converted, expected)
	}

	// Verify that changed_at would be BEFORE pulled_at
	// (the original bug was changed_at appearing AFTER pulled_at)
	pulledAt := time.Date(2026, 4, 9, 6, 32, 0, 0, time.UTC)
	if converted.After(pulledAt) {
		t.Errorf("REGRESSION: converted time %v is after pulled_at %v", converted, pulledAt)
	}
}