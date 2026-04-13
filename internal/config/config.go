package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/alert"
	"github.com/cnlangzi/dbkrab/internal/logging"
	"gopkg.in/yaml.v3"
)

type Config struct {
	MSSQL              MSSQLConfig            `yaml:"mssql"`
	Tables             []string               `yaml:"tables"`

	CDC                CDCConfig              `yaml:"cdc"`

	Listen          int                    `yaml:"listen"`
	App                AppConfig              `yaml:"app"`
	Sinks              SinksConfig            `yaml:"sinks"`
	GracefulDegradation GracefulDegradationConfig `yaml:"graceful_degradation"`
	Plugins            PluginsConfig          `yaml:"plugins"`
	Logging            logging.LoggingConfig  `yaml:"logging"`
}

// CDCConfig aggregates all CDC-related configuration
type CDCConfig struct {
	Interval        string                 `yaml:"interval"`
	Offset          OffsetConfig           `yaml:"offset"`
	Gap            CDCProtectionConfig    `yaml:"gap"`
	TransactionBuffer TransactionBufferConfig `yaml:"transaction_buffer"`
}

// PluginsConfig contains plugin configuration
type PluginsConfig struct {
	SQL PluginConfig `yaml:"sql"`
}

// PluginConfig contains plugin configuration for SQL plugins
type PluginConfig struct {
	Enabled *bool  `yaml:"enabled"` // true/on/1=enable, otherwise disabled
	Path    string `yaml:"path"`
}

// DatabaseConfig contains configuration for a named database
type DatabaseConfig struct {
	Id              string `yaml:"-"`                // Auto-generated 12-char hash ID (not in YAML)
	Name            string `yaml:"name"`             // Sink name (used as key in array format)
	Description    string `yaml:"description"`     // Human-readable description
	Type            string `yaml:"type"`            // sqlite, duckdb, mssql, etc.
	Path            string `yaml:"path"`             // Path for file-based databases
	Migrations  string `yaml:"migrations"`      // Path to migration SQL files
	ConnectionString string `yaml:"connection_string"` // Connection string for network databases
}

// IsEnabled returns true only if val is explicitly set to true, "on", or "1"
// All other values (nil, false, "off", "0", etc.) return false
func IsEnabled(val *bool) bool {
	if val == nil {
		return false
	}
	return *val
}

type MSSQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Timezone string `yaml:"timezone"` // SQL Server timezone (e.g., "Asia/Shanghai", "UTC+8") for CDC timestamp conversion
}

// GracefulDegradationConfig contains graceful degradation settings for MSSQL disconnection
type GracefulDegradationConfig struct {
	Enabled             bool   `yaml:"enabled"`
	MaxDisconnectDuration string `yaml:"max_disconnect_duration"` // e.g., "30m"`
	ReconnectBaseDelay  string `yaml:"reconnect_base_delay"`     // e.g., "5s"`
	ReconnectMaxDelay   string `yaml:"reconnect_max_delay"`      // e.g., "60s"`
}

// AppConfig contains the app-level database storage configuration
type AppConfig struct {
	Listen int    `yaml:"listen"`
	Host   string `yaml:"host"`
	Type   string `yaml:"type"`
	DB     string `yaml:"db"`  // Path for store/offset DB (transactions, poller_state, offsets)
	DLQ    string `yaml:"dlq"` // Path for DLQ DB (dlq_entries)
}

// SinksConfig is a slice of DatabaseConfig for business sinks.
// It unmarshals directly from a YAML array.
type SinksConfig []DatabaseConfig

// ToMap converts to a map keyed by Name (for plugin/API compatibility).
func (s SinksConfig) ToMap() map[string]DatabaseConfig {
	m := make(map[string]DatabaseConfig, len(s))
	for _, db := range s {
		m[db.Name] = db
	}
	return m
}

// BasePath returns the parent directory of the first sink's path.
// Used internally by the API server for file serving.
func (s SinksConfig) BasePath() string {
	if len(s) == 0 || s[0].Path == "" {
		return "./data/sinks"
	}
	return filepath.Dir(s[0].Path)
}

// OffsetConfig contains offset storage configuration
type OffsetConfig struct {
	Type       string `yaml:"type"`         // json or sqlite
	JSONPath   string `yaml:"json_path"`    // path to JSON file (for json type)
	SQLitePath string `yaml:"sqlite_path"`  // path to SQLite file (for sqlite type)
}

// CDCProtectionConfig contains CDC gap protection settings
type CDCProtectionConfig struct {
	Enabled           bool                 `yaml:"enabled"`
	CheckInterval     string               `yaml:"check_interval"`
	WarningLagBytes   int64                `yaml:"warning_lag_bytes"`
	CriticalLagBytes  int64                `yaml:"critical_lag_bytes"`
	WarningLagDuration string              `yaml:"warning_lag_duration"`
	CriticalLagDuration string             `yaml:"critical_lag_duration"`
	Recovery          RecoveryConfig       `yaml:"recovery"`
	Alert             alert.AlertConfig    `yaml:"alert"`
}

// TransactionBufferConfig contains transaction buffer settings (DEPRECATED - not used)
type TransactionBufferConfig struct {
	Enabled              bool   `yaml:"enabled"` // Deprecated: not used
	MaxWaitTime          string `yaml:"max_wait_time"`          // Deprecated: not used
	MaxTransactionsPerBatch int   `yaml:"max_transactions_per_batch"` // Deprecated: not used
	MaxBatchBytes        int    `yaml:"max_batch_bytes"`         // Deprecated: not used
}

// RecoveryConfig contains recovery strategy settings
type RecoveryConfig struct {
	Strategy string `yaml:"strategy"` // snapshot | timestamp | manual
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Set defaults
	if cfg.CDC.Interval == "" {
		cfg.CDC.Interval = "500ms"
	}
	if cfg.CDC.Offset.Type == "" {
		cfg.CDC.Offset.Type = "json"
	}
	if cfg.CDC.Offset.JSONPath == "" {
		cfg.CDC.Offset.JSONPath = "./data/offset.json"
	}
	if cfg.CDC.Offset.SQLitePath == "" {
		cfg.CDC.Offset.SQLitePath = "./data/offset.db"
	}
	if cfg.App.Type == "" {
		cfg.App.Type = "sqlite"
	}
	if cfg.App.DB == "" {
		cfg.App.DB = "./data/app/dbkrab.db"
	}
	if cfg.App.DLQ == "" {
		cfg.App.DLQ = "./data/app/dlq.db"
	}
	if cfg.App.Listen == 0 {
		cfg.App.Listen = 9020
	}

	// CDC protection defaults
	if cfg.CDC.Gap.CheckInterval == "" {
		cfg.CDC.Gap.CheckInterval = "1m"
	}
	if cfg.CDC.Gap.WarningLagBytes == 0 {
		cfg.CDC.Gap.WarningLagBytes = 100 * 1024 * 1024 // 100MB
	}
	if cfg.CDC.Gap.CriticalLagBytes == 0 {
		cfg.CDC.Gap.CriticalLagBytes = 1024 * 1024 * 1024 // 1GB
	}
	if cfg.CDC.Gap.WarningLagDuration == "" {
		cfg.CDC.Gap.WarningLagDuration = "1h"
	}
	if cfg.CDC.Gap.CriticalLagDuration == "" {
		cfg.CDC.Gap.CriticalLagDuration = "6h"
	}
	if cfg.CDC.Gap.Recovery.Strategy == "" {
		cfg.CDC.Gap.Recovery.Strategy = "manual" // Default to manual intervention
	}

	// Transaction buffer defaults
	if cfg.CDC.TransactionBuffer.MaxWaitTime == "" {
		cfg.CDC.TransactionBuffer.MaxWaitTime = "30s"
	}
	if cfg.CDC.TransactionBuffer.MaxTransactionsPerBatch == 0 {
		cfg.CDC.TransactionBuffer.MaxTransactionsPerBatch = 1000
	}
	if cfg.CDC.TransactionBuffer.MaxBatchBytes == 0 {
		cfg.CDC.TransactionBuffer.MaxBatchBytes = 10 * 1024 * 1024 // 10MB
	}

	// Plugin defaults
	if cfg.Plugins.SQL.Path == "" {
		cfg.Plugins.SQL.Path = "./skills/sql"
	}

	// Graceful degradation defaults
	if cfg.GracefulDegradation.Enabled {
		if cfg.GracefulDegradation.MaxDisconnectDuration == "" {
			cfg.GracefulDegradation.MaxDisconnectDuration = "30m"
		}
		if cfg.GracefulDegradation.ReconnectBaseDelay == "" {
			cfg.GracefulDegradation.ReconnectBaseDelay = "5s"
		}
		if cfg.GracefulDegradation.ReconnectMaxDelay == "" {
			cfg.GracefulDegradation.ReconnectMaxDelay = "60s"
		}
	}

	// Logging defaults
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Logging.File.Path == "" {
		cfg.Logging.File.Path = "./logs/dbkrab.log"
	}
	if cfg.Logging.File.MaxSizeMB == 0 {
		cfg.Logging.File.MaxSizeMB = 100
	}
	if cfg.Logging.File.MaxAgeDays == 0 {
		cfg.Logging.File.MaxAgeDays = 7
	}
	if cfg.Logging.File.MaxFiles == 0 {
		cfg.Logging.File.MaxFiles = 4
	}
	// Console enabled defaults to true if not specified
	// File enabled defaults to true if not specified
	if !cfg.Logging.Console.Enabled && !cfg.Logging.File.Enabled {
		cfg.Logging.Console.Enabled = true
		cfg.Logging.File.Enabled = true
	}

	// Generate sink IDs (12-char hash based on name+path)
	for i := range cfg.Sinks {
		if cfg.Sinks[i].Id == "" {
			cfg.Sinks[i].Id = generateSinkId(cfg.Sinks[i].Name, cfg.Sinks[i].Path)
		}
	}

	return &cfg, nil
}

// generateSinkId generates a 12-char hash ID from name+path
func generateSinkId(name, path string) string {
	h := sha256.Sum256([]byte(name + ":" + path))
	return hex.EncodeToString(h[:])[:12]
}

func (c *Config) Interval() (time.Duration, error) {
	return time.ParseDuration(c.CDC.Interval)
}

// CDCCheckInterval returns the CDC gap check interval
func (c *Config) CDCCheckInterval() (time.Duration, error) {
	if c.CDC.Gap.CheckInterval == "" {
		return 1 * time.Minute, nil
	}
	return time.ParseDuration(c.CDC.Gap.CheckInterval)
}

// WarningLagDuration returns the warning lag duration threshold
func (c *Config) WarningLagDuration() (time.Duration, error) {
	if c.CDC.Gap.WarningLagDuration == "" {
		return 1 * time.Hour, nil
	}
	return time.ParseDuration(c.CDC.Gap.WarningLagDuration)
}

// CriticalLagDuration returns the critical lag duration threshold
func (c *Config) CriticalLagDuration() (time.Duration, error) {
	if c.CDC.Gap.CriticalLagDuration == "" {
		return 6 * time.Hour, nil
	}
	return time.ParseDuration(c.CDC.Gap.CriticalLagDuration)
}

// Save writes the config to a YAML file
func Save(path string, cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}

	return nil
}

// MaxDisconnectDuration returns the maximum disconnect duration before alerting.
// It handles misconfiguration by falling back to a safe default.
func (c *Config) MaxDisconnectDuration() time.Duration {
	const defaultMaxDisconnectDuration = 30 * time.Minute

	if c.GracefulDegradation.MaxDisconnectDuration == "" {
		return defaultMaxDisconnectDuration
	}

	d, err := time.ParseDuration(c.GracefulDegradation.MaxDisconnectDuration)
	if err != nil || d <= 0 {
		// Malformed or non-positive durations are treated as misconfiguration,
		// so we fall back to the default instead of returning a zero duration.
		return defaultMaxDisconnectDuration
	}

	return d
}

// ReconnectBaseDelay returns the base delay for reconnection attempts.
// It handles misconfiguration by falling back to a safe default.
func (c *Config) ReconnectBaseDelay() time.Duration {
	const defaultReconnectBaseDelay = 5 * time.Second

	if c.GracefulDegradation.ReconnectBaseDelay == "" {
		return defaultReconnectBaseDelay
	}

	d, err := time.ParseDuration(c.GracefulDegradation.ReconnectBaseDelay)
	if err != nil || d <= 0 {
		// Malformed or non-positive durations are treated as misconfiguration,
		// so we fall back to the default instead of returning a zero duration.
		return defaultReconnectBaseDelay
	}

	return d
}

// ReconnectMaxDelay returns the maximum delay for reconnection attempts.
// It handles misconfiguration by falling back to a safe default.
func (c *Config) ReconnectMaxDelay() time.Duration {
	const defaultReconnectMaxDelay = 60 * time.Second

	if c.GracefulDegradation.ReconnectMaxDelay == "" {
		return defaultReconnectMaxDelay
	}

	d, err := time.ParseDuration(c.GracefulDegradation.ReconnectMaxDelay)
	if err != nil || d <= 0 {
		// Malformed or non-positive durations are treated as misconfiguration,
		// so we fall back to the default instead of returning a zero duration.
		return defaultReconnectMaxDelay
	}

	return d
}

// ParseTimezone parses timezone string to time.Location
// Supports IANA timezone names (e.g., "Asia/Shanghai") and UTC offsets (e.g., "UTC+8", "UTC-5")
// Returns time.Local if string is empty, time.UTC if invalid
func ParseTimezone(tzStr string) *time.Location {
	if tzStr == "" {
		return time.Local // Default to local time if not specified
	}

	// Try IANA timezone name first
	if loc, err := time.LoadLocation(tzStr); err == nil {
		return loc
	}

	// Try UTC offset format (UTC+8, UTC-5, etc.)
	if strings.HasPrefix(tzStr, "UTC") {
		offsetStr := strings.TrimPrefix(tzStr, "UTC")
		if offsetStr == "" {
			return time.UTC
		}

		// Parse offset as hours
		var hours = 0
		var sign = 1
		if strings.HasPrefix(offsetStr, "+") {
			offsetStr = strings.TrimPrefix(offsetStr, "+")
		} else if strings.HasPrefix(offsetStr, "-") {
			offsetStr = strings.TrimPrefix(offsetStr, "-")
			sign = -1
		}

		if _, err := fmt.Sscanf(offsetStr, "%d", &hours); err == nil {
			offsetSeconds := sign * hours * 3600
			return time.FixedZone(tzStr, offsetSeconds)
		}
	}

	// Fallback to UTC
	slog.Warn("invalid timezone format, using UTC", "timezone", tzStr)
	return time.UTC
}