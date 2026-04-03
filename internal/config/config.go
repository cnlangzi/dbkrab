package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MSSQL         MSSQLConfig    `yaml:"mssql"`
	Tables        []string       `yaml:"tables"`
	Interval      string         `yaml:"polling_interval"`
	Offset        string         `yaml:"offset_file"`
	Plugin        string         `yaml:"plugin"`
	APIPort       int            `yaml:"api_port"`
	Sink          SinkConfig     `yaml:"sink"`
	CDCProtection CDCProtectionConfig `yaml:"cdc_protection"`
}

type MSSQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

type SinkConfig struct {
	Type string `yaml:"type"`
	Path string `yaml:"path"`
}

// CDCProtectionConfig contains CDC gap protection settings
type CDCProtectionConfig struct {
	Enabled           bool          `yaml:"enabled"`
	CheckInterval     string        `yaml:"check_interval"`
	WarningLagBytes   int64         `yaml:"warning_lag_bytes"`
	CriticalLagBytes  int64         `yaml:"critical_lag_bytes"`
	WarningLagDuration string       `yaml:"warning_lag_duration"`
	CriticalLagDuration string      `yaml:"critical_lag_duration"`
	Recovery          RecoveryConfig `yaml:"recovery"`
	Alert             AlertConfig   `yaml:"alert"`
}

// RecoveryConfig contains recovery strategy settings
type RecoveryConfig struct {
	Strategy string `yaml:"strategy"` // snapshot | timestamp | manual
}

// AlertConfig contains alert notification settings
type AlertConfig struct {
	Enabled  bool            `yaml:"enabled"`
	WebhookURL string        `yaml:"webhook_url,omitempty"`
	Channels []AlertChannel `yaml:"channels"`
}

// AlertChannel represents a notification channel
type AlertChannel struct {
	Type       string   `yaml:"type"` // webhook, feishu, email
	URL        string   `yaml:"url,omitempty"`
	Recipients []string `yaml:"recipients,omitempty"`
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
	if cfg.Interval == "" {
		cfg.Interval = "500ms"
	}
	if cfg.Offset == "" {
		cfg.Offset = "./data/offset.json"
	}
	if cfg.Sink.Type == "" {
		cfg.Sink.Type = "sqlite"
	}
	if cfg.Sink.Path == "" {
		cfg.Sink.Path = "./data/cdc.db"
	}
	if cfg.APIPort == 0 {
		cfg.APIPort = 9020
	}

	// CDC protection defaults
	if cfg.CDCProtection.CheckInterval == "" {
		cfg.CDCProtection.CheckInterval = "1m"
	}
	if cfg.CDCProtection.WarningLagBytes == 0 {
		cfg.CDCProtection.WarningLagBytes = 100 * 1024 * 1024 // 100MB
	}
	if cfg.CDCProtection.CriticalLagBytes == 0 {
		cfg.CDCProtection.CriticalLagBytes = 1024 * 1024 * 1024 // 1GB
	}
	if cfg.CDCProtection.WarningLagDuration == "" {
		cfg.CDCProtection.WarningLagDuration = "1h"
	}
	if cfg.CDCProtection.CriticalLagDuration == "" {
		cfg.CDCProtection.CriticalLagDuration = "6h"
	}
	if cfg.CDCProtection.Recovery.Strategy == "" {
		cfg.CDCProtection.Recovery.Strategy = "manual" // Default to manual intervention
	}

	return &cfg, nil
}

func (c *Config) PollingInterval() (time.Duration, error) {
	return time.ParseDuration(c.Interval)
}

// CDCCheckInterval returns the CDC gap check interval
func (c *Config) CDCCheckInterval() (time.Duration, error) {
	if c.CDCProtection.CheckInterval == "" {
		return 1 * time.Minute, nil
	}
	return time.ParseDuration(c.CDCProtection.CheckInterval)
}

// WarningLagDuration returns the warning lag duration threshold
func (c *Config) WarningLagDuration() (time.Duration, error) {
	if c.CDCProtection.WarningLagDuration == "" {
		return 1 * time.Hour, nil
	}
	return time.ParseDuration(c.CDCProtection.WarningLagDuration)
}

// CriticalLagDuration returns the critical lag duration threshold
func (c *Config) CriticalLagDuration() (time.Duration, error) {
	if c.CDCProtection.CriticalLagDuration == "" {
		return 6 * time.Hour, nil
	}
	return time.ParseDuration(c.CDCProtection.CriticalLagDuration)
}