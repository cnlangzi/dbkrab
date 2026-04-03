package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MSSQL    MSSQLConfig `yaml:"mssql"`
	Tables   []string    `yaml:"tables"`
	Interval string      `yaml:"polling_interval"`
	Offset   string      `yaml:"offset_file"`
	Plugin   string      `yaml:"plugin"`
	Sink     SinkConfig  `yaml:"sink"`
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

	return &cfg, nil
}

func (c *Config) PollingInterval() (time.Duration, error) {
	return time.ParseDuration(c.Interval)
}