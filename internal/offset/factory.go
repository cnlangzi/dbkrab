package offset

import (
	"fmt"
	"strings"
)

// StoreType represents the type of offset storage
type StoreType string

const (
	// StoreTypeJSON stores offsets in a JSON file
	StoreTypeJSON StoreType = "json"
	// StoreTypeSQLite stores offsets in SQLite database
	StoreTypeSQLite StoreType = "sqlite"
)

// Config contains configuration for offset store
type Config struct {
	Type       string `yaml:"type"`
	JSONPath   string `yaml:"json_path,omitempty"`
	SQLitePath string `yaml:"sqlite_path,omitempty"`
}

// DefaultConfig returns a default configuration (JSON backend)
func DefaultConfig(path string) Config {
	return Config{
		Type:     "json",
		JSONPath: path,
	}
}

// NewStore creates an offset store based on type and paths
func NewStoreFromConfig(storeType, jsonPath, sqlitePath string) (StoreInterface, error) {
	switch ParseStoreType(storeType) {
	case StoreTypeJSON, "":
		if jsonPath == "" {
			return nil, fmt.Errorf("json_path is required for JSON store")
		}
		return NewStore(jsonPath), nil

	case StoreTypeSQLite:
		if sqlitePath == "" {
			return nil, fmt.Errorf("sqlite_path is required for SQLite store")
		}
		return NewSQLiteStore(sqlitePath)

	default:
		return nil, fmt.Errorf("unknown store type: %s", storeType)
	}
}

// ParseStoreType parses a string into StoreType
func ParseStoreType(s string) StoreType {
	switch strings.ToLower(s) {
	case "sqlite", "sqlite3":
		return StoreTypeSQLite
	case "json", "file", "":
		return StoreTypeJSON
	default:
		return StoreTypeJSON
	}
}
