package sql

import (
	"fmt"
	"strings"
	"time"
)

// Operation represents the CDC operation type
type Operation int

const (
	Insert Operation = 1 + iota
	Update
	Delete
)

// OnConflictStrategy defines how to handle conflicts on insert/update
type OnConflictStrategy int

const (
	// ConflictOverwrite replaces existing record (INSERT OR REPLACE, direct UPDATE)
	ConflictOverwrite OnConflictStrategy = iota
	// ConflictSkip ignores the operation if record exists (INSERT OR IGNORE, conditional UPDATE)
	ConflictSkip
	// ConflictError returns an error if record exists (INSERT with check, conditional UPDATE)
	ConflictError
)

// String returns the strategy name
func (s OnConflictStrategy) String() string {
	switch s {
	case ConflictOverwrite:
		return "overwrite"
	case ConflictSkip:
		return "skip"
	case ConflictError:
		return "error"
	default:
		return "skip"
	}
}

// ParseOnConflictStrategy parses string to OnConflictStrategy
func ParseOnConflictStrategy(s string) OnConflictStrategy {
	switch strings.ToLower(s) {
	case "overwrite":
		return ConflictOverwrite
	case "error":
		return ConflictError
	default:
		return ConflictSkip
	}
}

// String returns the operation name
func (o Operation) String() string {
	switch o {
	case Delete:
		return "Delete"
	case Insert:
		return "Insert"
	case Update:
		return "Update"
	default:
		return "Unknown"
	}
}

// Skill represents a SQL plugin configuration
type Skill struct {
	Name        string      `yaml:"name"`
	Description string      `yaml:"description"`
	On          []string    `yaml:"on"`            // Tables to monitor
	Database    string      `yaml:"database"`      // Target database name (maps to platform-configured storage)
	Sinks       []Sink      `yaml:"sinks"`

	File string `yaml:"-"` // Auto-assigned: relative path from config.plugins.sql.path
	Id   string `yaml:"-"` // Auto-assigned: SHA256(File)[:12]
	Raw  string `yaml:"-"` // Auto-assigned: raw YAML content
}

// Sink represents a single sink configuration with operation filter
type Sink struct {
	SinkConfig `yaml:",inline"` // Embedded SinkConfig for backward compatibility
	When       []string           `yaml:"when"` // Required: [insert, update] or [delete]
}

// SinkConfig represents a single job configuration
type SinkConfig struct {
	Name        string `yaml:"name"`          // Sink name
	On          string `yaml:"on"`            // Table filter (required for multi-table)
	SQL         string `yaml:"sql"`           // Inline SQL template
	SQLFile     string `yaml:"sql_file"`      // External SQL file path
	Output      string `yaml:"output"`        // Target table name
	PrimaryKey  string `yaml:"primary_key"`   // Primary key column
	OnConflict  string `yaml:"on_conflict"`   // Conflict strategy: overwrite | skip | error (default: skip)
}

// GetOnConflict returns the OnConflictStrategy for this job
func (s *SinkConfig) GetOnConflict() OnConflictStrategy {
	return ParseOnConflictStrategy(s.OnConflict)
}

// CDCParameters represents CDC data mapped to SQL parameters
type CDCParameters struct {
	CDCLSN       string
	CDCTxID      string
	CDCTable     string
	CDCOperation int
	Fields       map[string]interface{}
}

// DataSet represents query results
type DataSet struct {
	Columns []string
	Rows    [][]interface{}
}

// OperationToSinkType converts Operation to sink type string
func OperationToSinkType(op Operation) string {
	switch op {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	default:
		return "insert"
	}
}

// FilterByOperation returns sinks that handle the given operation type.
// The 'when' field must contain the operation string ("insert", "update", or "delete").
func (s *Skill) FilterByOperation(opType Operation) []SinkConfig {
	var opStr string
	switch opType {
	case Insert:
		opStr = "insert"
	case Update:
		opStr = "update"
	case Delete:
		opStr = "delete"
	default:
		return nil
	}

	var result []SinkConfig
	for _, sink := range s.Sinks {
		for _, when := range sink.When {
			if when == opStr {
				result = append(result, sink.SinkConfig)
				break
			}
		}
	}
	return result
}

// ValidateSinks validates the sinks configuration and returns an error if invalid.
// It ensures:
//   - 'when' field is required for all sinks
//   - Only [insert, update] or [delete] are valid 'when' values
//   - No mixing of insert/update with delete in the same sink
func (s *Skill) ValidateSinks() error {
	for i, sink := range s.Sinks {
		if len(sink.When) == 0 {
			return fmt.Errorf("sink[%d].when is required", i)
		}

		// Check valid combinations: [insert, update] or [delete]
		hasInsert := false
		hasUpdate := false
		hasDelete := false

		for _, w := range sink.When {
			switch w {
			case "insert":
				hasInsert = true
			case "update":
				hasUpdate = true
			case "delete":
				hasDelete = true
			default:
				return fmt.Errorf("sink[%d].when: invalid value '%s', must be 'insert', 'update', or 'delete'", i, w)
			}
		}

		// Validate: either (insert+update) or (delete only), never mixed
		if hasDelete && (hasInsert || hasUpdate) {
			return fmt.Errorf("sink[%d].when: cannot mix delete with insert/update", i)
		}
		if !hasDelete && (!hasInsert && !hasUpdate) {
			return fmt.Errorf("sink[%d].when: must be either [insert, update] or [delete]", i)
		}
		if hasInsert && !hasUpdate {
			return fmt.Errorf("sink[%d].when: insert requires update (use [insert, update] for shared SQL)", i)
		}
		if hasUpdate && !hasInsert {
			return fmt.Errorf("sink[%d].when: update requires insert (use [insert, update] for shared SQL)", i)
		}
	}
	return nil
}

// FilterSinks filters sinks by table name (for multi-table CDC)
func FilterSinks(sinks []SinkConfig, tableName string) []SinkConfig {
	if tableName == "" {
		return sinks
	}
	tableNameLower := strings.ToLower(tableName)
	var filtered []SinkConfig
	for _, sink := range sinks {
		if sink.On == "" || strings.ToLower(sink.On) == tableNameLower {
			filtered = append(filtered, sink)
		}
	}
	return filtered
}

// DataSetToMap converts DataSet to slice of maps for easier processing
func DataSetToMap(ds *DataSet) []map[string]interface{} {
	result := make([]map[string]interface{}, len(ds.Rows))
	for i, row := range ds.Rows {
		m := make(map[string]interface{})
		for j, col := range ds.Columns {
			if j < len(row) {
				m[col] = row[j]
			}
		}
		result[i] = m
	}
	return result
}

// String implements fmt.Stringer for DataSet
func (ds *DataSet) String() string {
	if ds == nil {
		return "nil"
	}
	return fmt.Sprintf("DataSet{Columns: %v, Rows: %d}", ds.Columns, len(ds.Rows))
}

// PluginMetadata contains metadata and file tracking information for a SQL plugin
type PluginMetadata struct {
	Name        string              `json:"name"`
	Type        string              `json:"type"`         // "sql"
	Status      string              `json:"status"`       // loaded | stale | error
	NeedsReload bool                `json:"needs_reload"`
	LoadCount   int                 `json:"load_count"`
	LastError   string              `json:"last_error,omitempty"`
	Files       map[string]FileMetadata `json:"files"`
}

// FileMetadata tracks the state of a single file (.yml or .sql)
type FileMetadata struct {
	Path        string    `json:"path"`
	IsSQL       bool      `json:"is_sql"`
	CurVersion  string    `json:"cur_version"`        // SHA256 hash of current loaded content
	CurModTime  time.Time `json:"cur_mod_time"`
	CurSize     int64     `json:"cur_size"`
	CurLoadedAt time.Time `json:"cur_loaded_at"`
	NewVersion  string    `json:"new_version"`        // SHA256 hash of new content (if changed)
	NewModTime  time.Time `json:"new_mod_time"`
	NewSize     int64     `json:"new_size"`
	NeedsReload bool      `json:"needs_reload"`
	IsDeleted   bool      `json:"is_deleted"`
}


