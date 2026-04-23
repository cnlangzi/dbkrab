package sql

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Knetic/govaluate"
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
	Name        string              `yaml:"name"`
	Description string              `yaml:"description"`
	On          []string            `yaml:"on"`       // Tables to monitor
	Database    string              `yaml:"database"` // Target database name (maps to platform-configured storage)
	Sinks       []Sink              `yaml:"sinks"`
	Outputs     map[string][]string `yaml:"-"` // key = output table name, value = field names

	File         string    `yaml:"-"` // Auto-assigned: relative path from config.plugins.sql.path
	Id           string    `yaml:"-"` // Auto-assigned: SHA256(File)[:12]
	Raw          string    `yaml:"-"` // Auto-assigned: raw YAML content
	Error        string    `yaml:"-"` // last load/parsing error message (if any)
	LastLoadedAt time.Time `yaml:"-"` // last successful load time
}

// Sink represents a single sink configuration with operation filter
type Sink struct {
	SinkConfig `yaml:",inline"` // Embedded SinkConfig for backward compatibility
	When       []string         `yaml:"when"` // Required: [insert, update] or [delete]
}

// SinkConfig represents a single job configuration
type SinkConfig struct {
	Name       string `yaml:"name"`        // Sink name
	On         string `yaml:"on"`          // Table filter (required for multi-table)
	If         string `yaml:"if"`          // SQL-style condition expression (govaluate)
	SQL        string `yaml:"sql"`         // Inline SQL template
	SQLFile    string `yaml:"sql_file"`    // External SQL file path
	Output     string `yaml:"output"`      // Target table name
	PrimaryKey string `yaml:"primary_key"` // Primary key column
	OnConflict string `yaml:"on_conflict"` // Conflict strategy: overwrite | skip | error (default: skip)

	compiledIf *govaluate.EvaluableExpression `yaml:"-"` // Precompiled expression (internal use)
	OnLower   string `yaml:"-"`            // Precomputed lowercase of On (for performance)
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
//   - 'if' expressions (if present) are valid govaluate expressions
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

		// Validate: delete cannot mix with insert/update
		// Valid combinations: [insert], [update], [delete], [insert, update]
		if hasDelete && (hasInsert || hasUpdate) {
			return fmt.Errorf("sink[%d].when: cannot mix delete with insert/update", i)
		}
		if !hasDelete && !hasInsert && !hasUpdate {
			return fmt.Errorf("sink[%d].when: must specify at least one of insert, update, or delete", i)
		}

		// Validate and precompile 'if' expression
		if s.Sinks[i].If != "" {
			// Normalize SQL-style expression to govaluate format
			normalizedIf := normalizeIfExpression(s.Sinks[i].If)
			expr, err := govaluate.NewEvaluableExpression(normalizedIf)
			if err != nil {
				return fmt.Errorf("sink[%d].if: invalid expression '%s': %w", i, s.Sinks[i].If, err)
			}
			s.Sinks[i].compiledIf = expr
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

// normalizeIfExpression converts SQL-style expressions to govaluate-compatible format.
// It replaces table.field names with table_field format and converts SQL operators to govaluate syntax.
// Note: govaluate uses == for equality (not =) and !== for inequality (not !=).
// Note: govaluate uses &&/|| (not AND/OR).
func normalizeIfExpression(expr string) string {
	result := expr

	// Step 1: Protect existing operators
	result = strings.ReplaceAll(result, "==", "\x00EQ\x00")
	result = strings.ReplaceAll(result, "!=", "\x00NE\x00")
	result = strings.ReplaceAll(result, ">=", "\x00GE\x00")
	result = strings.ReplaceAll(result, "<=", "\x00LE\x00")
	result = strings.ReplaceAll(result, "&&", "\x00AND\x00")
	result = strings.ReplaceAll(result, "||", "\x00OR\x00")

	// Step 2: Replace single = with == (only outside of string literals)
	var sb strings.Builder
	inString := false
	for i := 0; i < len(result); i++ {
		c := result[i]
		if c == '\'' {
			inString = !inString
			sb.WriteByte(c)
			continue
		}
		if !inString && c == '=' && (i+1 >= len(result) || result[i+1] != '=') {
			sb.WriteString("==")
			continue
		}
		sb.WriteByte(c)
	}
	result = sb.String()

	// Step 3: Replace AND/OR with &&/|| using regex for proper word boundaries
	andRegex := regexp.MustCompile(`\bAND\b`)
	orRegex := regexp.MustCompile(`\bOR\b`)
	result = andRegex.ReplaceAllString(result, "&&")
	result = orRegex.ReplaceAllString(result, "||")

	// Step 4: Restore protected operators
	result = strings.ReplaceAll(result, "\x00EQ\x00", "==")
	result = strings.ReplaceAll(result, "\x00NE\x00", "!=")
	result = strings.ReplaceAll(result, "\x00GE\x00", ">=")
	result = strings.ReplaceAll(result, "\x00LE\x00", "<=")
	result = strings.ReplaceAll(result, "\x00AND\x00", "&&")
	result = strings.ReplaceAll(result, "\x00OR\x00", "||")

	// Step 5: Replace table.field with table_field (only the dot, not the whole pattern)
	// Match . surrounded by word characters and replace with _
	// Also normalize to lowercase for case-insensitivity
	tableFieldRegex := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)`)
	result = tableFieldRegex.ReplaceAllStringFunc(result, func(match string) string {
		parts := strings.Split(match, ".")
		return strings.ToLower(parts[0]) + "_" + strings.ToLower(parts[1])
	})

	return result
}

// EvalIf evaluates the 'if' expression for a sink against CDC data.
// It returns true if:
//   - The sink has no 'if' expression (always true)
//   - The expression evaluates to true
//
// It returns false if the expression evaluates to false or an error occurs.
//
// Table and field names in expressions are treated case-insensitively:
//
//	"orders.status = 'vip'" and "ORDERS.STATUS = 'vip'" both map to the same fields.
//
// Literal values in expressions remain case-sensitive.
func (s *SinkConfig) EvalIf(cdcParams CDCParameters) bool {
	if s.compiledIf == nil {
		return true // No 'if' condition means always trigger
	}

	// Build parameters map with lowercase table.field keys for case-insensitivity
	params := make(map[string]interface{})

	// Add CDC metadata
	params["cdc_lsn"] = cdcParams.CDCLSN
	params["cdc_tx_id"] = cdcParams.CDCTxID
	params["cdc_table"] = cdcParams.CDCTable
	params["cdc_operation"] = cdcParams.CDCOperation

	// Add data fields with table prefix
	for k, v := range cdcParams.Fields {
		params[strings.ToLower(k)] = v
	}

	// Also add direct field names (without table prefix) for convenience
	// These are lowercased for case-insensitive matching
	for k, v := range cdcParams.Fields {
		// Extract field name without table prefix
		parts := strings.SplitN(k, "_", 2)
		if len(parts) == 2 {
			fieldName := strings.ToLower(parts[1])
			params[fieldName] = v
			// Also add table.field format (lowercase)
			params[strings.ToLower(k)] = v
		}
	}

	// Evaluate the expression
	result, err := s.compiledIf.Evaluate(params)
	if err != nil {
		return false
	}

	// If result is nil (missing parameter), return false
	if result == nil {
		return false
	}

	if boolResult, ok := result.(bool); ok {
		return boolResult
	}
	return false
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
	Name        string                  `json:"name"`
	Type        string                  `json:"type"`   // "sql"
	Status      string                  `json:"status"` // loaded | stale | error
	NeedsReload bool                    `json:"needs_reload"`
	LoadCount   int                     `json:"load_count"`
	LastError   string                  `json:"last_error,omitempty"`
	Files       map[string]FileMetadata `json:"files"`
}

// FileMetadata tracks the state of a single file (.yml or .sql)
type FileMetadata struct {
	Path        string    `json:"path"`
	IsSQL       bool      `json:"is_sql"`
	CurVersion  string    `json:"cur_version"` // SHA256 hash of current loaded content
	CurModTime  time.Time `json:"cur_mod_time"`
	CurSize     int64     `json:"cur_size"`
	CurLoadedAt time.Time `json:"cur_loaded_at"`
	NewVersion  string    `json:"new_version"` // SHA256 hash of new content (if changed)
	NewModTime  time.Time `json:"new_mod_time"`
	NewSize     int64     `json:"new_size"`
	NeedsReload bool      `json:"needs_reload"`
	IsDeleted   bool      `json:"is_deleted"`
}
