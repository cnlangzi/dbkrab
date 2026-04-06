package sqlplugin

import "fmt"

// Operation represents the CDC operation type
type Operation int

const (
	Insert Operation = 1 + iota
	Update
	Delete
)

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
	Name        string     `yaml:"name"`
	Description string     `yaml:"description"`
	On          []string   `yaml:"on"`           // Tables to monitor
	Stages      []Stage    `yaml:"stages"`       // Optional multi-step SQL
	Sinks       SinksConfig `yaml:"sinks"`        // Sink configuration
}

// Stage represents a SQL stage
type Stage struct {
	Name     string `yaml:"name"`     // Optional stage name (temp table name)
	SQL      string `yaml:"sql"`      // Inline SQL
	SQLFile  string `yaml:"sql_file"` // External SQL file path
	TempTable string `yaml:"temp_table"` // Temp table name (optional, for compatibility)
}

// SinksConfig represents sink configuration
type SinksConfig struct {
	Insert []SinkConfig `yaml:"insert"`  // Insert operations
	Update []SinkConfig `yaml:"update"`  // Update operations
	Delete []SinkConfig `yaml:"delete"`  // Delete operations
}

// SinkConfig represents a single sink configuration
type SinkConfig struct {
	Name       string `yaml:"name"`        // Sink name
	On         string `yaml:"on"`          // Table filter (required for multi-table)
	SQL        string `yaml:"sql"`         // Inline SQL
	SQLFile    string `yaml:"sql_file"`    // External SQL file path
	Output     string `yaml:"output"`      // Target table name
	PrimaryKey string `yaml:"primary_key"` // Primary key column
}

// CDCParameters represents CDC data mapped to SQL parameters
type CDCParameters struct {
	CDCLSN      string
	CDCTxID     string
	CDCTable    string
	CDCOperation int
	TableIDs    []interface{}
	Fields      map[string]interface{}
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

// GetSinks returns sinks for the given operation type
func (s *Skill) GetSinks(opType Operation) []SinkConfig {
	switch opType {
	case Insert:
		return s.Sinks.Insert
	case Update:
		return s.Sinks.Update
	case Delete:
		return s.Sinks.Delete
	default:
		return nil
	}
}

// FilterSinks filters sinks by table name (for multi-table CDC)
func FilterSinks(sinks []SinkConfig, tableName string) []SinkConfig {
	if tableName == "" {
		return sinks
	}
	var filtered []SinkConfig
	for _, sink := range sinks {
		if sink.On == "" || sink.On == tableName {
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
