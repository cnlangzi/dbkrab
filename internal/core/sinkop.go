package core

// DataSet represents query results from SQL execution
type DataSet struct {
	Columns []string
	Rows    [][]any
}

// SinkConfig represents sink configuration
type SinkConfig struct {
	Name       string
	Database   string // Database name (maps to platform-configured storage)
	Output     string
	PrimaryKey string
	OnConflict string
}

// Sink represents a sink operation from SQL plugin
type Sink struct {
	Config  SinkConfig
	DataSet *DataSet
	OpType  Operation // Uses core.Operation from transaction.go
}
