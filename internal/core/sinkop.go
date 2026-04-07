package core

// DataSet represents query results from SQL execution
type DataSet struct {
	Columns []string
	Rows    [][]any
}

// SinkOpConfig represents sink configuration
type SinkOpConfig struct {
	Name       string
	Output     string
	PrimaryKey string
	OnConflict string
}

// SinkOp represents a sink operation from SQL plugin
type SinkOp struct {
	Config  SinkOpConfig
	DataSet *DataSet
	OpType  Operation // Uses core.Operation from transaction.go
}
