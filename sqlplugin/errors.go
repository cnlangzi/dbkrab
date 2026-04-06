package sqlplugin

import "fmt"

// Plugin errors
var (
	ErrPluginNotFound    = fmt.Errorf("plugin not found")
	ErrInvalidConfig     = fmt.Errorf("invalid plugin configuration")
	ErrMissingField      = fmt.Errorf("missing required field")
	ErrInvalidSQL        = fmt.Errorf("invalid SQL template")
	ErrSQLFileNotFound   = fmt.Errorf("SQL file not found")
	ErrExecutionFailed  = fmt.Errorf("SQL execution failed")
	ErrRoutingFailed    = fmt.Errorf("result routing failed")
	ErrWriteFailed      = fmt.Errorf("sink write failed")
	ErrInvalidParameter  = fmt.Errorf("invalid parameter")
)

// ConfigError represents a configuration validation error
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config error: %s - %s", e.Field, e.Message)
}

// NewConfigError creates a new config error
func NewConfigError(field, message string) *ConfigError {
	return &ConfigError{Field: field, Message: message}
}

// ExecutionError represents a SQL execution error
type ExecutionError struct {
	SQL    string
	Params map[string]interface{}
	Err    error
}

func (e *ExecutionError) Error() string {
	return fmt.Sprintf("execution error: %v", e.Err)
}

func (e *ExecutionError) Unwrap() error {
	return e.Err
}

// NewExecutionError creates a new execution error
func NewExecutionError(sql string, params map[string]interface{}, err error) *ExecutionError {
	return &ExecutionError{SQL: sql, Params: params, Err: err}
}

// RoutingError represents a result routing error
type RoutingError struct {
	SinkName string
	Err      error
}

func (e *RoutingError) Error() string {
	return fmt.Sprintf("routing error for sink %s: %v", e.SinkName, e.Err)
}

func (e *RoutingError) Unwrap() error {
	return e.Err
}

// NewRoutingError creates a new routing error
func NewRoutingError(sinkName string, err error) *RoutingError {
	return &RoutingError{SinkName: sinkName, Err: err}
}

// WriteError represents a sink write error
type WriteError struct {
	Table string
	Err   error
}

func (e *WriteError) Error() string {
	return fmt.Sprintf("write error for table %s: %v", e.Table, e.Err)
}

func (e *WriteError) Unwrap() error {
	return e.Err
}

// NewWriteError creates a new write error
func NewWriteError(table string, err error) *WriteError {
	return &WriteError{Table: table, Err: err}
}
