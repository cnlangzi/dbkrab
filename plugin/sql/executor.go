package sql

import (
	"database/sql"
)

// Executor executes SQL templates with parameter substitution
type Executor struct {
	db     *sql.DB
	driver DriverExecutor
}

// NewExecutor creates a new SQL template executor
func NewExecutor(db *sql.DB) *Executor {
	return &Executor{db: db}
}

// NewExecutorWithDriver creates a new executor with a specific driver
// dbType: "mssql" or "sqlite"
func NewExecutorWithDriver(db *sql.DB, dbType DriverType) *Executor {
	var driver DriverExecutor
	switch dbType {
	case DriverMSSQL:
		driver = NewMSSQLExecutor(db)
	case DriverSQLite:
		driver = NewSQLiteExecutor(db)
	default:
		panic("unsupported driver type: " + string(dbType) + "")
	}
	return &Executor{db: db, driver: driver}
}

// ExecuteDriver executes a SQL template using driver-based parameterized queries
func (e *Executor) ExecuteDriver(sqlTmpl string, params map[string]interface{}) (*DataSet, error) {
	if e.driver == nil {
		panic("driver not configured - use NewExecutorWithDriver")
	}
	return e.driver.Execute(sqlTmpl, params)
}
