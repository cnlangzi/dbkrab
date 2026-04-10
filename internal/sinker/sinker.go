package sinker

import (
	"github.com/cnlangzi/dbkrab/internal/core"
)

// Sinker defines the interface for sink writers that write transformed data to sinks.
// Each implementation handles its own SQL syntax and migration strategy.
type Sinker interface {
	// DatabaseName returns the database name this sinker handles
	DatabaseName() string

	// DatabaseType returns the type of database (sqlite, duckdb, mssql, etc.)
	DatabaseType() string

	// Write writes a batch of sink operations to the sink
	Write(ops []core.Sink) error

	// RunMigrations runs any pending migrations
	RunMigrations() error

	// Close closes the sinker and releases resources
	Close() error
}
