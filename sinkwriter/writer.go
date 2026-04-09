package sinkwriter

import (
	"github.com/cnlangzi/dbkrab/internal/core"
)

// SinkWriter writes sink operations to a specific database type.
// Each implementation handles its own SQL syntax and migration strategy.
type SinkWriter interface {
	// DatabaseName returns the database name this writer handles
	DatabaseName() string

	// DatabaseType returns the type of database (sqlite, duckdb, mssql, etc.)
	DatabaseType() string

	// Write writes a batch of sink operations to the database
	Write(ops []core.Sink) error

	// RunMigrations runs any pending migrations
	RunMigrations() error

	// Close closes the writer and releases resources
	Close() error
}
