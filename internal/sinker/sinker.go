package sinker

import (
	"context"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// Sinker defines the interface for sink writers that write transformed data to sinks.
// Each implementation handles its own SQL syntax and migration strategy.
type Sinker interface {
	// DatabaseName returns the database name this sinker handles
	DatabaseName() string

	// DatabaseType returns the type of database (sqlite, duckdb, mssql, etc.)
	DatabaseType() string

	// Write writes a batch of sink operations to the sink with context for timeout/cancellation
	Write(ctx context.Context, ops []core.Sink) error

	// ExecContext executes a raw SQL query (used for table maintenance like TRUNCATE/DELETE)
	ExecContext(ctx context.Context, query string) error

	// Migrate runs the migration for this sinker's database.
	// It re-discovers and re-applies migrations from the configured migrations path.
	Migrate(ctx context.Context) error

	// Truncate deletes all data from the specified tables.
	// It disables foreign key checks during the operation.
	Truncate(ctx context.Context, tables []string) error

	// Close closes the sinker and releases resources
	Close() error
}
