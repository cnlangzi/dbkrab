package plugin

import (
	"context"
	"database/sql"
)

// Plugin is the common interface for all plugin types (WASM and SQL).
// It provides unified lifecycle management.
//
// Go naming convention: noun phrase, no -er suffix for multi-method interfaces.
type Plugin interface {
	// Name returns the plugin name
	Name() string

	// Type returns the plugin type: "wasm" or "sql"
	Type() string

	// Init initializes the plugin with the database connection.
	// Called once during manager initialization.
	Init(ctx context.Context, db *sql.DB) error

	// Start starts the plugin's background work (e.g., file watching for WASM).
	// Called after Init, non-blocking.
	Start(ctx context.Context) error

	// Stop stops the plugin and releases resources.
	Stop() error
}
