package plugin

// Plugin is the common interface for SQL plugins.
//
// Go naming convention: noun phrase, no -er suffix for multi-method interfaces.
// Note: Init/Start are handled internally by each plugin type's constructor;
// only Stop is called through this interface.
type Plugin interface {
	// Name returns the plugin name
	Name() string

	// Type returns the plugin type: "sql"
	Type() string

	// Stop stops the plugin and releases resources.
	Stop() error
}
