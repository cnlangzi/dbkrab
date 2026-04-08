package plugin

// Plugin is the common interface for all plugin types (WASM and SQL).
//
// Go naming convention: noun phrase, no -er suffix for multi-method interfaces.
// Note: Init/Start are handled internally by each plugin type's constructor
// and by Manager.Watch(); only Stop is called through this interface.
type Plugin interface {
	// Name returns the plugin name
	Name() string

	// Type returns the plugin type: "wasm" or "sql"
	Type() string

	// Stop stops the plugin and releases resources.
	Stop() error
}
