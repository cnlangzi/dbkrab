package plugin

import (
	"context"
	dbsql "database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/plugin/sql"
)

// Manager manages SQL plugins.
type Manager struct {
	plugins   map[string]Plugin  // SQL plugin registry
	sqlLoader *sql.Loader        // SQL plugin loader
	mu        sync.RWMutex
}

// NewManager creates a new plugin manager
func NewManager() *Manager {
	return &Manager{
		plugins: make(map[string]Plugin),
	}
}

// Init initializes all SQL plugins based on the provided config.
func (m *Manager) Init(_ context.Context, db *dbsql.DB, sqlCfg struct {
	Enabled bool
	Path    string
}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Load SQL plugins if enabled
	if sqlCfg.Enabled && sqlCfg.Path != "" {
		loader := sql.NewLoader(sqlCfg.Path)
		skills, err := loader.LoadAll()
		if err != nil {
			return fmt.Errorf("load SQL plugins: %w", err)
		}

		for name, skill := range skills {
			plug := sql.NewPlugin(name, skill, loader, db)
			// Each plugin manages its own file watching internally
			plug.StartWatch()
			m.plugins[name] = plug
		}

		m.sqlLoader = loader
	}

	return nil
}

// Stop stops all plugins and releases resources
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, p := range m.plugins {
		if err := p.Stop(); err != nil {
			fmt.Printf("Warning: failed to stop plugin %s: %v\n", name, err)
		}
	}
	return nil
}

// Unload unloads a plugin by name
func (m *Manager) Unload(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	plug, exists := m.plugins[name]
	if !exists {
		return fmt.Errorf("plugin %s not found", name)
	}

	if err := plug.Stop(); err != nil {
		return fmt.Errorf("plugin stop: %w", err)
	}

	delete(m.plugins, name)
	return nil
}

// Handle processes a transaction through all SQL plugins.
// SQL plugins return []core.Sink.
func (m *Manager) Handle(tx *core.Transaction) ([]core.Sink, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var allOps []core.Sink

	for _, p := range m.plugins {
		splug, ok := p.(*sql.Plugin)
		if !ok {
			continue
		}
		sinkOps, err := splug.Handle(tx)
		if err != nil {
			return nil, fmt.Errorf("SQL plugin %s handle: %w", splug.Name(), err)
		}
		allOps = append(allOps, sinkOps...)
	}

	return allOps, nil
}

// List returns all loaded plugins
func (m *Manager) List() []PluginInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []PluginInfo
	for _, p := range m.plugins {
		list = append(list, PluginInfo{
			Name:     p.Name(),
			Path:     m.pluginPath(p),
			LoadedAt: m.pluginLoadedAt(p),
			Type:     p.Type(),
		})
	}
	return list
}

// HasSQLPlugins returns true if any SQL plugins are loaded
func (m *Manager) HasSQLPlugins() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.plugins {
		if p.Type() == "sql" {
			return true
		}
	}
	return false
}

// HasWASMPlugins returns true if any WASM plugins are loaded
func (m *Manager) HasWASMPlugins() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.plugins {
		if p.Type() == "wasm" {
			return true
		}
	}
	return false
}

// pluginPath returns a human-readable path for a plugin
func (m *Manager) pluginPath(p Plugin) string {
	switch plug := p.(type) {
	case *sql.Plugin:
		return "skills/sql/" + plug.Name()
	}
	return ""
}

// pluginLoadedAt returns the actual load time of a plugin
func (m *Manager) pluginLoadedAt(p Plugin) time.Time {
	switch p.(type) {
	case *sql.Plugin:
		// SQL plugins don't track load time yet; return zero time
		return time.Time{}
	}
	return time.Time{}
}

// PluginInfo contains plugin metadata (used by API)
type PluginInfo struct {
	Name     string    `json:"name"`
	Path     string    `json:"path"`
	LoadedAt time.Time `json:"loaded_at"`
	Type     string    `json:"type"` // "sql"
}

// HandleAPI handles plugin management via HTTP API
func (m *Manager) HandleAPI(action string, params map[string]interface{}) APIResponse {
	switch action {
	case "list":
		return APIResponse{Success: true, Data: m.List()}

	case "get":
		name, _ := params["name"].(string)
		if name == "" {
			return APIResponse{Success: false, Error: "name required"}
		}
		m.mu.RLock()
		plug, ok := m.plugins[name]
		m.mu.RUnlock()
		if !ok {
			return APIResponse{Success: false, Error: "plugin not found"}
		}
		return APIResponse{Success: true, Data: PluginInfo{
			Name:     plug.Name(),
			Path:     m.pluginPath(plug),
			LoadedAt: m.pluginLoadedAt(plug),
			Type:     plug.Type(),
		}}

	case "load":
		// WASM plugins are no longer supported; SQL plugins are loaded via Init
		return APIResponse{Success: false, Error: "load not supported for SQL plugins (loaded via Init)"}

	case "unload":
		name, _ := params["name"].(string)
		if name == "" {
			return APIResponse{Success: false, Error: "name required"}
		}
		if err := m.Unload(name); err != nil {
			return APIResponse{Success: false, Error: err.Error()}
		}
		return APIResponse{Success: true}

	default:
		return APIResponse{Success: false, Error: "unknown action"}
	}
}

// APIResponse is the response format for plugin API
type APIResponse struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}
