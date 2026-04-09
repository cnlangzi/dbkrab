package plugin

import (
	"context"
	dbsql "database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/plugin/sql"
	"github.com/cnlangzi/dbkrab/sinkwriter"
)

// Manager manages SQL plugins.
type Manager struct {
	plugins   map[string]Plugin  // SQL plugin registry (key="sql" for single SQLPlugin)
	sqlPlugin *sql.Plugin       // direct reference to SQLPlugin for fast access
	swManager *sinkwriter.Manager // Routes sinks to appropriate writers
	mu        sync.RWMutex
}

// NewManager creates a new plugin manager
func NewManager() *Manager {
	return &Manager{
		plugins:   make(map[string]Plugin),
		swManager: sinkwriter.NewManager(),
	}
}

// Init initializes all SQL plugins based on the provided config.
func (m *Manager) Init(_ context.Context, db *dbsql.DB, sqlCfg struct {
	Enabled       bool
	Path          string
	SinkConfigs map[string]any // database name -> config
}, dbConfigs map[string]config.DatabaseConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Configure sink writer manager
	if dbConfigs != nil {
		m.swManager.Configure(dbConfigs)
	}

	// Load SQL plugins if enabled
	if sqlCfg.Enabled && sqlCfg.Path != "" {
		plugin := sql.New(sqlCfg.Path, db)
		// The plugin manages its own file watching internally
		plugin.StartWatch()

		m.sqlPlugin = plugin
		m.plugins["sql"] = plugin
	}

	return nil
}

// Stop stops all plugins and releases resources
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sqlPlugin != nil {
		if err := m.sqlPlugin.Stop(); err != nil {
			fmt.Printf("Warning: failed to stop SQL plugin: %v\n", err)
		}
	}

	// Close sink manager
	if m.swManager != nil {
		if err := m.swManager.Close(); err != nil {
			fmt.Printf("Warning: failed to close sink manager: %v\n", err)
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
	if name == "sql" {
		m.sqlPlugin = nil
	}
	return nil
}

// Handle processes a transaction through all SQL plugins.
// Each plugin transforms data and returns sinks with Database field set.
// The sink manager routes sinks to appropriate writers based on Database field.
func (m *Manager) Handle(tx *core.Transaction) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect all sink operations from all plugins
	var allSinks []core.Sink

	for _, p := range m.plugins {
		splug, ok := p.(*sql.Plugin)
		if !ok {
			continue
		}

		// Transform and get sinks with Database field
		sinks, err := splug.Handle(tx)
		if err != nil {
			return fmt.Errorf("SQL plugin %s handle: %w", splug.Name(), err)
		}

		allSinks = append(allSinks, sinks...)
	}

	// Route sinks to appropriate writers based on Database field
	if len(allSinks) > 0 {
		if err := m.swManager.Write(allSinks); err != nil {
			return fmt.Errorf("sink write: %w", err)
		}
	}

	return nil
}

// List returns all loaded plugins
func (m *Manager) List() []PluginInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []PluginInfo

	// List skills from the SQL plugin
	if m.sqlPlugin != nil {
		for _, skill := range m.sqlPlugin.Skills.List() {
			list = append(list, PluginInfo{
				Name:     skill.Name,
				Id:       skill.Id,
				File:     skill.File,
				Path:     "skills/sql/" + skill.File,
				LoadedAt: time.Now(),
				Type:     "sql",
			})
		}
	}

	return list
}

// HasSQLPlugins returns true if any SQL plugins are loaded
func (m *Manager) HasSQLPlugins() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sqlPlugin != nil
}

// HasWASMPlugins returns true if any WASM plugins are loaded
func (m *Manager) HasWASMPlugins() bool {
	return false
}

// PluginInfo contains plugin metadata (used by API)
type PluginInfo struct {
	Name     string    `json:"name"`        // YAML name field
	Id       string    `json:"id"`          // SHA256(file)[:12]
	File     string    `json:"file"`        // Relative file path
	Path     string    `json:"path"`
	LoadedAt time.Time `json:"loaded_at"`
	Type     string    `json:"type"`        // "sql"
}

// HandleAPI handles plugin management via HTTP API
func (m *Manager) HandleAPI(action string, params map[string]interface{}) APIResponse {
	switch action {
	case "list":
		return APIResponse{Success: true, Data: m.List()}

	case "get_skill":
		id, _ := params["id"].(string)
		if id == "" {
			return APIResponse{Success: false, Error: "id required"}
		}
		m.mu.RLock()
		defer m.mu.RUnlock()
		if m.sqlPlugin == nil {
			return APIResponse{Success: false, Error: "SQL plugin not initialized"}
		}
		skill, ok := m.sqlPlugin.Skills.Get(id)
		if !ok {
			return APIResponse{Success: false, Error: "skill not found: " + id}
		}
		return APIResponse{Success: true, Data: skill}

	case "get":
		name, _ := params["name"].(string)
		if name == "" {
			return APIResponse{Success: false, Error: "name required"}
		}
		m.mu.RLock()
		plug := m.sqlPlugin
		m.mu.RUnlock()
		if plug == nil {
			return APIResponse{Success: false, Error: "plugin not found"}
		}
		return APIResponse{Success: true, Data: PluginInfo{
			Name:     plug.Name(),
			Path:     "skills/sql/",
			LoadedAt: time.Now(),
			Type:     plug.Type(),
		}}

	case "load":
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
