package plugin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/sqlplugin"
)

// SQLPlugin represents a loaded SQL plugin
type SQLPlugin struct {
	Name   string
	Skill  *sqlplugin.Skill
	Engine *sqlplugin.Engine // SQL Plugin execution engine
	Loader *sqlplugin.Loader
}

// Manager manages SQL plugins and WASM plugins with hot-reload support
type Manager struct {
	plugins    map[string]*Plugin
	sqlPlugins map[string]*SQLPlugin
	mu         sync.RWMutex
	mssqlDB    *sql.DB
}

// Plugin represents a loaded WASM plugin
type Plugin struct {
	Name     string
	Path     string
	Config   string
	LoadedAt time.Time
	instance *WasmInstance
}

// NewManager creates a new plugin manager
func NewManager() *Manager {
	return &Manager{
		plugins:    make(map[string]*Plugin),
		sqlPlugins: make(map[string]*SQLPlugin),
	}
}

// InitSQLPlugins initializes SQL plugins with database connections
func (m *Manager) InitSQLPlugins(mssqlDB *sql.DB, sqlPluginsDir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mssqlDB = mssqlDB

	// Load all SQL plugins
	loader := sqlplugin.NewLoader(sqlPluginsDir)
	plugins, err := loader.LoadAll()
	if err != nil {
		return fmt.Errorf("load SQL plugins: %w", err)
	}

	for name, skill := range plugins {
		engine := sqlplugin.NewEngine(skill, mssqlDB)

		m.sqlPlugins[name] = &SQLPlugin{
			Name:   name,
			Skill:  skill,
			Engine: engine,
			Loader: loader,
		}
	}

	return nil
}

// Load loads a plugin from the given path
func (m *Manager) Load(name, path string, config string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already loaded
	if _, exists := m.plugins[name]; exists {
		return fmt.Errorf("plugin %s already loaded", name)
	}

	// Check if there's a SQL plugin with the same name (SQL plugin takes priority)
	if _, exists := m.sqlPlugins[name]; exists {
		return fmt.Errorf("plugin %s is a SQL plugin", name)
	}

	// Load WASM instance
	instance, err := NewWasmInstance(path)
	if err != nil {
		return fmt.Errorf("load wasm instance: %w", err)
	}

	// Call Init
	if err := instance.Init(config); err != nil {
		if closeErr := instance.Close(); closeErr != nil {
			fmt.Printf("Warning: instance.Close error: %v\n", closeErr)
		}
		return fmt.Errorf("plugin init: %w", err)
	}

	plugin := &Plugin{
		Name:     name,
		Path:     path,
		Config:   config,
		LoadedAt: time.Now(),
		instance: instance,
	}

	m.plugins[name] = plugin
	return nil
}

// Unload unloads a plugin by name
func (m *Manager) Unload(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if it's a SQL plugin
	if _, exists := m.sqlPlugins[name]; exists {
		delete(m.sqlPlugins, name)
		return nil
	}

	plugin, exists := m.plugins[name]
	if !exists {
		return fmt.Errorf("plugin %s not found", name)
	}

	if err := plugin.instance.Close(); err != nil {
		return fmt.Errorf("plugin close: %w", err)
	}

	delete(m.plugins, name)
	return nil
}

// Reload reloads a plugin (hot-reload)
func (m *Manager) Reload(name string) error {
	m.mu.RLock()
	plugin, exists := m.plugins[name]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("plugin %s not found", name)
	}

	// Unload
	if err := m.Unload(name); err != nil {
		return err
	}

	// Reload with same config
	return m.Load(name, plugin.Path, plugin.Config)
}

// Handle processes a transaction through all plugins
// Returns transformed operations for the caller to write via Store
func (m *Manager) Handle(tx *core.Transaction) ([]core.Sink, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var allOps []core.Sink

	// First, process through SQL plugins
	for name, sqlPlugin := range m.sqlPlugins {
		ops, err := m.handleSQLPlugin(sqlPlugin, tx)
		if err != nil {
			return nil, fmt.Errorf("SQL plugin %s handle: %w", name, err)
		}
		allOps = append(allOps, ops...)
	}

	// Then, process through WASM plugins (legacy - they process internally)
	for name, plugin := range m.plugins {
		if err := plugin.instance.Handle(tx); err != nil {
			return nil, fmt.Errorf("plugin %s handle: %w", name, err)
		}
	}

	return allOps, nil
}

// handleSQLPlugin processes a transaction through a SQL plugin
func (m *Manager) handleSQLPlugin(p *SQLPlugin, tx *core.Transaction) ([]core.Sink, error) {
	return p.Engine.Handle(tx)
}

// List returns all loaded plugins (both SQL and WASM)
func (m *Manager) List() []PluginInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []PluginInfo

	// List SQL plugins
	for name := range m.sqlPlugins {
		list = append(list, PluginInfo{
			Name:     name,
			Path:     "sql_plugins/" + name,
			LoadedAt: time.Now(),
			Type:     "sql",
		})
	}

	// List WASM plugins
	for name, p := range m.plugins {
		list = append(list, PluginInfo{
			Name:     name,
			Path:     p.Path,
			LoadedAt: p.LoadedAt,
			Type:     "wasm",
		})
	}

	return list
}

// PluginInfo contains plugin metadata
type PluginInfo struct {
	Name     string    `json:"name"`
	Path     string    `json:"path"`
	LoadedAt time.Time `json:"loaded_at"`
	Type     string    `json:"type"` // "sql" or "wasm"
}

// Watch watches a directory for new plugins and auto-loads them
func (m *Manager) Watch(ctx context.Context, dir string) error {
	// First, check for SQL plugins
	sqlPluginsDir := filepath.Join(dir, "..", "sql_plugins")
	if _, err := os.Stat(sqlPluginsDir); err == nil {
		// SQL plugins directory exists, load all SQL plugins
		if m.mssqlDB != nil {
			if err := m.InitSQLPlugins(m.mssqlDB, sqlPluginsDir); err != nil {
				fmt.Printf("Warning: failed to load SQL plugins: %v\n", err)
			}
		}
	}

	// Then watch for WASM plugins
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read plugin dir: %w", err)
	}

	for _, f := range files {
		if filepath.Ext(f.Name()) == ".wasm" {
			name := f.Name()[:len(f.Name())-5]
			path := filepath.Join(dir, f.Name())
			if err := m.Load(name, path, ""); err != nil {
				fmt.Printf("Failed to load plugin %s: %v\n", name, err)
			} else {
				fmt.Printf("Loaded plugin: %s\n", name)
			}
		}
	}

	// Watch for changes
	go m.watchLoop(ctx, dir)

	return nil
}

func (m *Manager) watchLoop(ctx context.Context, dir string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	lastMod := make(map[string]time.Time)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			files, err := os.ReadDir(dir)
			if err != nil {
				continue
			}

			for _, f := range files {
				if filepath.Ext(f.Name()) != ".wasm" {
					continue
				}

				info, err := f.Info()
				if err != nil {
					continue
				}

				name := f.Name()[:len(f.Name())-5]
				path := filepath.Join(dir, f.Name())

				// Check if new or modified
				if lastMod[name].IsZero() {
					// New plugin
					if err := m.Load(name, path, ""); err == nil {
						fmt.Printf("Auto-loaded plugin: %s\n", name)
					}
					lastMod[name] = info.ModTime()
				} else if info.ModTime().After(lastMod[name]) {
					// Modified, reload
					if err := m.Reload(name); err == nil {
						fmt.Printf("Reloaded plugin: %s\n", name)
					}
					lastMod[name] = info.ModTime()
				}
			}
		}
	}
}

// APIResponse is the response format for plugin API
type APIResponse struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
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
		sqlPlugin, sqlOK := m.sqlPlugins[name]
		plugin, wasmOK := m.plugins[name]
		m.mu.RUnlock()
		if !sqlOK && !wasmOK {
			return APIResponse{Success: false, Error: "plugin not found"}
		}
		if sqlOK {
			return APIResponse{Success: true, Data: PluginInfo{
				Name:     sqlPlugin.Name,
				Path:     "sql_plugins/" + sqlPlugin.Name,
				LoadedAt: time.Now(),
				Type:     "sql",
			}}
		}
		return APIResponse{Success: true, Data: PluginInfo{
			Name:     plugin.Name,
			Path:     plugin.Path,
			LoadedAt: plugin.LoadedAt,
			Type:     "wasm",
		}}

	case "load":
		name, _ := params["name"].(string)
		path, _ := params["path"].(string)
		config, _ := params["config"].(string)
		if name == "" || path == "" {
			return APIResponse{Success: false, Error: "name and path required"}
		}
		if err := m.Load(name, path, config); err != nil {
			return APIResponse{Success: false, Error: err.Error()}
		}
		// Return plugin info after successful load (with lock)
		m.mu.RLock()
		plugin := m.plugins[name]
		m.mu.RUnlock()
		return APIResponse{Success: true, Data: PluginInfo{
			Name:     plugin.Name,
			Path:     plugin.Path,
			LoadedAt: plugin.LoadedAt,
			Type:     "wasm",
		}}

	case "unload":
		name, _ := params["name"].(string)
		if name == "" {
			return APIResponse{Success: false, Error: "name required"}
		}
		if err := m.Unload(name); err != nil {
			return APIResponse{Success: false, Error: err.Error()}
		}
		return APIResponse{Success: true}

	case "reload":
		name, _ := params["name"].(string)
		if name == "" {
			return APIResponse{Success: false, Error: "name required"}
		}
		if err := m.Reload(name); err != nil {
			return APIResponse{Success: false, Error: err.Error()}
		}
		return APIResponse{Success: true}

	default:
		return APIResponse{Success: false, Error: "unknown action"}
	}
}

// MarshalJSON implements json.Marshaler
func (p *Plugin) MarshalJSON() ([]byte, error) {
	return json.Marshal(PluginInfo{
		Name:     p.Name,
		Path:     p.Path,
		LoadedAt: p.LoadedAt,
		Type:     "wasm",
	})
}
