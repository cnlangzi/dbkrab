package plugin

import (
	"context"
	dbsql "database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/plugin/sql"
	"github.com/cnlangzi/dbkrab/plugin/wasm"
)

// Manager manages both WASM and SQL plugins with hot-reload support for WASM.
type Manager struct {
	plugins    map[string]Plugin        // unified plugin registry
	sqlLoader  *sql.Loader               // SQL plugin loader
	mssqlDB    *dbsql.DB
	mu         sync.RWMutex
	watchCancel context.CancelFunc       // cancel WASM file watching
}

// NewManager creates a new plugin manager
func NewManager() *Manager {
	return &Manager{
		plugins: make(map[string]Plugin),
	}
}

// Init initializes all plugins based on the provided config.
// It replaces the separate Watch() and InitSQLPlugins() calls.
func (m *Manager) Init(ctx context.Context, db *dbsql.DB, wasmCfg struct {
	Enabled bool
	Path    string
}, sqlCfg struct {
	Enabled bool
	Path    string
}) error {
	// Initialize SQL plugins if enabled
	if sqlCfg.Enabled && sqlCfg.Path != "" {
		if err := m.initSQLPlugins(db, sqlCfg.Path); err != nil {
			return fmt.Errorf("init SQL plugins: %w", err)
		}
	}

	// Start WASM plugin watching if enabled
	if wasmCfg.Enabled && wasmCfg.Path != "" {
		if err := m.Watch(ctx, wasmCfg.Path); err != nil {
			return fmt.Errorf("watch WASM plugins: %w", err)
		}
	}

	return nil
}

// initSQLPlugins loads all SQL plugins from the given directory
func (m *Manager) initSQLPlugins(mssqlDB *dbsql.DB, sqlPluginsDir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mssqlDB = mssqlDB

	// Load all SQL plugins
	loader := sql.NewLoader(sqlPluginsDir)
	skills, err := loader.LoadAll()
	if err != nil {
		return fmt.Errorf("load SQL plugins: %w", err)
	}

	for name, skill := range skills {
		plug := sql.NewPlugin(name, skill, loader)
		if err := plug.Init(context.Background(), mssqlDB); err != nil {
			fmt.Printf("Warning: failed to init SQL plugin %s: %v\n", name, err)
			continue
		}
		m.plugins[name] = plug
	}

	m.sqlLoader = loader
	return nil
}

// Watch watches a directory for WASM plugin files (.wasm) and auto-loads them.
// SQL plugins are loaded via Init, not Watch.
func (m *Manager) Watch(ctx context.Context, dir string) error {
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

	// Start watching for changes in background
	watchCtx, cancel := context.WithCancel(ctx)
	m.watchCancel = cancel
	go m.watchLoop(watchCtx, dir)

	return nil
}

// Stop stops all plugins and releases resources
func (m *Manager) Stop() error {
	if m.watchCancel != nil {
		m.watchCancel()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for name, p := range m.plugins {
		if p.Type() == "wasm" {
			if err := p.Stop(); err != nil {
				fmt.Printf("Warning: failed to stop plugin %s: %v\n", name, err)
			}
		}
	}
	return nil
}

// Load loads a WASM plugin from the given path.
// SQL plugins are loaded via Init, not Load.
func (m *Manager) Load(name, path string, config string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.plugins[name]; exists {
		return fmt.Errorf("plugin %s already loaded", name)
	}

	plug, err := wasm.NewPlugin(name, path, config)
	if err != nil {
		return fmt.Errorf("load wasm plugin: %w", err)
	}

	m.plugins[name] = plug
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

// Reload reloads a WASM plugin (hot-reload)
func (m *Manager) Reload(name string) error {
	m.mu.RLock()
	plug, exists := m.plugins[name]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("plugin %s not found", name)
	}

	if plug.Type() != "wasm" {
		return fmt.Errorf("cannot reload non-WASM plugin %s", name)
	}

	// For WASM plugins, reload with same path and config
	// We need the path, so we get it from the underlying wasm plugin
	wplug, ok := plug.(*wasm.Plugin)
	if !ok {
		return fmt.Errorf("internal error: plugin type mismatch")
	}

	if err := m.Unload(name); err != nil {
		return err
	}
	return m.Load(name, wplug.Path(), wplug.Config())
}

// Handle processes a transaction through all plugins.
// SQL plugins return []core.Sink; WASM plugins process internally.
func (m *Manager) Handle(tx *core.Transaction) ([]core.Sink, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var allOps []core.Sink

	for _, p := range m.plugins {
		switch p.Type() {
		case "sql":
			sinkOps, err := p.(interface {
				Handle(*core.Transaction) ([]core.Sink, error)
			}).Handle(tx)
			if err != nil {
				return nil, fmt.Errorf("SQL plugin %s handle: %w", p.Name(), err)
			}
			allOps = append(allOps, sinkOps...)
		case "wasm":
			// WASM plugins currently process internally (no return)
			// In the future, WASM plugins may also return []core.Sink
		}
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
			LoadedAt: time.Now(),
			Type:     p.Type(),
		})
	}
	return list
}

// pluginPath returns a human-readable path for a plugin
func (m *Manager) pluginPath(p Plugin) string {
	switch plug := p.(type) {
	case *wasm.Plugin:
		return plug.Path()
	case *sql.Plugin:
		return "sql_plugins/" + plug.Name()
	}
	return ""
}

// PluginInfo contains plugin metadata (used by API)
type PluginInfo struct {
	Name     string    `json:"name"`
	Path     string    `json:"path"`
	LoadedAt time.Time `json:"loaded_at"`
	Type     string    `json:"type"` // "sql" or "wasm"
}

// watchLoop watches a directory for WASM plugin changes
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
					if err := m.Load(name, path, ""); err == nil {
						fmt.Printf("Auto-loaded plugin: %s\n", name)
					}
					lastMod[name] = info.ModTime()
				} else if info.ModTime().After(lastMod[name]) {
					if err := m.Reload(name); err == nil {
						fmt.Printf("Reloaded plugin: %s\n", name)
					}
					lastMod[name] = info.ModTime()
				}
			}
		}
	}
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
			LoadedAt: time.Now(),
			Type:     plug.Type(),
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
		return APIResponse{Success: true}

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

// APIResponse is the response format for plugin API
type APIResponse struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}
