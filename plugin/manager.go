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

// Manager manages both WASM and SQL plugins with hot-reload support.
type Manager struct {
	plugins     map[string]Plugin        // unified plugin registry
	sqlLoader   *sql.Loader              // SQL plugin loader
	mu          sync.RWMutex
	watchCancel context.CancelFunc       // cancel WASM file watching
	watchDone   chan struct{}            // closed when watchLoop exits
}

// NewManager creates a new plugin manager
func NewManager() *Manager {
	return &Manager{
		plugins: make(map[string]Plugin),
	}
}

// Init initializes all plugins based on the provided config.
func (m *Manager) Init(ctx context.Context, db *dbsql.DB, wasmCfg struct {
	Enabled bool
	Path    string
}, sqlCfg struct {
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

	// Watch for WASM plugins if enabled
	if wasmCfg.Enabled && wasmCfg.Path != "" {
		m.mu.Unlock()
		if err := m.Watch(ctx, wasmCfg.Path); err != nil {
			m.mu.Lock()
			return fmt.Errorf("watch WASM plugins: %w", err)
		}
		m.mu.Lock()
	}

	return nil
}

// Watch watches a directory for WASM plugin files (.wasm) and auto-loads them.
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
	m.watchDone = make(chan struct{})
	go m.watchLoop(watchCtx, dir)

	return nil
}

// Stop stops all plugins and releases resources
func (m *Manager) Stop() error {
	// Cancel WASM file watching and wait for watchLoop to exit
	if m.watchCancel != nil {
		m.watchCancel()
		if m.watchDone != nil {
			<-m.watchDone
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for name, p := range m.plugins {
		if err := p.Stop(); err != nil {
			fmt.Printf("Warning: failed to stop plugin %s: %v\n", name, err)
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

// Reload reloads a plugin (hot-reload for WASM, manual/scheduled for SQL)
func (m *Manager) Reload(name string) error {
	m.mu.RLock()
	plug, exists := m.plugins[name]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("plugin %s not found", name)
	}

	if plug.Type() != "wasm" {
		return fmt.Errorf("cannot reload plugin %s of unknown type", name)
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
			splug, ok := p.(*sql.Plugin)
			if !ok {
				continue
			}
			sinkOps, err := splug.Handle(tx)
			if err != nil {
				return nil, fmt.Errorf("SQL plugin %s handle: %w", splug.Name(), err)
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
	case *wasm.Plugin:
		return plug.Path()
	case *sql.Plugin:
		return "skills/sql/" + plug.Name()
	}
	return ""
}

// pluginLoadedAt returns the actual load time of a plugin
func (m *Manager) pluginLoadedAt(p Plugin) time.Time {
	switch plug := p.(type) {
	case *wasm.Plugin:
		return plug.LoadedAt()
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
	Type     string    `json:"type"` // "sql" or "wasm"
}

// watchLoop watches a directory for WASM plugin changes
func (m *Manager) watchLoop(ctx context.Context, dir string) {
	defer close(m.watchDone) // signal that watchLoop has exited
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
			LoadedAt: m.pluginLoadedAt(plug),
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
