package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// Manager manages WASM plugins with hot-reload support
type Manager struct {
	plugins map[string]*Plugin
	mu      sync.RWMutex
	watchCh chan string
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
		plugins: make(map[string]*Plugin),
		watchCh: make(chan string, 100),
	}
}

// Load loads a plugin from the given path
func (m *Manager) Load(name, path string, config string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already loaded
	if _, exists := m.plugins[name]; exists {
		return fmt.Errorf("plugin %s already loaded", name)
	}

	// Load WASM instance
	instance, err := NewWasmInstance(path)
	if err != nil {
		return fmt.Errorf("load wasm instance: %w", err)
	}

	// Call Init
	if err := instance.Init(config); err != nil {
		instance.Close()
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

// Handle processes a transaction through all loaded plugins
func (m *Manager) Handle(tx *core.Transaction) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, plugin := range m.plugins {
		if err := plugin.instance.Handle(tx); err != nil {
			return fmt.Errorf("plugin %s handle: %w", name, err)
		}
	}
	return nil
}

// List returns all loaded plugins
func (m *Manager) List() []PluginInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []PluginInfo
	for name, p := range m.plugins {
		list = append(list, PluginInfo{
			Name:     name,
			Path:     p.Path,
			LoadedAt: p.LoadedAt,
		})
	}
	return list
}

// PluginInfo contains plugin metadata
type PluginInfo struct {
	Name     string    `json:"name"`
	Path     string    `json:"path"`
	LoadedAt time.Time `json:"loaded_at"`
}

// Watch watches a directory for new plugins and auto-loads them
func (m *Manager) Watch(ctx context.Context, dir string) error {
	// Initial scan
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
		return APIResponse{Success: true, Data: m.plugins[name]}

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
	})
}