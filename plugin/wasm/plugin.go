package wasm

import (
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// Plugin implements the plugin.Plugin interface for WASM plugins.
type Plugin struct {
	name     string
	path     string
	config   string
	loadedAt time.Time
	instance *Instance
	mu       sync.Mutex
}

// NewPlugin creates a new WASM plugin
func NewPlugin(name, path, config string) (*Plugin, error) {
	instance, err := NewInstance(path)
	if err != nil {
		return nil, err
	}

	if err := instance.Init(config); err != nil {
		instance.Close()
		return nil, err
	}

	return &Plugin{
		name:     name,
		path:     path,
		config:   config,
		loadedAt: time.Now(),
		instance: instance,
	}, nil
}

// Name implements plugin.Plugin
func (p *Plugin) Name() string { return p.name }

// Type implements plugin.Plugin
func (p *Plugin) Type() string { return "wasm" }

// LoadedAt returns when the plugin was loaded
func (p *Plugin) LoadedAt() time.Time { return p.loadedAt }

// Path returns the plugin file path
func (p *Plugin) Path() string { return p.path }

// Config returns the plugin config
func (p *Plugin) Config() string { return p.config }

// Stop implements plugin.Plugin
func (p *Plugin) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.instance != nil {
		return p.instance.Close()
	}
	return nil
}

// Handle processes a transaction through this WASM plugin
func (p *Plugin) Handle(tx *core.Transaction) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.instance != nil {
		return p.instance.Handle(tx)
	}
	return nil
}
