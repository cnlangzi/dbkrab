package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// Plugin implements the plugin.Plugin interface for SQL plugins.
// Each SQL plugin wraps a skill (yaml config + SQL templates) with an execution engine.
type Plugin struct {
	name   string
	skill  *Skill
	engine *Engine
	loader *Loader
}

// NewPlugin creates a new SQL plugin from a skill
func NewPlugin(name string, skill *Skill, loader *Loader) *Plugin {
	return &Plugin{
		name:   name,
		skill:  skill,
		engine: NewEngine(skill, nil), // DB set in Init
		loader: loader,
	}
}

// Name implements plugin.Plugin
func (p *Plugin) Name() string { return p.name }

// Type implements plugin.Plugin
func (p *Plugin) Type() string { return "sql" }

// Init implements plugin.Plugin
func (p *Plugin) Init(ctx context.Context, db *sql.DB) error {
	// Engine stores the skill and uses the DB for queries
	p.engine = NewEngine(p.skill, db)
	return nil
}

// Start implements plugin.Plugin (no-op for SQL plugins)
func (p *Plugin) Start(ctx context.Context) error {
	return nil
}

// Stop implements plugin.Plugin
func (p *Plugin) Stop() error {
	// No resources to release for SQL plugins
	return nil
}

// Handle processes a CDC transaction through this SQL plugin
func (p *Plugin) Handle(tx *core.Transaction) ([]core.Sink, error) {
	if p.engine == nil {
		return nil, fmt.Errorf("engine not initialized, call Init first")
	}
	return p.engine.Handle(tx)
}
