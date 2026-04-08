package sql

import (
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

// NewPlugin creates a new SQL plugin from a skill.
// Pass db=nil if you need to call AttachDB later.
func NewPlugin(name string, skill *Skill, loader *Loader, db *sql.DB) *Plugin {
	return &Plugin{
		name:   name,
		skill:  skill,
		engine: NewEngine(skill, db),
		loader: loader,
	}
}

// AttachDB sets the database connection for this plugin's engine.
func (p *Plugin) AttachDB(db *sql.DB) {
	p.engine = NewEngine(p.skill, db)
}

// Name implements plugin.Plugin
func (p *Plugin) Name() string { return p.name }

// Type implements plugin.Plugin
func (p *Plugin) Type() string { return "sql" }

// Stop implements plugin.Plugin
func (p *Plugin) Stop() error {
	// No resources to release for SQL plugins
	return nil
}

// Handle processes a CDC transaction through this SQL plugin
func (p *Plugin) Handle(tx *core.Transaction) ([]core.Sink, error) {
	if p.engine == nil {
		return nil, fmt.Errorf("engine not initialized, call AttachDB first")
	}
	return p.engine.Handle(tx)
}
