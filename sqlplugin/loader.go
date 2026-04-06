package sqlplugin

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Loader handles loading and parsing SQL plugin configurations
type Loader struct {
	pluginsDir string
}

// NewLoader creates a new SQL plugin loader
func NewLoader(pluginsDir string) *Loader {
	return &Loader{pluginsDir: pluginsDir}
}

// Load loads and parses a SQL plugin from the given name
func (l *Loader) Load(name string) (*Skill, error) {
	skillPath := filepath.Join(l.pluginsDir, name, "skill.yml")

	// Check if skill.yml exists
	if _, err := os.Stat(skillPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrPluginNotFound, name)
		}
		return nil, fmt.Errorf("check skill.yml: %w", err)
	}

	// Read skill.yml
	data, err := os.ReadFile(skillPath)
	if err != nil {
		return nil, fmt.Errorf("read skill.yml: %w", err)
	}

	// Parse YAML
	var skill Skill
	if err := yaml.Unmarshal(data, &skill); err != nil {
		return nil, fmt.Errorf("parse skill.yml: %w", err)
	}

	// Validate required fields
	if err := l.validate(&skill); err != nil {
		return nil, err
	}

	// Load external SQL files
	if err := l.loadSQLFiles(&skill, name); err != nil {
		return nil, err
	}

	return &skill, nil
}

// validate validates the skill configuration
func (l *Loader) validate(skill *Skill) error {
	if skill.Name == "" {
		return NewConfigError("name", "name is required")
	}
	if len(skill.On) == 0 {
		return NewConfigError("on", "at least one table must be specified")
	}
	return nil
}

// loadSQLFiles loads external SQL files referenced in stages and sinks
func (l *Loader) loadSQLFiles(skill *Skill, pluginName string) error {
	pluginDir := filepath.Join(l.pluginsDir, pluginName)

	// Load stage SQL files
	for i := range skill.Stages {
		stage := &skill.Stages[i]
		if stage.SQLFile != "" {
			sqlPath := filepath.Join(pluginDir, stage.SQLFile)
			data, err := os.ReadFile(sqlPath)
			if err != nil {
				return fmt.Errorf("read stage SQL file %s: %w", stage.SQLFile, ErrSQLFileNotFound)
			}
			stage.SQL = string(data)
		}
	}

	// Load sink SQL files
	for _, sinkType := range [][]SinkConfig{skill.Sinks.Insert, skill.Sinks.Update, skill.Sinks.Delete} {
		for i := range sinkType {
			sink := &sinkType[i]
			if sink.SQLFile != "" {
				sqlPath := filepath.Join(pluginDir, sink.SQLFile)
				data, err := os.ReadFile(sqlPath)
				if err != nil {
					return fmt.Errorf("read sink SQL file %s: %w", sink.SQLFile, ErrSQLFileNotFound)
				}
				sink.SQL = string(data)
			}
		}
	}

	return nil
}

// LoadAll loads all SQL plugins from the plugins directory
func (l *Loader) LoadAll() (map[string]*Skill, error) {
	plugins := make(map[string]*Skill)

	// Read plugin directory
	entries, err := os.ReadDir(l.pluginsDir)
	if err != nil {
		return nil, fmt.Errorf("read plugins dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		skill, err := l.Load(name)
		if err != nil {
			// Log error but continue loading other plugins
			fmt.Printf("Warning: failed to load plugin %s: %v\n", name, err)
			continue
		}

		plugins[name] = skill
	}

	return plugins, nil
}

// Exists checks if a plugin exists
func (l *Loader) Exists(name string) bool {
	skillPath := filepath.Join(l.pluginsDir, name, "skill.yml")
	_, err := os.Stat(skillPath)
	return err == nil
}
