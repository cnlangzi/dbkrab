package sql

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	// Flattened structure: skills/{name}.yml
	skillPath := filepath.Join(l.pluginsDir, name+".yml")

	// Check if skill.yml exists
	if _, err := os.Stat(skillPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrPluginNotFound, name)
		}
		return nil, fmt.Errorf("check skill file: %w", err)
	}

	// Read skill file
	data, err := os.ReadFile(skillPath)
	if err != nil {
		return nil, fmt.Errorf("read skill file: %w", err)
	}

	// Parse YAML
	var skill Skill
	if err := yaml.Unmarshal(data, &skill); err != nil {
		return nil, fmt.Errorf("parse skill file: %w", err)
	}

	// Validate required fields
	if err := l.validate(&skill); err != nil {
		return nil, err
	}

	// Load external SQL files (skillDir is the directory containing the skill.yml)
	skillDir := l.pluginsDir
	if err := l.loadSQLFiles(&skill, skillDir); err != nil {
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

// loadSQLFiles loads external SQL files referenced in jobs and sinks
func (l *Loader) loadSQLFiles(skill *Skill, skillDir string) error {
	// skillDir is passed in - for flat structure, it's skills/{skillName}
	// This allows skills to organize their SQL files in subdirectories if needed

	// Load job SQL files
	for _, jobType := range [][]SinkConfig{skill.Sinks.Insert, skill.Sinks.Update, skill.Sinks.Delete} {
		for i := range jobType {
			job := &jobType[i]
			if job.SQLFile != "" {
				sqlPath := filepath.Join(skillDir, job.SQLFile)
				data, err := os.ReadFile(sqlPath)
				if err != nil {
					return fmt.Errorf("read job SQL file %s: %w", job.SQLFile, ErrSQLFileNotFound)
				}
				job.SQL = string(data)
			}
		}
	}

	return nil
}

// LoadAll loads all SQL plugins from the plugins directory
func (l *Loader) LoadAll() (map[string]*Skill, error) {
	plugins := make(map[string]*Skill)

	// Read plugin directory for *.yml files (flattened structure)
	entries, err := os.ReadDir(l.pluginsDir)
	if err != nil {
		return nil, fmt.Errorf("read plugins dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Only process .yml files
		name := entry.Name()
		if !strings.HasSuffix(name, ".yml") {
			continue
		}

		// Extract skill name from filename (e.g., "enrich_orders.yml" -> "enrich_orders")
		skillName := strings.TrimSuffix(name, ".yml")

		skill, err := l.Load(skillName)
		if err != nil {
			// Log error but continue loading other plugins
			fmt.Printf("Warning: failed to load plugin %s: %v\n", skillName, err)
			continue
		}

		plugins[skillName] = skill
	}

	return plugins, nil
}

// Exists checks if a plugin exists
func (l *Loader) Exists(name string) bool {
	skillPath := filepath.Join(l.pluginsDir, name+".yml")
	_, err := os.Stat(skillPath)
	return err == nil
}
