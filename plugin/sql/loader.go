package sql

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Loader handles loading and parsing SQL plugin configurations.
// Deprecated: The New() function should be used instead for new code.
type Loader struct {
	pluginsDir string
}

// NewLoader creates a new SQL plugin loader
func NewLoader(pluginsDir string) *Loader {
	return &Loader{pluginsDir: pluginsDir}
}

// hashFile generates a 12-character ID from a file path using SHA256
func hashFile(file string) string {
	h := sha256.Sum256([]byte(file))
	return hex.EncodeToString(h[:])[:12]
}

// normalizeSQLParams converts all SQL parameter names (@xxx) to lowercase
// This ensures consistent parameter matching regardless of case in templates
// MSSQL parameter names are case-insensitive, so this is safe
var sqlParamRegex = regexp.MustCompile(`@([a-zA-Z_][a-zA-Z0-9_]*)`)

func normalizeSQLParams(sql string) string {
	return sqlParamRegex.ReplaceAllStringFunc(sql, func(match string) string {
		// Keep the @ symbol, lowercase the rest
		return "@" + strings.ToLower(match[1:])
	})
}

// normalizeSkillSQL normalizes all SQL in a skill's sinks
// Called after loading to handle inline SQL
func normalizeSkillSQL(skill *Skill) {
	for i := range skill.Sinks {
		sink := &skill.Sinks[i]
		if sink.SQL != "" {
			sink.SQL = normalizeSQLParams(sink.SQL)
		}
	}
}

// Test helper for testing SQL normalization
func NormalizeSQLForTest(sql string) string {
	return normalizeSQLParams(sql)
}

// Load loads and parses a SQL plugin from the given relative file path.
// The file parameter is a relative path from the plugins directory (e.g., "orders.yml" or "f9/orders.yml").
func (l *Loader) Load(file string) (*Skill, error) {
	root, err := os.OpenRoot(l.pluginsDir)
	if err != nil {
		return nil, fmt.Errorf("create root: %w", err)
	}

	// Validate that file path doesn't escape the root directory (prevents path traversal)
	if _, err := root.Stat(file); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrPluginNotFound, file)
		}
		return nil, fmt.Errorf("validate path: %w", err)
	}

	skillPath := filepath.Join(l.pluginsDir, file)

	// Check if skill.yml exists
	if _, err := os.Stat(skillPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrPluginNotFound, file)
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

	// Auto-assign File and Id
	skill.File = file
	skill.Id = hashFile(file)

	// Load external SQL files (skillDir is the directory containing the skill.yml)
	skillDir := filepath.Dir(skillPath)
	if err := l.loadSQLFiles(&skill, skillDir); err != nil {
		return nil, err
	}

	// Validate sinks configuration
	if err := skill.ValidateSinks(); err != nil {
		return nil, fmt.Errorf("validate sinks: %w", err)
	}

	// Populate Skill.Outputs by parsing sink SQL
	populateSkillOutputs(&skill)

	return &skill, nil
}

// loadSQLFiles loads external SQL files referenced in sinks
// All SQL parameter names (@xxx) are normalized to lowercase for consistency
func (l *Loader) loadSQLFiles(skill *Skill, skillDir string) error {
	// skillDir is passed in - it's the directory containing the skill.yml file
	// This allows skills to organize their SQL files in subdirectories if needed

	// Load sink SQL files (flat structure)
	for i := range skill.Sinks {
		sink := &skill.Sinks[i]
		if sink.SQLFile != "" {
			sqlPath := filepath.Join(skillDir, sink.SQLFile)
			data, err := os.ReadFile(sqlPath)
			if err != nil {
				return fmt.Errorf("read sink SQL file %s: %w", sink.SQLFile, ErrSQLFileNotFound)
			}
			// Normalize SQL parameter names to lowercase
			sink.SQL = normalizeSQLParams(string(data))
		}
	}

	return nil
}

// LoadAll loads all SQL plugins from the plugins directory and its subdirectories.
// It scans for *.yml files recursively and returns a map keyed by file path.
func (l *Loader) LoadAll() (map[string]*Skill, error) {
	plugins := make(map[string]*Skill)

	// Walk the plugins directory recursively for *.yml files
	err := filepath.Walk(l.pluginsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Only process .yml files
		if !strings.HasSuffix(info.Name(), ".yml") {
			return nil
		}

		// Get relative path from pluginsDir
		relPath, err := filepath.Rel(l.pluginsDir, path)
		if err != nil {
			return nil
		}

		skill, err := l.Load(relPath)
		if err != nil {
			// Log error but continue loading other plugins
			fmt.Printf("Warning: failed to load plugin %s: %v\n", relPath, err)
			return nil
		}

		plugins[relPath] = skill
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("walk plugins dir: %w", err)
	}

	return plugins, nil
}

// Exists checks if a plugin file exists at the given relative path
func (l *Loader) Exists(file string) bool {
	root, err := os.OpenRoot(l.pluginsDir)
	if err != nil {
		return false
	}

	// Validate that file path doesn't escape the root directory
	if _, err := root.Stat(file); err != nil {
		return false
	}

	skillPath := filepath.Join(l.pluginsDir, file)
	_, err = os.Stat(skillPath)
	return err == nil
}
