package sql

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"gopkg.in/yaml.v3"
)

// debounceEntry tracks pending debounce state for a single file
type debounceEntry struct {
	debounceAt time.Time
	modTime    time.Time
	size       int64
	version    string
}

// Plugin implements the plugin.Plugin interface for SQL plugins.
// It manages multiple skills (yaml configs + SQL templates) with a shared execution engine.
// It supports hot-reloading when skill files change.
// Sink writing is handled by the platform layer based on the Database field in returned sinks.
type Plugin struct {
	name     string
	db       *sql.DB // database connection (set at init/reload time)
	engine   *Engine // shared, stateless engine
	watchDir string
	Skills   *Skills // thread-safe collection of skills

	mu sync.RWMutex // only for metadata updates

	// internal watch fields
	watchTicker  *time.Ticker
	watchQuit    chan struct{}
	watchWg      sync.WaitGroup
	watchPending map[string]debounceEntry
}

// New creates a new SQL plugin that manages multiple skills.
// Pass db=nil if you need to call AttachDB later.
// Sink writing is handled by the platform layer - this plugin only handles SQL execution.
func New(path string, db *sql.DB) *Plugin {
	p := &Plugin{
		name:       "sql",
		db:         db,
		watchDir:   path,
		Skills:     NewSkills(),
		watchPending: make(map[string]debounceEntry),
	}

	// Load all skills from the directory
	if err := p.loadAllSkills(); err != nil {
		fmt.Printf("Warning: failed to load initial skills: %v\n", err)
	}

	// Initialize engine with first skill (or nil if no skills)
	skills := p.Skills.List()
	if db != nil && len(skills) > 0 {
		p.engine = NewEngine(skills[0], db)
	} else if db != nil {
		p.engine = NewEngine(nil, db)
	}

	return p
}

// loadAllSkills loads all skills from the skills directory.
func (p *Plugin) loadAllSkills() error {
	entries, err := os.ReadDir(p.watchDir)
	if err != nil {
		return fmt.Errorf("read skills directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yml" {
			continue
		}

		filePath := filepath.Join(p.watchDir, entry.Name())
		skill, err := p.loadSkillFile(filePath)
		if err != nil {
			fmt.Printf("Warning: failed to load skill %s: %v\n", entry.Name(), err)
			continue
		}

		p.Skills.Set(skill)
	}

	return nil
}

// loadSkillFile loads a single skill file and returns the skill.
func (p *Plugin) loadSkillFile(filePath string) (*Skill, error) {
	// Read skill file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read skill file: %w", err)
	}

	// Calculate ID from file path
	id := hashFile(filePath)

	// Parse YAML
	var skill Skill
	if err := yaml.Unmarshal(data, &skill); err != nil {
		return nil, fmt.Errorf("parse skill file: %w", err)
	}

	// Set File and Id
	relPath, err := filepath.Rel(p.watchDir, filePath)
	if err != nil {
		return nil, fmt.Errorf("get relative path: %w", err)
	}
	skill.File = relPath
	skill.Id = id

	// Load external SQL files
	skillDir := filepath.Dir(filePath)
	if err := loadSkillSQLFiles(&skill, skillDir); err != nil {
		return nil, err
	}

	// Validate sinks configuration
	if err := skill.ValidateSinks(); err != nil {
		return nil, fmt.Errorf("validate sinks: %w", err)
	}

	return &skill, nil
}

// loadSkillSQLFiles loads external SQL files referenced in sinks
func loadSkillSQLFiles(skill *Skill, skillDir string) error {
	for i := range skill.Sinks {
		sink := &skill.Sinks[i]
		if sink.SQLFile != "" {
			sqlPath := filepath.Join(skillDir, sink.SQLFile)
			data, err := os.ReadFile(sqlPath)
			if err != nil {
				return fmt.Errorf("read sink SQL file %s: %w", sink.SQLFile, ErrSQLFileNotFound)
			}
			sink.SQL = string(data)
		}
	}
	return nil
}

// calcFileSHA256 calculates SHA256 hash of file content
func calcFileSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// StartWatch starts the internal file watcher for skill files.
func (p *Plugin) StartWatch() {
	p.watchQuit = make(chan struct{})
	p.watchTicker = time.NewTicker(1 * time.Second)
	p.watchWg.Add(1)
	go p.watchLoop()
}

// StopWatch stops the internal file watcher.
func (p *Plugin) StopWatch() {
	if p.watchQuit == nil {
		return
	}
	close(p.watchQuit)
	p.watchTicker.Stop()
	p.watchWg.Wait()
	p.watchQuit = nil
}

// watchLoop polls for file changes every second.
func (p *Plugin) watchLoop() {
	defer p.watchWg.Done()

	for {
		select {
		case <-p.watchQuit:
			return
		case <-p.watchTicker.C:
			p.checkChanges()
		}
	}
}

// checkChanges checks for file changes and triggers reload after debounce.
func (p *Plugin) checkChanges() {
	now := time.Now()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Get current file list
	currentFiles := make(map[string]bool)
	entries, err := os.ReadDir(p.watchDir)
	if err != nil {
		return
	}
	// Check for deleted or new files
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yml" {
			continue
		}
		filePath := filepath.Join(p.watchDir, entry.Name())
		currentFiles[filePath] = true

		info, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		cur := p.watchPending[filePath]
		if info.ModTime().After(cur.modTime) || info.Size() != cur.size {
			cur.debounceAt = now.Add(500 * time.Millisecond)
			cur.modTime = info.ModTime()
			cur.size = info.Size()
			p.watchPending[filePath] = cur
		}
	}

	// Check for files that no longer exist
	for filePath := range p.watchPending {
		relName := filepath.Base(filePath)
		if !currentFiles[relName] && relName != "" {
			// File was deleted
			cur := p.watchPending[filePath]
			cur.debounceAt = now.Add(500 * time.Millisecond)
			cur.modTime = time.Time{}
			cur.size = 0
			p.watchPending[filePath] = cur
		}
	}

	// Check if any debounce has expired
	for path, entry := range p.watchPending {
		if !entry.debounceAt.IsZero() && now.After(entry.debounceAt) {
			// Debounce expired - check if file exists
			if _, err := os.Stat(path); os.IsNotExist(err) {
				// File was deleted - remove skill
				entry.debounceAt = time.Time{}
				p.watchPending[path] = entry

				id := hashFile(path)
				p.Skills.Delete(id)
				fmt.Printf("Removed skill: %s\n", path)
				continue
			}

			// Verify content actually changed
			newVersion, err := calcFileSHA256(path)
			if err != nil {
				entry.debounceAt = time.Time{}
				p.watchPending[path] = entry
				continue
			}

			if newVersion != entry.version {
				// Content changed - reload skill
				entry.version = newVersion
				entry.debounceAt = time.Time{}
				p.watchPending[path] = entry

				skill, err := p.loadSkillFile(path)
				if err != nil {
					fmt.Printf("Warning: failed to reload skill %s: %v\n", path, err)
					continue
				}

				p.Skills.Set(skill)
				fmt.Printf("Reloaded skill: %s (id: %s)\n", path, skill.Id)
			} else {
				// Hash unchanged, clear debounce
				entry.debounceAt = time.Time{}
				p.watchPending[path] = entry
			}
		}
	}
}

// AttachDB sets the database connection for this plugin's engine.
func (p *Plugin) AttachDB(db *sql.DB) {
	p.db = db

	if db != nil {
		skills := p.Skills.List()
		if len(skills) > 0 {
			p.engine = NewEngine(skills[0], db)
		} else {
			p.engine = NewEngine(nil, db)
		}
	}
}

// Name implements plugin.Plugin
func (p *Plugin) Name() string { return p.name }

// Type implements plugin.Plugin
func (p *Plugin) Type() string { return "sql" }

// Stop implements plugin.Plugin
func (p *Plugin) Stop() error {
	p.StopWatch()
	return nil
}

// matchTable checks if a skill matches the given table name.
// A skill matches if its "on" list contains the table, or if "on" is empty (matches all).
func (p *Plugin) matchTable(skill *Skill, table string) bool {
	if len(skill.On) == 0 {
		return true
	}
	for _, t := range skill.On {
		if t == table {
			return true
		}
	}
	return false
}

// Handle processes a CDC transaction through all SQL skills.
// It iterates over all skills, checks if they match the transaction table,
// and executes the engine for each matching skill.
func (p *Plugin) Handle(tx *core.Transaction) ([]core.Sink, error) {
	if tx == nil || len(tx.Changes) == 0 {
		return nil, nil
	}

	if p.engine == nil {
		return nil, fmt.Errorf("engine not initialized, call AttachDB first")
	}

	var allSinks []core.Sink

	// Get table from first change (all changes in a tx are for the same table)
	var table string
	if len(tx.Changes) > 0 {
		table = tx.Changes[0].Table
	}

	// Iterate over all skills
	for _, skill := range p.Skills.List() { // internal RLock
		if !p.matchTable(skill, table) {
			continue
		}

		sinks, err := p.engine.HandleWithSkill(tx, skill)
		if err != nil {
			return nil, fmt.Errorf("skill %s handle: %w", skill.Name, err)
		}

		allSinks = append(allSinks, sinks...)
	}

	return allSinks, nil
}
