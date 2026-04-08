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
	"sync/atomic"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// debounceEntry tracks pending debounce state for a single file
type debounceEntry struct {
	debounceAt time.Time
	modTime    time.Time
	size       int64
	version    string
}

// Plugin implements the plugin.Plugin interface for SQL plugins.
// Each SQL plugin wraps a skill (yaml config + SQL templates) with an execution engine.
// It supports hot-reloading when skill files change.
type Plugin struct {
	name    string
	loader  *Loader
	db      *sql.DB // database connection (not atomic, set at init/reload time)

	skill   atomic.Value // *Skill - atomic, lock-free read in Handle()
	engine  atomic.Value // *Engine - atomic, lock-free read in Handle()

	mu      sync.RWMutex // only for Reload() and metadata updates
	metadata PluginMetadata

	// internal watch fields (each plugin watches its own files)
	watchTicker *time.Ticker
	watchQuit   chan struct{}
	watchWg     sync.WaitGroup
	watchDir    string
	watchPending map[string]debounceEntry
}

// NewPlugin creates a new SQL plugin from a skill.
// Pass db=nil if you need to call AttachDB later.
func NewPlugin(name string, skill *Skill, loader *Loader, db *sql.DB) *Plugin {
	p := &Plugin{
		name:   name,
		loader: loader,
		db:     db,
		watchDir:    loader.pluginsDir,
		watchPending: make(map[string]debounceEntry),
	}

	// Initialize skill and engine atomically
	p.skill.Store(skill)
	if db != nil {
		p.engine.Store(NewEngine(skill, db))
	}

	// Initialize metadata
	p.initMetadata(skill)

	return p
}

// StartWatch starts the internal file watcher for this plugin.
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
	ymlPath := filepath.Join(p.watchDir, p.name+".yml")
	sqlFiles := p.getSQLFiles()

	now := time.Now()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if yml file was deleted
	if _, err := os.Stat(ymlPath); os.IsNotExist(err) {
		// File was deleted - trigger reload which will mark it as stale
		entry := p.watchPending[ymlPath]
		entry.debounceAt = now.Add(500 * time.Millisecond)
		entry.modTime = time.Time{}
		entry.size = 0
		p.watchPending[ymlPath] = entry
	} else {
		info, err := os.Stat(ymlPath)
		if err == nil {
			cur := p.watchPending[ymlPath]
			if info.ModTime().After(cur.modTime) || info.Size() != cur.size {
				cur.debounceAt = now.Add(500 * time.Millisecond)
				cur.modTime = info.ModTime()
				cur.size = info.Size()
				p.watchPending[ymlPath] = cur
			}
		}
	}

	// Check SQL files
	for _, sqlPath := range sqlFiles {
		if _, err := os.Stat(sqlPath); os.IsNotExist(err) {
			cur := p.watchPending[sqlPath]
			cur.debounceAt = now.Add(500 * time.Millisecond)
			cur.modTime = time.Time{}
			cur.size = 0
			p.watchPending[sqlPath] = cur
		} else {
			info, err := os.Stat(sqlPath)
			if err == nil {
				cur := p.watchPending[sqlPath]
				if info.ModTime().After(cur.modTime) || info.Size() != cur.size {
					cur.debounceAt = now.Add(500 * time.Millisecond)
					cur.modTime = info.ModTime()
					cur.size = info.Size()
					p.watchPending[sqlPath] = cur
				}
			}
		}
	}

	// Check if any debounce has expired
	for path, entry := range p.watchPending {
		if !entry.debounceAt.IsZero() && now.After(entry.debounceAt) {
			// Debounce expired - verify content actually changed
			newVersion, err := calcFileSHA256(path)
			if err != nil {
				// File might be gone or unreadable, clear debounce
				entry.debounceAt = time.Time{}
				p.watchPending[path] = entry
				continue
			}

			if newVersion != entry.version {
				// Content actually changed - trigger reload
				entry.version = newVersion
				entry.debounceAt = time.Time{}
				p.watchPending[path] = entry

				// Call reload outside the lock to avoid deadlock
				p.mu.Unlock()
				if err := p.Reload(); err != nil {
					fmt.Printf("Warning: failed to reload SQL plugin %s: %v\n", p.name, err)
				} else {
					fmt.Printf("Reloaded SQL plugin: %s\n", p.name)
				}
				p.mu.Lock()
			} else {
				// Hash unchanged, clear debounce
				entry.debounceAt = time.Time{}
				p.watchPending[path] = entry
			}
		}
	}
}

// getSQLFiles returns the list of SQL files referenced by this plugin's skill.
func (p *Plugin) getSQLFiles() []string {
	skill := p.skill.Load().(*Skill)
	if skill == nil {
		return nil
	}

	var sqlFiles []string
	seen := make(map[string]bool)

	for _, job := range skill.Jobs {
		if job.SQLFile != "" {
			sqlPath := filepath.Join(p.watchDir, job.SQLFile)
			if !seen[sqlPath] {
				sqlFiles = append(sqlFiles, sqlPath)
				seen[sqlPath] = true
			}
		}
	}

	for _, sinkType := range [][]SinkConfig{skill.Sinks.Insert, skill.Sinks.Update, skill.Sinks.Delete} {
		for _, sink := range sinkType {
			if sink.SQLFile != "" {
				sqlPath := filepath.Join(p.watchDir, sink.SQLFile)
				if !seen[sqlPath] {
					sqlFiles = append(sqlFiles, sqlPath)
					seen[sqlPath] = true
				}
			}
		}
	}

	return sqlFiles
}

// initMetadata initializes plugin metadata from a skill
func (p *Plugin) initMetadata(skill *Skill) {
	p.metadata = PluginMetadata{
		Name:        p.name,
		Type:        "sql",
		Status:      "loaded",
		NeedsReload: false,
		LoadCount:   1,
		Files:       make(map[string]FileMetadata),
	}

	// Track the main .yml file
	ymlPath := filepath.Join(p.loader.pluginsDir, p.name+".yml")
	if info, err := os.Stat(ymlPath); err == nil {
		version, _ := calcFileSHA256(ymlPath)
		p.metadata.Files[ymlPath] = FileMetadata{
			Path:        ymlPath,
			IsSQL:       false,
			CurVersion:  version,
			CurModTime:  info.ModTime(),
			CurSize:     info.Size(),
			CurLoadedAt: time.Now(),
			NeedsReload: false,
			IsDeleted:   false,
		}
	}

	// Track referenced .sql files
	if skill != nil {
		p.trackSQLFiles(skill)
	}
}

// trackSQLFiles adds metadata for SQL files referenced in the skill
func (p *Plugin) trackSQLFiles(skill *Skill) {
	skillDir := p.loader.pluginsDir

	for _, job := range skill.Jobs {
		if job.SQLFile != "" {
			sqlPath := filepath.Join(skillDir, job.SQLFile)
			p.addSQLFileMetadata(sqlPath)
		}
	}

	for _, sinkType := range [][]SinkConfig{skill.Sinks.Insert, skill.Sinks.Update, skill.Sinks.Delete} {
		for _, sink := range sinkType {
			if sink.SQLFile != "" {
				sqlPath := filepath.Join(skillDir, sink.SQLFile)
				p.addSQLFileMetadata(sqlPath)
			}
		}
	}
}

// addSQLFileMetadata adds metadata for a single SQL file
func (p *Plugin) addSQLFileMetadata(sqlPath string) {
	if _, exists := p.metadata.Files[sqlPath]; exists {
		return // already tracked
	}

	if info, err := os.Stat(sqlPath); err == nil {
		version, _ := calcFileSHA256(sqlPath)
		p.metadata.Files[sqlPath] = FileMetadata{
			Path:        sqlPath,
			IsSQL:       true,
			CurVersion:  version,
			CurModTime:  info.ModTime(),
			CurSize:     info.Size(),
			CurLoadedAt: time.Now(),
			NeedsReload: false,
			IsDeleted:   false,
		}
	}
}

// AttachDB sets the database connection for this plugin's engine.
func (p *Plugin) AttachDB(db *sql.DB) {
	p.db = db

	skill := p.skill.Load().(*Skill)
	if skill != nil && db != nil {
		p.engine.Store(NewEngine(skill, db))
	}
}

// Name implements plugin.Plugin
func (p *Plugin) Name() string { return p.name }

// Type implements plugin.Plugin
func (p *Plugin) Type() string { return "sql" }

// Status returns the current plugin status
func (p *Plugin) Status() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.metadata.Status
}

// Metadata returns a copy of the plugin metadata
func (p *Plugin) Metadata() PluginMetadata {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.metadata
}

// Reload reloads the plugin from disk, respecting the issue requirements:
// - Check if file was deleted → mark IsDeleted, keep old engine
// - Parse new file → if fail, set Status=error, LastError=err, keep old version
// - Calculate SHA256 hash for each file → compare with CurVersion
// - If no changes, set Status=loaded, skip
// - If changes detected, update metadata, atomic replace skill via atomic.Value.Store(), rebuild engine
func (p *Plugin) Reload() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	ymlPath := filepath.Join(p.loader.pluginsDir, p.name+".yml")

	// Check if main file was deleted
	if _, err := os.Stat(ymlPath); os.IsNotExist(err) {
		// File was deleted - mark as deleted and keep old engine
		p.metadata.Status = "stale"
		if existing, ok := p.metadata.Files[ymlPath]; ok {
			p.metadata.Files[ymlPath] = FileMetadata{
				Path:        ymlPath,
				IsSQL:       false,
				CurVersion:  existing.CurVersion,
				NeedsReload: false,
				IsDeleted:   true,
			}
		}
		return fmt.Errorf("skill file deleted: %s", ymlPath)
	}

	// Try to parse the new file
	newSkill, err := p.loader.Load(p.name)
	if err != nil {
		// Parse failed - keep old version, set error status
		p.metadata.Status = "error"
		p.metadata.LastError = err.Error()
		return fmt.Errorf("failed to reload skill %s: %w", p.name, err)
	}

	// Check if content actually changed by comparing SHA256 hashes
	curVersion := ""
	if existing, ok := p.metadata.Files[ymlPath]; ok {
		curVersion = existing.CurVersion
	}
	newVersion, err := calcFileSHA256(ymlPath)
	if err != nil {
		p.metadata.Status = "error"
		p.metadata.LastError = fmt.Sprintf("failed to hash file: %v", err)
		return err
	}

	if newVersion == curVersion {
		// No actual content change - just update status
		p.metadata.Status = "loaded"
		p.metadata.NeedsReload = false
		if existing, ok := p.metadata.Files[ymlPath]; ok {
			existing.NewVersion = ""
			existing.NeedsReload = false
			p.metadata.Files[ymlPath] = existing
		}
		return nil
	}

	// Content changed - update metadata and atomic replace
	if existing, ok := p.metadata.Files[ymlPath]; ok {
		p.metadata.Files[ymlPath] = FileMetadata{
			Path:         ymlPath,
			IsSQL:        false,
			CurVersion:   newVersion,
			CurModTime:   existing.CurModTime,
			CurSize:      existing.CurSize,
			CurLoadedAt:  time.Now(),
			NewVersion:   "",
			NeedsReload:  false,
			IsDeleted:    false,
		}
	} else {
		info, _ := os.Stat(ymlPath)
		var modTime time.Time
		var size int64
		if info != nil {
			modTime = info.ModTime()
			size = info.Size()
		}
		p.metadata.Files[ymlPath] = FileMetadata{
			Path:         ymlPath,
			IsSQL:        false,
			CurVersion:   newVersion,
			CurModTime:   modTime,
			CurSize:      size,
			CurLoadedAt:  time.Now(),
			NewVersion:   "",
			NeedsReload:  false,
			IsDeleted:    false,
		}
	}

	// Track any new/removed SQL files
	p.trackSQLFiles(newSkill)

	// Atomic replace skill
	p.skill.Store(newSkill)

	// Rebuild engine if db is available
	if p.db != nil {
		p.engine.Store(NewEngine(newSkill, p.db))
	}

	// Update metadata
	p.metadata.Status = "loaded"
	p.metadata.NeedsReload = false
	p.metadata.LastError = ""
	p.metadata.LoadCount++

	return nil
}

// Stop implements plugin.Plugin
func (p *Plugin) Stop() error {
	// Stop internal watcher
	p.StopWatch()
	// No resources to release for SQL plugins
	return nil
}

// Handle processes a CDC transaction through this SQL plugin.
// Uses atomic.Value for lock-free read of skill and engine.
func (p *Plugin) Handle(tx *core.Transaction) ([]core.Sink, error) {
	skill := p.skill.Load().(*Skill) // atomic.Value, lock-free read
	if skill == nil {
		return nil, fmt.Errorf("skill not loaded")
	}

	engine := p.engine.Load().(*Engine) // atomic.Value, lock-free read
	if engine == nil {
		return nil, fmt.Errorf("engine not initialized, call AttachDB first")
	}

	return engine.Handle(tx)
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
