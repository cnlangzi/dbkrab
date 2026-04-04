package config

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
)

// Watcher monitors config file changes and triggers safe reloads
type Watcher struct {
	path     string
	cfg      *Config
	watcher  *fsnotify.Watcher
	mu       sync.RWMutex
	done     chan struct{}
	stopOnce sync.Once
}

// NewWatcher creates a new config watcher
func NewWatcher(path string, initialCfg *Config) (*Watcher, error) {
	if initialCfg == nil {
		return nil, fmt.Errorf("initialCfg cannot be nil")
	}

	w := Watcher{
		path: path,
		cfg:  initialCfg,
		done: make(chan struct{}),
	}

	// Create fsnotify watcher
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create fsnotify watcher: %w", err)
	}
	w.watcher = fw

	// Watch the parent directory instead of the file itself so that
	// atomic renames/rotations of the config file (new inode) are still observed.
	dir := filepath.Dir(path)
	if err := w.watcher.Add(dir); err != nil {
		w.watcher.Close()
		return nil, fmt.Errorf("watch config dir %q: %w", dir, err)
	}

	return &w, nil
}

// Start begins watching for config changes
func (w *Watcher) Start(onReload func(*Config)) {
	go func() {
		defer func() {
			slog.Info("config watcher stopped", "path", w.path)
		}()

		eventsCh := w.watcher.Events
		errorsCh := w.watcher.Errors

		for {
			select {
			case <-w.done:
				return
			case event, ok := <-eventsCh:
				if !ok {
					// Channel closed, exit gracefully
					return
				}
				// Filter by filename to only handle events for the target config file
				if filepath.Base(event.Name) != filepath.Base(w.path) {
					continue
				}
				if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) || event.Has(fsnotify.Chmod) {
					slog.Info("config file changed, reloading...", "path", w.path)
					newCfg, err := Load(w.path)
					if err != nil {
						slog.Error("failed to reload config", "error", err)
						continue
					}

					// Apply safe reloads
					w.applySafeReload(newCfg, onReload)
				}
			case err, ok := <-errorsCh:
				if !ok {
					// Channel closed, exit gracefully
					return
				}
				if err != nil {
					slog.Warn("config watcher error", "error", err)
				}
			}
		}
	}()

	slog.Info("config watcher started", "path", w.path)
}

// applySafeReload applies non-disruptive config changes
func (w *Watcher) applySafeReload(newCfg *Config, onReload func(*Config)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check which fields changed and log warnings for changes that require restart
	if w.cfg.Interval != newCfg.Interval {
		slog.Warn("polling_interval changed, will apply on next cycle", "old", w.cfg.Interval, "new", newCfg.Interval)
	}
	if w.cfg.APIPort != newCfg.APIPort {
		slog.Warn("api_port change requires restart", "old", w.cfg.APIPort, "new", newCfg.APIPort)
	}
	if w.cfg.MSSQL.Host != newCfg.MSSQL.Host || w.cfg.MSSQL.Port != newCfg.MSSQL.Port {
		slog.Warn("MSSQL connection change requires restart", "old", fmt.Sprintf("%s:%d", w.cfg.MSSQL.Host, w.cfg.MSSQL.Port), "new", fmt.Sprintf("%s:%d", newCfg.MSSQL.Host, newCfg.MSSQL.Port))
	}
	if w.cfg.Sink.Path != newCfg.Sink.Path {
		slog.Warn("sink path change requires restart")
	}
	if w.cfg.Offset.Type != newCfg.Offset.Type {
		slog.Warn("offset type change requires restart")
	}

	// Update config
	w.cfg = newCfg

	// Trigger callback with new config
	if onReload != nil {
		onReload(newCfg)
	}
}

// Get returns a copy of the current config
// Callers must not modify the returned config; use config file edits and hot reload instead.
func (w *Watcher) Get() *Config {
	w.mu.RLock()
	defer w.mu.RUnlock()
	// Return a copy to prevent callers from mutating internal state
	cfgCopy := *w.cfg
	return &cfgCopy
}

// Stop stops the watcher
func (w *Watcher) Stop() error {
	w.stopOnce.Do(func() {
		close(w.done)
	})
	return w.watcher.Close()
}
