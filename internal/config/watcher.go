package config

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/fsnotify/fsnotify"
)

// Watcher monitors config file changes and triggers safe reloads
type Watcher struct {
	path    string
	cfg     *Config
	watcher *fsnotify.Watcher
	mu      sync.RWMutex
	done    chan struct{}
}

// NewWatcher creates a new config watcher
func NewWatcher(path string, initialCfg *Config) (*Watcher, error) {
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

	// Watch the config file
	if err := w.watcher.Add(path); err != nil {
		w.watcher.Close()
		return nil, fmt.Errorf("watch config file: %w", err)
	}

	return &w, nil
}

// Start begins watching for config changes
func (w *Watcher) Start(onReload func(*Config)) {
	go func() {
		for {
			select {
			case <-w.done:
				return
			case event := <-w.watcher.Events:
				if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
					slog.Info("config file changed, reloading...", "path", w.path)
					newCfg, err := Load(w.path)
					if err != nil {
						slog.Error("failed to reload config", "error", err)
						continue
					}

					// Apply safe reloads
					w.applySafeReload(newCfg, onReload)
				}
			case err := <-w.watcher.Errors:
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

// Get returns the current config
func (w *Watcher) Get() *Config {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cfg
}

// Stop stops the watcher
func (w *Watcher) Stop() error {
	close(w.done)
	return w.watcher.Close()
}
