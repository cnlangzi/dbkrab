package config

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
)

// Watcher monitors config file changes and signals safe reloads
type Watcher struct {
	path     string
	cfg      *Config
	watcher  *fsnotify.Watcher
	reloadCh chan *Config // Channel to signal config reload
	mu       sync.RWMutex
	done     chan struct{}
	stopOnce sync.Once
}

// NewWatcher creates a new config watcher
func NewWatcher(path string, initialCfg *Config) (*Watcher, error) {
	if initialCfg == nil {
		return nil, fmt.Errorf("initialCfg cannot be nil")
	}

	// Create reload channel (buffered to avoid blocking on send)
	reloadCh := make(chan *Config, 1)

	w := Watcher{
		path:     path,
		cfg:      initialCfg,
		reloadCh: reloadCh,
		done:     make(chan struct{}),
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
		_ = w.watcher.Close()
		return nil, fmt.Errorf("watch config dir %q: %w", dir, err)
	}

	return &w, nil
}

// ReloadChan returns the channel that signals config reload
// Poller should listen on this channel and apply config at transaction boundary
func (w *Watcher) ReloadChan() <-chan *Config {
	return w.reloadCh
}

// Start begins watching for config changes
func (w *Watcher) Start() {
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

					// Log warnings for fields that require restart
					w.logRestartWarnings(newCfg)

					// Update internal config reference
					w.mu.Lock()
					w.cfg = newCfg
					w.mu.Unlock()

					// Signal reload via channel, always keeping the most recent config.
					// If a reload is already pending, drop the older one and enqueue the new config.
					select {
					case w.reloadCh <- newCfg:
						slog.Info("config reload signaled")
					default:
						// Channel full: drain pending reload (older config) and send new one.
						// Use blocking send after drain to guarantee new config is enqueued.
						select {
						case <-w.reloadCh:
							slog.Info("dropped pending reload in favor of newer config")
						default:
						}
						w.reloadCh <- newCfg // blocking send, guaranteed success after drain
						slog.Info("config reload signaled (replaced pending)")
					}
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

// logRestartWarnings logs warnings for config changes that require restart
func (w *Watcher) logRestartWarnings(newCfg *Config) {
	w.mu.RLock()
	oldCfg := w.cfg
	w.mu.RUnlock()

	if oldCfg.MSSQL.Host != newCfg.MSSQL.Host || oldCfg.MSSQL.Port != newCfg.MSSQL.Port || oldCfg.MSSQL.Database != newCfg.MSSQL.Database {
		slog.Warn("MSSQL connection change requires restart",
			"old", fmt.Sprintf("%s:%d/%s", oldCfg.MSSQL.Host, oldCfg.MSSQL.Port, oldCfg.MSSQL.Database),
			"new", fmt.Sprintf("%s:%d/%s", newCfg.MSSQL.Host, newCfg.MSSQL.Port, newCfg.MSSQL.Database))
	}
	if oldCfg.MSSQL.User != newCfg.MSSQL.User || oldCfg.MSSQL.Password != newCfg.MSSQL.Password {
		slog.Warn("MSSQL credentials change requires restart",
			"old_user", oldCfg.MSSQL.User,
			"new_user", newCfg.MSSQL.User)
	}
	if oldCfg.App.DB != newCfg.App.DB || oldCfg.App.DLQ != newCfg.App.DLQ {
		slog.Warn("app DB path change requires restart", "old_db", oldCfg.App.DB, "new_db", newCfg.App.DB, "old_dlq", oldCfg.App.DLQ, "new_dlq", newCfg.App.DLQ)
	}
	if oldCfg.CDC.Offset.Type != newCfg.CDC.Offset.Type {
		slog.Warn("offset type change requires restart", "old", oldCfg.CDC.Offset.Type, "new", newCfg.CDC.Offset.Type)
	}
	if oldCfg.App.Listen != newCfg.App.Listen {
		slog.Warn("api_port change requires restart", "old", oldCfg.App.Listen, "new", newCfg.App.Listen)
	}
}

// Get returns the current config (for external queries)
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
	if err := w.watcher.Close(); err != nil {
		slog.Warn("error closing watcher", "error", err)
		return err
	}
	return nil
}
