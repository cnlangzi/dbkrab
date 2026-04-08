// Package logging provides structured logging with slog and lumberjack file rotation.
package logging

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/natefinch/lumberjack"
)

// LoggingConfig defines the logging configuration structure.
type LoggingConfig struct {
	Level   string           `yaml:"level"` // debug|info|warn|error
	File    FileLogConfig    `yaml:"file"`
	Console ConsoleLogConfig `yaml:"console"`
}

// FileLogConfig defines file-based logging configuration.
type FileLogConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Path       string `yaml:"path"`         // ./logs/dbkrab.log
	MaxSizeMB  int    `yaml:"max_size_mb"`  // 100MB
	MaxAgeDays int    `yaml:"max_age_days"` // 7
	MaxFiles   int    `yaml:"max_files"`    // 4
	Compress   bool   `yaml:"compress"`     // gzip
}

// ConsoleLogConfig defines console logging configuration.
type ConsoleLogConfig struct {
	Enabled bool `yaml:"enabled"`
}

// Init initializes the global slog logger with console and file handlers using lumberjack.
func Init(cfg LoggingConfig) error {
	var handlers []slog.Handler

	// Console handler (text format for human readability)
	if cfg.Console.Enabled {
		opts := &slog.HandlerOptions{
			Level: parseLevel(cfg.Level),
		}
		handlers = append(handlers, slog.NewTextHandler(os.Stdout, opts))
	}

	// File handler (JSON format for structured logging)
	if cfg.File.Enabled {
		// Ensure log directory exists
		logDir := filepath.Dir(cfg.File.Path)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return err
		}

		lumberjackLogger := &lumberjack.Logger{
			Filename:   cfg.File.Path,
			MaxSize:    cfg.File.MaxSizeMB,
			MaxAge:     cfg.File.MaxAgeDays,
			MaxBackups: cfg.File.MaxFiles,
			Compress:   cfg.File.Compress,
		}

		opts := &slog.HandlerOptions{
			Level: parseLevel(cfg.Level),
		}
		handlers = append(handlers, slog.NewJSONHandler(lumberjackLogger, opts))
	}

	// Use the first handler as base, wrap with multi-handler if multiple
	var handler slog.Handler
	if len(handlers) == 1 {
		handler = handlers[0]
	} else {
		handler = newMultiHandler(handlers...)
	}

	slog.SetDefault(slog.New(handler))
	return nil
}

// multiHandler implements slog.Handler by dispatching to multiple handlers.
type multiHandler struct {
	handlers []slog.Handler
}

// newMultiHandler creates a handler that writes to all provided handlers.
func newMultiHandler(handlers ...slog.Handler) slog.Handler {
	return &multiHandler{handlers: handlers}
}

func (h *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, handler := range h.handlers {
		if err := handler.Handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (h *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithAttrs(attrs)
	}
	return &multiHandler{handlers: handlers}
}

func (h *multiHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithGroup(name)
	}
	return &multiHandler{handlers: handlers}
}

// parseLevel converts a level string to slog.Level.
func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
