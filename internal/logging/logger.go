// Package logging provides structured logging with slog and lumberjack file rotation.
package logging

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/natefinch/lumberjack"
)

// LoggingConfig defines the logging configuration structure.
type LoggingConfig struct {
	Level   string          `yaml:"level"`   // debug|info|warn|error
	Format  string          `yaml:"format"`  // text|json
	File    FileLogConfig   `yaml:"file"`
	Console ConsoleLogConfig `yaml:"console"`
}

// FileLogConfig defines file-based logging configuration.
type FileLogConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Path       string `yaml:"path"`        // ./logs/dbkrab.log
	MaxSizeMB  int    `yaml:"max_size_mb"` // 100MB
	MaxAgeDays int    `yaml:"max_age_days"` // 7
	MaxFiles   int    `yaml:"max_files"`   // 4
	Compress   bool   `yaml:"compress"`    // gzip
}

// ConsoleLogConfig defines console logging configuration.
type ConsoleLogConfig struct {
	Enabled bool `yaml:"enabled"`
}

// DefaultLoggingConfig returns the default logging configuration.
func DefaultLoggingConfig() LoggingConfig {
	return LoggingConfig{
		Level:  "info",
		Format: "json",
		File: FileLogConfig{
			Enabled:    true,
			Path:       "./logs/dbkrab.log",
			MaxSizeMB:  100,
			MaxAgeDays: 7,
			MaxFiles:   4,
			Compress:   true,
		},
		Console: ConsoleLogConfig{
			Enabled: true,
		},
	}
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
		handler = slog.NewMultiHandler(handlers...)
	}

	slog.SetDefault(slog.New(handler))
	return nil
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

// WriterAdapter wraps lumberjack.Logger to implement io.Writer for compatibility.
type WriterAdapter struct {
	*lumberjack.Logger
}

var _ io.Writer = (*WriterAdapter)(nil)

// Write implements io.Writer.
func (w *WriterAdapter) Write(p []byte) (n int, err error) {
	return w.Logger.Write(p)
}
