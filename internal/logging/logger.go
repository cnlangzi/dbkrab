// Package logging provides structured logging with slog and lumberjack file rotation.
package logging

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/natefinch/lumberjack"
)

// LoggingConfig defines the logging configuration structure.
type LoggingConfig struct {
	Level      string `yaml:"level"`      // debug|info|warn|error
	Path       string `yaml:"path"`        // ./logs/dbkrab.log
	MaxSizeMB  int    `yaml:"max_size_mb"` // 100MB
	MaxAgeDays int    `yaml:"max_age_days"` // 7
	MaxFiles   int    `yaml:"max_files"`    // 4
	Compress   bool   `yaml:"compress"`     // gzip
}

// Init initializes the global slog logger with a lumberjack file handler.
// File logging is always enabled by default.
func Init(cfg LoggingConfig) error {
	// Ensure log directory exists
	logDir := filepath.Dir(cfg.Path)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	lumberjackLogger := &lumberjack.Logger{
		Filename:   cfg.Path,
		MaxSize:    cfg.MaxSizeMB,
		MaxAge:     cfg.MaxAgeDays,
		MaxBackups: cfg.MaxFiles,
		Compress:   cfg.Compress,
	}

	opts := &slog.HandlerOptions{
		Level: parseLevel(cfg.Level),
	}
	handler := slog.NewJSONHandler(lumberjackLogger, opts)
	slog.SetDefault(slog.New(handler))
	return nil
}

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
