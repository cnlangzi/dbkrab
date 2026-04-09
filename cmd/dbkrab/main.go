package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cnlangzi/dbkrab/api"
	"github.com/cnlangzi/dbkrab/internal/cdcadmin"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/logging"
	"github.com/cnlangzi/dbkrab/internal/offset"
	"github.com/cnlangzi/dbkrab/plugin"
	"github.com/cnlangzi/dbkrab/app/sqlite"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	configPath = flag.String("config", "config.yml", "Path to config file")
	apiPort    = flag.Int("api-port", 3000, "API/Dashboard server port (default: 3000)")

	// Version and BuildTime are set via ldflags during build
	Version   = "dev"
	BuildTime = "unknown"
)

func main() {
	flag.Parse()

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logging first, before any other initialization
	if err := logging.Init(cfg.Logging); err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logging: %v\n", err)
		os.Exit(1)
	}

	// Connect to MSSQL
	connStr := fmt.Sprintf("server=%s;port=%d;user id=%s;password=%s;database=%s;encrypt=disable",
		cfg.MSSQL.Host,
		cfg.MSSQL.Port,
		cfg.MSSQL.User,
		cfg.MSSQL.Password,
		cfg.MSSQL.Database,
	)

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		slog.Error("failed to connect to MSSQL", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.Warn("db.Close error", "error", err)
		}
	}()

	// Configure connection pool (P0-5: prevent connection exhaustion and stale connections)
	db.SetMaxOpenConns(10)                     // Max concurrent connections to MSSQL
	db.SetMaxIdleConns(5)                      // Keep warm connections for fast response
	db.SetConnMaxLifetime(30 * time.Minute)   // Recycle connections to prevent server-side timeouts
	db.SetConnMaxIdleTime(5 * time.Minute)    // Close idle connections

	if err := db.Ping(); err != nil {
		slog.Error("failed to ping MSSQL", "error", err)
		os.Exit(1)
	}

	slog.Info("connected to MSSQL",
		"user", cfg.MSSQL.User,
		"host", cfg.MSSQL.Host,
		"port", cfg.MSSQL.Port,
		"database", cfg.MSSQL.Database,
		"pool_max_open", 10,
		"pool_max_idle", 5,
		"pool_max_lifetime", "30m",
		"pool_max_idle_time", "5m")

	// Create offset store
	offsetStore, err := offset.NewStoreFromConfig(cfg.Offset.Type, cfg.Offset.JSONPath, cfg.Offset.SQLitePath)
	if err != nil {
		slog.Error("failed to create offset store", "error", err)
		os.Exit(1)
	}
	slog.Info("offset store initialized", "type", cfg.Offset.Type)

	// Create store
	var store *app.Store
	switch cfg.Sink.Type {
	case "sqlite":
		store, err = app.NewStore(cfg.Sink.Path)
		if err != nil {
			slog.Error("failed to create SQLite store", "error", err)
			os.Exit(1)
		}
		defer func() {
			if err := store.Close(); err != nil {
				slog.Warn("store.Close error", "error", err)
			}
		}()
		slog.Info("SQLite store initialized", "path", cfg.Sink.Path)
	default:
		slog.Error("unknown store type", "type", cfg.Sink.Type)
		os.Exit(1)
	}

	// Create DLQ (use same SQLite path as store for simplicity)
	dlqStore, err := dlq.New(cfg.Sink.Path)
	if err != nil {
		slog.Error("failed to create DLQ", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := dlqStore.CloseAndDB(); err != nil {
			slog.Warn("dlq.CloseAndDB error", "error", err)
		}
	}()
	slog.Info("dead letter queue initialized")

	// Create plugin manager
	pluginManager := plugin.NewManager()

	// Create config watcher for hot reload
	configWatcher, err := config.NewWatcher(*configPath, cfg)
	if err != nil {
		slog.Error("failed to create config watcher", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := configWatcher.Stop(); err != nil {
			slog.Warn("error stopping config watcher", "error", err)
			}
	}()
	slog.Info("config watcher initialized", "path", *configPath)

	// Create poller with dynamic plugin support
	poller := core.NewPoller(cfg, db, store, offsetStore, dlqStore)
	poller.SetHandler(core.PluginHandler(func(tx *core.Transaction) error {
		return pluginManager.Handle(tx)
	}))
	
	// Set config reload channel for hot reload
	poller.SetReloadChan(configWatcher.ReloadChan())

	// Start config watcher
	go configWatcher.Start()

	// Handle shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Create CDC admin
	cdcAdmin := cdcadmin.NewAdmin(&cfg.MSSQL)
	slog.Info("CDC admin initialized")

	// Check and set CDC retention to 7 days
	retention, err := cdcAdmin.CheckAndSetCDCRetention()
	if err != nil {
		slog.Warn("failed to check/set CDC retention", "error", err)
	} else {
		retentionDays := float64(retention) / 1440
		slog.Info("CDC retention configured",
			"retention_minutes", retention,
			"retention_days", retentionDays)
	}

	// Start API/Dashboard server
	apiServer := api.NewServerWithCDC(pluginManager, dlqStore, cdcAdmin, store, *apiPort, *configPath, cfg, configWatcher)
	go func() {
		slog.Info("Dashboard starting", "port", *apiPort, "url", fmt.Sprintf("http://localhost:%d", *apiPort))
		if err := apiServer.Start(); err != nil {
			slog.Warn("Dashboard stopped", "error", err)
		}
	}()

	// Initialize SQL plugins
	if err := pluginManager.Init(ctx, db, struct {
		Enabled       bool
		Path          string
		SinkConfigs map[string]any
	}{
		Enabled:   config.IsEnabled(cfg.Plugins.SQL.Enabled),
		Path:      cfg.Plugins.SQL.Path,
		SinkConfigs: nil, // passed via dbConfigs
	}, cfg.Sinks.Databases); err != nil {
		slog.Warn("plugin initialization failed", "error", err)
	}

	go func() {
		<-sigCh
		slog.Info("shutting down")
		cancel()
		poller.Stop()
		if err := apiServer.Stop(); err != nil {
			slog.Warn("Dashboard stop error", "error", err)
		}
	}()

	// Start polling
	slog.Info("starting dbkrab", "version", Version, "built", BuildTime)
	if err := poller.Start(ctx); err != nil && err != context.Canceled {
		slog.Error("poller error", "error", err)
		os.Exit(1)
	}

	slog.Info("goodbye")
}
