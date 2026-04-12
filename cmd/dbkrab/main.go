package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cnlangzi/dbkrab/api"
	"github.com/cnlangzi/dbkrab/internal/cdcadmin"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/logging"
	"github.com/cnlangzi/dbkrab/internal/offset"
	"github.com/cnlangzi/dbkrab/internal/sinker"
	internal_store "github.com/cnlangzi/dbkrab/internal/store"
	storeSQLite "github.com/cnlangzi/dbkrab/internal/store/sqlite"
	"github.com/cnlangzi/dbkrab/plugin"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	configPath = flag.String("config", "config.yml", "Path to config file")

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

	mssqlDB, err := sql.Open("sqlserver", connStr)
	if err != nil {
		slog.Error("failed to connect to MSSQL", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := mssqlDB.Close(); err != nil {
			slog.Warn("mssqlDB.Close error", "error", err)
		}
	}()

	// Configure connection pool (P0-5: prevent connection exhaustion and stale connections)
	mssqlDB.SetMaxOpenConns(10)                     // Max concurrent connections to MSSQL
	mssqlDB.SetMaxIdleConns(5)                      // Keep warm connections for fast response
	mssqlDB.SetConnMaxLifetime(30 * time.Minute)   // Recycle connections to prevent server-side timeouts
	mssqlDB.SetConnMaxIdleTime(5 * time.Minute)    // Close idle connections

	if err := mssqlDB.Ping(); err != nil {
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

	// Create unified app DB with migrations
	ctx := context.Background()

	// Resolve migration path - default to embedded migrations
	migrationPath := cfg.App.MigrationPath
	if migrationPath == "" {
		// Default: look for migrations relative to executable
		execPath, err := os.Executable()
		if err != nil {
			slog.Error("failed to get executable path", "error", err)
			os.Exit(1)
		}
		migrationPath = filepath.Join(filepath.Dir(execPath), "internal", "store", "migrations")
		if _, err := os.Stat(migrationPath); os.IsNotExist(err) {
			// Fallback: try relative to current working directory
			migrationPath = "./internal/store/migrations"
		}
	}

	var appDB *internal_store.DB
	var store internal_store.Store

	switch cfg.App.Type {
	case "sqlite":
		appDB, err = internal_store.New(ctx, internal_store.Config{
			File:          cfg.App.Path,
			ModuleName:    "dbkrab-store",
			MigrationPath: migrationPath,
		})
		if err != nil {
			slog.Error("failed to create unified SQLite DB", "error", err)
			os.Exit(1)
		}
		defer func() {
			if err := appDB.Close(); err != nil {
				slog.Warn("appDB.Close error", "error", err)
			}
		}()

		// Create store using the unified DB
		store, err = storeSQLite.NewStore(appDB)
		if err != nil {
			slog.Error("failed to create SQLite store", "error", err)
			os.Exit(1)
		}
		defer func() {
			if err := store.Close(); err != nil {
				slog.Warn("store.Close error", "error", err)
			}
		}()
		slog.Info("unified SQLite store initialized", "path", cfg.App.Path, "migrations", migrationPath)
	default:
		slog.Error("unknown store type", "type", cfg.App.Type)
		os.Exit(1)
	}

	// Create offset store using the unified DB
	offsetStore := offset.NewUnifiedStore(appDB)
	slog.Info("offset store initialized (unified)", "type", "sqlite")

	// Create DLQ using the unified DB
	dlqStore, err := dlq.NewWithStoreDB(appDB)
	if err != nil {
		slog.Error("failed to create DLQ", "error", err)
		os.Exit(1)
	}
	// Note: DLQ shares the unified DB, so we don't close it separately
	slog.Info("dead letter queue initialized (unified)")

	// Create sinker manager
	sinkerMgr := sinker.NewManager()
	sinkerMgr.Configure(cfg.Sinks.ToMap())
	defer func() {
		if err := sinkerMgr.Close(); err != nil {
			slog.Warn("sinkerMgr.Close error", "error", err)
		}
	}()
	slog.Info("sinker manager initialized")

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
	poller := core.NewPoller(cfg, mssqlDB, store, offsetStore, dlqStore)
	poller.SetHandler(core.PluginHandler(func(ctx context.Context, tx *core.Transaction) error {
		return pluginManager.Handle(ctx, tx)
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
	apiPort := cfg.App.Listen
	if apiPort == 0 {
		apiPort = 9020 // fallback
	}
	apiServer := api.NewServerWithCDCAndMetrics(pluginManager, dlqStore, cdcAdmin, store, sinkerMgr, apiPort, *configPath, cfg, configWatcher, poller)
	go func() {
		slog.Info("Dashboard starting", "port", apiPort, "url", fmt.Sprintf("http://localhost:%d", apiPort))
		if err := apiServer.Start(); err != nil {
			slog.Warn("Dashboard stopped", "error", err)
		}
	}()

	// Initialize SQL plugins
	if err := pluginManager.Init(ctx, mssqlDB, struct {
		Enabled       bool
		Path          string
		SinkConfigs map[string]any
	}{
		Enabled:   config.IsEnabled(cfg.Plugins.SQL.Enabled),
		Path:      cfg.Plugins.SQL.Path,
		SinkConfigs: nil, // passed via dbConfigs
	}, cfg.Sinks.ToMap()); err != nil {
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
