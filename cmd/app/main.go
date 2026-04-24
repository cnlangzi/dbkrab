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

	"github.com/cnlangzi/dbkrab/internal/cdc"
	"github.com/cnlangzi/dbkrab/internal/cdcadmin"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/logging"
	"github.com/cnlangzi/dbkrab/internal/monitor"
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
	cfg, warnings, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logging first, before any other initialization
	if err := logging.Init(cfg.Logging); err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logging: %v\n", err)
		os.Exit(1)
	}

	// Emit config warnings now that logging is initialized
	for _, w := range warnings {
		slog.Warn(w)
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
	// Use config values with defaults
	maxOpenConns := cfg.MSSQL.PoolMaxOpenConns
	if maxOpenConns <= 0 {
		maxOpenConns = 30 // default
	}
	maxIdleConns := cfg.MSSQL.PoolMaxIdleConns
	if maxIdleConns <= 0 {
		maxIdleConns = 15 // default
	}
	connMaxLifetime := 30 * time.Minute
	if cfg.MSSQL.PoolConnMaxLifetime != "" {
		if d, err := time.ParseDuration(cfg.MSSQL.PoolConnMaxLifetime); err == nil {
			connMaxLifetime = d
		}
	}
	connMaxIdleTime := 5 * time.Minute
	if cfg.MSSQL.PoolConnMaxIdleTime != "" {
		if d, err := time.ParseDuration(cfg.MSSQL.PoolConnMaxIdleTime); err == nil {
			connMaxIdleTime = d
		}
	}
	mssqlDB.SetMaxOpenConns(maxOpenConns)
	mssqlDB.SetMaxIdleConns(maxIdleConns)
	mssqlDB.SetConnMaxLifetime(connMaxLifetime)
	mssqlDB.SetConnMaxIdleTime(connMaxIdleTime)

	if err := mssqlDB.Ping(); err != nil {
		slog.Error("failed to ping MSSQL", "error", err)
		os.Exit(1)
	}

		fmt.Fprintf(os.Stdout, "MSSQL: %s@%s:%d/%s pool:%d/%d lifetime:%v idle:%v\n",
		cfg.MSSQL.User, cfg.MSSQL.Host, cfg.MSSQL.Port, cfg.MSSQL.Database,
		maxOpenConns, maxIdleConns, connMaxLifetime, connMaxIdleTime)

	// Create CDC store DB and Offset DB as separate databases
	ctx := context.Background()

	var cdcDB *internal_store.DB
	var appStore internal_store.Store

	switch cfg.App.Type {
	case "sqlite":
		// Create CDC store DB (changes, poller_state) with buffered writer
		cdcDB, err = internal_store.New(ctx, internal_store.Config{
			File:       cfg.App.DB.CDC,
			ModuleName: "dbkrab-store",
		})
		if err != nil {
			slog.Error("failed to create CDC store SQLite DB", "error", err)
			os.Exit(1)
		}
		defer func() {
			if err := cdcDB.Close(); err != nil {
				slog.Warn("cdcDB.Close error", "error", err)
			}
		}()

		// Create store using the CDC DB
		appStore, err = storeSQLite.New(cdcDB)
		if err != nil {
			slog.Error("failed to create SQLite store", "error", err)
			os.Exit(1)
		}
		defer func() {
			if err := appStore.Close(); err != nil {
				slog.Warn("appStore.Close error", "error", err)
			}
		}()
			fmt.Fprintf(os.Stdout, "CDC store: %s\n", cfg.App.DB.CDC)
	default:
		slog.Error("unknown store type", "type", cfg.App.Type)
		os.Exit(1)
	}

	// Create offset store with its own separate DB
	offsetStore, err := offset.New(ctx, cfg.App.DB.Offset)
	if err != nil {
		slog.Error("failed to create offset store", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := offsetStore.Close(); err != nil {
			slog.Warn("offsetStore.Close error", "error", err)
		}
	}()
		fmt.Fprintf(os.Stdout, "Offset store: %s\n", cfg.App.DB.Offset)

	// Create DLQ with its own separate DB
	dlqStore, err := dlq.New(ctx, cfg.App.DB.DLQ)
	if err != nil {
		slog.Error("failed to create DLQ", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := dlqStore.Close(); err != nil {
			slog.Warn("dlqStore.Close error", "error", err)
		}
	}()
		fmt.Fprintf(os.Stdout, "DLQ: %s\n", cfg.App.DB.DLQ)

	// Create monitor DB
	monitorDB, err := monitor.New(ctx, cfg.App.DB.Monitor)
	if err != nil {
		slog.Error("failed to create monitor DB", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := monitorDB.Close(); err != nil {
			slog.Warn("monitorDB.Close error", "error", err)
		}
	}()
		fmt.Fprintf(os.Stdout, "Monitor: %s\n", cfg.App.DB.Monitor)

	// Create sinker manager
	sinkerMgr := sinker.NewManager()
	sinkerMgr.Configure(cfg.Sinks.ToMap())
	// Set MSSQL timezone for datetime conversion
	sinkerMgr.SetTimezone(config.ParseTimezone(cfg.MSSQL.Timezone))

	// Initialize all sinks and run migrations
	if err := sinkerMgr.InitAll(ctx); err != nil {
		slog.Warn("sinkerMgr.InitAll error", "error", err)
	}

	defer func() {
		if err := sinkerMgr.Close(); err != nil {
			slog.Warn("sinkerMgr.Close error", "error", err)
		}
	}()
		fmt.Fprintf(os.Stdout, "Sinker: %d databases\n", len(cfg.Sinks.ToMap()))

	// Create plugin manager
	pluginManager := plugin.NewManager(monitorDB)

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
		fmt.Fprintf(os.Stdout, "Config: %s\n", *configPath)

	// Create StateManager for process-level coordination
	stateManager := core.NewStateManager()

	// Create Runtime for unified pipeline orchestration
	runtime := core.NewRuntime(pluginManager, stateManager, offsetStore, appStore, dlqStore, monitorDB)

	// Create ChangeCapturer for CDC incremental polling
	cdcQuerier := cdc.NewQuerier(mssqlDB, config.ParseTimezone(cfg.MSSQL.Timezone))
	interval, _ := cfg.Interval()
	changeCapturer := cdc.NewChangeCapturer(cdcQuerier, cfg.Tables, offsetStore, interval)

	// Create poller with dynamic plugin support (kept for shutdown and CDC admin)
	poller := core.NewPoller(cfg, mssqlDB, pluginManager, appStore, offsetStore, dlqStore, monitorDB, stateManager)

	// Set config reload channel for hot reload
	poller.SetReloadChan(configWatcher.ReloadChan())

	// Start config watcher
	go configWatcher.Start()

	// Listen for config reload and update sinker manager
	go func() {
		for newCfg := range configWatcher.ReloadChan() {
			if newCfg != nil && newCfg.Sinks != nil {
				sinkerMgr.Configure(newCfg.Sinks.ToMap())
				slog.Info("sinker manager reconfigured", "databases", len(newCfg.Sinks.ToMap()))
			}
		}
	}()

	// Handle shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Create CDC admin
	cdcAdmin := cdcadmin.NewAdmin(&cfg.MSSQL)
		fmt.Fprintf(os.Stdout, "CDC admin: ok\n")

	// Check and set CDC retention to 7 days
	retention, err := cdcAdmin.CheckAndSetCDCRetention()
	if err != nil {
		slog.Warn("failed to check/set CDC retention", "error", err)
	} else {
		retentionDays := float64(retention) / 1440
		fmt.Fprintf(os.Stdout, "CDC retention: %.1f days\n", retentionDays)
	}

	// Start API/Dashboard server
	apiPort := cfg.App.Listen
	if apiPort == 0 {
		apiPort = 9020 // fallback
	}
	apiServer := NewServer(pluginManager, dlqStore, cdcAdmin, appStore, sinkerMgr, monitorDB, apiPort, *configPath, cfg, configWatcher, stateManager, poller, offsetStore, mssqlDB)
	go func() {
			fmt.Fprintf(os.Stdout, "Dashboard: http://localhost:%d\n", apiPort)
		if err := apiServer.Start(); err != nil {
			slog.Warn("Dashboard stopped", "error", err)
		}
	}()

	// Initialize SQL plugins
	if err := pluginManager.Init(ctx, mssqlDB, struct {
		Enabled bool
		Path    string
	}{
		Enabled: config.IsEnabled(cfg.Plugins.SQL.Enabled),
		Path:    cfg.Plugins.SQL.Path,
	}, cfg.Sinks.ToMap()); err != nil {
		slog.Warn("plugin initialization failed", "error", err)
	}

	go func() {
		<-sigCh
		fmt.Fprintf(os.Stdout, "shutting down...\n")
		cancel()
		poller.Stop()
		if err := apiServer.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "Dashboard stop error: %v\n", err)
		}
		fmt.Fprintf(os.Stdout, "shutdown complete\n")
	}()

// Start polling via Runtime
	fmt.Fprintf(os.Stdout, "dbkrab %s (built %s)\n", Version, BuildTime)
	fmt.Fprintf(os.Stdout, "CDC polling: %d tables\n", len(cfg.Tables))
	if err := runtime.Run(ctx, changeCapturer); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "runtime error: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "goodbye\n")
}
