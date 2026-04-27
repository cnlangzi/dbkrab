package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
	"github.com/cnlangzi/dbkrab/internal/replay"
	"github.com/cnlangzi/dbkrab/internal/sinker"
	"github.com/cnlangzi/dbkrab/internal/snapshot"
	internal_store "github.com/cnlangzi/dbkrab/internal/store"
	storeSQLite "github.com/cnlangzi/dbkrab/internal/store/sqlite"
	"github.com/cnlangzi/dbkrab/plugin"
	_ "github.com/denisenkom/go-mssqldb"
)

const configFile = "config.yml"

// Version and BuildTime are set via ldflags during build
var (
	Version   = "dev"
	BuildTime = "unknown"
)

func main() {
	// Load config
	cfg, warnings, err := config.Load(configFile)
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

	log.Printf("MSSQL: %s@%s:%d/%s pool:%d/%d lifetime:%v idle:%v",
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
		log.Println("CDC store:", cfg.App.DB.CDC)
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
	log.Println("Offset store:", cfg.App.DB.Offset)

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
	log.Println("DLQ:", cfg.App.DB.DLQ)

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
	log.Println("Monitor:", cfg.App.DB.Monitor)

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
	log.Printf("Sinker: %d databases", len(cfg.Sinks.ToMap()))

	// Create plugin manager
	pluginManager := plugin.NewManager(monitorDB)

	// Create config watcher for hot reload
	configWatcher, err := config.NewWatcher(configFile, cfg)
	if err != nil {
		slog.Error("failed to create config watcher", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := configWatcher.Stop(); err != nil {
			slog.Warn("error stopping config watcher", "error", err)
		}
	}()
	log.Println("Config:", configFile)

	// Create StateManager for process-level coordination
	stateManager := core.NewStateManager()

	// Create ChangeCapturer for CDC incremental polling
	changeCapturer, err := cdc.NewChangeCapturer(mssqlDB, cfg, offsetStore, appStore)
	if err != nil {
		slog.Error("failed to create ChangeCapturer", "error", err)
		os.Exit(1)
	}

	// Create snapshot Querier and capturer
	snapshotConfig := &snapshot.Config{BatchSize: cfg.Snapshot.BatchSize}
	snapshotQuerier := snapshot.NewQuerier(mssqlDB, config.ParseTimezone(cfg.MSSQL.Timezone), snapshotConfig)
	snapshotTables := snapshot.GetCDCTables(cfg)
	snapshotCapturer := snapshot.NewSnapshotCapturer(snapshotQuerier, snapshotTables, offsetStore)

	// Create replay capturer (reuses appStore which implements Store interface)
	replayCapturer := replay.NewReplayCapturer(appStore)

	// Create capturers map for Runtime
	capturers := map[core.CapturerName]core.Capturer{
		core.CapturerCDC:      changeCapturer,
		core.CapturerSnapshot: snapshotCapturer,
		core.CapturerReplay:   replayCapturer,
	}

	// Create Runtime for unified pipeline orchestration
	runtime := core.NewRuntime(capturers, pluginManager, dlqStore, monitorDB)

	// Start config watcher
	go configWatcher.Start()

	// Listen for config reload and update components
	go func() {
		for newCfg := range configWatcher.ReloadChan() {
			if newCfg == nil {
				continue
			}
			// Update sinker manager
			if newCfg.Sinks != nil {
				sinkerMgr.Configure(newCfg.Sinks.ToMap())
				slog.Info("sinker manager reconfigured", "databases", len(newCfg.Sinks.ToMap()))
			}
			// Update ChangeCapturer tables
			changeCapturer.UpdateTables(newCfg.Tables)
			// Update SnapshotCapturer tables
			snapshotCapturer.UpdateTables(snapshot.GetCDCTables(newCfg))
		}
	}()

	// Handle shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Create CDC admin
	cdcAdmin := cdcadmin.NewAdmin(&cfg.MSSQL)
	log.Println("CDC admin: ok")

	// Check and set CDC retention to 7 days
	retention, err := cdcAdmin.CheckAndSetCDCRetention()
	if err != nil {
		slog.Warn("failed to check/set CDC retention", "error", err)
	} else {
		retentionDays := float64(retention) / 1440
		log.Printf("CDC retention: %.1f days", retentionDays)
	}

	// Start API/Dashboard server
	apiPort := cfg.App.Listen
	if apiPort == 0 {
		apiPort = 9020 // fallback
	}
	apiServer := NewServer(pluginManager, dlqStore, cdcAdmin, appStore, sinkerMgr, monitorDB, apiPort, configFile, cfg, configWatcher, stateManager, nil, offsetStore, mssqlDB, runtime)
	go func() {
		log.Printf("Dashboard: http://localhost:%d", apiPort)
		if err := apiServer.Start(); err != nil {
			slog.Warn("Dashboard stopped", "error", err)
			panic(err)
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
		log.Println("shutting down...")
		cancel()
		changeCapturer.Stop()
		if err := apiServer.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "Dashboard stop error: %v\n", err)
		}
		log.Println("shutdown complete")
	}()

	// Start polling via Runtime
	log.Printf("dbkrab %s (built %s)", Version, BuildTime)
	log.Printf("CDC polling: %d tables", len(cfg.Tables))
	if err := runtime.Run(ctx); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "runtime error: %v\n", err)
		os.Exit(1)
	}
	log.Println("goodbye")
}
