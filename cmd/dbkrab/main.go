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

	"github.com/cnlangzi/dbkrab/api"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/offset"
	"github.com/cnlangzi/dbkrab/plugin"
	"github.com/cnlangzi/dbkrab/sink/sqlite"
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
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Connect to MSSQL
	connStr := fmt.Sprintf("server=%s;port=%d;user id=%s;password=%s;database=%s",
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

	if err := db.Ping(); err != nil {
		slog.Error("failed to ping MSSQL", "error", err)
		os.Exit(1)
	}

	slog.Info("connected to MSSQL",
		"user", cfg.MSSQL.User,
		"host", cfg.MSSQL.Host,
		"port", cfg.MSSQL.Port,
		"database", cfg.MSSQL.Database)

	// Create offset store
	offsetStore, err := offset.NewStoreFromConfig(cfg.Offset.Type, cfg.Offset.JSONPath, cfg.Offset.SQLitePath)
	if err != nil {
		slog.Error("failed to create offset store", "error", err)
		os.Exit(1)
	}
	slog.Info("offset store initialized", "type", cfg.Offset.Type)

	// Create sink
	var sink core.Sink
	switch cfg.Sink.Type {
	case "sqlite":
		sink, err = sqlite.NewSink(cfg.Sink.Path)
		if err != nil {
			slog.Error("failed to create SQLite sink", "error", err)
			os.Exit(1)
		}
		defer func() {
			if err := sink.Close(); err != nil {
				slog.Warn("sink.Close error", "error", err)
			}
		}()
		slog.Info("SQLite sink initialized", "path", cfg.Sink.Path)
	default:
		slog.Error("unknown sink type", "type", cfg.Sink.Type)
		os.Exit(1)
	}

	// Create DLQ (use same SQLite path as sink for simplicity)
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

	// Create poller with dynamic plugin support
	poller := core.NewPoller(cfg, db, sink, offsetStore, dlqStore)
	poller.SetHandler(core.PluginHandler(func(tx *core.Transaction) error {
		return pluginManager.Handle(tx)
	}))

	// Handle shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start API/Dashboard server
	apiServer := api.NewServerWithDLQ(pluginManager, dlqStore, *apiPort)
	go func() {
		slog.Info("Dashboard starting", "port", *apiPort, "url", fmt.Sprintf("http://localhost:%d", *apiPort))
		if err := apiServer.Start(); err != nil {
			slog.Warn("Dashboard stopped", "error", err)
		}
	}()

	// Watch plugin directory if configured
	if cfg.Plugin != "" {
		go func() {
			if err := pluginManager.Watch(ctx, cfg.Plugin); err != nil {
				slog.Warn("plugin watch error", "error", err)
			}
		}()
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
