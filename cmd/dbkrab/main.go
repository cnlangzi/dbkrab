package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cnlangzi/dbkrab/api"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/plugin"
	"github.com/cnlangzi/dbkrab/sink/sqlite"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	configPath = flag.String("config", "config.yml", "Path to config file")
	apiPort    = flag.Int("api-port", 9020, "API server port")

	// Version and BuildTime are set via ldflags during build
	Version   = "dev"
	BuildTime = "unknown"
)

func main() {
	flag.Parse()

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
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
		log.Fatalf("Failed to connect to MSSQL: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("db.Close error: %v", err)
		}
	}()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping MSSQL: %v", err)
	}

	log.Printf("Connected to MSSQL: %s@%s:%d/%s", cfg.MSSQL.User, cfg.MSSQL.Host, cfg.MSSQL.Port, cfg.MSSQL.Database)

	// Create sink
	var sink core.Sink
	switch cfg.Sink.Type {
	case "sqlite":
		sink, err = sqlite.NewSink(cfg.Sink.Path)
		if err != nil {
			log.Fatalf("Failed to create SQLite sink: %v", err)
		}
		defer func() {
			if err := sink.Close(); err != nil {
				log.Printf("sink.Close error: %v", err)
			}
		}()
		log.Printf("SQLite sink initialized: %s", cfg.Sink.Path)
	default:
		log.Fatalf("Unknown sink type: %s", cfg.Sink.Type)
	}

	// Create plugin manager
	pluginManager := plugin.NewManager()

	// Create poller with dynamic plugin support
	poller := core.NewPoller(cfg, db, sink)
	poller.SetHandler(core.PluginHandler(func(tx *core.Transaction) error {
		return pluginManager.Handle(tx)
	}))

	// Handle shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start API server
	apiServer := api.NewServer(pluginManager, *apiPort)
	go func() {
		log.Printf("API server starting on port %d", *apiPort)
		if err := apiServer.Start(); err != nil {
			log.Printf("API server stopped: %v", err)
		}
	}()

	// Watch plugin directory if configured
	if cfg.Plugin != "" {
		go func() {
			if err := pluginManager.Watch(ctx, cfg.Plugin); err != nil {
				log.Printf("Plugin watch error: %v", err)
			}
		}()
	}

	go func() {
		<-sigCh
		log.Println("Shutting down...")
		cancel()
		poller.Stop()
		if err := apiServer.Stop(); err != nil {
			log.Printf("API server stop error: %v", err)
		}
	}()

	// Start polling
	log.Printf("Starting dbkrab %s (built %s)", Version, BuildTime)
	if err := poller.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Poller error: %v", err)
	}

	log.Println("Goodbye!")
}