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

	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/sink/sqlite"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	configPath = flag.String("config", "config.yml", "Path to config file")
	version    = "dev"
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
	defer db.Close()

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
		defer sink.Close()
		log.Printf("SQLite sink initialized: %s", cfg.Sink.Path)
	default:
		log.Fatalf("Unknown sink type: %s", cfg.Sink.Type)
	}

	// Create poller
	poller := core.NewPoller(cfg, db, sink)

	// Handle shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")
		cancel()
		poller.Stop()
	}()

	// Start polling
	log.Printf("Starting dbkrab %s", version)
	if err := poller.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Poller error: %v", err)
	}

	log.Println("Goodbye!")
}