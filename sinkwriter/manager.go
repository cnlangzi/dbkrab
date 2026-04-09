package sinkwriter

import (
	"fmt"
	"sync"

	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
)

// Manager manages SinkWriters and routes sink operations to appropriate writers.
type Manager struct {
	writers   map[string]SinkWriter // keyed by database name
	dbConfigs map[string]config.DatabaseConfig
	mu        sync.RWMutex
}

// NewManager creates a new SinkWriter manager
func NewManager() *Manager {
	return &Manager{
		writers:   make(map[string]SinkWriter),
		dbConfigs: make(map[string]config.DatabaseConfig),
	}
}

// Configure configures the manager with database configurations
func (m *Manager) Configure(dbConfigs map[string]config.DatabaseConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dbConfigs = dbConfigs
}

// GetWriter returns a SinkWriter for the given database name, creating one if needed
func (m *Manager) GetWriter(dbName string) (SinkWriter, error) {
	m.mu.RLock()
	writer, exists := m.writers[dbName]
	m.mu.RUnlock()

	if exists {
		return writer, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if writer, exists = m.writers[dbName]; exists {
		return writer, nil
	}

	// Look up database config
	dbConfig, exists := m.dbConfigs[dbName]
	if !exists {
		return nil, fmt.Errorf("database %s not configured", dbName)
	}

	// Create appropriate writer based on type
	switch dbConfig.Type {
	case "sqlite":
		writer = nil // Will be created below
	default:
		// For now, only sqlite is implemented
		// DuckDB, MSSQL, etc. would be added here
		return nil, fmt.Errorf("unsupported database type: %s", dbConfig.Type)
	}

	// Create SQLite writer
	if dbConfig.Type == "sqlite" {
		path := dbConfig.Path
		if path == "" {
			// Default path for SQLite
			path = fmt.Sprintf("./data/sinks/%s.db", dbName)
		}
		sw, err := NewSQLiteWriter(dbName, dbConfig.Type, path, dbConfig.MigrationPath)
		if err != nil {
			return nil, fmt.Errorf("create sqlite writer: %w", err)
		}
		writer = sw

		// Run initial migrations
		if err := sw.RunMigrations(); err != nil {
			return nil, fmt.Errorf("run migrations: %w", err)
		}
	}

	m.writers[dbName] = writer
	return writer, nil
}

// WriteRoutes sink operations to appropriate writers based on Database field
func (m *Manager) Write(sinks []core.Sink) error {
	if len(sinks) == 0 {
		return nil
	}

	// Group sinks by database
	sinksByDB := make(map[string][]core.Sink)
	for _, sink := range sinks {
		dbName := sink.Config.Database
		if dbName == "" {
			return fmt.Errorf("sink %s has no database configured", sink.Config.Name)
		}
		sinksByDB[dbName] = append(sinksByDB[dbName], sink)
	}

	// Write to each database
	for dbName, dbSinks := range sinksByDB {
		writer, err := m.GetWriter(dbName)
		if err != nil {
			return fmt.Errorf("get writer for %s: %w", dbName, err)
		}

		if err := writer.Write(dbSinks); err != nil {
			return fmt.Errorf("write to %s: %w", dbName, err)
		}
	}

	return nil
}

// Close closes all writers
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for dbName, writer := range m.writers {
		if err := writer.Close(); err != nil {
			lastErr = fmt.Errorf("close writer %s: %w", dbName, err)
		}
	}

	m.writers = make(map[string]SinkWriter)
	return lastErr
}

// ListDatabases returns all configured database names
func (m *Manager) ListDatabases() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.dbConfigs))
	for name := range m.dbConfigs {
		names = append(names, name)
	}
	return names
}
