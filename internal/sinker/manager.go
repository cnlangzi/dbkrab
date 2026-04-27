package sinker

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/monitor"
	sinkSqlite "github.com/cnlangzi/dbkrab/internal/sinker/sqlite"
)

// Manager manages Sinkers and routes sink operations to appropriate sinkers.
type Manager struct {
	sinkers   map[string]Sinker // keyed by database name
	dbConfigs map[string]config.SinkConfig
	mu        sync.RWMutex
	timezone  *time.Location // MSSQL timezone for datetime conversion
}

// NewManager creates a new Sinker manager
func NewManager() *Manager {
	return &Manager{
		sinkers:   make(map[string]Sinker),
		dbConfigs: make(map[string]config.SinkConfig),
		timezone:  time.Local, // Default to local timezone
	}
}

// Configure configures the manager with database configurations
func (m *Manager) Configure(dbConfigs map[string]config.SinkConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dbConfigs = dbConfigs
}

// InitAll initializes all configured sinks and runs their migrations.
// This should be called during startup to ensure all sink databases exist
// and are migrated before any CDC processing begins.
func (m *Manager) InitAll(ctx context.Context) error {
	m.mu.RLock()
	configs := make(map[string]config.SinkConfig, len(m.dbConfigs))
	for k, v := range m.dbConfigs {
		configs[k] = v
	}
	m.mu.RUnlock()

	if len(configs) == 0 {
		slog.Debug("sinker.Manager.InitAll: no sinks configured")
		return nil
	}

	slog.Info("sinker.Manager.InitAll: initializing all sinks", "count", len(configs))

	for name, cfg := range configs {
		slog.Info("sinker.Manager.InitAll: initializing sink", "name", name, "type", cfg.Type)

		sinker, err := m.GetSinker(name)
		if err != nil {
			return fmt.Errorf("get sinker %s: %w", name, err)
		}

		// Run migrations to ensure database file is created and tables exist
		if err := sinker.Migrate(ctx); err != nil {
			return fmt.Errorf("migrate sinker %s: %w", name, err)
		}

		slog.Info("sinker.Manager.InitAll: sink initialized", "name", name)
	}

	slog.Info("sinker.Manager.InitAll: all sinks initialized", "count", len(configs))
	return nil
}

// GetSinker returns a Sinker for the given database name, creating one if needed
func (m *Manager) GetSinker(dbName string) (Sinker, error) {
	m.mu.RLock()
	sinker, exists := m.sinkers[dbName]
	m.mu.RUnlock()

	if exists {
		return sinker, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if sinker, exists = m.sinkers[dbName]; exists {
		return sinker, nil
	}

	// Look up database config
	dbConfig, exists := m.dbConfigs[dbName]
	if !exists {
		return nil, fmt.Errorf("database %s not configured", dbName)
	}

	// Create appropriate sinker based on type
	switch dbConfig.Type {
	case "sqlite":
		s, err := m.createSQLiteSinker(dbName, dbConfig)
		if err != nil {
			return nil, fmt.Errorf("create sqlite sinker: %w", err)
		}
		sinker = s
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbConfig.Type)
	}

	m.sinkers[dbName] = sinker
	return sinker, nil
}

// createSQLiteSinker creates a SQLite sinker for the given database config
func (m *Manager) createSQLiteSinker(name string, dbConfig config.SinkConfig) (*sinkSqlite.Sinker, error) {
	path := dbConfig.DSN
	if path == "" {
		// Default path for SQLite
		path = fmt.Sprintf("./data/sinks/%s.db", name)
	}

	s, err := sinkSqlite.NewSinker(name, path, dbConfig.Migrations)
	if err != nil {
		return nil, fmt.Errorf("create sqlite sinker: %w", err)
	}

	return s, nil
}

// Write routes sink operations to appropriate sinkers based on Database field.
// BatchCtx provides batch_id for sink_logs correlation.
// monitorDB receives sink_logs for each sink × table × operation.
func (m *Manager) Write(ctx context.Context, sinks []core.Sink, batchCtx *core.BatchContext, monitorDB *monitor.DB) error {
	if len(sinks) == 0 {
		slog.Debug("SinkerManager.Write: no sinks to write")
		return nil
	}

	slog.Info("SinkerManager.Write: routing sinks",
		"total_sinks", len(sinks))

	// Group sinks by database; skip sinks with missing database config instead of aborting
	sinksByDB := make(map[string][]core.Sink)
	for _, sink := range sinks {
		dbName := sink.Config.Database
		if dbName == "" {
			slog.Warn("SinkerManager.Write: sink has no database configured, skipping",
				"sink_name", sink.Config.Name)
			continue
		}
		sinksByDB[dbName] = append(sinksByDB[dbName], sink)
	}

	slog.Debug("SinkerManager.Write: sinks grouped by database",
		"databases", len(sinksByDB))

	// Write to each database
	for dbName, dbSinks := range sinksByDB {
		slog.Debug("SinkerManager.Write: writing to database",
			"database", dbName,
			"sinks", len(dbSinks))

		sinker, err := m.GetSinker(dbName)
		if err != nil {
			slog.Warn("SinkerManager.Write: failed to get sinker, skipping database",
				"database", dbName,
				"error", err)
			continue
		}

		// Track sink write stats for observability
		writeStart := time.Now()
		writeErr := sinker.Write(ctx, dbSinks)
		writeDuration := time.Since(writeStart)

		// Write sink_logs for observability (per sink × table × operation)
		if batchCtx != nil && monitorDB != nil {
			for _, sink := range dbSinks {
				rowsWritten := 0
				if sink.DataSet != nil {
					rowsWritten = len(sink.DataSet.Rows)
				}

				sinkStatus := monitor.SinkStatusSuccess
				var errMsg string
				if writeErr != nil {
					sinkStatus = monitor.SinkStatusError
					errMsg = writeErr.Error()
				}

				// Note: SkillName is not available at sinker level, only sink config name
				sinkLog := &monitor.SinkLog{
					BatchID:      batchCtx.BatchID,
					SkillName:    batchCtx.SkillName,
					SinkName:     sink.Config.Name,
					Database:     sink.Config.Database,
					OutputTable:  sink.Config.Output,
					Operation:    sink.OpType.String(),
					RowsWritten:  rowsWritten,
					Status:       sinkStatus,
					ErrorMessage: errMsg,
					DurationMs:   writeDuration.Milliseconds(),
					CreatedAt:    time.Now(),
				}
				if logWriteErr := monitorDB.WriteSinkLog(sinkLog); logWriteErr != nil {
					slog.Warn("failed to write sink_log", "batch_id", batchCtx.BatchID, "error", logWriteErr)
				}
			}
		}

		if writeErr != nil {
			slog.Error("SinkerManager.Write: write failed for database, skipping",
				"database", dbName,
				"error", writeErr)
			continue
		}

		slog.Debug("SinkerManager.Write: write completed",
			"database", dbName,
			"sinks_written", len(dbSinks))
	}

	slog.Info("SinkerManager.Write: completed successfully",
		"total_sinks", len(sinks),
		"databases", len(sinksByDB))

	return nil
}

// Close closes all sinkers
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for dbName, sinker := range m.sinkers {
		if err := sinker.Close(); err != nil {
			lastErr = fmt.Errorf("close sinker %s: %w", dbName, err)
		}
	}

	m.sinkers = make(map[string]Sinker)
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

// GetSinkConfig returns the configuration for a named sink
func (m *Manager) GetSinkConfig(dbName string) (config.SinkConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cfg, ok := m.dbConfigs[dbName]
	return cfg, ok
}

// QueryTables returns the list of tables in a sink database.
// Uses the Sinker's existing Reader connection instead of creating a temporary one.
func (m *Manager) QueryTables(dbName string) ([]string, error) {
	// Get existing sinker to reuse its Reader connection
	skr, err := m.GetSinker(dbName)
	if err != nil {
		return nil, fmt.Errorf("get sinker %s: %w", dbName, err)
	}

	return skr.QueryTables()
}

// Truncate deletes all data from specified tables in a sink database
func (m *Manager) Truncate(ctx context.Context, dbName string, tables []string) error {
	m.mu.RLock()
	_, ok := m.dbConfigs[dbName]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("sink %s not configured", dbName)
	}

	if len(tables) == 0 {
		return fmt.Errorf("no tables specified")
	}

	sinker, err := m.GetSinker(dbName)
	if err != nil {
		return fmt.Errorf("get sinker: %w", err)
	}

	// Delegate to the specific sinker implementation
	if err := sinker.Truncate(ctx, tables); err != nil {
		return fmt.Errorf("truncate via sinker: %w", err)
	}

	slog.Info("sinker.Manager.Truncate: completed",
		"database", dbName,
		"tables", tables)

	return nil
}

// Query executes a read-only SELECT query on a sink and returns results.
// Uses the Sinker's existing Reader connection instead of creating a temporary one.
func (m *Manager) Query(dbName, query string) ([]string, []map[string]any, error) {
	// Get existing sinker to reuse its Reader connection
	skr, err := m.GetSinker(dbName)
	if err != nil {
		return nil, nil, fmt.Errorf("get sinker %s: %w", dbName, err)
	}

	// Execute query via sinker (limit 1000 for safety)
	results, err := skr.Query(query, 1000)
	if err != nil {
		return nil, nil, fmt.Errorf("query: %w", err)
	}

	// Get columns from first result and sort for stable ordering
	if len(results) == 0 {
		return []string{}, []map[string]any{}, nil
	}
	columns := make([]string, 0, len(results[0]))
	for col := range results[0] {
		columns = append(columns, col)
	}
	sort.Strings(columns)  // Stable column order for UI display

	// Handle datetime conversion for API response
	for _, row := range results {
		for col, val := range row {
			colNameLower := strings.ToLower(col)
			isDatetimeCol := strings.Contains(colNameLower, "date") ||
				strings.Contains(colNameLower, "time") ||
				strings.Contains(colNameLower, "dt") ||
				strings.Contains(colNameLower, "ts")

			if isDatetimeCol {
				if t, ok := val.(time.Time); ok {
					if !t.IsZero() {
						row[col] = t.UTC().Format("2006-01-02 15:04:05")
					} else {
						row[col] = nil
					}
				}
			}
		}
	}

	return columns, results, nil
}

// GetTimezone returns the configured MSSQL timezone
func (m *Manager) GetTimezone() *time.Location {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.timezone == nil {
		return time.Local
	}
	return m.timezone
}

// SetTimezone sets the MSSQL timezone for datetime conversion in API responses
func (m *Manager) SetTimezone(tz *time.Location) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timezone = tz
}
