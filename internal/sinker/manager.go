package sinker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/monitor"
	sinkSqlite "github.com/cnlangzi/dbkrab/internal/sinker/sqlite"
	_ "github.com/mattn/go-sqlite3"
)

// datetimeFormats contains supported datetime formats for parsing SQLite datetime columns.
var datetimeFormats = []string{
	"2006-01-02 15:04:05",      // SQLite native format
	"2006-01-02T15:04:05Z07:00", // RFC3339 with timezone
	time.RFC3339,                // RFC3339 standard
	"2006-01-02T15:04:05",      // ISO8601 without timezone
	"2006-01-02",               // date only
	"2006-01-02 15:04:05 -0700 MST", // Go's time.String() format (legacy)
}

// parseDatetime attempts to parse a datetime string using multiple formats.
// Returns the parsed time if successful, or the original value if parsing fails.
func parseDatetime(val any) any {
	if val == nil {
		return nil
	}

	// Handle time.Time directly from SQLite driver
	if t, ok := val.(time.Time); ok {
		return t.Format(time.RFC3339)
	}

	// Handle string values
	str, ok := val.(string)
	if !ok {
		return val
	}

	// Try parsing with each format
	for _, format := range datetimeFormats {
		if t, err := time.Parse(format, str); err == nil {
			return t.Format(time.RFC3339)
		}
	}

	// Return original string if no format matched
	return str
}

// tryParseDatetimeString attempts to parse a string as datetime and returns RFC3339 format.
// Returns the original string if parsing fails.
func tryParseDatetimeString(s string) string {
	if s == "" {
		return s
	}
	// Try common datetime formats
	for _, format := range datetimeFormats {
		if t, err := time.Parse(format, s); err == nil {
			return t.Format(time.RFC3339)
		}
	}
	return s
}

// Manager manages Sinkers and routes sink operations to appropriate sinkers.
type Manager struct {
	sinkers   map[string]Sinker // keyed by database name
	dbConfigs map[string]config.SinkConfig
	mu        sync.RWMutex
}

// NewManager creates a new Sinker manager
func NewManager() *Manager {
	return &Manager{
		sinkers:   make(map[string]Sinker),
		dbConfigs: make(map[string]config.SinkConfig),
	}
}

// Configure configures the manager with database configurations
func (m *Manager) Configure(dbConfigs map[string]config.SinkConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dbConfigs = dbConfigs
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

// QueryTables returns the list of tables in a sink database
func (m *Manager) QueryTables(dbName string) ([]string, error) {
	m.mu.RLock()
	dbConfig, ok := m.dbConfigs[dbName]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("sink %s not configured", dbName)
	}

	if dbConfig.Type != "sqlite" {
		return nil, fmt.Errorf("QueryTables only supported for sqlite sinks")
	}

	path := dbConfig.DSN
	if path == "" {
		path = fmt.Sprintf("./data/sinks/%s.db", dbName)
	}

	// Open read-only connection
	db, err := sql.Open("sqlite3", path+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Query tables from sqlite_master
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		// Skip internal SQLite tables
		if !strings.HasPrefix(tableName, "sqlite_") {
			tables = append(tables, tableName)
		}
	}

	return tables, rows.Err()
}

// Query executes a read-only SELECT query on a sink and returns results
func (m *Manager) Query(dbName, query string) ([]string, []map[string]any, error) {
	m.mu.RLock()
	dbConfig, ok := m.dbConfigs[dbName]
	m.mu.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("sink %s not configured", dbName)
	}

	if dbConfig.Type != "sqlite" {
		return nil, nil, fmt.Errorf("Query only supported for sqlite sinks")
	}

	path := dbConfig.DSN
	if path == "" {
		path = fmt.Sprintf("./data/sinks/%s.db", dbName)
	}

	// Open read-only connection
	db, err := sql.Open("sqlite3", path+"?mode=ro")
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Add LIMIT if not present
	limitQuery := query
	if !strings.Contains(strings.ToUpper(query), "LIMIT") {
		limitQuery = fmt.Sprintf("%s LIMIT 1000", query)
	}

	rows, err := db.Query(limitQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("execute query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("get columns: %w", err)
	}

	// Get column types to identify datetime columns
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, fmt.Errorf("get column types: %w", err)
	}

	// Identify datetime columns and attempt to parse time values
	datetimeCols := make(map[int]bool)
	for i, ct := range colTypes {
		// SQLite datetime type is stored as "datetime" in schema
		if strings.EqualFold(ct.DatabaseTypeName(), "datetime") {
			datetimeCols[i] = true
		}
	}

	// Fetch results
	results := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, fmt.Errorf("scan row: %w", err)
		}

		rowMap := make(map[string]any)
		for i, col := range columns {
			val := values[i]
			// Parse datetime columns
			if datetimeCols[i] {
				val = parseDatetime(val)
			}
			// Also attempt to parse string values that look like datetime
			if s, ok := val.(string); ok && !datetimeCols[i] {
				if parsed := tryParseDatetimeString(s); parsed != s {
					val = parsed
				}
			}
			rowMap[col] = val
		}
		results = append(results, rowMap)
	}

	return columns, results, rows.Err()
}
