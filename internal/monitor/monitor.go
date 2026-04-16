package monitor

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite"
	"github.com/yaitoo/sqle"
	"github.com/yaitoo/sqle/migrate"
)

//go:embed migrations
var migrationsFS embed.FS

// PullStatus represents the status of a pull cycle
type PullStatus string

const (
	PullStatusSuccess  PullStatus = "SUCCESS"
	PullStatusPartial  PullStatus = "PARTIAL"
	PullStatusFailed   PullStatus = "FAILED"
)

// SkillStatus represents the status of a skill execution
type SkillStatus string

const (
	SkillStatusSkip     SkillStatus = "SKIP"
	SkillStatusExecuted SkillStatus = "EXECUTED"
	SkillStatusError    SkillStatus = "ERROR"
)

// SinkStatus represents the status of a sink write
type SinkStatus string

const (
	SinkStatusSuccess SinkStatus = "SUCCESS"
	SinkStatusError   SinkStatus = "ERROR"
)

// PullLog represents a batch log entry
type PullLog struct {
	BatchID       string     `json:"batch_id"`       // UUID with short timestamp (primary key)
	FetchedRows  int        `json:"fetched_rows"`  // Total CDC rows fetched
	TxCount      int        `json:"tx_count"`      // Number of transactions
	DLQCount     int        `json:"dlq_count"`     // Number of DLQ entries
	DurationMs   int64      `json:"duration_ms"`   // Total batch duration
	Status       PullStatus `json:"status"`        // SUCCESS/PARTIAL/FAILED
	CreatedAt    time.Time  `json:"created_at"`
}

// SkillLog represents a skill execution log entry
type SkillLog struct {
	ID            int64      `json:"id"`
	BatchID        string     `json:"batch_id"`        // Links to batch_logs
	SkillID       string     `json:"skill_id"`       // Skill hash ID
	SkillName     string     `json:"skill_name"`     // Skill name
	Operation     string     `json:"operation"`      // INSERT/UPDATE/DELETE
	RowsProcessed int        `json:"rows_processed"` // Rows processed by this skill
	Status        SkillStatus `json:"status"`        // SKIP/EXECUTED/ERROR
	ErrorMessage  string     `json:"error_message,omitempty"`
	DurationMs    int64      `json:"duration_ms"`
	CreatedAt     time.Time  `json:"created_at"`
}

// SinkLog represents a sink write log entry
type SinkLog struct {
	ID           int64      `json:"id"`
	BatchID       string     `json:"batch_id"`        // Links to batch_logs
	SkillName    string     `json:"skill_name"`     // Skill that produced this sink
	SinkName     string     `json:"sink_name"`      // Sink config name
	OutputTable  string     `json:"output_table"`   // Target table name
	Operation    string     `json:"operation"`      // INSERT/UPDATE/DELETE
	RowsWritten  int        `json:"rows_written"`   // Rows written to sink
	Status       SinkStatus `json:"status"`         // SUCCESS/ERROR
	ErrorMessage string     `json:"error_message,omitempty"`
	DurationMs   int64      `json:"duration_ms"`
	CreatedAt    time.Time  `json:"created_at"`
}

// DB manages the observability logs database
type DB struct {
	db     *sqlite.DB
	mu     sync.RWMutex
	closed bool
}

// New creates a new DB and runs migrations
func New(ctx context.Context, dbPath string) (*DB, error) {
	db, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("open logs database: %w", err)
	}

	if err := runMigrations(db); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			slog.Warn("logs db close error", "error", closeErr)
		}
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	slog.Info("observability logs database initialized", "path", dbPath)

	return &DB{
		db: db,
	}, nil
}

// runMigrations discovers and applies logs schema migrations
func runMigrations(db *sqlite.DB) error {
	sqleDB := sqle.Open(db.Writer.DB)
	migrator := migrate.New(sqleDB)
	if err := migrator.Discover(migrationsFS, migrate.WithModule("dbkrab-monitor")); err != nil {
		return fmt.Errorf("discover migrations: %w", err)
	}

	if err := migrator.Init(context.Background()); err != nil {
		return fmt.Errorf("init migrations: %w", err)
	}

	return migrator.Migrate(context.Background())
}

// Flush ensures all buffered writes are committed
func (l *DB) Flush() error {
	return l.db.Flush()
}

// WritePullLog writes a pull cycle log entry
func (l *DB) WritePullLog(log *PullLog) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogsClosed
	}

	now := time.Now()
	if log.CreatedAt.IsZero() {
		log.CreatedAt = now
	}

	_, err := l.db.Writer.Exec(`
		INSERT INTO batch_logs (
			batch_id, fetched_rows, tx_count, dlq_count, duration_ms, status, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, log.BatchID, log.FetchedRows, log.TxCount, log.DLQCount,
		log.DurationMs, log.Status, log.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert batch_log: %w", err)
	}

	slog.Debug("batch_log written",
		"batch_id", log.BatchID,
		"fetched_rows", log.FetchedRows,
		"tx_count", log.TxCount,
		"dlq_count", log.DLQCount,
		"duration_ms", log.DurationMs,
		"status", log.Status)

	return nil
}

// WriteSkillLog writes a skill execution log entry
func (l *DB) WriteSkillLog(log *SkillLog) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogsClosed
	}

	now := time.Now()
	if log.CreatedAt.IsZero() {
		log.CreatedAt = now
	}

	result, err := l.db.Writer.Exec(`
		INSERT INTO skill_logs (
			batch_id, skill_id, skill_name, operation, rows_processed,
			status, error_message, duration_ms, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, log.BatchID, log.SkillID, log.SkillName, log.Operation,
		log.RowsProcessed, log.Status, log.ErrorMessage,
		log.DurationMs, log.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert skill_log: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("get last insert id: %w", err)
	}
	log.ID = id

	slog.Debug("skill_log written",
		"batch_id", log.BatchID,
		"skill_id", log.SkillID,
		"skill_name", log.SkillName,
		"operation", log.Operation,
		"rows_processed", log.RowsProcessed,
		"status", log.Status)

	return nil
}

// WriteSinkLog writes a sink write log entry
func (l *DB) WriteSinkLog(log *SinkLog) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogsClosed
	}

	now := time.Now()
	if log.CreatedAt.IsZero() {
		log.CreatedAt = now
	}

	result, err := l.db.Writer.Exec(`
		INSERT INTO sink_logs (
			batch_id, skill_name, sink_name, output_table, operation,
			rows_written, status, error_message, duration_ms, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, log.BatchID, log.SkillName, log.SinkName, log.OutputTable,
		log.Operation, log.RowsWritten, log.Status, log.ErrorMessage,
		log.DurationMs, log.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert sink_log: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("get last insert id: %w", err)
	}
	log.ID = id

	slog.Debug("sink_log written",
		"batch_id", log.BatchID,
		"skill_name", log.SkillName,
		"sink_name", log.SinkName,
		"output_table", log.OutputTable,
		"rows_written", log.RowsWritten,
		"status", log.Status)

	return nil
}

// ListPullLogs retrieves pull logs with optional limit
func (l *DB) ListPullLogs(limit int) ([]*PullLog, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogsClosed
	}

	query := `
		SELECT batch_id, fetched_rows, tx_count, dlq_count, duration_ms, status, created_at
		FROM batch_logs
		ORDER BY created_at DESC
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := l.db.Reader.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query batch_logs: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("rows.Close error", "error", closeErr)
		}
	}()

	var logs []*PullLog
	for rows.Next() {
		log := &PullLog{}
		if err := rows.Scan(
			&log.BatchID, &log.FetchedRows, &log.TxCount, &log.DLQCount,
			&log.DurationMs, &log.Status, &log.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan batch_log: %w", err)
		}
		logs = append(logs, log)
	}

	return logs, rows.Err()
}

// ListSkillLogs retrieves skill logs for a specific batch_id
func (l *DB) ListSkillLogs(pullID string, limit int) ([]*SkillLog, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogsClosed
	}

	query := `
		SELECT batch_id, skill_id, skill_name, operation, rows_processed,
			   status, error_message, duration_ms, created_at
		FROM skill_logs
		WHERE batch_id = ?
		ORDER BY created_at DESC
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := l.db.Reader.Query(query, pullID)
	if err != nil {
		return nil, fmt.Errorf("query skill_logs: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("rows.Close error", "error", closeErr)
		}
	}()

	var logs []*SkillLog
	for rows.Next() {
		log := &SkillLog{}
		var errMsg string
		if err := rows.Scan(
			&log.BatchID, &log.SkillID, &log.SkillName, &log.Operation,
			&log.RowsProcessed, &log.Status, &errMsg, &log.DurationMs, &log.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan skill_log: %w", err)
		}
		if errMsg != "" {
			log.ErrorMessage = errMsg
		}
		logs = append(logs, log)
	}

	return logs, rows.Err()
}

// ListSinkLogs retrieves sink logs for a specific batch_id
func (l *DB) ListSinkLogs(pullID string, limit int) ([]*SinkLog, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogsClosed
	}

	query := `
		SELECT batch_id, skill_name, sink_name, output_table, operation,
			   rows_written, status, error_message, duration_ms, created_at
		FROM sink_logs
		WHERE batch_id = ?
		ORDER BY created_at DESC
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := l.db.Reader.Query(query, pullID)
	if err != nil {
		return nil, fmt.Errorf("query sink_logs: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("rows.Close error", "error", closeErr)
		}
	}()

	var logs []*SinkLog
	for rows.Next() {
		log := &SinkLog{}
		var errMsg string
		if err := rows.Scan(
			&log.BatchID, &log.SkillName, &log.SinkName, &log.OutputTable,
			&log.Operation, &log.RowsWritten, &log.Status, &errMsg,
			&log.DurationMs, &log.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan sink_log: %w", err)
		}
		if errMsg != "" {
			log.ErrorMessage = errMsg
		}
		logs = append(logs, log)
	}

	return logs, rows.Err()
}

// Close closes the database connection
func (l *DB) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true
	return l.db.Close()
}

// GetPullLogStats returns statistics for recent pull logs
func (l *DB) GetPullLogStats(since time.Time) (map[string]interface{}, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogsClosed
	}

	// Total pulls
	var totalPulls int
	err := l.db.Reader.QueryRow(`
		SELECT COUNT(*) FROM batch_logs WHERE created_at >= ?
	`, since).Scan(&totalPulls)
	if err != nil {
		return nil, fmt.Errorf("count pulls: %w", err)
	}

	// Success/Partial/Failed counts
	var success, partial, failed int
	err = l.db.Reader.QueryRow(`
		SELECT
			SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END),
			SUM(CASE WHEN status = 'PARTIAL' THEN 1 ELSE 0 END),
			SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)
		FROM batch_logs WHERE created_at >= ?
	`, since).Scan(&success, &partial, &failed)
	if err != nil {
		return nil, fmt.Errorf("count by status: %w", err)
	}

	// Total rows and duration
	var totalRows, totalDLQ int
	var avgDurationMs int64
	err = l.db.Reader.QueryRow(`
		SELECT
			SUM(fetched_rows),
			SUM(dlq_count),
			AVG(duration_ms)
		FROM batch_logs WHERE created_at >= ?
	`, since).Scan(&totalRows, &totalDLQ, &avgDurationMs)
	if err != nil {
		return nil, fmt.Errorf("aggregate stats: %w", err)
	}

	return map[string]interface{}{
		"total_pulls":     totalPulls,
		"success_count":   success,
		"partial_count":   partial,
		"failed_count":    failed,
		"total_rows":      totalRows,
		"total_dlq":       totalDLQ,
		"avg_duration_ms": avgDurationMs,
	}, nil
}