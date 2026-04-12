package dlq

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cnlangzi/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// Schema contains the SQL schema for the DLQ table
const Schema = `
CREATE TABLE IF NOT EXISTS dlq_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id TEXT,
    source TEXT,
    lsn TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    change_data TEXT NOT NULL,
    error_message TEXT NOT NULL,
    retry_count INTEGER DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    resolved_by TEXT,
    resolved_at DATETIME,
    resolved_note TEXT
);

CREATE INDEX IF NOT EXISTS idx_dlq_status ON dlq_entries(status);
CREATE INDEX IF NOT EXISTS idx_dlq_table ON dlq_entries(table_name);
CREATE INDEX IF NOT EXISTS idx_dlq_created_at ON dlq_entries(created_at);
CREATE INDEX IF NOT EXISTS idx_dlq_lsn ON dlq_entries(lsn);
`

// Status represents the DLQ entry status
type Status string

const (
	StatusPending   Status = "pending"
	StatusResolved  Status = "resolved"
	StatusIgnored   Status = "ignored"
	StatusRetrying  Status = "retrying"
)

// DLQEntry represents a dead letter queue entry
type DLQEntry struct {
	ID           int64     `json:"id"`
	TraceID      string    `json:"trace_id"`      // Trace ID for log correlation
	Source       string    `json:"source"`        // Error source: handler, store, flush_handler, etc.
	LSN          string    `json:"lsn"`
	TableName    string    `json:"table_name"`
	Operation    string    `json:"operation"`
	ChangeData   string    `json:"change_data"`   // JSON-encoded change data
	ErrorMessage string    `json:"error_message"`
	RetryCount   int       `json:"retry_count"`
	Status       Status    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	ResolvedBy   string    `json:"resolved_by,omitempty"`
	ResolvedAt   time.Time `json:"resolved_at,omitempty"`
	ResolvedNote string    `json:"resolved_note,omitempty"`
}

// DLQ manages the dead letter queue
type DLQ struct {
	db     *sql.DB
	mu     sync.RWMutex
	closed bool
}

// New creates a new DLQ manager and initializes the database
func New(dbPath string) (*DLQ, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Enable WAL mode for better concurrency
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			slog.Warn("db.Close error", "error", closeErr)
		}
		return nil, fmt.Errorf("enable WAL mode: %w", err)
	}

	// Create table
	if _, err := db.Exec(Schema); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			slog.Warn("db.Close error", "error", closeErr)
		}
		return nil, fmt.Errorf("create table: %w", err)
	}

	return &DLQ{
		db: db,
	}, nil
}

// NewWithDB creates a new DLQ manager using an existing database connection
func NewWithDB(db *sql.DB) (*DLQ, error) {
	// Create table if not exists
	if _, err := db.Exec(Schema); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	return &DLQ{
		db: db,
	}, nil
}

// NewWithStoreDB creates a new DLQ manager using the unified store database.
// The DLQ uses the dlq_entries table which is managed by sqle/migrate migrations.
// The storeDB's lifecycle (including closing) is managed externally.
func NewWithStoreDB(storeDB *sqlite.DB) (*DLQ, error) {
	return &DLQ{
		db:     storeDB.Writer.DB, // Use the store's underlying sql.DB connection
		mu:     sync.RWMutex{},
		closed: false,
	}, nil
}

// Write writes a new entry to the dead letter queue
func (d *DLQ) Write(entry *DLQEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDLQClosed
	}

	now := time.Now()
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = now
	}
	entry.UpdatedAt = now
	if entry.Status == "" {
		entry.Status = StatusPending
	}

	result, err := d.db.Exec(`
		INSERT INTO dlq_entries (
			lsn, table_name, operation, change_data, error_message,
			retry_count, status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, entry.LSN, entry.TableName, entry.Operation, entry.ChangeData,
		entry.ErrorMessage, entry.RetryCount, entry.Status,
		entry.CreatedAt, entry.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("get last insert id: %w", err)
	}
	entry.ID = id

	slog.Info("DLQ entry written",
		"entry_id", entry.ID,
		"table", entry.TableName,
		"lsn", entry.LSN,
		"error", entry.ErrorMessage)

	return nil
}

// List retrieves entries from the dead letter queue
// If status is empty, returns all entries
func (d *DLQ) List(status string) ([]*DLQEntry, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, ErrDLQClosed
	}

	// Validate status if provided
	if status != "" {
		valid := false
		for _, s := range []Status{StatusPending, StatusRetrying, StatusResolved, StatusIgnored} {
			if string(s) == status {
				valid = true
				break
			}
		}
		if !valid {
			return nil, ErrInvalidStatus
		}
	}

	var query string
	var args []interface{}

	if status != "" {
		query = `
			SELECT id, trace_id, source, lsn, table_name, operation, change_data, error_message,
				   retry_count, status, created_at, updated_at, resolved_by, resolved_at, resolved_note
			FROM dlq_entries
			WHERE status = ?
			ORDER BY created_at DESC
		`
		args = []interface{}{status}
	} else {
		query = `
			SELECT id, trace_id, source, lsn, table_name, operation, change_data, error_message,
				   retry_count, status, created_at, updated_at, resolved_by, resolved_at, resolved_note
			FROM dlq_entries
			ORDER BY created_at DESC
		`
	}

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query entries: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("rows.Close error", "error", closeErr)
		}
	}()

	var entries []*DLQEntry
	for rows.Next() {
		entry := &DLQEntry{}
		var traceID, source, resolvedBy, resolvedNote sql.NullString
		var resolvedAt sql.NullTime

		err := rows.Scan(
			&entry.ID, &traceID, &source, &entry.LSN, &entry.TableName, &entry.Operation,
			&entry.ChangeData, &entry.ErrorMessage, &entry.RetryCount,
			&entry.Status, &entry.CreatedAt, &entry.UpdatedAt,
			&resolvedBy, &resolvedAt, &resolvedNote,
		)
		if err != nil {
			return nil, fmt.Errorf("scan entry: %w", err)
		}

		if traceID.Valid {
			entry.TraceID = traceID.String
		}
		if source.Valid {
			entry.Source = source.String
		}
		if resolvedBy.Valid {
			entry.ResolvedBy = resolvedBy.String
		}
		if resolvedAt.Valid {
			entry.ResolvedAt = resolvedAt.Time
		}
		if resolvedNote.Valid {
			entry.ResolvedNote = resolvedNote.String
		}

		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

// Get retrieves a single entry by ID
func (d *DLQ) Get(id int64) (*DLQEntry, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, ErrDLQClosed
	}

	entry := &DLQEntry{}
	var resolvedBy, resolvedNote sql.NullString
	var resolvedAt sql.NullTime

	err := d.db.QueryRow(`
		SELECT id, lsn, table_name, operation, change_data, error_message,
			   retry_count, status, created_at, updated_at, resolved_by, resolved_at, resolved_note
		FROM dlq_entries
		WHERE id = ?
	`, id).Scan(
		&entry.ID, &entry.LSN, &entry.TableName, &entry.Operation,
		&entry.ChangeData, &entry.ErrorMessage, &entry.RetryCount,
		&entry.Status, &entry.CreatedAt, &entry.UpdatedAt,
		&resolvedBy, &resolvedAt, &resolvedNote,
	)

	if err == sql.ErrNoRows {
		return nil, ErrEntryNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query entry: %w", err)
	}

	if resolvedBy.Valid {
		entry.ResolvedBy = resolvedBy.String
	}
	if resolvedAt.Valid {
		entry.ResolvedAt = resolvedAt.Time
	}
	if resolvedNote.Valid {
		entry.ResolvedNote = resolvedNote.String
	}

	return entry, nil
}

// Replay attempts to replay a DLQ entry
// The handler function is called to process the entry
// If successful, the entry status is updated to resolved
func (d *DLQ) Replay(ctx context.Context, id int64, handler func(entry *DLQEntry) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDLQClosed
	}

	if handler == nil {
		return ErrHandlerRequired
	}

	// Check context
	if err := ctx.Err(); err != nil {
		return err
	}

	// Get the entry
	entry := &DLQEntry{}
	var resolvedBy, resolvedNote sql.NullString
	var resolvedAt sql.NullTime

	err := d.db.QueryRowContext(ctx, `
		SELECT id, lsn, table_name, operation, change_data, error_message,
			   retry_count, status, created_at, updated_at, resolved_by, resolved_at, resolved_note
		FROM dlq_entries
		WHERE id = ? AND status = ?
	`, id, StatusPending).Scan(
		&entry.ID, &entry.LSN, &entry.TableName, &entry.Operation,
		&entry.ChangeData, &entry.ErrorMessage, &entry.RetryCount,
		&entry.Status, &entry.CreatedAt, &entry.UpdatedAt,
		&resolvedBy, &resolvedAt, &resolvedNote,
	)

	if err == sql.ErrNoRows {
		return ErrEntryNotFound
	}
	if err != nil {
		return fmt.Errorf("query entry: %w", err)
	}

	// Update status to retrying
	_, err = d.db.ExecContext(ctx, `
		UPDATE dlq_entries SET status = ?, updated_at = ?, retry_count = retry_count + 1
		WHERE id = ?
	`, StatusRetrying, time.Now(), id)
	if err != nil {
		return fmt.Errorf("update status to retrying: %w", err)
	}

	// Call the handler
	if err := handler(entry); err != nil {
		// Handler failed - revert to pending
		_, updateErr := d.db.Exec(`
			UPDATE dlq_entries SET status = ?, updated_at = ?, error_message = ?
			WHERE id = ?
		`, StatusPending, time.Now(),
			fmt.Sprintf("%s (replay failed: %v)", entry.ErrorMessage, err), id)
		if updateErr != nil {
			slog.Warn("failed to revert status", "error", updateErr)
		}
		return fmt.Errorf("handler failed: %w", err)
	}

	// Handler succeeded - mark as resolved
	_, err = d.db.Exec(`
		UPDATE dlq_entries SET
			status = ?, updated_at = ?, resolved_by = ?, resolved_at = ?, resolved_note = ?
		WHERE id = ?
	`, StatusResolved, time.Now(), "replay", time.Now(), "Successfully replayed", id)
	if err != nil {
		return fmt.Errorf("update status to resolved: %w", err)
	}

	slog.Info("DLQ entry replayed", "entry_id", id)
	return nil
}

// Ignore marks an entry as ignored with a note
func (d *DLQ) Ignore(id int64, note string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDLQClosed
	}

	result, err := d.db.Exec(`
		UPDATE dlq_entries SET
			status = ?, updated_at = ?, resolved_by = ?, resolved_at = ?, resolved_note = ?
		WHERE id = ? AND status = ?
	`, StatusIgnored, time.Now(), "manual", time.Now(), note, id, StatusPending)
	if err != nil {
		return fmt.Errorf("update entry: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrEntryNotFound
	}

	slog.Info("DLQ entry ignored", "entry_id", id, "note", note)
	return nil
}

// Delete physically removes an entry from the DLQ
func (d *DLQ) Delete(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDLQClosed
	}

	result, err := d.db.Exec(`DELETE FROM dlq_entries WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete entry: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrEntryNotFound
	}

	slog.Info("DLQ entry deleted", "entry_id", id)
	return nil
}

// Stats returns statistics about the DLQ
func (d *DLQ) Stats() (map[Status]int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, ErrDLQClosed
	}

	rows, err := d.db.Query(`
		SELECT status, COUNT(*) as count
		FROM dlq_entries
		GROUP BY status
	`)
	if err != nil {
		return nil, fmt.Errorf("query stats: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("rows.Close error", "error", closeErr)
		}
	}()

	stats := make(map[Status]int)
	for rows.Next() {
		var status Status
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan stats: %w", err)
		}
		stats[status] = count
	}

	return stats, rows.Err()
}

// Close closes the database connection
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true

	// Only close if we own the connection (created via New, not NewWithDB)
	// For NewWithDB, the caller manages the connection
	return nil // Let caller manage DB lifecycle
}

// CloseAndDB closes both the DLQ and the underlying database
func (d *DLQ) CloseAndDB() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true
	return d.db.Close()
}

// EncodeChangeData JSON-encodes change data for storage
func EncodeChangeData(data map[string]interface{}) (string, error) {
	if data == nil {
		return "", nil
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("encode change data: %w", err)
	}
	return string(jsonData), nil
}

// DecodeChangeData JSON-decodes change data from storage
func DecodeChangeData(jsonStr string) (map[string]interface{}, error) {
	if jsonStr == "" {
		return nil, nil
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("decode change data: %w", err)
	}
	return data, nil
}

// Count returns the total number of entries with a given status
func (d *DLQ) Count(status Status) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return 0, ErrDLQClosed
	}

	var count int
	err := d.db.QueryRow(`
		SELECT COUNT(*) FROM dlq_entries WHERE status = ?
	`, status).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count entries: %w", err)
	}

	return count, nil
}

// CountAll returns the total number of entries
func (d *DLQ) CountAll() (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return 0, ErrDLQClosed
	}

	var count int
	err := d.db.QueryRow(`SELECT COUNT(*) FROM dlq_entries`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count entries: %w", err)
	}

	return count, nil
}