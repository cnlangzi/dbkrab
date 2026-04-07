package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/cnlangzi/dbkrab/internal/core"
	_ "modernc.org/sqlite"
)

// Sink implements core.Sink for SQLite
type Sink struct {
	db   *sql.DB
	path string
}

// NewSink creates a new SQLite sink with WAL mode and optimized settings
func NewSink(path string) (*Sink, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Build DSN with WAL mode and optimized parameters
	params := url.Values{}
	params.Add("_journal_mode", "WAL")           // WAL mode for concurrent read/write
	params.Add("_synchronous", "NORMAL")          // Balance between safety and performance
	params.Add("_busy_timeout", "5000")           // Wait 5s for locks
	params.Add("_pragma", "temp_store(MEMORY)")   // Temp tables in memory
	params.Add("_pragma", "cache_size(-100000)")  // ~100MB cache
	params.Add("_pragma", "mmap_size(1000000000)") // 1GB mmap for faster reads
	params.Add("cache", "shared")                 // Shared cache for concurrency

	dsn := path + "?" + params.Encode()

	// Open database
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Set connection pool - writer needs single connection to avoid SQLITE_BUSY
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Create tables
	if err := createTables(db); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			slog.Warn("db.Close error", "error", closeErr)
		}
		return nil, fmt.Errorf("create tables: %w", err)
	}

	sink := &Sink{db: db, path: path}
	// Initialize poller state
	if err := sink.initPollerState(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			slog.Warn("db.Close error", "error", closeErr)
		}
		return nil, fmt.Errorf("init poller state: %w", err)
	}

	return sink, nil
}

// createTables creates the necessary tables
func createTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS transactions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			transaction_id TEXT NOT NULL,
			table_name TEXT NOT NULL,
			operation TEXT NOT NULL,
			data TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS poller_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			last_poll_time TIMESTAMP,
			last_lsn TEXT,
			total_changes INTEGER DEFAULT 0,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE INDEX IF NOT EXISTS idx_transaction_id ON transactions(transaction_id);
		CREATE INDEX IF NOT EXISTS idx_table_name ON transactions(table_name);
		CREATE INDEX IF NOT EXISTS idx_created_at ON transactions(created_at);
	`)
	return err
}

// initPollerState initializes the poller state row
func (s *Sink) initPollerState() error {
	_, err := s.db.Exec(`
		INSERT OR IGNORE INTO poller_state (id, last_poll_time, last_lsn, total_changes)
		VALUES (1, NULL, NULL, 0)
	`)
	return err
}

// UpdatePollerState updates the poller state after successful poll
func (s *Sink) UpdatePollerState(lastLSN string, changeCount int) error {
	// Use COALESCE to keep existing LSN if new one is empty
	_, err := s.db.Exec(`
		UPDATE poller_state 
		SET last_poll_time = CURRENT_TIMESTAMP,
			last_lsn = COALESCE(NULLIF(?, ''), last_lsn),
			total_changes = total_changes + ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = 1
	`, lastLSN, changeCount)
	return err
}

// GetPollerState returns the current poller state
func (s *Sink) GetPollerState() (map[string]interface{}, error) {
	row := s.db.QueryRow(`
		SELECT last_poll_time, last_lsn, total_changes, updated_at
		FROM poller_state
		WHERE id = 1
	`)

	var lastPollTime, lastLSN, updatedAt sql.NullString
	var totalChanges int

	if err := row.Scan(&lastPollTime, &lastLSN, &totalChanges, &updatedAt); err != nil {
		return nil, err
	}

	state := map[string]interface{}{
		"total_changes": totalChanges,
	}

	if lastPollTime.Valid {
		state["last_poll_time"] = lastPollTime.String
	} else {
		state["last_poll_time"] = nil
	}

	if lastLSN.Valid {
		state["last_lsn"] = lastLSN.String
	} else {
		state["last_lsn"] = nil
	}

	if updatedAt.Valid {
		state["updated_at"] = updatedAt.String
	} else {
		state["updated_at"] = nil
	}

	return state, nil
}

// Write writes a transaction to SQLite
func (s *Sink) Write(tx *core.Transaction) error {
	sqlTx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err := sqlTx.Rollback(); err != nil {
			// After commit, rollback returns "transaction has already been committed"
			// This is expected, so we ignore it. Other errors are unexpected.
			if !strings.Contains(err.Error(), "transaction has already") && !strings.Contains(err.Error(), "already been committed") {
				// Unexpected rollback error - could log here if needed
				_ = err // explicitly ignore for now
			}
		}
	}()

	stmt, err := sqlTx.Prepare(`
		INSERT INTO transactions (transaction_id, table_name, operation, data)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			slog.Warn("stmt.Close error", "error", err)
		}
	}()

	for _, change := range tx.Changes {
		dataJSON, err := json.Marshal(change.Data)
		if err != nil {
			dataJSON = []byte("{}")
		}

		_, err = stmt.Exec(
			tx.ID,
			change.Table,
			change.Operation.String(),
			string(dataJSON),
		)
		if err != nil {
			return fmt.Errorf("insert change: %w", err)
		}
	}

	return sqlTx.Commit()
}

// Close closes the database connection
func (s *Sink) Close() error {
	return s.db.Close()
}

// GetChanges retrieves changes from the database
func (s *Sink) GetChanges(limit int) ([]map[string]interface{}, error) {
	return s.GetChangesWithFilter(limit, "", "", "")
}

// GetChangesWithFilter retrieves changes with optional filters
func (s *Sink) GetChangesWithFilter(limit int, tableName, operation, txID string) ([]map[string]interface{}, error) {
	query := `SELECT id, transaction_id, table_name, operation, data, created_at FROM transactions WHERE 1=1`
	args := []interface{}{}

	if tableName != "" {
		query += " AND table_name = ?"
		args = append(args, tableName)
	}
	if operation != "" {
		query += " AND operation = ?"
		args = append(args, operation)
	}
	if txID != "" {
		query += " AND transaction_id = ?"
		args = append(args, txID)
	}

	query += " ORDER BY id DESC"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Warn("rows.Close error", "error", err)
		}
	}()

	var results []map[string]interface{}
	for rows.Next() {
		var id int
		var resultTxID, resultTableName, resultOperation, dataStr string
		var createdAt interface{}

		if err := rows.Scan(&id, &resultTxID, &resultTableName, &resultOperation, &dataStr, &createdAt); err != nil {
			return nil, err
		}

		var data map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
			data = make(map[string]interface{})
		}

		results = append(results, map[string]interface{}{
			"id":             id,
			"transaction_id": resultTxID,
			"table_name":     resultTableName,
			"operation":      resultOperation,
			"data":           data,
			"created_at":     createdAt,
		})
	}

	return results, rows.Err()
}