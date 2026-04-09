package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	_ "modernc.org/sqlite"
)

// Store implements core.Store for SQLite
type Store struct {
	db   *sql.DB
	path string
}

// NewStore creates a new SQLite store with WAL mode and optimized settings
func NewStore(path string) (*Store, error) {
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

	store := &Store{db: db, path: path}
	// Initialize poller state
	if err := store.initPollerState(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			slog.Warn("db.Close error", "error", closeErr)
		}
		return nil, fmt.Errorf("init poller state: %w", err)
	}

	return store, nil
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
			changed_at TIMESTAMP,
			pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
		CREATE INDEX IF NOT EXISTS idx_changed_at ON transactions(changed_at);
	`)
	return err
}

// initPollerState initializes the poller state row
func (s *Store) initPollerState() error {
	_, err := s.db.Exec(`
		INSERT OR IGNORE INTO poller_state (id, last_poll_time, last_lsn, total_changes)
		VALUES (1, NULL, NULL, 0)
	`)
	return err
}

// UpdatePollerState updates the poller state after successful poll
func (s *Store) UpdatePollerState(lastLSN string, changeCount int) error {
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
func (s *Store) GetPollerState() (map[string]interface{}, error) {
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
func (s *Store) Write(tx *core.Transaction) error {
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
		INSERT INTO transactions (transaction_id, table_name, operation, data, changed_at, pulled_at)
		VALUES (?, ?, ?, ?, ?, ?)
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

		var cdcTime interface{}
		if !change.CommitTime.IsZero() {
			cdcTime = change.CommitTime // Already converted to UTC in CDC query layer
		}

		_, err = stmt.Exec(
			tx.ID,
			change.Table,
			change.Operation.String(),
			string(dataJSON),
			cdcTime,
			time.Now().UTC(), // pulled_at - store in UTC for consistency
		)
		if err != nil {
			return fmt.Errorf("insert change: %w", err)
		}
	}

	return sqlTx.Commit()
}

// WriteOps writes transformed DataSets from SQL plugins to SQLite
func (s *Store) WriteOps(ops []core.Sink) error {
	if len(ops) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, op := range ops {
		switch op.OpType {
		case core.OpInsert:
			if err := s.insertInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("insert %s: %w", op.Config.Output, err)
			}
		case core.OpUpdateAfter:
			if err := s.updateInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("update %s: %w", op.Config.Output, err)
			}
		case core.OpDelete:
			if err := s.deleteInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("delete %s: %w", op.Config.Output, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// insertInTx inserts DataSet into table
func (s *Store) insertInTx(tx *sql.Tx, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Ensure table exists
	if err := s.ensureTable(tx, config.Output, ds.Columns); err != nil {
		return err
	}

	for _, row := range ds.Rows {
		// Build INSERT SQL
		escapedCols := make([]string, len(ds.Columns))
		for i, col := range ds.Columns {
			escapedCols[i] = fmt.Sprintf("[%s]", col)
		}

		placeholders := make([]string, len(ds.Columns))
		for i := range ds.Columns {
			placeholders[i] = "?"
		}

		sqlStr := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
			config.Output,
			strings.Join(escapedCols, ", "),
			strings.Join(placeholders, ", "))

		_, err := tx.Exec(sqlStr, row...)
		if err != nil {
			return fmt.Errorf("execute: %w", err)
		}
	}

	return nil
}

// updateInTx updates records in table
func (s *Store) updateInTx(tx *sql.Tx, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Find PK index
	pkIndex := -1
	for i, col := range ds.Columns {
		if col == config.PrimaryKey {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return fmt.Errorf("primary key %s not found", config.PrimaryKey)
	}

	for _, row := range ds.Rows {
		// Build UPDATE SQL
		var setClauses []string
		var values []any
		for i, col := range ds.Columns {
			if col == config.PrimaryKey {
				continue
			}
			setClauses = append(setClauses, fmt.Sprintf("[%s] = ?", col))
			values = append(values, row[i])
		}

		pkValue := row[pkIndex]
		sqlStr := fmt.Sprintf("UPDATE %s SET %s WHERE [%s] = ?",
			config.Output,
			strings.Join(setClauses, ", "),
			config.PrimaryKey)
		values = append(values, pkValue)

		_, err := tx.Exec(sqlStr, values...)
		if err != nil {
			return fmt.Errorf("execute: %w", err)
		}
	}

	return nil
}

// deleteInTx deletes records from table
func (s *Store) deleteInTx(tx *sql.Tx, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Find PK index
	pkIndex := -1
	for i, col := range ds.Columns {
		if col == config.PrimaryKey {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return fmt.Errorf("primary key %s not found", config.PrimaryKey)
	}

	pkValues := make([]any, len(ds.Rows))
	for i, row := range ds.Rows {
		pkValues[i] = row[pkIndex]
	}

	// Build IN clause
	placeholders := make([]string, len(ds.Rows))
	for i := range ds.Rows {
		placeholders[i] = "?"
	}

	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE [%s] IN (%s)",
		config.Output,
		config.PrimaryKey,
		strings.Join(placeholders, ", "))

	_, err := tx.Exec(sqlStr, pkValues...)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}

	return nil
}

// ensureTable ensures the table exists with the given columns
func (s *Store) ensureTable(tx *sql.Tx, table string, columns []string) error {
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = fmt.Sprintf("[%s] TEXT", col)
	}

	sqlStr := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		table,
		strings.Join(escapedCols, ", "))

	_, err := tx.Exec(sqlStr)
	return err
}

// Close closes the database connection
func (s *Store) Close() error {
	return s.db.Close()
}

// GetChanges retrieves changes from the database
func (s *Store) GetChanges(limit int) ([]map[string]interface{}, error) {
	return s.GetChangesWithFilter(limit, "", "", "")
}

// GetChangesWithFilter retrieves changes with optional filters
func (s *Store) GetChangesWithFilter(limit int, tableName, operation, txID string) ([]map[string]interface{}, error) {
	query := `SELECT id, transaction_id, table_name, operation, data, changed_at, pulled_at FROM transactions WHERE 1=1`
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
		var cdcTime, pulledAt interface{}

		if err := rows.Scan(&id, &resultTxID, &resultTableName, &resultOperation, &dataStr, &cdcTime, &pulledAt); err != nil {
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
			"changed_at":     cdcTime,
			"pulled_at":      pulledAt,
		})
	}

	return results, rows.Err()
}

// GetDB returns the underlying database connection
func (s *Store) GetDB() *sql.DB {
	return s.db
}