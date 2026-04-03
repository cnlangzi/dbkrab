package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cnlangzi/dbkrab/internal/core"
	_ "modernc.org/sqlite"
)

// Sink implements core.Sink for SQLite
type Sink struct {
	db   *sql.DB
	path string
}

// NewSink creates a new SQLite sink
func NewSink(path string) (*Sink, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Open database
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Create tables
	if err := createTables(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("create tables: %w", err)
	}

	return &Sink{db: db, path: path}, nil
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
		
		CREATE INDEX IF NOT EXISTS idx_transaction_id ON transactions(transaction_id);
		CREATE INDEX IF NOT EXISTS idx_table_name ON transactions(table_name);
		CREATE INDEX IF NOT EXISTS idx_created_at ON transactions(created_at);
	`)
	return err
}

// Write writes a transaction to SQLite
func (s *Sink) Write(tx *core.Transaction) error {
	sqlTx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer sqlTx.Rollback()

	stmt, err := sqlTx.Prepare(`
		INSERT INTO transactions (transaction_id, table_name, operation, data)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

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
	query := `SELECT id, transaction_id, table_name, operation, data, created_at FROM transactions ORDER BY id DESC`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id int
		var txID, tableName, operation, dataStr string
		var createdAt interface{}

		if err := rows.Scan(&id, &txID, &tableName, &operation, &dataStr, &createdAt); err != nil {
			return nil, err
		}

		var data map[string]interface{}
		json.Unmarshal([]byte(dataStr), &data)

		results = append(results, map[string]interface{}{
			"id":            id,
			"transaction_id": txID,
			"table_name":    tableName,
			"operation":     operation,
			"data":          data,
			"created_at":    createdAt,
		})
	}

	return results, rows.Err()
}