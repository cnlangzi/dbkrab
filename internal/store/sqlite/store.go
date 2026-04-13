package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/sqliteutil"
	"github.com/cnlangzi/dbkrab/internal/store"
)

// Store implements store.Store for SQLite
type Store struct {
	db *store.DB
}

// New creates a new SQLite store.
// The DB should be created via internal/store.New() which runs migrations first.
func New(db *store.DB) (*Store, error) {
	s := &Store{db: db}
	return s, nil
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
	sqlTx, err := s.db.Writer.Begin()
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
		INSERT OR IGNORE INTO transactions (id, transaction_id, table_name, operation, data, lsn, changed_at, pulled_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

		// Convert LSN bytes to hex string with 0x prefix for readability and deterministic sorting
		var lsnStr string
		if len(change.LSN) > 0 {
			lsnStr = "0x" + hex.EncodeToString(change.LSN)
		}

		// Compute content-based id: SHA256(transaction_id + table_name + data + lsn + operation)
		// Take first 16 bytes and encode as 32-character hex string (deterministic, unique per change)
		hashInput := tx.ID + change.Table + string(dataJSON) + lsnStr + change.Operation.String()
		hash := sha256.Sum256([]byte(hashInput))
		id := hex.EncodeToString(hash[:16]) // first 16 bytes = 32 hex chars

		_, err = stmt.Exec(
			id,
			tx.ID,
			change.Table,
			change.Operation.String(),
			string(dataJSON),
			lsnStr,
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
	tx, err := s.db.Writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, op := range ops {
		config := sqliteutil.TableConfig{
			Output:     op.Config.Output,
			PrimaryKey: op.Config.PrimaryKey,
		}

		switch op.OpType {
		case core.OpInsert:
			if err := sqliteutil.InsertInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows); err != nil {
				return fmt.Errorf("insert %s: %w", op.Config.Output, err)
			}
		case core.OpUpdateAfter:
			if err := sqliteutil.UpdateInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows); err != nil {
				return fmt.Errorf("update %s: %w", op.Config.Output, err)
			}
		case core.OpDelete:
			if err := sqliteutil.DeleteInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows); err != nil {
				return fmt.Errorf("delete %s: %w", op.Config.Output, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
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
	query := `SELECT id, transaction_id, table_name, operation, data, lsn, changed_at, pulled_at FROM transactions WHERE 1=1`
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

	rows, err := s.db.Reader.Query(query, args...)
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
		var id string
		var resultTxID, resultTableName, resultOperation, dataStr, lsnStr string
		var cdcTime, pulledAt interface{}

		if err := rows.Scan(&id, &resultTxID, &resultTableName, &resultOperation, &dataStr, &lsnStr, &cdcTime, &pulledAt); err != nil {
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
			"lsn":            lsnStr,
			"changed_at":     cdcTime,
			"pulled_at":      pulledAt,
		})
	}

	return results, rows.Err()
}

// Ensure the Store implements the store.Store interface
var _ store.Store = (*Store)(nil)
