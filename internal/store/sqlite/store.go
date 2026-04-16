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

// UpdatePollerState updates the poller state after successful poll.
// fetchedCount is total CDC rows fetched; insertedCount is rows actually written (after dedup).
func (s *Store) UpdatePollerState(lastLSN string, fetchedCount, insertedCount int) error {
	// Use COALESCE to keep existing LSN if new one is empty
	_, err := s.db.Exec(`
		UPDATE poller_state
		SET last_poll_time = CURRENT_TIMESTAMP,
			last_lsn = COALESCE(NULLIF(?, ''), last_lsn),
			total_changes = total_changes + ?,
			total_inserted = total_inserted + ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = 1
	`, lastLSN, fetchedCount, insertedCount)
	return err
}

// GetPollerState returns the current poller state
func (s *Store) GetPollerState() (map[string]interface{}, error) {
	row := s.db.QueryRow(`
		SELECT last_poll_time, last_lsn, total_changes, total_inserted, updated_at
		FROM poller_state
		WHERE id = 1
	`)

	var lastPollTime, lastLSN, updatedAt sql.NullString
	var totalChanges, totalInserted int

	if err := row.Scan(&lastPollTime, &lastLSN, &totalChanges, &totalInserted, &updatedAt); err != nil {
		return nil, err
	}

	state := map[string]interface{}{
		"total_changes":  totalChanges,
		"total_inserted": totalInserted,
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

// Write writes a transaction to SQLite.
// Returns the number of rows actually inserted (0 for rows skipped by INSERT OR IGNORE dedup).
func (s *Store) Write(tx *core.Transaction) (int, error) {
	sqlTx, err := s.db.Writer.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
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

	const insertSQL = `
		INSERT OR IGNORE INTO changes (id, transaction_id, table_name, operation, data, lsn, changed_at, pulled_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`

	rowsInserted := 0
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

		// Use pre-computed ID from poller layer (computed using core.ComputeChangeID)
		// If not available (e.g., changes from other sources), compute it here
		id := change.ID
		if id == "" {
			// Fallback: compute hash (should not happen for CDC-sourced changes)
			hashInput := tx.ID + change.Table + string(dataJSON) + lsnStr + change.Operation.String()
			hash := sha256.Sum256([]byte(hashInput))
			id = hex.EncodeToString(hash[:16])
		}

		res, err := sqlTx.Exec(
			insertSQL,
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
			return 0, fmt.Errorf("insert change: %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			rowsInserted++
		}
	}

	if err := sqlTx.Commit(); err != nil {
		return 0, err
	}
	return rowsInserted, nil
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

// Flush ensures all buffered writes are committed to the underlying database.
func (s *Store) Flush() error {
	return s.db.Flush()
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
	query := `SELECT id, transaction_id, table_name, operation, data, lsn, changed_at, pulled_at FROM changes WHERE 1=1`
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

	query += " ORDER BY changed_at DESC"
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

// GetLSNs returns all unique LSNs from the store, ordered by LSN
func (s *Store) GetLSNs() ([]string, error) {
	rows, err := s.db.Reader.Query(`
		SELECT DISTINCT lsn
		FROM changes
		WHERE lsn IS NOT NULL AND lsn != ''
		ORDER BY lsn ASC
	`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Warn("rows.Close error", "error", err)
		}
	}()

	var lsns []string
	for rows.Next() {
		var lsn string
		if err := rows.Scan(&lsn); err != nil {
			return nil, err
		}
		lsns = append(lsns, lsn)
	}

	return lsns, rows.Err()
}

// GetChangesWithLSN returns all changes for a specific LSN, as core.Change
func (s *Store) GetChangesWithLSN(lsn string) ([]core.Change, error) {
	rows, err := s.db.Reader.Query(`
		SELECT id, transaction_id, table_name, operation, data, lsn, changed_at
		FROM changes
		WHERE lsn = ?
		ORDER BY id
	`, lsn)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Warn("rows.Close error", "error", err)
		}
	}()

	var changes []core.Change
	for rows.Next() {
		var id, txID, tableName, operation, dataStr, lsnStr string
		var changedAt interface{}

		if err := rows.Scan(&id, &txID, &tableName, &operation, &dataStr, &lsnStr, &changedAt); err != nil {
			return nil, err
		}

		// Convert operation string to core.Operation
		op := core.Operation(operationStringToInt(operation))

		// Convert LSN hex string to bytes
		var lsnBytes []byte
		if lsnStr != "" && len(lsnStr) > 2 {
			hexStr := lsnStr
			if len(hexStr) >= 2 && hexStr[:2] == "0x" {
				hexStr = hexStr[2:]
			}
			if decoded, err := hex.DecodeString(hexStr); err != nil {
				slog.Warn("failed to decode LSN hex", "lsn", lsnStr, "error", err)
				lsnBytes = nil
			} else {
				lsnBytes = decoded
			}
		}

		// Parse JSON data to map
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
			slog.Warn("failed to parse JSON data", "id", id, "lsn", lsnStr, "error", err)
			data = make(map[string]interface{})
		}

		// Parse commit time
		var commitTime time.Time
		if changedAt != nil {
			if t, ok := changedAt.(time.Time); ok {
				commitTime = t
			}
		}

		changes = append(changes, core.Change{
			Table:         tableName,
			TransactionID: txID,
			LSN:           lsnBytes,
			Operation:     op,
			CommitTime:    commitTime,
			Data:          data,
			ID:            id, // Use the stored ID
		})
	}

	return changes, rows.Err()
}

// operationStringToInt converts operation string to int
// INSERT→2, DELETE→1, UPDATE_BEFORE→3, UPDATE_AFTER→4
func operationStringToInt(op string) int {
	switch op {
	case "INSERT":
		return 2
	case "DELETE":
		return 1
	case "UPDATE_BEFORE":
		return 3
	case "UPDATE_AFTER":
		return 4
	default:
		return 2 // Default to INSERT for safety
	}
}

// Ensure the Store implements the store.Store interface
var _ store.Store = (*Store)(nil)
