package cdc

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	scannerpkg "github.com/cnlangzi/dbkrab/internal/scanner"
)

// SnapshotConfig holds configuration for snapshot sync
type SnapshotConfig struct {
	BatchSize int `yaml:"batch_size"`
}

// DefaultSnapshotConfig returns default snapshot configuration
func DefaultSnapshotConfig() *SnapshotConfig {
	return &SnapshotConfig{
		BatchSize: 10000,
	}
}

// SnapshotQuerier performs full-table snapshot sync (initial sync without CDC)
type SnapshotQuerier struct {
	db       *sql.DB
	timezone *time.Location
	factory  *ScannerFactory
	config   *SnapshotConfig
}

// NewSnapshotQuerier creates a new SnapshotQuerier
func NewSnapshotQuerier(db *sql.DB, timezone *time.Location, config *SnapshotConfig) *SnapshotQuerier {
	if config == nil {
		config = DefaultSnapshotConfig()
	}
	return &SnapshotQuerier{
		db:       db,
		timezone: timezone,
		factory:  NewScannerFactory(timezone),
		config:   config,
	}
}

// PrimaryKeyInfo holds primary key column information for a table
type PrimaryKeyInfo struct {
	Columns []string // Column names in key ordinal order
}

// DiscoverPrimaryKey discovers primary key columns for a table
// Uses sys.index_columns / sys.indexes / sys.columns ordered by key_ordinal
func (q *SnapshotQuerier) DiscoverPrimaryKey(ctx context.Context, schema, table string) (*PrimaryKeyInfo, error) {
	query := `
		SELECT c.name AS column_name
		FROM sys.index_columns ic
		JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		WHERE i.is_primary_key = 1
			AND i.object_id = OBJECT_ID(@p1)
		ORDER BY ic.key_ordinal
	`
	var pkColumns []string
	rows, err := q.db.QueryContext(ctx, query, fmt.Sprintf("%s.%s", schema, table))
	if err != nil {
		return nil, fmt.Errorf("discover PK: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("failed to close rows", "error", closeErr)
		}
	}()

	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("scan PK column: %w", err)
		}
		pkColumns = append(pkColumns, colName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PK columns: %w", err)
	}

	if len(pkColumns) == 0 {
		return nil, fmt.Errorf("no primary key found for table %s.%s", schema, table)
	}

	return &PrimaryKeyInfo{Columns: pkColumns}, nil
}

// BuildOrderBy builds ORDER BY clause from PK columns
func (pki *PrimaryKeyInfo) BuildOrderBy() string {
	return strings.Join(pki.Columns, ", ")
}

// BuildPagedQuery builds a paginated SELECT query using OFFSET/FETCH
// Uses lexicographic ordering by PK columns
func (pki *PrimaryKeyInfo) BuildPagedQuery(schema, table string, batchSize int, offset int) string {
	orderBy := pki.BuildOrderBy()
	return fmt.Sprintf(`
		SELECT * FROM %s.%s
		ORDER BY %s
		OFFSET %d ROWS FETCH FIRST %d ROWS ONLY
	`, schema, table, orderBy, offset, batchSize)
}

// GetMaxLSN returns the current max LSN (outside read transaction)
// This is used to capture a stable LSN checkpoint before snapshot
func (q *SnapshotQuerier) GetMaxLSN(ctx context.Context) ([]byte, error) {
	var lsn []byte
	err := q.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&lsn)
	return lsn, err
}

// CheckSnapshotIsolation checks if ALLOW_SNAPSHOT_ISOLATION is enabled on the database
// and attempts to enable it if not. Returns error if not supported.
func (q *SnapshotQuerier) CheckSnapshotIsolation(ctx context.Context) error {
	// Check current snapshot isolation state
	var isAllowed int
	err := q.db.QueryRowContext(ctx, `
		SELECT snapshot_isolation_state
		FROM sys.databases
		WHERE name = DB_NAME()
	`).Scan(&isAllowed)
	if err != nil {
		return fmt.Errorf("check snapshot isolation: %w", err)
	}

	if isAllowed == 1 {
		slog.Debug("snapshot isolation already enabled")
		return nil
	}

	// Try to enable snapshot isolation
	slog.Info("enabling snapshot isolation on database")
	_, err = q.db.ExecContext(ctx, `
		ALTER DATABASE SET ALLOW_SNAPSHOT_ISOLATION ON
	`)
	if err != nil {
		return fmt.Errorf("enable snapshot isolation: %w", err)
	}

	slog.Info("snapshot isolation enabled successfully")
	return nil
}

// IncrementLSN returns the next LSN after the given one
func (q *SnapshotQuerier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error) {
	var nextLSN []byte
	err := q.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_increment_lsn(@p1)", lsn).Scan(&nextLSN)
	return nextLSN, err
}

// SnapshotBatch represents a batch of rows from snapshot
type SnapshotBatch struct {
	Table   string
	Offset  int
	Rows    []map[string]interface{}
	HasMore bool
}

// RunSnapshot runs a full-table snapshot for the given table
// It reads the table in batches and processes each batch through the handler
// Returns the startLSN checkpoint that should be stored for subsequent CDC sync
func (q *SnapshotQuerier) RunSnapshot(ctx context.Context, schema, table string, handler SnapshotHandler) ([]byte, error) {
	// Step 0: Check and enable snapshot isolation on the database (must be done before starting transaction)
	if err := q.CheckSnapshotIsolation(ctx); err != nil {
		return nil, fmt.Errorf("snapshot isolation check: %w", err)
	}

	// Step 1: Capture startLSN outside any read transaction (before snapshot reads)
	startLSN, err := q.GetMaxLSN(ctx)
	if err != nil {
		return nil, fmt.Errorf("capture start LSN: %w", err)
	}
	slog.Info("snapshot: captured start LSN", "table", table, "start_lsn", hex.EncodeToString(startLSN))

	// Step 2: Discover primary key for pagination ordering
	pkInfo, err := q.DiscoverPrimaryKey(ctx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("discover primary key: %w", err)
	}
	slog.Debug("snapshot: discovered PK", "table", table, "columns", pkInfo.Columns)

	// Step 3: Start a read-only transaction with snapshot isolation for consistent reads
	// This ensures we get a stable view of the table during snapshot
	tx, err := q.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSnapshot, // Use SNAPSHOT isolation for MVCC consistency
		ReadOnly:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("begin snapshot transaction: %w", err)
	}
	defer func() {
		if closeErr := tx.Rollback(); closeErr != nil {
			slog.Warn("failed to rollback transaction", "error", closeErr)
		}
	}()

	// Step 4: Read table in batches using OFFSET/FETCH pagination
	offset := 0
	batchSize := q.config.BatchSize
	totalRows := 0

	for {
		// Build paginated query
		query := pkInfo.BuildPagedQuery(schema, table, batchSize, offset)
		slog.Debug("snapshot: fetching batch", "table", table, "offset", offset, "batch_size", batchSize)

		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("fetch batch at offset %d: %w", offset, err)
		}

		// Get column information
		columns, err := rows.Columns()
		if err != nil {
			if closeErr := rows.Close(); closeErr != nil {
				slog.Warn("failed to close rows", "error", closeErr)
			}
			return nil, fmt.Errorf("get columns: %w", err)
		}
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			if closeErr := rows.Close(); closeErr != nil {
				slog.Warn("failed to close rows", "error", closeErr)
			}
			return nil, fmt.Errorf("get column types: %w", err)
		}

		// Create scanner destinations
		dest := q.factory.CreateDest(columns, colTypes)

		// Scan rows into batch
		batchRows := make([]map[string]interface{}, 0, batchSize)
		for rows.Next() {
			if err := rows.Scan(dest...); err != nil {
				if closeErr := rows.Close(); closeErr != nil {
					slog.Warn("failed to close rows", "error", closeErr)
				}
				return nil, fmt.Errorf("scan row: %w", err)
			}

			// Build data map from scanned values
			data := make(map[string]interface{})
			for i, col := range columns {
				if s, ok := dest[i].(scannerpkg.Scanner); ok {
					if val, err := s.Value(); err == nil {
						data[strings.ToLower(col)] = val
					}
				}
			}
			batchRows = append(batchRows, data)
		}
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("failed to close rows", "error", closeErr)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate rows: %w", err)
		}

		// Process batch if we have rows
		if len(batchRows) > 0 {
			// Convert to cdc.Change format for skill pipeline
			// Snapshot rows are treated as INSERT operations
			changes := make([]Change, len(batchRows))
			for i, row := range batchRows {
				changes[i] = Change{
					Table:     table,
					Operation: 2, // OpInsert = 2
					Data:      row,
					// LSN is set to empty bytes for snapshot rows (they don't come from CDC)
					// The checkpoint is stored after snapshot completes
					LSN: []byte{},
				}
			}

			// Process batch through handler (skill pipeline + sink)
			if err := handler.HandleSnapshotBatch(ctx, changes); err != nil {
				return nil, fmt.Errorf("handle batch at offset %d: %w", offset, err)
			}

			totalRows += len(batchRows)
			slog.Debug("snapshot: processed batch", "table", table, "offset", offset, "rows", len(batchRows), "total", totalRows)
		}

		// Check if we have more rows
		hasMore := len(batchRows) == batchSize
		if !hasMore {
			break
		}

		// Move to next batch
		offset += batchSize

		// Check for cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	// Commit transaction (read-only, but just to be clean)
	if err := tx.Commit(); err != nil {
		slog.Warn("snapshot: commit transaction failed", "error", err)
	}

	slog.Info("snapshot: completed", "table", table, "total_rows", totalRows, "start_lsn", hex.EncodeToString(startLSN))

	// Return startLSN checkpoint - this should be stored in offset store
	// Subsequent CDC sync should start from increment(startLSN)
	return startLSN, nil
}

// SnapshotHandler processes snapshot batches through skill pipeline and sink
// Uses cdc.Change type to avoid import cycle with core package
type SnapshotHandler interface {
	HandleSnapshotBatch(ctx context.Context, changes []Change) error
}

// SnapshotHandlerFunc is a function type adapter for SnapshotHandler
type SnapshotHandlerFunc func(ctx context.Context, changes []Change) error

// HandleSnapshotBatch implements SnapshotHandler interface
func (f SnapshotHandlerFunc) HandleSnapshotBatch(ctx context.Context, changes []Change) error {
	return f(ctx, changes)
}

// OffsetUpdater updates offset storage after snapshot completes
type OffsetUpdater interface {
	Set(table string, lastLSN string, nextLSN string) error
	Flush() error
}

// SnapshotRunner coordinates full snapshot run with offset update
type SnapshotRunner struct {
	querier     *SnapshotQuerier
	offsetStore OffsetUpdater
}

// NewSnapshotRunner creates a new snapshot runner
func NewSnapshotRunner(querier *SnapshotQuerier, offsetStore OffsetUpdater) *SnapshotRunner {
	return &SnapshotRunner{
		querier:     querier,
		offsetStore: offsetStore,
	}
}

// RunFullSnapshot runs snapshot for a table and updates offset store on success
// schema should be like "dbo", table is the table name without schema
func (r *SnapshotRunner) RunFullSnapshot(ctx context.Context, schema, table string, handler SnapshotHandler) error {
	fullTableName := schema + "." + table

	// Run snapshot and get start LSN checkpoint
	startLSN, err := r.querier.RunSnapshot(ctx, schema, table, handler)
	if err != nil {
		return fmt.Errorf("snapshot run: %w", err)
	}

	// Calculate next LSN for CDC to resume from
	// CDC should start from increment(startLSN), not startLSN itself
	// This ensures no duplicates with snapshot data
	nextLSN, err := r.querier.IncrementLSN(ctx, startLSN)
	if err != nil {
		return fmt.Errorf("increment LSN: %w", err)
	}

	// Update offset store with startLSN and nextLSN
	// last_lsn = startLSN (the checkpoint captured before snapshot)
	// next_lsn = increment(startLSN) (where CDC should resume)
	startLSNStr := hex.EncodeToString(startLSN)
	nextLSNStr := hex.EncodeToString(nextLSN)

	if err := r.offsetStore.Set(fullTableName, startLSNStr, nextLSNStr); err != nil {
		return fmt.Errorf("set offset: %w", err)
	}

	if err := r.offsetStore.Flush(); err != nil {
		return fmt.Errorf("flush offset: %w", err)
	}

	slog.Info("snapshot: offset updated",
		"table", fullTableName,
		"last_lsn", startLSNStr,
		"next_lsn", nextLSNStr)

	return nil
}