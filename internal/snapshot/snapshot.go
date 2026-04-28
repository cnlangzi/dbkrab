package snapshot

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	scannerpkg "github.com/cnlangzi/dbkrab/internal/types"
)

// Config holds configuration for snapshot sync operations.
// BatchSize determines how many rows are fetched per batch during snapshot.
type Config struct {
	// BatchSize is the number of rows to fetch per batch (default: 10000).
	BatchSize int `yaml:"batch_size"`
}

// DefaultConfig returns the default snapshot configuration with BatchSize=10000.
func DefaultConfig() *Config {
	return &Config{
		BatchSize: 10000,
	}
}

// snapshotTxOptions returns the sql.TxOptions for starting a snapshot transaction.
// Callers should pass the returned *sql.TxOptions to db.BeginTx.
//
// NOTE: ReadOnly is intentionally set to false (not omitted). On SQL Server
// (mssql-go driver), combining sql.LevelSnapshot with ReadOnly:true causes
// "read-only transactions are not supported" at BeginTx. Snapshot isolation
// already provides MVCC consistent reads, so ReadOnly is not needed and
// would trigger the error. See: https://docs.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql
func snapshotTxOptions() *sql.TxOptions {
	return &sql.TxOptions{
		Isolation: sql.LevelSnapshot,
		ReadOnly:  false,
	}
}

// Querier performs full-table snapshot sync (initial sync without CDC).
// It reads all rows from a table using snapshot isolation and converts them
// to core.Change records for downstream processing.
type Querier struct {
	db       *sql.DB
	timezone *time.Location
	factory  *scannerpkg.Codec
	config   *Config
}

// NewQuerier creates a new snapshot Querier with the given database connection,
// timezone for timestamp conversion, and optional configuration.
// If config is nil, DefaultConfig() is used.
func NewQuerier(db *sql.DB, timezone *time.Location, config *Config) *Querier {
	if config == nil {
		config = DefaultConfig()
	}
	return &Querier{
		db:       db,
		timezone: timezone,
		factory:  scannerpkg.NewMSSQLCodecWithOptions(timezone, ""),
		config:   config,
	}
}

// PrimaryKeyInfo holds primary key column information for pagination queries.
// Columns are ordered by key_ordinal from sys.index_columns.
type PrimaryKeyInfo struct {
	Columns []string // Column names in key ordinal order
}

// DiscoverPrimaryKey discovers primary key columns for a table
// Uses sys.index_columns / sys.indexes / sys.columns ordered by key_ordinal
func (q *Querier) DiscoverPrimaryKey(ctx context.Context, schema, table string) (*PrimaryKeyInfo, error) {
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

// BuildOrderBy builds ORDER BY clause from PK columns for pagination queries.
func (pki *PrimaryKeyInfo) BuildOrderBy() string {
	return strings.Join(pki.Columns, ", ")
}

// BuildPagedQuery builds a paginated SELECT query using OFFSET ... FETCH NEXT.
// This is a single-statement T-SQL pagination that is safe within snapshot-isolation transactions.
// Note: DiscoverPrimaryKey guarantees pki.Columns is non-empty.
func (pki *PrimaryKeyInfo) BuildPagedQuery(schema, table string, batchSize int, offset int) string {
	orderBy := "ORDER BY " + strings.Join(pki.Columns, ", ")
	return fmt.Sprintf(
		`SELECT * FROM %s.%s %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`,
		schema, table, orderBy, offset, batchSize,
	)
}

// GetMaxLSN returns the current max LSN (outside read transaction)
// This is used to capture a stable LSN checkpoint before snapshot
func (q *Querier) GetMaxLSN(ctx context.Context) ([]byte, error) {
	var lsn []byte
	err := q.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&lsn)
	return lsn, err
}

// CheckSnapshotIsolation checks if ALLOW_SNAPSHOT_ISOLATION is enabled on the database
// and attempts to enable it if not. Returns error if not supported.
func (q *Querier) CheckSnapshotIsolation(ctx context.Context) error {
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
func (q *Querier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error) {
	var nextLSN []byte
	err := q.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_increment_lsn(@p1)", lsn).Scan(&nextLSN)
	return nextLSN, err
}

// GetApproxRowCount returns the approximate row count for a table using sys.partitions.
// This avoids a full COUNT(*) scan and returns results in milliseconds.
// The count may be slightly off if rows were inserted/deleted recently and stats not yet updated.
func (q *Querier) GetApproxRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := q.db.QueryRowContext(ctx, `
		SELECT ISNULL(SUM(p.rows), 0)
		FROM sys.partitions p
		JOIN sys.objects o ON p.object_id = o.object_id
		JOIN sys.schemas s ON o.schema_id = s.schema_id
		WHERE s.name = @p1
		  AND o.name = @p2
		  AND p.index_id IN (0, 1)
	`, schema, table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("get approx row count for %s.%s: %w", schema, table, err)
	}
	return count, nil
}

// ScanBatch scans all rows from the given *sql.Rows into a slice of maps.
// Closes rows when done. The dest scanners are recreated per call using column metadata.
func (q *Querier) ScanBatch(rows *sql.Rows) ([]map[string]interface{}, error) {
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("snapshot: failed to close rows", "error", closeErr)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}

	dest := q.factory.CreateDest(colTypes)
	result := make([]map[string]interface{}, 0)

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		data := make(map[string]interface{})
		for i, col := range columns {
			if s, ok := dest[i].(scannerpkg.DBType); ok {
				if val, scanErr := s.Value(); scanErr == nil {
					data[strings.ToLower(col)] = val
				} else {
					slog.Debug("snapshot: skip column due to Value() error", "column", col, "err", scanErr)
				}
			}
		}
		result = append(result, data)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return result, nil
}

// BeginSnapshotTx opens a snapshot-isolation transaction on the querier's db.
func (q *Querier) BeginSnapshotTx(ctx context.Context) (*sql.Tx, error) {
	tx, err := q.db.BeginTx(ctx, snapshotTxOptions())
	if err != nil {
		return nil, fmt.Errorf("begin snapshot tx: %w", err)
	}
	return tx, nil
}

// Batch represents a batch of rows fetched during snapshot pagination.
// HasMore indicates whether more rows exist beyond the current batch.
type Batch struct {
	Table   string
	Offset  int
	Rows    []map[string]interface{}
	HasMore bool
}

// Run runs a full-table snapshot for the given table
// It reads the table in batches and processes each batch through the handler
// Returns the startLSN checkpoint that should be stored for subsequent CDC sync
func (q *Querier) Run(ctx context.Context, schema, table string, handler TableHandler) ([]byte, error) {
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

	// Step 3: Start a transaction with snapshot isolation for consistent reads.
	// This ensures we get a stable view of the table during snapshot.
	tx, err := q.db.BeginTx(ctx, snapshotTxOptions())
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
		dest := q.factory.CreateDest(colTypes)

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
				if s, ok := dest[i].(scannerpkg.DBType); ok {
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
			// Convert to core.Change format for skill pipeline
			// Snapshot rows are treated as INSERT operations
			changes := make([]core.Change, len(batchRows))
			for i, row := range batchRows {
				changes[i] = core.Change{
					Table:     table,
					Operation: core.OpInsert, // OpInsert = 2
					Data:      row,
					// LSN is set to empty bytes for snapshot rows (they don't come from CDC)
					// The checkpoint is stored after snapshot completes
					LSN: []byte{},
				}
			}

			// Process batch through handler (skill pipeline + sink)
			if err := handler.HandleTable(ctx, changes); err != nil {
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

// TableHandler processes snapshot batches through skill pipeline and sink.
// Implementations should handle each batch of core.Change records per table.
type TableHandler interface {
	HandleTable(ctx context.Context, changes []core.Change) error
}

// TableHandlerFunc is a function type adapter that allows using ordinary
// functions as TableHandler implementations.
type TableHandlerFunc func(ctx context.Context, changes []core.Change) error

// HandleTable implements the TableHandler interface by calling the function itself.
func (f TableHandlerFunc) HandleTable(ctx context.Context, changes []core.Change) error {
	return f(ctx, changes)
}

// OffsetUpdater updates offset storage after snapshot completes.
// It persists the LSN checkpoint so subsequent CDC sync can resume correctly.
type OffsetUpdater interface {
	Set(table string, lastLSN string, nextLSN string) error
	Flush() error
}

// Runner coordinates full snapshot run with offset update.
// It runs the snapshot querier and persists the resulting LSN checkpoint.
type Runner struct {
	querier     *Querier
	offsetStore OffsetUpdater
}

// NewRunner creates a new snapshot Runner with the given querier and offset store.
func NewRunner(querier *Querier, offsetStore OffsetUpdater) *Runner {
	return &Runner{
		querier:     querier,
		offsetStore: offsetStore,
	}
}

// RunFull runs snapshot for a table and updates offset store on success.
// The schema parameter should be like "dbo", and table is the table name without schema.
// After snapshot completes, the LSN checkpoint is stored so CDC can resume.
func (r *Runner) RunFull(ctx context.Context, schema, table string, handler TableHandler) error {
	fullTableName := schema + "." + table

	// Run snapshot and get start LSN checkpoint
	startLSN, err := r.querier.Run(ctx, schema, table, handler)
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
