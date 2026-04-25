package integration

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnlangzi/dbkrab/internal/cdc"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/offset"
	"github.com/cnlangzi/dbkrab/internal/store"
	"github.com/cnlangzi/dbkrab/internal/store/sqlite"
	"github.com/cnlangzi/dbkrab/tests/integration/mockmssql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupMockDB sets up a mock MSSQL connection for testing
func setupMockDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mockmssql", "")
	require.NoError(t, err, "Failed to open mock MSSQL connection")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	require.NoError(t, err, "Failed to ping mock MSSQL")

	return db
}

// setupSQLiteTestDB sets up a real SQLite database for testing
func setupSQLiteTestDB(t *testing.T) (*store.DB, func()) {
	tmpDir, err := os.MkdirTemp("", "dbkrab-integration-test-*")
	require.NoError(t, err, "Failed to create temp directory")

	dbPath := filepath.Join(tmpDir, "test.db")

	ctx := context.Background()
	db, err := store.New(ctx, store.Config{
		File:       dbPath,
		ModuleName: "store",
	})
	require.NoError(t, err, "Failed to create SQLite store")

	cleanup := func() {
		if db != nil {
			_ = db.Close()
		}
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

// setupOffsetStore sets up a real SQLite offset store
func setupOffsetStore(t *testing.T) (*offset.SQLiteStore, func()) {
	tmpDir, err := os.MkdirTemp("", "dbkrab-offset-test-*")
	require.NoError(t, err, "Failed to create temp directory")

	dbPath := filepath.Join(tmpDir, "offset.db")

	ctx := context.Background()
	offsetStore, err := offset.New(ctx, dbPath)
	require.NoError(t, err, "Failed to create offset store")

	cleanup := func() {
		if offsetStore != nil {
			_ = offsetStore.Close()
		}
		os.RemoveAll(tmpDir)
	}

	return offsetStore, cleanup
}

// TestMockMSSQLDriver verifies that the mock MSSQL driver works correctly
func TestMockMSSQLDriver(t *testing.T) {
	db := setupMockDB(t)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test basic query - GetMaxLSN
	var lsn []byte
	err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&lsn)
	require.NoError(t, err, "Failed to query max LSN")
	assert.NotNil(t, lsn, "LSN should not be nil")
	t.Logf("Max LSN: %x", lsn)

	// Test GetMinLSN for TestProducts
	var minLSN []byte
	err = db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn('dbo_TestProducts')").Scan(&minLSN)
	require.NoError(t, err, "Failed to query min LSN")
	assert.NotNil(t, minLSN, "Min LSN should not be nil")
	t.Logf("Min LSN for TestProducts: %x", minLSN)
}

// TestMockCDCQuerier verifies that the mock CDCQuerier works correctly
func TestMockCDCQuerier(t *testing.T) {
	handler := mockmssql.HandlerForTest()
	querier := mockmssql.NewCDCQuerier(handler)

	ctx := context.Background()

	// Test GetMinLSN
	minLSN, err := querier.GetMinLSN(ctx, "dbo_TestProducts")
	require.NoError(t, err, "Failed to get min LSN")
	assert.NotNil(t, minLSN, "Min LSN should not be nil")
	t.Logf("Min LSN: %x", minLSN)

	// Test GetMaxLSN
	maxLSN, err := querier.GetMaxLSN(ctx)
	require.NoError(t, err, "Failed to get max LSN")
	assert.NotNil(t, maxLSN, "Max LSN should not be nil")
	t.Logf("Max LSN: %x", maxLSN)

	// Test IncrementLSN
	nextLSN, err := querier.IncrementLSN(ctx, minLSN)
	require.NoError(t, err, "Failed to increment LSN")
	assert.NotNil(t, nextLSN, "Next LSN should not be nil")
	t.Logf("Next LSN: %x", nextLSN)

	// Test GetChanges
	fromLSN := minLSN
	toLSN := maxLSN
	changes, err := querier.GetChanges(ctx, "dbo_TestProducts", "TestProducts", fromLSN, toLSN)
	require.NoError(t, err, "Failed to get changes")
	// When fromLSN == toLSN, no changes are returned
	t.Logf("Changes count: %d", len(changes))
}

// TestMockCDCQuerierWithChanges verifies that changes are returned when LSNs differ
func TestMockCDCQuerierWithChanges(t *testing.T) {
	handler := mockmssql.HandlerForTest()
	querier := mockmssql.NewCDCQuerier(handler)

	ctx := context.Background()

	// Get a base LSN and increment it to get different LSNs
	baseLSN, err := querier.GetMinLSN(ctx, "dbo_TestProducts")
	require.NoError(t, err, "Failed to get min LSN")

	nextLSN, err := querier.IncrementLSN(ctx, baseLSN)
	require.NoError(t, err, "Failed to increment LSN")

	// Now GetChanges should return changes
	changes, err := querier.GetChanges(ctx, "dbo_TestProducts", "TestProducts", baseLSN, nextLSN)
	require.NoError(t, err, "Failed to get changes")
	assert.Greater(t, len(changes), 0, "Should have changes when LSNs differ")

	if len(changes) > 0 {
		t.Logf("First change: table=%s, op=%d, data=%v",
			changes[0].Table, changes[0].Operation, changes[0].Data)
	}
}

// TestIntegrationWithSQLite verifies the full integration flow with mock MSSQL and real SQLite
func TestIntegrationWithSQLite(t *testing.T) {
	// Setup mock MSSQL
	mockDB := setupMockDB(t)
	defer func() { _ = mockDB.Close() }()

	// Setup real SQLite store (migrations are run automatically by store.New)
	sqliteDB, cleanupSQLite := setupSQLiteTestDB(t)
	defer cleanupSQLite()

	// Setup real offset store
	offsetStore, cleanupOffset := setupOffsetStore(t)
	defer cleanupOffset()

	ctx := context.Background()

	// Create a store wrapper for SQLite
	storeWrapper, err := sqlite.New(sqliteDB)
	require.NoError(t, err, "Failed to create SQLite store wrapper")

	// Create mock CDCQuerier
	handler := mockmssql.HandlerForTest()
	mockQuerier := mockmssql.NewCDCQuerier(handler)

	// Test that we can use the mock querier to get CDC changes
	minLSN, err := mockQuerier.GetMinLSN(ctx, "dbo_TestProducts")
	require.NoError(t, err, "Failed to get min LSN")

	nextLSN, err := mockQuerier.IncrementLSN(ctx, minLSN)
	require.NoError(t, err, "Failed to increment LSN")

	// Get changes
	changes, err := mockQuerier.GetChanges(ctx, "dbo_TestProducts", "TestProducts", minLSN, nextLSN)
	require.NoError(t, err, "Failed to get changes")

	// Verify we got changes
	assert.Greater(t, len(changes), 0, "Should have changes")

	if len(changes) > 0 {
		// Convert core.Change format for store
		coreChanges := make([]core.Change, len(changes))
		for i, c := range changes {
			coreChanges[i] = core.Change{
				Table:         c.Table,
				TransactionID: c.TransactionID,
				LSN:           c.LSN,
				Operation:     core.Operation(c.Operation),
				Data:          c.Data,
				CommitTime:    c.CommitTime,
			}
		}

		// Write to SQLite store (schema should exist after Migrate)
		_, err = storeWrapper.Write(coreChanges)
		require.NoError(t, err, "Failed to write changes to SQLite store")

		// Test offset store
		err = offsetStore.Set("dbo.TestProducts", "000000000100000001", "000000000100000002")
		require.NoError(t, err, "Failed to set offset")
		err = offsetStore.Flush()
		require.NoError(t, err, "Failed to flush offset")
		t.Logf("Successfully wrote offset to SQLite store")
	}

	t.Logf("Integration test completed: got %d changes from mock MSSQL", len(changes))
}

// TestMockMSQLWithRealPoller verifies that the mock querier can be used with real poller components
func TestMockMSQLWithRealPoller(t *testing.T) {
	// This test verifies the integration points work correctly
	// We don't run the full poller loop, but verify the interfaces are compatible

	// Setup mock MSSQL
	mockDB := setupMockDB(t)
	defer func() { _ = mockDB.Close() }()

	// Setup real SQLite offset store
	offsetStore, cleanupOffset := setupOffsetStore(t)
	defer cleanupOffset()

	// Create real cdc.Querier to verify it can query the mock database
	ctx := context.Background()
	q := cdc.NewQuerier(mockDB, time.UTC)

	// Verify cdc.Querier can use the mock database for LSN queries
	maxLSN, err := q.GetMaxLSN(ctx)
	require.NoError(t, err, "Failed to get max LSN via cdc.Querier")
	assert.NotNil(t, maxLSN, "Max LSN should not be nil")
	t.Logf("Max LSN via cdc.Querier: %x", maxLSN)

	// Test offset store basic operations
	err = offsetStore.Set("dbo.TestProducts", "000000000100000001", "000000000100000002")
	require.NoError(t, err, "Failed to set offset")
	err = offsetStore.Flush()
	require.NoError(t, err, "Failed to flush offset")

	// Get offset back
	off, err := offsetStore.Get("dbo.TestProducts")
	require.NoError(t, err, "Failed to get offset")
	assert.Equal(t, "000000000100000001", off.LastLSN, "LastLSN should match")
	assert.Equal(t, "000000000100000002", off.NextLSN, "NextLSN should match")

	t.Log("Mock MSSQL is compatible with CDCQuerier interface")
}

// TestCDCChangeTypes verifies all CDC operation types are handled correctly
func TestCDCChangeTypes(t *testing.T) {
	handler := mockmssql.HandlerForTest()
	querier := mockmssql.NewCDCQuerier(handler)

	ctx := context.Background()

	// Get a base LSN and increment it to get different LSNs
	baseLSN, err := querier.GetMinLSN(ctx, "dbo_TestProducts")
	require.NoError(t, err)

	nextLSN, err := querier.IncrementLSN(ctx, baseLSN)
	require.NoError(t, err)

	// Test INSERT operation
	changes, err := querier.GetChanges(ctx, "dbo_TestProducts", "TestProducts", baseLSN, nextLSN)
	require.NoError(t, err)
	require.Greater(t, len(changes), 0, "Should have at least one change when LSNs differ")

	change := changes[0]

	// Verify operation types (2 = INSERT per MSSQL CDC)
	assert.Equal(t, 2, change.Operation, "Operation should be INSERT (2)")

	// Verify data fields exist
	assert.Contains(t, change.Data, "productid")
	assert.Contains(t, change.Data, "productname")
	assert.Contains(t, change.Data, "price")
	assert.Contains(t, change.Data, "stock")

	t.Logf("CDC Change: op=%d, table=%s, data=%v",
		change.Operation, change.Table, change.Data)
}

// TestSQLiteStoreWrites verifies that real SQLite store can write data
func TestSQLiteStoreWrites(t *testing.T) {
	// Setup real SQLite store (migrations are run automatically by store.New)
	sqliteDB, cleanupSQLite := setupSQLiteTestDB(t)
	defer cleanupSQLite()

	// Create a store wrapper for SQLite
	storeWrapper, err := sqlite.New(sqliteDB)
	require.NoError(t, err, "Failed to create SQLite store wrapper")

	// Create test changes
	changes := []core.Change{
		{
			Table:         "TestProducts",
			TransactionID: "tx-001",
			LSN:           []byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 1},
			Operation:     core.OpInsert,
			Data: map[string]interface{}{
				"productid":   1,
				"productname": "Test Product",
				"price":       19.99,
				"stock":       100,
			},
			CommitTime: time.Now(),
		},
	}

	// Write to store (schema should exist after Migrate)
	count, err := storeWrapper.Write(changes)
	require.NoError(t, err, "Failed to write changes to SQLite store")
	assert.Equal(t, 1, count, "Should write 1 change")
	t.Logf("Successfully wrote %d changes to SQLite", count)
}

// memOffsetStore implements offset.StoreInterface for testing
type memOffsetStore struct {
	offsets map[string]offset.Offset
}

func newMemOffsetStore() *memOffsetStore {
	return &memOffsetStore{
		offsets: make(map[string]offset.Offset),
	}
}

func (s *memOffsetStore) Load() error                         { return nil }
func (s *memOffsetStore) Save() error                         { return nil }
func (s *memOffsetStore) Flush() error                        { return nil }
func (s *memOffsetStore) Get(table string) (offset.Offset, error) {
	if o, ok := s.offsets[table]; ok {
		return o, nil
	}
	return offset.Offset{}, nil
}
func (s *memOffsetStore) Set(table string, lastLSN string, nextLSN string) error {
	s.offsets[table] = offset.Offset{
		LastLSN:   lastLSN,
		NextLSN:   nextLSN,
		UpdatedAt: time.Now(),
	}
	return nil
}
func (s *memOffsetStore) GetAll() (map[string]offset.Offset, error) {
	// Return a copy to avoid mutation issues
	result := make(map[string]offset.Offset)
	for k, v := range s.offsets {
		result[k] = v
	}
	return result, nil
}

// memStore implements store.Store for testing
type memStore struct {
	writes       [][]core.Change
	writeErr     error
	pollerState  map[string]interface{}
	closeErr     error
}

func newMemStore() *memStore {
	return &memStore{
		writes:      make([][]core.Change, 0),
		pollerState: make(map[string]interface{}),
	}
}

func (s *memStore) Write(changes []core.Change) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	s.writes = append(s.writes, changes)
	return len(changes), nil
}

func (s *memStore) Flush() error                        { return nil }
func (s *memStore) Close() error                        { return s.closeErr }
func (s *memStore) WriteOps(ops []core.Sink) error      { return nil }
func (s *memStore) GetChanges(limit int) ([]map[string]interface{}, error) {
	return nil, nil
}
func (s *memStore) GetChangesWithFilter(limit int, tableName, operation, txID string) ([]map[string]interface{}, error) {
	return nil, nil
}
func (s *memStore) UpdatePollerState(lastLSN string, fetchedCount, insertedCount int) error {
	s.pollerState["last_lsn"] = lastLSN
	s.pollerState["total_changes"] = fetchedCount
	s.pollerState["total_inserted"] = insertedCount
	return nil
}
func (s *memStore) GetPollerState() (map[string]interface{}, error) {
	return s.pollerState, nil
}
func (s *memStore) GetLSNs() ([]string, error)                    { return nil, nil }
func (s *memStore) GetChangesWithLSN(lsn string) ([]core.Change, error) {
	return nil, nil
}

// TestCDCStatus tests the Status() method of ChangeCapturer
func TestCDCStatus(t *testing.T) {
	// Setup mock MSSQL
	mockDB := setupMockDB(t)
	defer func() { _ = mockDB.Close() }()

	// Setup stores
	memOffsetStore := newMemOffsetStore()
	memStore := newMemStore()

	// Create mock CDCQuerier
	handler := mockmssql.HandlerForTest()
	mockQuerier := mockmssql.NewCDCQuerier(handler)

	// Manually create a ChangeCapturer with mock dependencies
	// We can't use NewChangeCapturer because it requires a real db connection,
	// but we can construct the struct directly
	capturer := &cdc.ChangeCapturer{
		Querier:   mockQuerier,
		Tables:    []string{"dbo.TestProducts"},
		OffsetMgr: cdc.NewOffsetManager(memOffsetStore, mockQuerier),
		Store:     memStore,
		Interval:  time.Second,
	}

	// Initial status should have zero values
	status := capturer.Status()
	assert.NotNil(t, status, "Status should not be nil")
	assert.Equal(t, 0, status["total_changes"], "Initial total_changes should be 0")
	assert.Equal(t, 0, status["total_inserted"], "Initial total_inserted should be 0")

	// Call Fetch to trigger a poll cycle
	ctx := context.Background()
	result := capturer.Fetch(ctx)

	// Verify Fetch completed without error
	assert.NotNil(t, result, "Fetch result should not be nil")
	assert.Equal(t, core.CapturerCDC, result.NextCapturer, "Next capturer should be CDC")

	// Status should now reflect the poll time
	status = capturer.Status()
	assert.NotNil(t, status, "Status should not be nil after Fetch")
	assert.NotNil(t, status["last_poll_time"], "last_poll_time should be set after Fetch")
	assert.NotEmpty(t, status["last_poll_time"], "last_poll_time should not be empty")

	// Pre-populate the offset store to test Status() with stored offsets
	// (Fetch may not produce changes in mock environment, but Status should still work)
	memOffsetStore.Set("dbo.TestProducts", "000000000100000001", "000000000100000002")

	// Status should now include last_lsn from offset
	status = capturer.Status()
	assert.NotNil(t, status["last_lsn"], "last_lsn should be available from offset")
	assert.Equal(t, "000000000100000001", status["last_lsn"], "last_lsn should match the stored offset")

	t.Logf("CDC Status after Fetch: last_poll_time=%v, last_lsn=%v, total_changes=%v",
		status["last_poll_time"], status["last_lsn"], status["total_changes"])
}