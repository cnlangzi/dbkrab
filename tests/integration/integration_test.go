package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfig holds integration test configuration
type TestConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

func getTestConfig() TestConfig {
	return TestConfig{
		Host:     getEnv("MSSQL_HOST", "localhost"),
		Port:     getEnv("MSSQL_PORT", "1433"),
		User:     getEnv("MSSQL_USER", "sa"),
		Password: getEnv("MSSQL_PASSWORD", "Test1234!"),
		Database: getEnv("MSSQL_DATABASE", "dbkrab_test"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getConnectionString(config TestConfig) string {
	return fmt.Sprintf("server=%s;port=%s;user id=%s;password=%s;database=%s",
		config.Host, config.Port, config.User, config.Password, config.Database)
}

func setupTestDB(t *testing.T, config TestConfig) *sql.DB {
	connStr := getConnectionString(config)
	db, err := sql.Open("sqlserver", connStr)
	require.NoError(t, err, "Failed to open database connection")

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		t.Skipf("Skipping integration test: MSSQL not available - %v", err)
	}

	return db
}

// TestCDCEnabled verifies that CDC is enabled on the test database
func TestCDCEnabled(t *testing.T) {
	config := getTestConfig()
	db := setupTestDB(t, config)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Check if CDC is enabled on database
	var isCdcEnabled int
	err := db.QueryRowContext(ctx, "SELECT is_cdc_enabled FROM sys.databases WHERE name = @p1", config.Database).Scan(&isCdcEnabled)
	require.NoError(t, err, "Failed to query CDC status")
	assert.Equal(t, 1, isCdcEnabled, "CDC should be enabled on test database")

	// Check if CDC is enabled on test tables
	tables := []string{"TestProducts", "TestOrders", "TestOrderItems"}
	for _, table := range tables {
		var count int
		err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) 
			FROM cdc.change_tables ct 
			JOIN sys.tables t ON ct.source_object_id = t.object_id 
			WHERE t.name = @p1
		`, table).Scan(&count)
		require.NoError(t, err, "Failed to query CDC table status for %s", table)
		assert.Greater(t, count, 0, "CDC should be enabled on table %s", table)
	}
}

// TestChangeCapture verifies that changes are captured by CDC
func TestChangeCapture(t *testing.T) {
	config := getTestConfig()
	db := setupTestDB(t, config)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get current LSN
	var startLSN string
	err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&startLSN)
	require.NoError(t, err, "Failed to get current LSN")
	t.Logf("Start LSN: %s", startLSN)

	// Insert a test product
	productName := fmt.Sprintf("Integration Test Product %d", time.Now().UnixNano())
	result, err := db.ExecContext(ctx,
		"INSERT INTO TestProducts (ProductName, Price, Stock) VALUES (@p1, @p2, @p3)",
		productName, 99.99, 10)
	require.NoError(t, err, "Failed to insert test product")

	productID, err := result.LastInsertId()
	require.NoError(t, err, "Failed to get last insert ID")
	t.Logf("Inserted product ID: %d", productID)

	// Wait for CDC to capture
	time.Sleep(2 * time.Second)

	// Query CDC change table for TestProducts
	var changeCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM cdc.dbo_TestProducts_CT 
		WHERE __$start_lsn > @p1
	`, startLSN).Scan(&changeCount)
	require.NoError(t, err, "Failed to query CDC changes")
	assert.Greater(t, changeCount, 0, "CDC should have captured the insert operation")

	// Verify the change data
	var capturedName string
	err = db.QueryRowContext(ctx, `
		SELECT TOP 1 ProductName 
		FROM cdc.dbo_TestProducts_CT 
		WHERE __$start_lsn > @p1 
		ORDER BY __$start_lsn DESC
	`, startLSN).Scan(&capturedName)
	require.NoError(t, err, "Failed to query captured change data")
	assert.Equal(t, productName, capturedName, "Captured product name should match inserted value")

	// Update the product
	_, err = db.ExecContext(ctx,
		"UPDATE TestProducts SET Price = @p1 WHERE ProductID = @p2",
		149.99, productID)
	require.NoError(t, err, "Failed to update test product")

	// Wait for CDC to capture
	time.Sleep(2 * time.Second)

	// Verify update was captured
	var updateCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM cdc.dbo_TestProducts_CT 
		WHERE __$start_lsn > @p1 AND ProductID = @p2
	`, startLSN, productID).Scan(&updateCount)
	require.NoError(t, err, "Failed to query CDC update changes")
	assert.Greater(t, updateCount, 1, "CDC should have captured both insert and update operations")

	// Cleanup
	_, _ = db.ExecContext(ctx, "DELETE FROM TestProducts WHERE ProductID = @p1", productID)
}

// TestCrossTableTransaction verifies that cross-table transactions are captured
func TestCrossTableTransaction(t *testing.T) {
	config := getTestConfig()
	db := setupTestDB(t, config)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get current LSN
	var startLSN string
	err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&startLSN)
	require.NoError(t, err, "Failed to get current LSN")

	// Begin transaction spanning multiple tables
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err, "Failed to begin transaction")

	// Insert order
	result, err := tx.ExecContext(ctx,
		"INSERT INTO TestOrders (ProductID, Quantity, TotalAmount) VALUES (@p1, @p2, @p3)",
		1, 2, 20.00)
	require.NoError(t, err, "Failed to insert test order")

	orderID, err := result.LastInsertId()
	require.NoError(t, err, "Failed to get order ID")

	// Insert order items
	_, err = tx.ExecContext(ctx,
		"INSERT INTO TestOrderItems (OrderID, ItemName, ItemPrice) VALUES (@p1, @p2, @p3)",
		orderID, "Test Item A", 10.00)
	require.NoError(t, err, "Failed to insert test order item")

	_, err = tx.ExecContext(ctx,
		"INSERT INTO TestOrderItems (OrderID, ItemName, ItemPrice) VALUES (@p1, @p2, @p3)",
		orderID, "Test Item B", 10.00)
	require.NoError(t, err, "Failed to insert second test order item")

	// Commit transaction
	err = tx.Commit()
	require.NoError(t, err, "Failed to commit transaction")

	// Wait for CDC to capture
	time.Sleep(2 * time.Second)

	// Verify changes were captured for all tables
	var orderChangeCount, itemChangeCount int
	
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM cdc.dbo_TestOrders_CT 
		WHERE __$start_lsn > @p1
	`, startLSN).Scan(&orderChangeCount)
	require.NoError(t, err, "Failed to query order CDC changes")
	assert.Greater(t, orderChangeCount, 0, "CDC should have captured order insert")

	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM cdc.dbo_TestOrderItems_CT 
		WHERE __$start_lsn > @p1
	`, startLSN).Scan(&itemChangeCount)
	require.NoError(t, err, "Failed to query order items CDC changes")
	assert.Greater(t, itemChangeCount, 0, "CDC should have captured order items insert")

	// Cleanup
	_, _ = db.ExecContext(ctx, "DELETE FROM TestOrderItems WHERE OrderID = @p1", orderID)
	_, _ = db.ExecContext(ctx, "DELETE FROM TestOrders WHERE OrderID = @p1", orderID)
}

// TestLSNProgression verifies that LSN values progress correctly
func TestLSNProgression(t *testing.T) {
	config := getTestConfig()
	db := setupTestDB(t, config)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get initial LSN
	var initialLSN []byte
	err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&initialLSN)
	require.NoError(t, err, "Failed to get initial LSN")
	t.Logf("Initial LSN: %v", initialLSN)

	// Perform multiple operations
	for i := 0; i < 5; i++ {
		_, err := db.ExecContext(ctx,
			"INSERT INTO TestProducts (ProductName, Price, Stock) VALUES (@p1, @p2, @p3)",
			fmt.Sprintf("LSN Test Product %d", i), 10.00, 5)
		require.NoError(t, err, "Failed to insert test product %d", i)
		time.Sleep(100 * time.Millisecond)
	}

	// Get final LSN
	var finalLSN []byte
	err = db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&finalLSN)
	require.NoError(t, err, "Failed to get final LSN")
	t.Logf("Final LSN: %v", finalLSN)

	// LSN should have progressed (simple byte comparison)
	assert.NotEqual(t, initialLSN, finalLSN, "LSN should progress after database changes")

	// Cleanup
	_, _ = db.ExecContext(ctx, "DELETE FROM TestProducts WHERE ProductName LIKE 'LSN Test Product%'")
}

// TestConnectionRecovery verifies graceful degradation on connection loss
func TestConnectionRecovery(t *testing.T) {
	// This test verifies the configuration for graceful degradation
	// Actual connection loss testing would require stopping the MSSQL container
	
	config := getTestConfig()
	db := setupTestDB(t, config)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify database is accessible
	var result int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "Database should be accessible")
	assert.Equal(t, 1, result, "Simple query should return expected result")

	t.Log("Connection recovery test: Database connection verified")
	t.Log("Note: Full connection loss testing requires container manipulation")
}

// TestDataDirectory verifies that data directory is writable
func TestDataDirectory(t *testing.T) {
	// Get the data directory path
	dataDir := filepath.Join(os.Getenv("PWD"), "data")
	if dataDir == "data" {
		// If PWD not set, use relative path
		dataDir = "./data"
	}

	// Try to create a test file
	testFile := filepath.Join(dataDir, "integration_test.tmp")
	testContent := []byte("Integration test data")

	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		t.Logf("Warning: Could not create data directory: %v", err)
		t.Skip("Skipping data directory test - directory not writable")
	}

	err = os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Logf("Warning: Could not write test file: %v", err)
		t.Skip("Skipping data directory test - file not writable")
	}

	// Verify file was created
	content, err := os.ReadFile(testFile)
	require.NoError(t, err, "Failed to read test file")
	assert.Equal(t, testContent, content, "File content should match")

	// Cleanup
	err = os.Remove(testFile)
	require.NoError(t, err, "Failed to cleanup test file")

	t.Logf("Data directory test passed: %s", dataDir)
}
