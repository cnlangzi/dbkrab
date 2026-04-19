package sqliteutil_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/cnlangzi/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLiteTimeHandling verifies that time.Time values are correctly
// serialized and deserialized by the SQLite driver for both TEXT and DATETIME columns.
func TestSQLiteTimeHandling(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		columnType string
	}{
		{
			name:       "TEXT column",
			columnType: "TEXT",
		},
		{
			name:       "DATETIME column",
			columnType: "DATETIME",
		},
		{
			name:       "TIMESTAMP column",
			columnType: "TIMESTAMP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create in-memory database
			db, err := sqlite.Open(ctx, ":memory:")
			require.NoError(t, err)
			defer func() { _ = db.Close() }()

			// Create table with specified column type
			createSQL := `CREATE TABLE test_times (
				id INTEGER PRIMARY KEY,
				created_at ` + tt.columnType + `
			)`
			_, err = db.Writer.Exec(createSQL)
			require.NoError(t, err)

			// Test time with nanosecond precision
			testTime := time.Date(2026, 4, 19, 12, 30, 45, 123456789, time.UTC)

			// Insert time.Time directly (driver should serialize it)
			_, err = db.Writer.Exec("INSERT INTO test_times (created_at) VALUES (?)", testTime)
			require.NoError(t, err)

			// Read back as string first to see raw format
			var strVal string
			err = db.Reader.QueryRow("SELECT created_at FROM test_times WHERE id = 1").Scan(&strVal)
			require.NoError(t, err)
			t.Logf("Raw stored format (%s): %s", tt.columnType, strVal)

			// Parse the string - driver uses different formats based on column type
			// TEXT: "2006-01-02 15:04:05.999999999-07:00" (space separator)
			// DATETIME/TIMESTAMP: "2006-01-02T15:04:05.999999999Z" (T separator, Z for UTC)
			var readTime time.Time
			readTime, err = time.Parse("2006-01-02 15:04:05.999999999-07:00", strVal)
			if err != nil {
				// Try ISO8601 format with T separator
				readTime, err = time.Parse(time.RFC3339Nano, strVal)
			}
			require.NoError(t, err)

			// Verify the time is equal (with nanosecond precision)
			assert.True(t, testTime.Equal(readTime),
				"Expected %v, got %v", testTime, readTime)

			// Verify location is UTC (or has correct offset)
			_, offset := readTime.Zone()
			assert.Equal(t, 0, offset, "Expected UTC offset, got %d", offset)
		})
	}
}

// TestSQLiteTimeHandlingWithTimezone verifies time handling with different timezones.
func TestSQLiteTimeHandlingWithTimezone(t *testing.T) {
	ctx := context.Background()

	// Create in-memory database
	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table with DATETIME column
	_, err = db.Writer.Exec(`CREATE TABLE test_tz (
		id INTEGER PRIMARY KEY,
		created_at DATETIME
	)`)
	require.NoError(t, err)

	shanghai, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	newyork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	tests := []struct {
		name     string
		input    time.Time
		expected time.Time // What we expect to read back (in UTC)
	}{
		{
			name:     "UTC time",
			input:    time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC),
			expected: time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "Shanghai time (UTC+8)",
			input:    time.Date(2026, 4, 19, 20, 0, 0, 0, shanghai), // 20:00 CST = 12:00 UTC
			expected: time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "New York time (UTC-4/EDT)",
			input:    time.Date(2026, 4, 19, 8, 0, 0, 0, newyork), // 08:00 EDT = 12:00 UTC
			expected: time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "Time with nanoseconds",
			input:    time.Date(2026, 4, 19, 12, 30, 45, 123456789, time.UTC),
			expected: time.Date(2026, 4, 19, 12, 30, 45, 123456789, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Insert
			_, err = db.Writer.Exec("INSERT INTO test_tz (created_at) VALUES (?)", tt.input)
			require.NoError(t, err)

			// Read back as string and parse
			var strVal string
			err = db.Reader.QueryRow("SELECT created_at FROM test_tz WHERE id = (SELECT MAX(id) FROM test_tz)").Scan(&strVal)
			require.NoError(t, err)

			// Try multiple formats
			var readTime time.Time
			readTime, err = time.Parse(time.RFC3339Nano, strVal)
			if err != nil {
				readTime, err = time.Parse("2006-01-02 15:04:05.999999999-07:00", strVal)
			}
			require.NoError(t, err)

			// Verify
			assert.True(t, tt.expected.Equal(readTime),
				"Expected %v (%v), got %v (%v)", tt.expected, tt.expected.Location(), readTime, readTime.Location())

			// Verify nanoseconds are preserved
			if tt.input.Nanosecond() > 0 {
				assert.Equal(t, tt.input.Nanosecond(), readTime.Nanosecond(),
					"Nanoseconds should be preserved")
			}
		})
	}
}

// TestSQLiteTimeHandlingZeroAndNil verifies handling of zero and nil time values.
func TestSQLiteTimeHandlingZeroAndNil(t *testing.T) {
	ctx := context.Background()

	// Create in-memory database
	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table with TEXT column to avoid driver auto-conversion
	_, err = db.Writer.Exec(`CREATE TABLE test_null (
		id INTEGER PRIMARY KEY,
		created_at TEXT
	)`)
	require.NoError(t, err)

	t.Run("Zero time stored by driver", func(t *testing.T) {
		// Note: mattn/go-sqlite3 driver does NOT automatically convert zero time to NULL
		// It stores it as an empty string
		zeroTime := time.Time{}

		// Insert zero time - driver will format it
		_, err = db.Writer.Exec("INSERT INTO test_null (created_at) VALUES (?)", zeroTime)
		require.NoError(t, err)

		// Read as string
		var strVal string
		err = db.Reader.QueryRow("SELECT created_at FROM test_null WHERE id = 1").Scan(&strVal)
		require.NoError(t, err)

		// Driver stores zero time as "0001-01-01 00:00:00+00:00" (not empty)
		t.Logf("Zero time stored as: '%s' (len=%d)", strVal, len(strVal))
		// Note: This is actually fine - it's a valid representation of zero time
		assert.Contains(t, strVal, "0001-01-01", "Zero time should contain year 0001")
	})

	t.Run("Nil time.Time pointer should be stored as NULL", func(t *testing.T) {
		var nilTime *time.Time

		_, err = db.Writer.Exec("INSERT INTO test_null (created_at) VALUES (?)", nilTime)
		require.NoError(t, err)

		// Check if it's NULL
		var isNull sql.NullString
		err = db.Reader.QueryRow("SELECT created_at FROM test_null WHERE id = 2").Scan(&isNull)
		require.NoError(t, err)

		assert.False(t, isNull.Valid, "Nil time pointer should be stored as NULL")
	})

	t.Run("Valid time should not be NULL", func(t *testing.T) {
		validTime := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)

		_, err = db.Writer.Exec("INSERT INTO test_null (created_at) VALUES (?)", validTime)
		require.NoError(t, err)

		var strVal string
		err = db.Reader.QueryRow("SELECT created_at FROM test_null WHERE id = 3").Scan(&strVal)
		require.NoError(t, err)

		assert.NotEmpty(t, strVal, "Valid time should not be empty")
		t.Logf("Valid time stored as: '%s'", strVal)
	})
}

// TestSQLiteTimeHandlingMixedFormats verifies that the driver can read
// different time formats stored in SQLite.
func TestSQLiteTimeHandlingMixedFormats(t *testing.T) {
	ctx := context.Background()

	// Create in-memory database
	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table
	_, err = db.Writer.Exec(`CREATE TABLE test_mixed (
		id INTEGER PRIMARY KEY,
		created_at DATETIME
	)`)
	require.NoError(t, err)

	tests := []struct {
		name       string
		insertSQL  string
		insertArgs []interface{}
		expectErr  bool
	}{
		{
			name:       "RFC3339Nano with timezone",
			insertSQL:  "INSERT INTO test_mixed (created_at) VALUES (?)",
			insertArgs: []interface{}{"2026-04-19T12:30:45.123456789+08:00"},
			expectErr:  false,
		},
		{
			name:       "SQLite native format",
			insertSQL:  "INSERT INTO test_mixed (created_at) VALUES (?)",
			insertArgs: []interface{}{"2026-04-19 12:30:45"},
			expectErr:  false,
		},
		{
			name:       "ISO8601 without timezone",
			insertSQL:  "INSERT INTO test_mixed (created_at) VALUES (?)",
			insertArgs: []interface{}{"2026-04-19T12:30:45"},
			expectErr:  false,
		},
		{
			name:       "Date only",
			insertSQL:  "INSERT INTO test_mixed (created_at) VALUES (?)",
			insertArgs: []interface{}{"2026-04-19"},
			expectErr:  false,
		},
		{
			name:       "CURRENT_TIMESTAMP",
			insertSQL:  "INSERT INTO test_mixed (created_at) VALUES (CURRENT_TIMESTAMP)",
			insertArgs: []interface{}{},
			expectErr:  false,
		},
		{
			name:       "datetime('now')",
			insertSQL:  "INSERT INTO test_mixed (created_at) VALUES (datetime('now'))",
			insertArgs: []interface{}{},
			expectErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err = db.Writer.Exec(tt.insertSQL, tt.insertArgs...)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Try to read back as string
			var readStr string
			err = db.Reader.QueryRow("SELECT created_at FROM test_mixed WHERE id = (SELECT MAX(id) FROM test_mixed)").Scan(&readStr)

			if tt.expectErr {
				assert.Error(t, err, "Expected error reading %s", tt.name)
			} else {
				require.NoError(t, err, "Failed to read %s: %v", tt.name, err)
				t.Logf("Read value (%s): %s", tt.name, readStr)
			}
		})
	}
}

// TestSQLiteTimeHandlingDriverSerialization verifies that Go time.Time
// is serialized by the driver in the expected format.
func TestSQLiteTimeHandlingDriverSerialization(t *testing.T) {
	ctx := context.Background()

	// Create in-memory database
	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table with TEXT column to see raw format
	_, err = db.Writer.Exec(`CREATE TABLE test_raw (
		id INTEGER PRIMARY KEY,
		created_at TEXT
	)`)
	require.NoError(t, err)

	// Insert time.Time
	testTime := time.Date(2026, 4, 19, 12, 30, 45, 123456789, time.UTC)
	_, err = db.Writer.Exec("INSERT INTO test_raw (created_at) VALUES (?)", testTime)
	require.NoError(t, err)

	// Read as string to see the raw format
	var rawStr string
	err = db.Reader.QueryRow("SELECT created_at FROM test_raw WHERE id = 1").Scan(&rawStr)
	require.NoError(t, err)

	t.Logf("Driver serialized time.Time to: %s", rawStr)

	// Verify format contains timezone info
	assert.Contains(t, rawStr, "+00:00", "Expected RFC3339Nano format with timezone")

	// Verify we can parse it back (driver uses space instead of T)
	parsed, err := time.Parse("2006-01-02 15:04:05.999999999-07:00", rawStr)
	require.NoError(t, err)

	assert.True(t, testTime.Equal(parsed), "Parsed time should match original")
}

// TestSQLiteTimeHandlingConcurrentAccess verifies time handling under concurrent access.
func TestSQLiteTimeHandlingConcurrentAccess(t *testing.T) {
	ctx := context.Background()

	// Create in-memory database
	db, err := sqlite.Open(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create table
	_, err = db.Writer.Exec(`CREATE TABLE test_concurrent (
		id INTEGER PRIMARY KEY,
		created_at DATETIME
	)`)
	require.NoError(t, err)

	// Insert multiple times concurrently
	numGoroutines := 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			testTime := time.Date(2026, 4, 19, 12, 0, idx, 0, time.UTC)
			_, err := db.Writer.Exec("INSERT INTO test_concurrent (created_at) VALUES (?)", testTime)
			if err != nil {
				t.Errorf("Goroutine %d failed to insert: %v", idx, err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all inserts succeeded
	var count int
	err = db.Reader.QueryRow("SELECT COUNT(*) FROM test_concurrent").Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, numGoroutines, count, "All inserts should succeed")

	// Verify all times can be read correctly
	rows, err := db.Reader.Query("SELECT created_at FROM test_concurrent ORDER BY id")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	readCount := 0
	for rows.Next() {
		var readStr string
		err = rows.Scan(&readStr)
		require.NoError(t, err)
		
		// Try multiple formats
		_, err := time.Parse(time.RFC3339Nano, readStr)
		if err != nil {
			_, err = time.Parse("2006-01-02 15:04:05.999999999-07:00", readStr)
		}
		require.NoError(t, err, "Failed to parse time from row %d: %s", readCount, readStr)
		readCount++
	}

	assert.Equal(t, numGoroutines, readCount, "All times should be readable")
}
