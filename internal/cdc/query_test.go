package cdc

import (
	"testing"
	"time"
)

func TestConvertCommitTime(t *testing.T) {
	// Setup test timezone - Asia/Shanghai is UTC+8
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("failed to load Asia/Shanghai timezone: %v", err)
	}

	tests := []struct {
		name      string
		driverTime time.Time
		timezone  *time.Location
		wantUTC   string // Expected UTC time in RFC3339 format
	}{
		{
			name:      "UTC+8 timezone - Beijing noon becomes UTC 04:00",
			// Driver returns 12:26 incorrectly marked as UTC, but it's actually Beijing time
			driverTime: time.Date(2026, 4, 9, 12, 26, 0, 0, time.UTC),
			timezone:  shanghai,
			wantUTC:   "2026-04-09T04:26:00Z", // 12:26 Beijing = 04:26 UTC
		},
		{
			name:      "UTC+8 timezone - Beijing morning becomes previous day UTC evening",
			driverTime: time.Date(2026, 4, 9, 2, 0, 0, 0, time.UTC),
			timezone:  shanghai,
			wantUTC:   "2026-04-08T18:00:00Z", // 02:00 Beijing = 18:00 UTC (prev day)
		},
		{
			name:      "No timezone configured - use driver value as-is",
			driverTime: time.Date(2026, 4, 9, 12, 26, 0, 0, time.UTC),
			timezone:  nil,
			wantUTC:   "2026-04-09T12:26:00Z", // No conversion
		},
		{
			name:      "Local timezone - use driver value as-is",
			driverTime: time.Date(2026, 4, 9, 12, 26, 0, 0, time.UTC),
			timezone:  time.Local,
			wantUTC:   "2026-04-09T12:26:00Z", // No conversion
		},
		{
			name:      "UTC timezone - identity conversion",
			driverTime: time.Date(2026, 4, 9, 12, 26, 0, 0, time.UTC),
			timezone:  time.UTC,
			wantUTC:   "2026-04-09T12:26:00Z",
		},
		{
			name:      "Preserves nanoseconds",
			driverTime: time.Date(2026, 4, 9, 12, 26, 30, 123456789, time.UTC),
			timezone:  shanghai,
			wantUTC:   "2026-04-09T04:26:30.123456789Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertCommitTime(tt.driverTime, tt.timezone)
			gotStr := got.Format(time.RFC3339Nano)
			if gotStr != tt.wantUTC {
				t.Errorf("convertCommitTime() = %v, want %v", gotStr, tt.wantUTC)
			}
		})
	}
}

func TestConvertCommitTimeChangedBeforePulled(t *testing.T) {
	// This test specifically verifies the regression fix:
	// Changed time should NOT be later than Pulled time

	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("failed to load Asia/Shanghai timezone: %v", err)
	}

	// Scenario: SQL Server (in Beijing UTC+8) creates a record at 12:26 local time
	// MSSQL driver incorrectly returns 12:26 UTC
	// dbkrab pulls the record at 06:32 UTC (which is 14:32 Beijing time)
	//
	// Without fix: changed_at = 12:26 UTC, pulled_at = 06:32 UTC
	//             changed_at appears 6 hours AFTER pulled_at (BUG!)
	//
	// With fix: changed_at = 04:26 UTC (12:26 Beijing converted)
	//           pulled_at = 06:32 UTC
	//           changed_at is 2 hours BEFORE pulled_at (CORRECT!)

	driverTime := time.Date(2026, 4, 9, 12, 26, 0, 0, time.UTC) // Driver's incorrect UTC
	pulledAt := time.Date(2026, 4, 9, 6, 32, 0, 0, time.UTC)    // Actual pull time

	changedAt := convertCommitTime(driverTime, shanghai)

	if changedAt.After(pulledAt) {
		t.Errorf("REGRESSION: changed_at (%v) is after pulled_at (%v)",
			changedAt.Format(time.RFC3339), pulledAt.Format(time.RFC3339))
	}

	// Verify correct 2-hour gap
	expectedChanged := time.Date(2026, 4, 9, 4, 26, 0, 0, time.UTC)
	if !changedAt.Equal(expectedChanged) {
		t.Errorf("changed_at = %v, want %v", changedAt, expectedChanged)
	}

	// Verify the gap is approximately 2 hours (record created at 12:26 Beijing, pulled at 14:32 Beijing)
	gap := pulledAt.Sub(changedAt)
	expectedGap := 2 * time.Hour + 6 * time.Minute
	if gap != expectedGap {
		t.Errorf("gap between changed and pulled = %v, want %v", gap, expectedGap)
	}
}

func TestConvertCommitTimeFixedZone(t *testing.T) {
	// Test with fixed zone offset (UTC+8 as fixed zone)
	utcPlus8 := time.FixedZone("UTC+8", 8*3600)

	driverTime := time.Date(2026, 4, 9, 12, 26, 0, 0, time.UTC)
	changedAt := convertCommitTime(driverTime, utcPlus8)

	expected := time.Date(2026, 4, 9, 4, 26, 0, 0, time.UTC)
	if !changedAt.Equal(expected) {
		t.Errorf("convertCommitTime with FixedZone = %v, want %v", changedAt, expected)
	}
}

func TestNewQuerierDefaultTimezone(t *testing.T) {
	// Verify NewQuerier defaults timezone to time.Local when nil is passed
	q := NewQuerier(nil, nil)
	if q.timezone != time.Local {
		t.Errorf("NewQuerier(nil, nil) timezone = %v, want time.Local", q.timezone)
	}
}

func TestNewQuerierExplicitTimezone(t *testing.T) {
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("failed to load timezone: %v", err)
	}

	q := NewQuerier(nil, shanghai)
	if q.timezone != shanghai {
		t.Errorf("NewQuerier timezone = %v, want %v", q.timezone, shanghai)
	}
}