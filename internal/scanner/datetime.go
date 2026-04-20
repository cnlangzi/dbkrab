package scanner

import (
	"database/sql"
	"database/sql/driver"
	"time"
)

// DateTimeScanner is a nullable time.Time scanner for MSSQL DATETIME/SMALLDATETIME.
// It performs timezone conversion at scan time (MSSQL local time -> UTC).
// This is needed because MSSQL driver incorrectly treats datetime as UTC.
type DateTimeScanner struct {
	val      time.Time
	valid   bool
	timezone *time.Location
}

// NewDateTimeScanner creates a DateTimeScanner with the given timezone.
// timezone is the MSSQL Server's timezone (e.g., Asia/Shanghai for UTC+8).
func NewDateTimeScanner(timezone *time.Location) *DateTimeScanner {
	if timezone == nil {
		timezone = time.UTC
	}
	return &DateTimeScanner{timezone: timezone}
}

// Scan implements sql.Scanner for DateTimeScanner.
// Scan implements sql.Scanner for DateTimeScanner.
func (d *DateTimeScanner) Scan(src interface{}) error {
	if src == nil {
		d.val, d.valid = time.Time{}, false
		return nil
	}
	switch v := src.(type) {
	case time.Time:
		d.val = d.convertTime(v)
		d.valid = true
	case []byte:
		// 存储格式: RFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"
		// 先尝试解析 RFC3339Nano
		if parsed, err := time.Parse(time.RFC3339Nano, string(v)); err == nil {
			d.val = d.convertTime(parsed)
			d.valid = true
		} else {
			// Try RFC3339 format
			if parsed, err := time.Parse(time.RFC3339, string(v)); err == nil {
				d.val = d.convertTime(parsed)
				d.valid = true
			} else {
				d.val, d.valid = time.Time{}, false
			}
		}
	case string:
		// 存储格式: RFC3339Nano
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			d.val = d.convertTime(parsed)
			d.valid = true
		} else {
			// Try RFC3339 format
			if parsed, err := time.Parse(time.RFC3339, v); err == nil {
				d.val = d.convertTime(parsed)
				d.valid = true
			} else {
				d.val, d.valid = time.Time{}, false
			}
		}
	default:
		d.val, d.valid = time.Time{}, false
	}
	return nil
}

// convertTime reinterprets MSSQL driver's "UTC" time as MSSQL's local timezone,
// then converts to UTC for storage.
// 
// Problem: MSSQL driver returns time.Time with Location=UTC but the actual value
// is in MSSQL Server's local timezone. We need to reinterpret the time value
// as if it were in the configured timezone, then convert to UTC.
func (d *DateTimeScanner) convertTime(driverTime time.Time) time.Time {
	if d.timezone == nil || d.timezone == time.Local {
		return driverTime.UTC()
	}
	
	// The driver reports the time as UTC but it's actually in MSSQL's timezone.
	// We need to reinterpret: take the wall clock time and apply MSSQL timezone.
	// Example: driverTime = 18:08:01 UTC (but should be 18:08:01 Asia/Shanghai)
	// After reinterpret: 18:08:01 Asia/Shanghai → 10:08:01 UTC
	year, month, day := driverTime.Year(), driverTime.Month(), driverTime.Day()
	hour, min, sec := driverTime.Hour(), driverTime.Minute(), driverTime.Second()
	nsec := driverTime.Nanosecond()
	
	// Reinterpret the time as if it were in MSSQL's timezone
	reinterpreted := time.Date(year, month, day, hour, min, sec, nsec, d.timezone)
	
	// Now convert to UTC
	return reinterpreted.UTC()
}

// Value implements driver.Valuer for DateTimeScanner.
func (d DateTimeScanner) Value() (driver.Value, error) {
	if !d.valid || d.val.IsZero() {
		return nil, nil // Invalid/zero time stored as NULL
	}
	// Return time.Time directly - let SQLite driver handle serialization
	return d.val, nil
}

// Ensure DateTimeScanner implements required interfaces
var _ driver.Valuer = (*DateTimeScanner)(nil)
var _ sql.Scanner = (*DateTimeScanner)(nil)

// DateTimeOffsetScanner handles MSSQL DATETIMEOFFSET type.
// It parses the offset from the string representation.
type DateTimeOffsetScanner struct {
	val      time.Time
	valid   bool
	timezone *time.Location
}

// NewDateTimeOffsetScanner creates a DateTimeOffsetScanner.
func NewDateTimeOffsetScanner(timezone *time.Location) *DateTimeOffsetScanner {
	if timezone == nil {
		timezone = time.UTC
	}
	return &DateTimeOffsetScanner{timezone: timezone}
}

// Scan implements sql.Scanner for DateTimeOffsetScanner.
func (d *DateTimeOffsetScanner) Scan(src interface{}) error {
	if src == nil {
		d.val, d.valid = time.Time{}, false
		return nil
	}
	switch v := src.(type) {
	case time.Time:
		d.val = v.UTC()
		d.valid = true
	case []byte:
		if parsed, err := time.Parse(time.RFC3339Nano, string(v)); err == nil {
			d.val = parsed.UTC()
			d.valid = true
		} else {
			d.val, d.valid = time.Time{}, false
		}
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			d.val = parsed.UTC()
			d.valid = true
		} else {
			d.val, d.valid = time.Time{}, false
		}
	default:
		d.val, d.valid = time.Time{}, false
	}
	return nil
}

// Value implements driver.Valuer for DateTimeOffsetScanner.
func (d DateTimeOffsetScanner) Value() (driver.Value, error) {
	if !d.valid || d.val.IsZero() {
		return nil, nil
	}
	return d.val, nil
}

var _ driver.Valuer = (*DateTimeOffsetScanner)(nil)
var _ sql.Scanner = (*DateTimeOffsetScanner)(nil)