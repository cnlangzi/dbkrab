package cdc

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/cnlangzi/dbkrab/internal/scanner"
)

// Nullable represents a value that may be NULL.
// It implements sql.Scanner and driver.Valuer.
// When Valid is false, Value() returns nil (SQL NULL).
type Nullable[T any] struct {
	val   T
	valid bool
}

// Scan implements sql.Scanner.
func (n *Nullable[T]) Scan(src interface{}) error {
	if src == nil {
		n.val, n.valid = *new(T), false
		return nil
	}
	val, ok := src.(T)
	if !ok {
		return fmt.Errorf("Nullable[T].Scan: cannot convert %T to %T", src, *new(T))
	}
	n.val, n.valid = val, true
	return nil
}

// Value implements driver.Valuer.
func (n Nullable[T]) Value() (driver.Value, error) {
	if !n.valid {
		return nil, nil
	}
	return n.val, nil
}

// DateTime is a nullable time.Time scanner for MSSQL DATETIME/SMALLDATETIME.
// It performs timezone conversion at scan time and returns RFC3339Nano UTC string.
type DateTime struct {
	val      time.Time
	valid    bool
	timezone *time.Location
}

// NewDateTime creates a DateTime scanner with the given timezone for conversion.
func NewDateTime(tz *time.Location) *DateTime {
	if tz == nil {
		tz = time.UTC
	}
	return &DateTime{timezone: tz}
}

// Scan implements sql.Scanner for DateTime.
func (d *DateTime) Scan(src interface{}) error {
	if src == nil {
		d.val, d.valid = time.Time{}, false
		return nil
	}
	switch v := src.(type) {
	case time.Time:
		d.val = d.convertTime(v)
		d.valid = true
	case []byte:
		if parsed, err := time.Parse(time.RFC3339Nano, string(v)); err == nil {
			d.val = d.convertTime(parsed)
			d.valid = true
		} else {
			d.val, d.valid = time.Time{}, false
		}
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			d.val = d.convertTime(parsed)
			d.valid = true
		} else {
			d.val, d.valid = time.Time{}, false
		}
	default:
		d.val, d.valid = time.Time{}, false
	}
	return nil
}

// convertTime reinterprets MSSQL driver's "UTC" time as SQL Server's local timezone,
// then converts to UTC for storage.
func (d *DateTime) convertTime(driverTime time.Time) time.Time {
	if d.timezone == nil || d.timezone == time.Local {
		return driverTime.UTC()
	}

	// Reinterpret the time in MSSQL timezone
	year, month, day := driverTime.Year(), driverTime.Month(), driverTime.Day()
	hour, min, sec := driverTime.Hour(), driverTime.Minute(), driverTime.Second()
	nsec := driverTime.Nanosecond()

	// Create time with MSSQL timezone (reinterpretation)
	reinterpreted := time.Date(year, month, day, hour, min, sec, nsec, d.timezone)

	// Convert to UTC
	return reinterpreted.UTC()
}

// Value implements driver.Valuer for DateTime.
// Returns time.Time for valid times (driver will serialize to SQLite format),
// nil for invalid/zero times (stored as NULL in SQLite).
func (d DateTime) Value() (driver.Value, error) {
	if !d.valid || d.val.IsZero() {
		return nil, nil // Invalid/zero time stored as NULL
	}
	// Return time.Time directly - let SQLite driver handle serialization
	// Driver will use RFC3339Nano format with timezone: "2006-01-02 15:04:05.999999999-07:00"
	return d.val, nil
}

// Deprecated: convertCommitTime is kept for test compatibility.
// Use DateTime.Scan with ScannerFactory instead.
// convertCommitTime reinterprets MSSQL driver's "UTC" time as SQL Server's local timezone
// MSSQL sys.fn_cdc_map_lsn_to_time() returns datetime without timezone info.
// The value is in SQL Server's local timezone (e.g., Beijing UTC+8),
// but Go driver incorrectly treats it as UTC.
// We reinterpret it using the configured timezone and convert to UTC for storage.
//
//nolint:unused
func convertCommitTime(driverTime time.Time, timezone *time.Location) time.Time {
	if timezone == nil || timezone == time.Local {
		return driverTime.UTC()
	}
	return time.Date(
		driverTime.Year(), driverTime.Month(), driverTime.Day(),
		driverTime.Hour(), driverTime.Minute(), driverTime.Second(), driverTime.Nanosecond(),
		timezone,
	).UTC()
}

// Scanner is the interface implemented by types that can scan from SQL rows.
// Alias for backward compatibility.
type Scanner = scanner.Scanner

// FormatMSSQLGUID re-exports from scanner package for backward compatibility.
var FormatMSSQLGUID = scanner.FormatMSSQLGUID
