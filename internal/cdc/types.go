package cdc

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"time"
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
	// Use safe type assertion to avoid panic for mismatched driver types
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

// Int64 is a nullable int64 scanner for MSSQL BIGINT.
type Int64 struct {
	val   int64
	valid bool
}

// Scan implements sql.Scanner for Int64.
func (i *Int64) Scan(src interface{}) error {
	if src == nil {
		i.val, i.valid = 0, false
		return nil
	}
	switch v := src.(type) {
	case int64:
		i.val, i.valid = v, true
		return nil
	case int32:
		i.val, i.valid = int64(v), true
		return nil
	case int:
		i.val, i.valid = int64(v), true
		return nil
	case []byte:
		if _, err := fmt.Sscanf(string(v), "%d", &i.val); err != nil {
			i.val, i.valid = 0, false
			return fmt.Errorf("Int64.Scan: cannot parse %q as int64: %w", string(v), err)
		}
		i.valid = true
		return nil
	default:
		i.val, i.valid = 0, false
		return fmt.Errorf("Int64.Scan: unsupported type %T", src)
	}
}

// Value implements driver.Valuer for Int64.
func (i Int64) Value() (driver.Value, error) {
	if !i.valid {
		return nil, nil
	}
	return i.val, nil
}

// Float64 is a nullable float64 scanner for MSSQL FLOAT/REAL.
type Float64 struct {
	val   float64
	valid bool
}

// Scan implements sql.Scanner for Float64.
func (f *Float64) Scan(src interface{}) error {
	if src == nil {
		f.val, f.valid = 0, false
		return nil
	}
	switch v := src.(type) {
	case float64:
		f.val, f.valid = v, true
		return nil
	case float32:
		f.val, f.valid = float64(v), true
		return nil
	case int64:
		f.val, f.valid = float64(v), true
		return nil
	case []byte:
		if _, err := fmt.Sscanf(string(v), "%f", &f.val); err != nil {
			f.val, f.valid = 0, false
			return fmt.Errorf("Float64.Scan: cannot parse %q as float64: %w", string(v), err)
		}
		f.valid = true
		return nil
	default:
		f.val, f.valid = 0, false
		return fmt.Errorf("Float64.Scan: unsupported type %T", src)
	}
}

// Value implements driver.Valuer for Float64.
func (f Float64) Value() (driver.Value, error) {
	if !f.valid {
		return nil, nil
	}
	return f.val, nil
}

// NumericString is a nullable string scanner for MSSQL DECIMAL/NUMERIC.
// It preserves numeric string representation to avoid precision loss.
type NumericString struct {
	val   string
	valid bool
}

// Scan implements sql.Scanner for NumericString.
func (s *NumericString) Scan(src interface{}) error {
	if src == nil {
		s.val, s.valid = "", false
		return nil
	}
	switch v := src.(type) {
	case []byte:
		s.val, s.valid = string(v), true
	case string:
		s.val, s.valid = v, true
	default:
		s.val, s.valid = fmt.Sprintf("%v", v), true
	}
	return nil
}

// Value implements driver.Valuer for NumericString.
func (s NumericString) Value() (driver.Value, error) {
	if !s.valid {
		return nil, nil
	}
	return s.val, nil
}

// GUID is a nullable string scanner for MSSQL UNIQUEIDENTIFIER.
// MSSQL uniqueidentifier stores GUID with mixed byte order:
// - First 4 bytes: little-endian
// - Next 2 bytes: little-endian
// - Next 2 bytes: little-endian
// - Last 8 bytes: big-endian
// This type normalizes to standard GUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
type GUID struct {
	val   string
	valid bool
}

// Scan implements sql.Scanner for GUID.
func (g *GUID) Scan(src interface{}) error {
	if src == nil {
		g.val, g.valid = "", false
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		// Try to use as string directly
		if s, ok := src.(string); ok {
			g.val, g.valid = s, true
			return nil
		}
		g.val, g.valid = fmt.Sprintf("%v", src), true
		return nil
	}
	g.val, g.valid = formatMSSQLGUID(bytes), true
	return nil
}

// Value implements driver.Valuer for GUID.
func (g GUID) Value() (driver.Value, error) {
	if !g.valid {
		return nil, nil
	}
	return g.val, nil
}

// formatMSSQLGUID wraps FormatMSSQLGUID for internal use.
func formatMSSQLGUID(b []byte) string {
	return FormatMSSQLGUID(b)
}

// FormatMSSQLGUID formats a 16-byte MSSQL GUID to standard string format.
// MSSQL uniqueidentifier uses mixed endian: first 3 groups little-endian, last group big-endian.
func FormatMSSQLGUID(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	// MSSQL GUID byte order: 3-2-1-0, 5-4, 7-6, 8-9-10-11-12-13-14-15
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0], // First 4 bytes (LE)
		b[5], b[4], // Next 2 bytes (LE)
		b[7], b[6], // Next 2 bytes (LE)
		b[8], b[9], // Start of last group - keep as-is (BE)
		b[10], b[11], b[12], b[13], b[14], b[15],
	)
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
		// Try parsing as time string
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
	return time.Date(
		driverTime.Year(), driverTime.Month(), driverTime.Day(),
		driverTime.Hour(), driverTime.Minute(), driverTime.Second(), driverTime.Nanosecond(),
		d.timezone,
	).UTC()
}

// Value implements driver.Valuer for DateTime.
// Returns RFC3339Nano UTC string or nil for zero/Invalid time.
func (d DateTime) Value() (driver.Value, error) {
	if !d.valid || d.val.IsZero() {
		return nil, nil
	}
	return d.val.Format(time.RFC3339Nano), nil
}

// String is a nullable string scanner for MSSQL VARCHAR/NVARCHAR/CHAR/NCHAR.
type String struct {
	val   string
	valid bool
}

// Scan implements sql.Scanner for String.
func (s *String) Scan(src interface{}) error {
	if src == nil {
		s.val, s.valid = "", false
		return nil
	}
	switch v := src.(type) {
	case []byte:
		s.val, s.valid = string(v), true
	case string:
		s.val, s.valid = v, true
	default:
		s.val, s.valid = fmt.Sprintf("%v", v), true
	}
	return nil
}

// Value implements driver.Valuer for String.
func (s String) Value() (driver.Value, error) {
	if !s.valid {
		return nil, nil
	}
	return s.val, nil
}

// Bool is a nullable bool scanner for MSSQL BIT/BOOLEAN.
type Bool struct {
	val   bool
	valid bool
}

// Scan implements sql.Scanner for Bool.
func (b *Bool) Scan(src interface{}) error {
	if src == nil {
		b.val, b.valid = false, false
		return nil
	}
	switch v := src.(type) {
	case bool:
		b.val, b.valid = v, true
	case int64:
		b.val, b.valid = v != 0, true
	case []byte:
		if len(v) > 0 {
			b.val, b.valid = v[0] != 0, true
		} else {
			b.val, b.valid = false, false
		}
	default:
		b.val, b.valid = false, false
	}
	return nil
}

// Value implements driver.Valuer for Bool.
func (b Bool) Value() (driver.Value, error) {
	if !b.valid {
		return nil, nil
	}
	return b.val, nil
}

// Bytes is a nullable []byte scanner for MSSQL VARBINARY/BINARY/IMAGE.
type Bytes struct {
	val   []byte
	valid bool
}

// Scan implements sql.Scanner for Bytes.
func (b *Bytes) Scan(src interface{}) error {
	if src == nil {
		b.val, b.valid = nil, false
		return nil
	}
	switch v := src.(type) {
	case []byte:
		b.val, b.valid = v, true
	case string:
		b.val, b.valid = []byte(v), true
	default:
		b.val, b.valid = nil, false
	}
	return nil
}

// Value implements driver.Valuer for Bytes.
func (b Bytes) Value() (driver.Value, error) {
	if !b.valid {
		return nil, nil
	}
	return b.val, nil
}

// Scanner is the interface implemented by types that can scan from SQL rows.
// All our scanner types implement both sql.Scanner and driver.Valuer.
type Scanner interface {
	sql.Scanner
	driver.Valuer
}

// Deprecated: convertCommitTime is kept for test compatibility.
// Use DateTime.Scan with ScannerFactory instead.
// convertCommitTime reinterprets MSSQL driver's "UTC" time as SQL Server's local timezone
// MSSQL sys.fn_cdc_map_lsn_to_time() returns datetime without timezone info.
// The value is in SQL Server's local timezone (e.g., Beijing UTC+8),
// but Go driver incorrectly treats it as UTC.
// We reinterpret it using the configured timezone and convert to UTC for storage.
//nolint:unused
func convertCommitTime(driverTime time.Time, timezone *time.Location) time.Time {
	if timezone == nil || timezone == time.Local {
		// No timezone configured - use driver's value as-is
		return driverTime.UTC()
	}
	// Reinterpret driver's "UTC" time as SQL Server's local timezone, then convert to UTC
	return time.Date(
		driverTime.Year(), driverTime.Month(), driverTime.Day(),
		driverTime.Hour(), driverTime.Minute(), driverTime.Second(), driverTime.Nanosecond(),
		timezone,
	).UTC()
}
