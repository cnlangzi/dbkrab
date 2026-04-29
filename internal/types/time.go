package types

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// TimeFormat defines how NullTime values are serialized for JSON output.
type TimeFormat string

const (
	TimeFormatRFC3339Nano TimeFormat = "RFC3339Nano"
	TimeFormatRFC3339     TimeFormat = "RFC3339"
	TimeFormatDatetime    TimeFormat = "datetime"
	TimeFormatDate        TimeFormat = "date"
	TimeFormatUnix        TimeFormat = "unix"
	TimeFormatUnixMilli   TimeFormat = "unixMilli"
	TimeFormatUnixNano    TimeFormat = "unixNano"
)

// DefaultTimeFormat applies when a NullTime has no per-instance format override.
// Defaults to "datetime" ("2006-01-02 15:04:05") so SQLite storage and JSON
// output share the same human-readable format.
var DefaultTimeFormat TimeFormat = TimeFormatDatetime

// tzAwareFormats include an explicit offset; time.Parse honours the embedded zone.
var tzAwareFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999-07:00", // SQLite driver TEXT format
	"2006-01-02 15:04:05-07:00",
}

// noTZFormats have no zone info; must be parsed with ParseInLocation so that
// timezone-naive wall-clock strings are interpreted in the configured timezone.
var noTZFormats = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// knownParseFormats is the combined list used by ParseTimeValue (no timezone context).
var knownParseFormats = append(tzAwareFormats, noTZFormats...)

// NullTime is the unified nullable time type for MSSQL/SQLite scan + SQL write + JSON.
// Scan-time timezone reinterpretation corrects MSSQL driver's DATETIME location issue.
type NullTime struct {
	val      time.Time
	valid    bool
	timezone *time.Location // MSSQL server timezone for scan-time reinterpretation
	format   TimeFormat     // JSON output format; empty uses DefaultTimeFormat
}

// MSSQLTime is kept as a backward-compatible alias of NullTime.
type MSSQLTime = NullTime

var (
	_ sql.Scanner      = (*NullTime)(nil)
	_ driver.Valuer    = NullTime{}
	_ json.Marshaler   = NullTime{}
	_ json.Unmarshaler = (*NullTime)(nil)
)

func NewNullTime(tz *time.Location, format TimeFormat) *NullTime {
	if tz == nil {
		tz = time.UTC
	}
	return &NullTime{timezone: tz, format: format}
}

// NewMSSQLTime is kept as a backward-compatible constructor alias.
func NewMSSQLTime(tz *time.Location, format TimeFormat) *NullTime {
	return NewNullTime(tz, format)
}

func (m *NullTime) Time() time.Time { return m.val }

func (m *NullTime) Valid() bool { return m.valid }

func (m NullTime) effectiveFormat() TimeFormat {
	if m.format != "" {
		return m.format
	}
	return DefaultTimeFormat
}

func (m *NullTime) Scan(src interface{}) error {
	if src == nil {
		m.val, m.valid = time.Time{}, false
		return nil
	}

	switch v := src.(type) {
	case time.Time:
		m.val = m.reinterpret(v)
		m.valid = true
	case []byte:
		return m.scanString(string(v))
	case string:
		return m.scanString(v)
	case int64:
		m.val = time.Unix(v, 0).UTC()
		m.valid = true
	default:
		return fmt.Errorf("NullTime.Scan: unsupported type %T", src)
	}

	return nil
}

func (m *NullTime) scanString(s string) error {
	if s == "" {
		m.val, m.valid = time.Time{}, false
		return nil
	}
	// Try formats that carry an explicit timezone offset first.
	for _, layout := range tzAwareFormats {
		if t, err := time.Parse(layout, s); err == nil {
			m.val = t.UTC()
			m.valid = true
			return nil
		}
	}
	// For timezone-naive formats, interpret the wall-clock time in m.timezone
	// so that local-time strings are correctly converted to UTC.
	tz := m.timezone
	if tz == nil {
		tz = time.UTC
	}
	for _, layout := range noTZFormats {
		if t, err := time.ParseInLocation(layout, s, tz); err == nil {
			m.val = t.UTC()
			m.valid = true
			return nil
		}
	}
	return fmt.Errorf("NullTime.Scan: cannot parse %q as time", s)
}

func (m NullTime) reinterpret(driverTime time.Time) time.Time {
	if m.timezone == nil || m.timezone == time.UTC {
		return driverTime.UTC()
	}
	return time.Date(
		driverTime.Year(), driverTime.Month(), driverTime.Day(),
		driverTime.Hour(), driverTime.Minute(), driverTime.Second(),
		driverTime.Nanosecond(), m.timezone,
	).UTC()
}

func (m NullTime) formatOutput() interface{} {
	tz := m.timezone
	if tz == nil {
		tz = time.UTC
	}
	switch m.effectiveFormat() {
	case TimeFormatRFC3339:
		return m.val.In(tz).Format(time.RFC3339)
	case TimeFormatRFC3339Nano:
		return m.val.In(tz).Format(time.RFC3339Nano)
	case TimeFormatDatetime:
		return m.val.In(tz).Format("2006-01-02 15:04:05")
	case TimeFormatDate:
		return m.val.In(tz).Format("2006-01-02")
	case TimeFormatUnix:
		return m.val.Unix()
	case TimeFormatUnixMilli:
		return m.val.UnixMilli()
	case TimeFormatUnixNano:
		return m.val.UnixNano()
	default:
		return m.val.In(tz).Format(string(m.effectiveFormat()))
	}
}

// Value returns a formatted value matching the JSON output format so that
// SQLite storage and JSON serialization are always consistent.
// For Unix timestamp formats (int64), the raw int64 is returned.
func (m NullTime) Value() (driver.Value, error) {
	if !m.valid || m.val.IsZero() {
		return nil, nil
	}
	tz := m.timezone
	if tz == nil {
		tz = time.UTC
	}
	switch v := m.formatOutput().(type) {
	case string:
		return v, nil
	case int64:
		return v, nil
	}
	return m.val.In(tz), nil
}

func (m NullTime) MarshalJSON() ([]byte, error) {
	if !m.valid || m.val.IsZero() {
		return []byte("null"), nil
	}
	switch v := m.formatOutput().(type) {
	case string:
		return json.Marshal(v)
	case int64:
		return json.Marshal(v)
	}
	return []byte("null"), nil
}

func (m *NullTime) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		m.val, m.valid = time.Time{}, false
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		return m.scanString(s)
	}

	var n int64
	if err := json.Unmarshal(data, &n); err == nil {
		m.val = time.Unix(n, 0).UTC()
		m.valid = true
		return nil
	}

	return fmt.Errorf("NullTime.UnmarshalJSON: cannot parse %s", string(data))
}

// ParseTimeValue extracts a UTC time and validity from common date-like values.
func ParseTimeValue(v interface{}) (time.Time, bool) {
	if v == nil {
		return time.Time{}, false
	}
	for {
		switch val := v.(type) {
		case time.Time:
			return val.UTC(), !val.IsZero()
		case *NullTime:
			if val != nil {
				return val.val, val.valid
			}
			return time.Time{}, false
		case NullTime:
			return val.val, val.valid
		case string:
			for _, layout := range knownParseFormats {
				if t, err := time.Parse(layout, val); err == nil {
					return t.UTC(), true
				}
			}
			return time.Time{}, false
		case []byte:
			v = string(val)
		case int64:
			return time.Unix(val, 0).UTC(), true
		default:
			return time.Time{}, false
		}
	}
}

// ConvertCommitTime reinterprets driverTime as timezone local wall clock and returns UTC.
func ConvertCommitTime(driverTime time.Time, timezone *time.Location) time.Time {
	if timezone == nil || timezone == time.Local {
		return driverTime.UTC()
	}
	return time.Date(
		driverTime.Year(), driverTime.Month(), driverTime.Day(),
		driverTime.Hour(), driverTime.Minute(), driverTime.Second(),
		driverTime.Nanosecond(), timezone,
	).UTC()
}

// FormatTimeValue weakly formats date-like values for map[string]interface{} payloads.
func FormatTimeValue(v interface{}, format TimeFormat) interface{} {
	t, ok := ParseTimeValue(v)
	if !ok {
		return v
	}
	m := NullTime{val: t, valid: true, format: format}
	return m.formatOutput()
}
