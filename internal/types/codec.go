package types

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DBType is the unified interface for SQL scan/write + JSON encode/decode.
type DBType interface {
	sql.Scanner
	driver.Valuer
	json.Marshaler
	json.Unmarshaler
}

// Dialect identifies database type mapping strategy.
type Dialect string

const (
	DialectMSSQL  Dialect = "mssql"
	DialectSQLite Dialect = "sqlite"
)

// MSSQL type names from DatabaseTypeName().
const (
	TypeBigInt           = "BIGINT"
	TypeInt              = "INT"
	TypeSmallInt         = "SMALLINT"
	TypeTinyInt          = "TINYINT"
	TypeBit              = "BIT"
	TypeFloat            = "FLOAT"
	TypeReal             = "REAL"
	TypeNumeric          = "NUMERIC"
	TypeDecimal          = "DECIMAL"
	TypeMoney            = "MONEY"
	TypeSmallMoney       = "SMALLMONEY"
	TypeDateTime         = "DATETIME"
	TypeDateTime2        = "DATETIME2"
	TypeSmallDateTime    = "SMALLDATETIME"
	TypeDate             = "DATE"
	TypeTime             = "TIME"
	TypeDateTimeOffset   = "DATETIMEOFFSET"
	TypeUniqueIdentifier = "UNIQUEIDENTIFIER"
	TypeChar             = "CHAR"
	TypeVarchar          = "VARCHAR"
	TypeNChar            = "NCHAR"
	TypeNVarchar         = "NVARCHAR"
	TypeNText            = "NTEXT"
	TypeText             = "TEXT"
	TypeBinary           = "BINARY"
	TypeVarBinary        = "VARBINARY"
	TypeImage            = "IMAGE"
	TypeXML              = "XML"
	TypeSQLVariant       = "SQL_VARIANT"
)

// SQLite type names from DatabaseTypeName().
const (
	TypeSQLiteText      = "TEXT"
	TypeSQLiteInteger   = "INTEGER"
	TypeSQLiteReal      = "REAL"
	TypeSQLiteBlob      = "BLOB"
	TypeSQLiteNumeric   = "NUMERIC"
	TypeSQLiteDatetime  = "DATETIME"
	TypeSQLiteTimestamp = "TIMESTAMP"
	TypeSQLiteBoolean   = "BOOLEAN"
)

// defaultScannerTimezone is the timezone used by NewMSSQLCodec when none is
// explicitly provided. Set it at programme startup via SetDefaultTimezone.
var defaultScannerTimezone *time.Location = time.UTC

// SetDefaultTimezone sets the package-level default timezone for MSSQL codecs
// created by NewMSSQLCodec (no explicit timezone option).
// Call this once at startup, e.g. from config.ParseTimezone.
func SetDefaultTimezone(tz *time.Location) {
	if tz == nil {
		tz = time.UTC
	}
	defaultScannerTimezone = tz
}

// Codec creates typed DB destinations from sql.ColumnType metadata.
type Codec struct {
	dialect  Dialect
	timezone *time.Location
	format   TimeFormat
}

// NewMSSQLCodec creates an MSSQL codec using the package-level default timezone
// and no format override.
func NewMSSQLCodec() *Codec {
	return &Codec{dialect: DialectMSSQL, timezone: defaultScannerTimezone}
}

// NewMSSQLCodecWithOptions creates an MSSQL codec with explicit timezone and
// output format. Pass nil timezone to use time.UTC; pass "" format to use
// DefaultTimeFormat in JSON output.
func NewMSSQLCodecWithOptions(tz *time.Location, format TimeFormat) *Codec {
	if tz == nil {
		tz = time.UTC
	}
	return &Codec{dialect: DialectMSSQL, timezone: tz, format: format}
}

// NewSQLiteCodec creates a SQLite codec that shares the package-level default
// timezone with MSSQL codecs. Call SetDefaultTimezone once at startup to keep
// both dialects in sync.
func NewSQLiteCodec() *Codec {
	return &Codec{dialect: DialectSQLite, timezone: defaultScannerTimezone}
}

// create creates a new destination type for the given database type name.
func (c *Codec) create(typeName string) DBType {
	upper := strings.ToUpper(typeName)
	if c.dialect == DialectSQLite {
		switch upper {
		case TypeSQLiteInteger, TypeInt, TypeBigInt, TypeSmallInt, TypeTinyInt:
			return &NullInt64{}
		case TypeSQLiteReal, TypeFloat:
			return &NullFloat64{}
		case TypeSQLiteNumeric, TypeDecimal:
			return &NullDecimal{}
		case TypeSQLiteBlob, TypeBinary, TypeVarBinary, TypeImage:
			return &NullBytes{}
		case TypeSQLiteBoolean, TypeBit:
			return &NullBool{}
		case TypeSQLiteDatetime, TypeSQLiteTimestamp:
			return NewNullTime(c.timezone, c.format)
		default:
			return &NullString{}
		}
	}

	switch upper {
	case TypeBigInt, TypeInt, TypeSmallInt, TypeTinyInt:
		return &NullInt64{}
	case TypeFloat, TypeReal, TypeMoney, TypeSmallMoney:
		return &NullFloat64{}
	case TypeNumeric, TypeDecimal:
		return &NullDecimal{}
	case TypeUniqueIdentifier:
		return &NullGUID{}
	case TypeBit:
		return &NullBool{}
	case TypeDateTime, TypeDateTime2, TypeSmallDateTime, TypeDate, TypeTime, TypeDateTimeOffset:
		return NewNullTime(c.timezone, c.format)
	case TypeChar, TypeVarchar, TypeNChar, TypeNVarchar, TypeNText, TypeText, TypeXML, TypeSQLVariant:
		return &NullString{}
	case TypeBinary, TypeVarBinary, TypeImage:
		return &NullBytes{}
	default:
		return &NullString{}
	}
}

// CreateDest creates a []interface{} of DBType pointers for rows.Scan.
func (c *Codec) CreateDest(colTypes []*sql.ColumnType) []interface{} {
	dest := make([]interface{}, len(colTypes))
	for i, ct := range colTypes {
		dest[i] = c.create(ct.DatabaseTypeName())
	}
	return dest
}

type NullInt64 struct{ Nullable[int64] }

// Scan implements sql.Scanner for NullInt64.
func (i *NullInt64) Scan(src interface{}) error {
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
		s := string(v)
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			i.val, i.valid = 0, false
			return fmt.Errorf("NullInt64.Scan: cannot parse %q as int64: %w", s, err)
		}
		i.val, i.valid = n, true
		return nil
	default:
		i.val, i.valid = 0, false
		return fmt.Errorf("NullInt64.Scan: unsupported type %T", src)
	}
}

type NullFloat64 struct{ Nullable[float64] }

// Scan implements sql.Scanner for NullFloat64.
func (f *NullFloat64) Scan(src interface{}) error {
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
		s := string(v)
		f64, err := strconv.ParseFloat(s, 64)
		if err != nil {
			f.val, f.valid = 0, false
			return fmt.Errorf("NullFloat64.Scan: cannot parse %q as float64: %w", s, err)
		}
		f.val, f.valid = f64, true
		return nil
	default:
		f.val, f.valid = 0, false
		return fmt.Errorf("NullFloat64.Scan: unsupported type %T", src)
	}
}

// NullDecimal preserves numeric string representation to avoid precision loss.
type NullDecimal struct{ Nullable[string] }

// Scan implements sql.Scanner for NullDecimal.
func (s *NullDecimal) Scan(src interface{}) error {
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

// NullGUID scans MSSQL UNIQUEIDENTIFIER values.
type NullGUID struct{ Nullable[string] }

// Scan implements sql.Scanner for NullGUID.
func (g *NullGUID) Scan(src interface{}) error {
	if src == nil {
		g.val, g.valid = "", false
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
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

// formatMSSQLGUID formats a 16-byte MSSQL GUID to standard string format.
func formatMSSQLGUID(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0],
		b[5], b[4],
		b[7], b[6],
		b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
	)
}

// FormatMSSQLGUID exports formatMSSQLGUID for use by other packages.
func FormatMSSQLGUID(b []byte) string {
	return formatMSSQLGUID(b)
}

type NullString struct{ Nullable[string] }

// Scan implements sql.Scanner for NullString.
func (s *NullString) Scan(src interface{}) error {
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

type NullBool struct{ Nullable[bool] }

// Scan implements sql.Scanner for NullBool.
func (b *NullBool) Scan(src interface{}) error {
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
		if len(v) == 0 {
			b.val, b.valid = false, false
			return nil
		}
		parsed, err := strconv.ParseBool(string(v))
		if err != nil {
			b.val, b.valid = false, false
			return fmt.Errorf("NullBool.Scan: cannot parse %q as bool: %w", string(v), err)
		}
		b.val, b.valid = parsed, true
	default:
		b.val, b.valid = false, false
		return fmt.Errorf("NullBool.Scan: unsupported type %T", src)
	}
	return nil
}

type NullBytes struct{ Nullable[[]byte] }

// Scan implements sql.Scanner for NullBytes.
func (b *NullBytes) Scan(src interface{}) error {
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
		return fmt.Errorf("NullBytes.Scan: unsupported type %T", src)
	}
	return nil
}

var (
	_ DBType = (*NullInt64)(nil)
	_ DBType = (*NullFloat64)(nil)
	_ DBType = (*NullDecimal)(nil)
	_ DBType = (*NullGUID)(nil)
	_ DBType = (*NullString)(nil)
	_ DBType = (*NullBool)(nil)
	_ DBType = (*NullBytes)(nil)
	_ DBType = (*NullTime)(nil)
)