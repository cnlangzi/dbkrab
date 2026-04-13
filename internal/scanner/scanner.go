package scanner

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strings"
)

// Scanner is the interface implemented by types that can scan from SQL rows.
// All scanner types implement both sql.Scanner and driver.Valuer.
type Scanner interface {
	sql.Scanner
	driver.Valuer
}

// MSSQL type names from DatabaseTypeName()
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

// ScannerFactory creates typed scanner instances based on MSSQL column types.
type ScannerFactory struct{}

// NewScannerFactory creates a new ScannerFactory.
func NewScannerFactory() *ScannerFactory {
	return &ScannerFactory{}
}

// createScanner creates a new scanner instance for the given MSSQL type name.
func (sf *ScannerFactory) createScanner(typeName string) Scanner {
	switch strings.ToUpper(typeName) {
	case TypeBigInt, TypeInt, TypeSmallInt, TypeTinyInt:
		return &Int64{}
	case TypeFloat, TypeReal, TypeMoney, TypeSmallMoney:
		return &Float64{}
	case TypeNumeric, TypeDecimal:
		return &NumericString{}
	case TypeUniqueIdentifier:
		return &GUID{}
	case TypeBit:
		return &Bool{}
	case TypeChar, TypeVarchar, TypeNChar, TypeNVarchar, TypeNText, TypeText, TypeXML, TypeSQLVariant:
		return &String{}
	case TypeBinary, TypeVarBinary, TypeImage:
		return &Bytes{}
	default:
		// Fallback to String for unknown types
		return &String{}
	}
}

// CreateDest creates a []interface{} of scanner pointers for rows.Scan.
// columns is the column names, colTypes is the result of rows.ColumnTypes().
func (sf *ScannerFactory) CreateDest(colTypes []*sql.ColumnType) []interface{} {
	dest := make([]interface{}, len(colTypes))
	for i, ct := range colTypes {
		scanner := sf.createScanner(ct.DatabaseTypeName())
		dest[i] = scanner
	}
	return dest
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
