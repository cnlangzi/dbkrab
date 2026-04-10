package cdc

import (
	"database/sql"
	"strings"
	"time"
)

// ScannerFactory creates typed scanner instances based on MSSQL column types.
type ScannerFactory struct {
	timezone *time.Location
}

// NewScannerFactory creates a new ScannerFactory with the given timezone.
func NewScannerFactory(tz *time.Location) *ScannerFactory {
	return &ScannerFactory{timezone: tz}
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
	case TypeDateTime, TypeDateTime2, TypeSmallDateTime, TypeDate, TypeTime, TypeDateTimeOffset:
		return NewDateTime(sf.timezone)
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
func (sf *ScannerFactory) CreateDest(columns []string, colTypes []*sql.ColumnType) []interface{} {
	dest := make([]interface{}, len(columns))
	for i, ct := range colTypes {
		scanner := sf.createScanner(ct.DatabaseTypeName())
		dest[i] = scanner
	}
	return dest
}
