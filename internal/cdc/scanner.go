package cdc

import (
	"database/sql"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/scanner"
)

// MSSQL type names - re-export from scanner package for compatibility
const (
	TypeBigInt           = scanner.TypeBigInt
	TypeInt              = scanner.TypeInt
	TypeSmallInt         = scanner.TypeSmallInt
	TypeTinyInt          = scanner.TypeTinyInt
	TypeBit              = scanner.TypeBit
	TypeFloat            = scanner.TypeFloat
	TypeReal             = scanner.TypeReal
	TypeNumeric          = scanner.TypeNumeric
	TypeDecimal          = scanner.TypeDecimal
	TypeMoney            = scanner.TypeMoney
	TypeSmallMoney       = scanner.TypeSmallMoney
	TypeDateTime         = scanner.TypeDateTime
	TypeDateTime2        = scanner.TypeDateTime2
	TypeSmallDateTime    = scanner.TypeSmallDateTime
	TypeDate             = scanner.TypeDate
	TypeTime            = scanner.TypeTime
	TypeDateTimeOffset   = scanner.TypeDateTimeOffset
	TypeUniqueIdentifier = scanner.TypeUniqueIdentifier
	TypeChar             = scanner.TypeChar
	TypeVarchar          = scanner.TypeVarchar
	TypeNChar            = scanner.TypeNChar
	TypeNVarchar         = scanner.TypeNVarchar
	TypeNText            = scanner.TypeNText
	TypeText             = scanner.TypeText
	TypeBinary           = scanner.TypeBinary
	TypeVarBinary        = scanner.TypeVarBinary
	TypeImage            = scanner.TypeImage
	TypeXML              = scanner.TypeXML
	TypeSQLVariant       = scanner.TypeSQLVariant
)

// ScannerFactory creates typed scanner instances based on MSSQL column types.
type ScannerFactory struct {
	timezone *time.Location
}

// NewScannerFactory creates a new ScannerFactory with the given timezone.
func NewScannerFactory(tz *time.Location) *ScannerFactory {
	return &ScannerFactory{timezone: tz}
}

// createScanner creates a new scanner instance for the given MSSQL type name.
func (sf *ScannerFactory) createScanner(typeName string) scanner.Scanner {
	switch strings.ToUpper(typeName) {
	case TypeBigInt, TypeInt, TypeSmallInt, TypeTinyInt:
		return &scanner.Int64{}
	case TypeFloat, TypeReal, TypeMoney, TypeSmallMoney:
		return &scanner.Float64{}
	case TypeNumeric, TypeDecimal:
		return &scanner.NumericString{}
	case TypeUniqueIdentifier:
		return &scanner.GUID{}
	case TypeDateTime, TypeDateTime2, TypeSmallDateTime, TypeDate, TypeTime, TypeDateTimeOffset:
		return NewDateTime(sf.timezone)
	case TypeBit:
		return &scanner.Bool{}
	case TypeChar, TypeVarchar, TypeNChar, TypeNVarchar, TypeNText, TypeText, TypeXML, TypeSQLVariant:
		return &scanner.String{}
	case TypeBinary, TypeVarBinary, TypeImage:
		return &scanner.Bytes{}
	default:
		return &scanner.String{}
	}
}

// CreateDest creates a []interface{} of scanner pointers for rows.Scan.
func (sf *ScannerFactory) CreateDest(columns []string, colTypes []*sql.ColumnType) []interface{} {
	dest := make([]interface{}, len(columns))
	for i, ct := range colTypes {
		scanner := sf.createScanner(ct.DatabaseTypeName())
		dest[i] = scanner
	}
	return dest
}

// Re-export scanner types for backward compatibility
type (
	Int64         = scanner.Int64
	Float64       = scanner.Float64
	NumericString = scanner.NumericString
	GUID          = scanner.GUID
	String        = scanner.String
	Bool          = scanner.Bool
	Bytes         = scanner.Bytes
)
