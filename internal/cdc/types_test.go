package cdc

import (
	"database/sql/driver"
	"testing"
	"time"
)

func TestInt64ScanValue(t *testing.T) {
	tests := []struct {
		name    string
		src     interface{}
		wantVal driver.Value
		wantNil bool
	}{
		{
			name:    "valid int64",
			src:     int64(123),
			wantVal: int64(123),
			wantNil: false,
		},
		{
			name:    "nil",
			src:     nil,
			wantVal: nil,
			wantNil: true,
		},
		{
			name:    "int32",
			src:     int32(456),
			wantVal: int64(456),
			wantNil: false,
		},
		{
			name:    "int",
			src:     int(789),
			wantVal: int64(789),
			wantNil: false,
		},
		{
			name:    "[]byte numeric",
			src:     []byte("999"),
			wantVal: int64(999),
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var i Int64
			if err := i.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			got, err := i.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.wantNil && got != nil {
				t.Errorf("Value() = %v, want nil", got)
			}
			if !tt.wantNil && got != tt.wantVal {
				t.Errorf("Value() = %v, want %v", got, tt.wantVal)
			}
		})
	}
}

func TestFloat64ScanValue(t *testing.T) {
	tests := []struct {
		name    string
		src     interface{}
		wantVal driver.Value
		wantNil bool
	}{
		{
			name:    "valid float64",
			src:     float64(123.45),
			wantVal: float64(123.45),
			wantNil: false,
		},
		{
			name:    "nil",
			src:     nil,
			wantVal: nil,
			wantNil: true,
		},
		{
			name:    "float32",
			src:     float32(78.9),
			// float32(78.9) converts to float64(78.9000015258789) due to binary FP representation
			wantVal: float64(float32(78.9)),
			wantNil: false,
		},
		{
			name:    "int64",
			src:     int64(100),
			wantVal: float64(100),
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f Float64
			if err := f.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			got, err := f.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.wantNil && got != nil {
				t.Errorf("Value() = %v, want nil", got)
			}
			if !tt.wantNil && got != tt.wantVal {
				t.Errorf("Value() = %v, want %v", got, tt.wantVal)
			}
		})
	}
}

func TestNumericStringScanValue(t *testing.T) {
	tests := []struct {
		name    string
		src     interface{}
		wantVal driver.Value
		wantNil bool
	}{
		{
			name:    "valid numeric string",
			src:     []byte("123.45"),
			wantVal: "123.45",
			wantNil: false,
		},
		{
			name:    "string input",
			src:     "700.0000",
			wantVal: "700.0000",
			wantNil: false,
		},
		{
			name:    "nil",
			src:     nil,
			wantVal: nil,
			wantNil: true,
		},
		{
			name:    "negative numeric",
			src:     []byte("-123.45"),
			wantVal: "-123.45",
			wantNil: false,
		},
		{
			name:    "scientific notation",
			src:     []byte("1.23e10"),
			wantVal: "1.23e10",
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s NumericString
			if err := s.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			got, err := s.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.wantNil && got != nil {
				t.Errorf("Value() = %v, want nil", got)
			}
			if !tt.wantNil && got != tt.wantVal {
				t.Errorf("Value() = %v, want %v", got, tt.wantVal)
			}
		})
	}
}

func TestGUIDScanValue(t *testing.T) {
	// 16-byte GUID in MSSQL format (mixed endian)
	guidBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}

	tests := []struct {
		name    string
		src     interface{}
		wantVal driver.Value
		wantNil bool
	}{
		{
			name:    "valid GUID bytes",
			src:     guidBytes,
			wantVal: "04030201-0605-0807-090a-0b0c0d0e0f10",
			wantNil: false,
		},
		{
			name:    "nil",
			src:     nil,
			wantVal: nil,
			wantNil: true,
		},
		{
			name:    "string input",
			src:     "some-string-value",
			wantVal: "some-string-value",
			wantNil: false,
		},
		{
			name:    "invalid length bytes",
			src:     []byte{0x01, 0x02, 0x03},
			wantVal: "010203",
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var g GUID
			if err := g.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			got, err := g.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.wantNil && got != nil {
				t.Errorf("Value() = %v, want nil", got)
			}
			if !tt.wantNil && got != tt.wantVal {
				t.Errorf("Value() = %v, want %v", got, tt.wantVal)
			}
		})
	}
}

func TestDateTimeScanValue(t *testing.T) {
	shanghai, _ := time.LoadLocation("Asia/Shanghai")

	tests := []struct {
		name    string
		src     interface{}
		wantVal driver.Value
		wantNil bool
	}{
		{
			name:    "valid time.Time",
			src:     time.Date(2026, 4, 9, 12, 26, 0, 0, time.UTC),
			wantVal: "2026-04-09T04:26:00Z", // Converted from UTC to Shanghai then back to UTC
			wantNil: false,
		},
		{
			name:    "nil",
			src:     nil,
			wantVal: nil,
			wantNil: true,
		},
		{
			name:    "RFC3339Nano string",
			src:     "2026-04-09T12:26:00Z",
			wantVal: "2026-04-09T04:26:00Z", // With Shanghai timezone
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := NewDateTime(shanghai)
			if err := dt.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			got, err := dt.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.wantNil && got != nil {
				t.Errorf("Value() = %v, want nil", got)
			}
			if !tt.wantNil && got != tt.wantVal {
				t.Errorf("Value() = %v, want %v", got, tt.wantVal)
			}
		})
	}
}

func TestDateTimeZeroValue(t *testing.T) {
	// Zero time should return nil
	dt := NewDateTime(time.UTC)
	dt.Scan(time.Time{})
	got, err := dt.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}
	if got != nil {
		t.Errorf("Value() for zero time = %v, want nil", got)
	}
}

func TestStringScanValue(t *testing.T) {
	tests := []struct {
		name    string
		src     interface{}
		wantVal driver.Value
		wantNil bool
	}{
		{
			name:    "valid string",
			src:     "hello world",
			wantVal: "hello world",
			wantNil: false,
		},
		{
			name:    "[]byte",
			src:     []byte("test data"),
			wantVal: "test data",
			wantNil: false,
		},
		{
			name:    "nil",
			src:     nil,
			wantVal: nil,
			wantNil: true,
		},
		{
			name:    "empty string",
			src:     "",
			wantVal: "",
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s String
			if err := s.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			got, err := s.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.wantNil && got != nil {
				t.Errorf("Value() = %v, want nil", got)
			}
			if !tt.wantNil && got != tt.wantVal {
				t.Errorf("Value() = %v, want %v", got, tt.wantVal)
			}
		})
	}
}

func TestBoolScanValue(t *testing.T) {
	tests := []struct {
		name    string
		src     interface{}
		wantVal driver.Value
		wantNil bool
	}{
		{
			name:    "true bool",
			src:     true,
			wantVal: true,
			wantNil: false,
		},
		{
			name:    "false bool",
			src:     false,
			wantVal: false,
			wantNil: false,
		},
		{
			name:    "nil",
			src:     nil,
			wantVal: nil,
			wantNil: true,
		},
		{
			name:    "int64 1",
			src:     int64(1),
			wantVal: true,
			wantNil: false,
		},
		{
			name:    "int64 0",
			src:     int64(0),
			wantVal: false,
			wantNil: false,
		},
		{
			name:    "[]byte with 1",
			src:     []byte{1},
			wantVal: true,
			wantNil: false,
		},
		{
			name:    "[]byte with 0",
			src:     []byte{0},
			wantVal: false,
			wantNil: false,
		},
		{
			name:    "empty []byte",
			src:     []byte{},
			wantVal: nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b Bool
			if err := b.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			got, err := b.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.wantNil && got != nil {
				t.Errorf("Value() = %v, want nil", got)
			}
			if !tt.wantNil && got != tt.wantVal {
				t.Errorf("Value() = %v, want %v", got, tt.wantVal)
			}
		})
	}
}

func TestBytesScanValue(t *testing.T) {
	tests := []struct {
		name    string
		src     interface{}
		wantVal driver.Value
		wantNil bool
	}{
		{
			name:    "valid []byte",
			src:     []byte{0x01, 0x02, 0x03},
			wantVal: []byte{0x01, 0x02, 0x03},
			wantNil: false,
		},
		{
			name:    "string as []byte",
			src:     "binary data",
			wantVal: []byte("binary data"),
			wantNil: false,
		},
		{
			name:    "nil",
			src:     nil,
			wantVal: nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b Bytes
			if err := b.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			got, err := b.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.wantNil && got != nil {
				t.Errorf("Value() = %v, want nil", got)
			}
			if !tt.wantNil {
				gotBytes, ok := got.([]byte)
				wantBytes := tt.wantVal.([]byte)
				if !ok || string(gotBytes) != string(wantBytes) {
					t.Errorf("Value() = %v, want %v", got, tt.wantVal)
				}
			}
		})
	}
}

func TestScannerFactoryCreateDest(t *testing.T) {
	// We can't easily mock sql.ColumnType here since it's from database/sql package.
	// Instead, we test the individual scanners and createScanner indirectly.
	factory := NewScannerFactory(nil)

	// Test that createScanner returns correct types
	scanner := factory.createScanner("BIGINT")
	if _, ok := scanner.(*Int64); !ok {
		t.Errorf("createScanner(BIGINT) should return *Int64")
	}

	scanner = factory.createScanner("VARCHAR")
	if _, ok := scanner.(*String); !ok {
		t.Errorf("createScanner(VARCHAR) should return *String")
	}

	scanner = factory.createScanner("UNIQUEIDENTIFIER")
	if _, ok := scanner.(*GUID); !ok {
		t.Errorf("createScanner(UNIQUEIDENTIFIER) should return *GUID")
	}

	scanner = factory.createScanner("DATETIME")
	if _, ok := scanner.(*DateTime); !ok {
		t.Errorf("createScanner(DATETIME) should return *DateTime")
	}

	scanner = factory.createScanner("DECIMAL")
	if _, ok := scanner.(*NumericString); !ok {
		t.Errorf("createScanner(DECIMAL) should return *NumericString")
	}

	scanner = factory.createScanner("UNKNOWN_TYPE")
	if _, ok := scanner.(*String); !ok {
		t.Errorf("createScanner(UNKNOWN_TYPE) should fallback to *String")
	}
}

func TestFormatMSSQLGUID(t *testing.T) {
	// Standard GUID: 01020304-0506-0708-090a-0b0c0d0e0f10
	input := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	want := "04030201-0605-0807-090a-0b0c0d0e0f10"

	got := FormatMSSQLGUID(input)
	if got != want {
		t.Errorf("FormatMSSQLGUID() = %v, want %v", got, want)
	}

	// Invalid length
	got = FormatMSSQLGUID([]byte{0x01, 0x02})
	if got != "0102" {
		t.Errorf("FormatMSSQLGUID() for invalid = %v, want 0102", got)
	}
}
