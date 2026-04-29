package cdc

import (
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	"github.com/cnlangzi/dbkrab/internal/types"
)

func TestNullableScanValue(t *testing.T) {
	t.Run("int64_nil_src", func(t *testing.T) {
		var n types.Nullable[int64]

		if err := n.Scan(nil); err != nil {
			t.Fatalf("Scan(nil) error = %v, want nil", err)
		}

		if n.Valid() {
			t.Fatalf("n.valid = true after Scan(nil), want false")
		}

		v, err := n.Value()
		if err != nil {
			t.Fatalf("Value() error = %v, want nil", err)
		}
		if v != nil {
			t.Fatalf("Value() = %v, want nil when not valid", v)
		}
	})

	t.Run("int64_exact_type", func(t *testing.T) {
		var n types.Nullable[int64]

		const want int64 = 123
		if err := n.Scan(want); err != nil {
			t.Fatalf("Scan(int64) error = %v, want nil", err)
		}

		if !n.Valid() {
			t.Fatalf("n.valid = false after Scan(int64), want true")
		}
		if n.Val() != want {
			t.Fatalf("n.val = %v, want %v", n.Val(), want)
		}

		v, err := n.Value()
		if err != nil {
			t.Fatalf("Value() error = %v, want nil", err)
		}
		if v != want {
			t.Fatalf("Value() = %v, want %v", v, want)
		}
	})

	t.Run("int64_mismatched_type_returns_error", func(t *testing.T) {
		var n types.Nullable[int64]

		// database/sql may return int32 for some drivers; should return error, not panic
		if err := n.Scan(int32(123)); err == nil {
			t.Fatalf("Scan(int32) error = nil, want error for mismatched type")
		}
	})

	t.Run("string_nil_src", func(t *testing.T) {
		var n types.Nullable[string]

		if err := n.Scan(nil); err != nil {
			t.Fatalf("Scan(nil) error = %v, want nil", err)
		}

		if n.Valid() {
			t.Fatalf("n.valid = true after Scan(nil), want false")
		}

		v, err := n.Value()
		if err != nil {
			t.Fatalf("Value() error = %v, want nil", err)
		}
		if v != nil {
			t.Fatalf("Value() = %v, want nil when not valid", v)
		}
	})

	t.Run("string_exact_type", func(t *testing.T) {
		var n types.Nullable[string]

		const want = "hello"
		if err := n.Scan(want); err != nil {
			t.Fatalf("Scan(string) error = %v, want nil", err)
		}

		if !n.Valid() {
			t.Fatalf("n.valid = false after Scan(string), want true")
		}
		if n.Val() != want {
			t.Fatalf("n.val = %q, want %q", n.Val(), want)
		}

		v, err := n.Value()
		if err != nil {
			t.Fatalf("Value() error = %v, want nil", err)
		}
		if v != want {
			t.Fatalf("Value() = %v, want %v", v, want)
		}
	})

	t.Run("string_mismatched_type_returns_error", func(t *testing.T) {
		var n types.Nullable[string]

		// database/sql commonly returns []byte for textual columns; should return error
		if err := n.Scan([]byte("hello")); err == nil {
			t.Fatalf("Scan([]byte) error = nil, want error for mismatched type")
		}
	})

	t.Run("zero_value_never_scanned", func(t *testing.T) {
		var n types.Nullable[int64] // never populated via Scan

		if n.Valid() {
			t.Fatalf("zero-value Nullable.valid = true, want false")
		}

		v, err := n.Value()
		if err != nil {
			t.Fatalf("Value() on zero-value Nullable error = %v, want nil", err)
		}
		if v != nil {
			t.Fatalf("Value() on zero-value Nullable = %v, want nil when not valid", v)
		}
	})
}

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
			var i types.NullInt64
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
			name: "float32",
			src:  float32(78.9),
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
			var f types.NullFloat64
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
			var s types.NullDecimal
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
			var g types.NullGUID
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
		wantVal string // Value() now returns formatted string
		wantNil bool
	}{
		{
			// Driver delivers time.Time tagged as UTC but it's actually Shanghai local time.
			// NullTime reinterprets the wall clock in Shanghai and converts it to UTC.
			// Value() now outputs in Shanghai timezone, so wall clock time is preserved.
			name:    "valid time.Time (driver UTC, reinterpreted as Shanghai)",
			src:     time.Date(2026, 4, 9, 12, 26, 0, 0, time.UTC),
			wantVal: "2026-04-09 12:26:00",
			wantNil: false,
		},
		{
			name:    "nil",
			src:     nil,
			wantNil: true,
		},
		{
			// A string already carries explicit timezone (Z = UTC); no reinterpretation.
			// Value() now outputs in configured timezone (Shanghai), so UTC time appears as Shanghai.
			name:    "RFC3339 string stays as-is",
			src:     "2026-04-09T12:26:00Z",
			wantVal: "2026-04-09 20:26:00",
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := types.NewNullTime(shanghai, "")
			if err := dt.Scan(tt.src); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			got, err := dt.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.wantNil {
				if got != nil {
					t.Errorf("Value() = %v, want nil", got)
				}
				return
			}
			gotStr, ok := got.(string)
			if !ok {
				t.Fatalf("Value() type = %T, want string", got)
			}
			if gotStr != tt.wantVal {
				t.Errorf("Value() = %q, want %q", gotStr, tt.wantVal)
			}
		})
	}
}

func TestDateTimeZeroValue(t *testing.T) {
	// Zero/invalid time should return nil (stored as NULL).
	dt := types.NewNullTime(time.UTC, "")
	_ = dt.Scan(time.Time{})
	got, err := dt.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}
	if got != nil {
		t.Errorf("Value() for zero time = %v, want nil", got)
	}
}

// TestNullTimeFormats verifies all TimeFormat serialization options.
func TestNullTimeFormats(t *testing.T) {
	utcTime := time.Date(2026, 4, 9, 10, 30, 0, 500000000, time.UTC)

	cases := []struct {
		format  types.TimeFormat
		wantStr string
		wantInt int64
		isInt   bool
	}{
		{types.TimeFormatRFC3339Nano, "2026-04-09T10:30:00.5Z", 0, false},
		{types.TimeFormatRFC3339, "2026-04-09T10:30:00Z", 0, false},
		{types.TimeFormatDatetime, "2026-04-09 10:30:00", 0, false},
		{types.TimeFormatDate, "2026-04-09", 0, false},
		{types.TimeFormatUnix, "", utcTime.Unix(), true},
		{types.TimeFormatUnixMilli, "", utcTime.UnixMilli(), true},
		{types.TimeFormatUnixNano, "", utcTime.UnixNano(), true},
	}

	for _, c := range cases {
		t.Run(string(c.format), func(t *testing.T) {
			m := types.NewNullTime(time.UTC, c.format)
			if err := m.Scan(utcTime); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			v, err := m.Value()
			if err != nil {
				t.Fatalf("Value: %v", err)
			}
			// Value() and MarshalJSON() must produce identical results.
			if c.isInt {
				gotInt, ok := v.(int64)
				if !ok {
					t.Fatalf("Value() type = %T, want int64", v)
				}
				if gotInt != c.wantInt {
					t.Errorf("Value() = %d, want %d", gotInt, c.wantInt)
				}
			} else {
				gotStr, ok := v.(string)
				if !ok {
					t.Fatalf("Value() type = %T, want string", v)
				}
				if gotStr != c.wantStr {
					t.Errorf("Value() = %q, want %q", gotStr, c.wantStr)
				}
			}

			marshaled, err := json.Marshal(m)
			if err != nil {
				t.Fatalf("MarshalJSON: %v", err)
			}
			if c.isInt {
				var gotInt int64
				if err := json.Unmarshal(marshaled, &gotInt); err != nil {
					t.Fatalf("UnmarshalJSON int: %v", err)
				}
				if gotInt != c.wantInt {
					t.Errorf("MarshalJSON() = %d, want %d", gotInt, c.wantInt)
				}
			} else {
				var gotStr string
				if err := json.Unmarshal(marshaled, &gotStr); err != nil {
					t.Fatalf("UnmarshalJSON string: %v", err)
				}
				if gotStr != c.wantStr {
					t.Errorf("MarshalJSON() = %q, want %q", gotStr, c.wantStr)
				}
			}
		})
	}
}

// TestNullTimeJSON verifies json.Marshal and json.Unmarshal round-trips.
func TestNullTimeJSON(t *testing.T) {
	utcTime := time.Date(2026, 4, 9, 10, 30, 0, 0, time.UTC)

	t.Run("marshal RFC3339Nano", func(t *testing.T) {
		m := types.NewNullTime(time.UTC, types.TimeFormatRFC3339Nano)
		_ = m.Scan(utcTime)
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		want := `"2026-04-09T10:30:00Z"`
		if string(b) != want {
			t.Errorf("Marshal() = %s, want %s", b, want)
		}
	})

	t.Run("marshal unix", func(t *testing.T) {
		m := types.NewNullTime(time.UTC, types.TimeFormatUnix)
		_ = m.Scan(utcTime)
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		wantNum := utcTime.Unix()
		var got int64
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal number: %v", err)
		}
		if got != wantNum {
			t.Errorf("Marshal unix = %d, want %d", got, wantNum)
		}
	})

	t.Run("marshal null", func(t *testing.T) {
		m := types.NewNullTime(time.UTC, types.TimeFormatRFC3339Nano)
		_ = m.Scan(nil)
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if string(b) != "null" {
			t.Errorf("Marshal null = %s, want null", b)
		}
	})

	t.Run("unmarshal RFC3339 string", func(t *testing.T) {
		m := types.NewNullTime(time.UTC, "")
		if err := json.Unmarshal([]byte(`"2026-04-09T10:30:00Z"`), m); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !m.Valid() {
			t.Fatal("Valid() = false, want true")
		}
		if !m.Time().Equal(utcTime) {
			t.Errorf("Time() = %v, want %v", m.Time(), utcTime)
		}
	})

	t.Run("unmarshal unix number", func(t *testing.T) {
		m := types.NewNullTime(time.UTC, "")
		data, _ := json.Marshal(utcTime.Unix())
		if err := json.Unmarshal(data, m); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !m.Valid() {
			t.Fatal("Valid() = false, want true")
		}
		if !m.Time().Equal(utcTime) {
			t.Errorf("Time() = %v, want %v", m.Time(), utcTime)
		}
	})

	t.Run("unmarshal null", func(t *testing.T) {
		m := types.NewNullTime(time.UTC, "")
		if err := json.Unmarshal([]byte("null"), m); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if m.Valid() {
			t.Fatal("Valid() = true after null, want false")
		}
	})
}

// TestFormatTimeValue verifies the weak-typed FormatTimeValue helper.
func TestFormatTimeValue(t *testing.T) {
	utcTime := time.Date(2026, 4, 9, 10, 30, 0, 0, time.UTC)
	rfc := "2026-04-09T10:30:00Z"

	cases := []struct {
		name   string
		input  interface{}
		format types.TimeFormat
		want   interface{}
	}{
		{"time.Time to RFC3339", utcTime, types.TimeFormatRFC3339, rfc},
		{"string to RFC3339", rfc, types.TimeFormatRFC3339, rfc},
		{"string to unix", rfc, types.TimeFormatUnix, utcTime.Unix()},
		{"nil passthrough", nil, types.TimeFormatRFC3339, nil},
		{"non-date passthrough", "hello", types.TimeFormatRFC3339, "hello"},
		{"int64 unix to RFC3339", utcTime.Unix(), types.TimeFormatRFC3339, rfc},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := types.FormatTimeValue(c.input, c.format)
			if got != c.want {
				t.Errorf("FormatTimeValue(%v, %s) = %v (%T), want %v (%T)",
					c.input, c.format, got, got, c.want, c.want)
			}
		})
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
			var s types.NullString
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
			var b types.NullBool
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
			var b types.NullBytes
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
