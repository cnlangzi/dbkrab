package types

import (
	"testing"
	"time"
)

func TestCodecCreateDest(t *testing.T) {
	codec := NewMSSQLCodec()

	sc := codec.create("BIGINT")
	if _, ok := sc.(*NullInt64); !ok {
		t.Errorf("create(BIGINT) should return *NullInt64")
	}

	sc = codec.create("VARCHAR")
	if _, ok := sc.(*NullString); !ok {
		t.Errorf("create(VARCHAR) should return *NullString")
	}

	sc = codec.create("UNIQUEIDENTIFIER")
	if _, ok := sc.(*NullGUID); !ok {
		t.Errorf("create(UNIQUEIDENTIFIER) should return *NullGUID")
	}

	sc = codec.create("DATETIME")
	if _, ok := sc.(*NullTime); !ok {
		t.Errorf("create(DATETIME) should return *NullTime")
	}

	sc = codec.create("DECIMAL")
	if _, ok := sc.(*NullDecimal); !ok {
		t.Errorf("create(DECIMAL) should return *NullDecimal")
	}

	// Ensure VARBINARY uses *NullBytes scanner and round-trips []byte values.
	varbinaryScanner := codec.create("VARBINARY")
	bytesScanner, ok := varbinaryScanner.(*NullBytes)
	if !ok {
		t.Fatalf("create(VARBINARY) should return *NullBytes, got %T", varbinaryScanner)
	}

	original := []byte{0x01, 0x02, 0x03}
	if err := bytesScanner.Scan(original); err != nil {
		t.Fatalf("NullBytes.Scan() error = %v", err)
	}

	gotVal, err := bytesScanner.Value()
	if err != nil {
		t.Fatalf("NullBytes.Value() error = %v", err)
	}

	gotBytes, ok := gotVal.([]byte)
	if !ok {
		t.Fatalf("NullBytes.Value() type = %T, want []byte", gotVal)
	}
	if string(gotBytes) != string(original) {
		t.Errorf("NullBytes.Value() = %v, want %v", gotBytes, original)
	}

	sc = codec.create("UNKNOWN_TYPE")
	if _, ok := sc.(*NullString); !ok {
		t.Errorf("create(UNKNOWN_TYPE) should fallback to *NullString")
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

func TestSQLiteCodecCreate(t *testing.T) {
	shanghai, _ := time.LoadLocation("Asia/Shanghai")
	SetDefaultTimezone(shanghai)
	defer SetDefaultTimezone(time.UTC) // restore after test

	codec := NewSQLiteCodec()

	cases := []struct {
		typeName string
		wantType string // friendly name for error messages
		check    func(DBType) bool
	}{
		{"INTEGER", "*NullInt64", func(d DBType) bool { _, ok := d.(*NullInt64); return ok }},
		{"INT", "*NullInt64", func(d DBType) bool { _, ok := d.(*NullInt64); return ok }},
		{"BIGINT", "*NullInt64", func(d DBType) bool { _, ok := d.(*NullInt64); return ok }},
		{"REAL", "*NullFloat64", func(d DBType) bool { _, ok := d.(*NullFloat64); return ok }},
		{"NUMERIC", "*NullDecimal", func(d DBType) bool { _, ok := d.(*NullDecimal); return ok }},
		{"DECIMAL", "*NullDecimal", func(d DBType) bool { _, ok := d.(*NullDecimal); return ok }},
		{"BLOB", "*NullBytes", func(d DBType) bool { _, ok := d.(*NullBytes); return ok }},
		{"BOOLEAN", "*NullBool", func(d DBType) bool { _, ok := d.(*NullBool); return ok }},
		{"DATETIME", "*NullTime", func(d DBType) bool { _, ok := d.(*NullTime); return ok }},
		{"TIMESTAMP", "*NullTime", func(d DBType) bool { _, ok := d.(*NullTime); return ok }},
		{"TEXT", "*NullString", func(d DBType) bool { _, ok := d.(*NullString); return ok }},
		{"UNKNOWN", "*NullString", func(d DBType) bool { _, ok := d.(*NullString); return ok }},
	}

	for _, c := range cases {
		t.Run(c.typeName, func(t *testing.T) {
			got := codec.create(c.typeName)
			if !c.check(got) {
				t.Errorf("create(%q) = %T, want %s", c.typeName, got, c.wantType)
			}
		})
	}

	// Verify DATETIME scanner inherits the timezone set via SetDefaultTimezone.
	t.Run("DATETIME timezone matches SetDefaultTimezone", func(t *testing.T) {
		d := codec.create("DATETIME")
		nt, ok := d.(*NullTime)
		if !ok {
			t.Fatalf("create(DATETIME) = %T, want *NullTime", d)
		}
		// Scan a timezone-naive string; it should be interpreted in Shanghai time.
		if err := nt.Scan("2026-04-09 12:00:00"); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got, err := nt.Value()
		if err != nil {
			t.Fatalf("Value: %v", err)
		}
		// 2026-04-09 12:00:00 Shanghai (UTC+8) → 2026-04-09 04:00:00 UTC → "2026-04-09 04:00:00"
		want := "2026-04-09 04:00:00"
		if got != want {
			t.Errorf("Value() = %q, want %q", got, want)
		}
	})
}
