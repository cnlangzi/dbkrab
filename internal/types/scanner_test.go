package types

import (
	"testing"
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
