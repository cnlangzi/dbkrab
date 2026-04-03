package core

import (
	"testing"
)

func TestLSNString(t *testing.T) {
	lsn := LSN{0x01, 0x02, 0x03, 0x04}
	expected := "01020304"

	if got := lsn.String(); got != expected {
		t.Errorf("LSN.String() = %v, want %v", got, expected)
	}
}

func TestParseLSN(t *testing.T) {
	tests := []struct {
		input    string
		expected LSN
		hasError bool
	}{
		{"01020304", LSN{0x01, 0x02, 0x03, 0x04}, false},
		{"", LSN{}, false},
		{"invalid", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseLSN(tt.input)
			if tt.hasError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if string(got) != string(tt.expected) {
				t.Errorf("ParseLSN() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestLSNCompare(t *testing.T) {
	tests := []struct {
		name     string
		a, b     LSN
		expected int
	}{
		{"equal", LSN{0x01, 0x02}, LSN{0x01, 0x02}, 0},
		{"a < b", LSN{0x01, 0x01}, LSN{0x01, 0x02}, -1},
		{"a > b", LSN{0x01, 0x02}, LSN{0x01, 0x01}, 1},
		{"zero vs non-zero", LSN{0x00, 0x00}, LSN{0x00, 0x01}, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.expected {
				t.Errorf("LSN.Compare() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestLSNIsZero(t *testing.T) {
	tests := []struct {
		name     string
		lsn      LSN
		expected bool
	}{
		{"zero", LSN{0x00, 0x00, 0x00}, true},
		{"non-zero", LSN{0x00, 0x01, 0x00}, false},
		{"empty", LSN{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lsn.IsZero(); got != tt.expected {
				t.Errorf("LSN.IsZero() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMinLSN(t *testing.T) {
	lsns := []LSN{
		{0x03, 0x00},
		{0x01, 0x00},
		{0x02, 0x00},
	}

	min, err := MinLSN(lsns)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := LSN{0x01, 0x00}
	if string(min) != string(expected) {
		t.Errorf("MinLSN() = %v, want %v", min, expected)
	}

	// Empty slice
	_, err = MinLSN([]LSN{})
	if err == nil {
		t.Error("Expected error for empty slice")
	}
}