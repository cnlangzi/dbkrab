package core

import (
	"encoding/hex"
	"fmt"
)

// LSN represents a Log Sequence Number
type LSN []byte

// String returns hex representation
func (l LSN) String() string {
	return hex.EncodeToString(l)
}

// ParseLSN creates LSN from hex string
func ParseLSN(s string) (LSN, error) {
	return hex.DecodeString(s)
}

// Compare returns -1 if l < other, 0 if equal, 1 if l > other
func (l LSN) Compare(other LSN) int {
	if len(l) != len(other) {
		// Different lengths, compare byte by byte
		minLen := len(l)
		if len(other) < minLen {
			minLen = len(other)
		}
		for i := 0; i < minLen; i++ {
			if l[i] < other[i] {
				return -1
			} else if l[i] > other[i] {
				return 1
			}
		}
		if len(l) < len(other) {
			return -1
		}
		return 1
	}
	
	for i := 0; i < len(l); i++ {
		if l[i] < other[i] {
			return -1
		} else if l[i] > other[i] {
			return 1
		}
	}
	return 0
}

// IsZero returns true if LSN is all zeros
func (l LSN) IsZero() bool {
	for _, b := range l {
		if b != 0 {
			return false
		}
	}
	return true
}

// MinLSN returns the minimum LSN from a slice
func MinLSN(lsns []LSN) (LSN, error) {
	if len(lsns) == 0 {
		return nil, fmt.Errorf("empty LSN slice")
	}
	
	min := lsns[0]
	for _, l := range lsns[1:] {
		if l.Compare(min) < 0 {
			min = l
		}
	}
	return min, nil
}