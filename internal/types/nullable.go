package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// Nullable represents a value that may be NULL.
// It implements sql.Scanner, driver.Valuer, json.Marshaler and json.Unmarshaler.
// When Valid is false, Value() returns nil (SQL NULL) and MarshalJSON returns null.
type Nullable[T any] struct {
	val   T
	valid bool
}

// Val returns the underlying value. Check Valid() before using it.
func (n Nullable[T]) Val() T { return n.val }

// Valid reports whether the value is non-NULL.
func (n Nullable[T]) Valid() bool { return n.valid }

// Scan implements sql.Scanner.
func (n *Nullable[T]) Scan(src interface{}) error {
	if src == nil {
		n.val, n.valid = *new(T), false
		return nil
	}
	val, ok := src.(T)
	if !ok {
		return fmt.Errorf("Nullable[T].Scan: cannot convert %T to %T", src, *new(T))
	}
	n.val, n.valid = val, true
	return nil
}

// Value implements driver.Valuer.
func (n Nullable[T]) Value() (driver.Value, error) {
	if !n.valid {
		return nil, nil
	}
	return n.val, nil
}

// MarshalJSON implements json.Marshaler.
// Returns JSON null when not valid; otherwise marshals the underlying value.
func (n Nullable[T]) MarshalJSON() ([]byte, error) {
	if !n.valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.val)
}

// UnmarshalJSON implements json.Unmarshaler.
// JSON null sets the value to invalid; any other JSON value is unmarshalled into T.
func (n *Nullable[T]) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.val, n.valid = *new(T), false
		return nil
	}
	if err := json.Unmarshal(data, &n.val); err != nil {
		return fmt.Errorf("Nullable.UnmarshalJSON: %w", err)
	}
	n.valid = true
	return nil
}
