package dlq

import "errors"

// Error definitions for the DLQ package
var (
	// ErrDLQClosed is returned when attempting to operate on a closed DLQ
	ErrDLQClosed = errors.New("dlq is closed")

	// ErrEntryNotFound is returned when an entry is not found
	ErrEntryNotFound = errors.New("dlq entry not found")

	// ErrInvalidStatus is returned when an invalid status is provided
	ErrInvalidStatus = errors.New("invalid dlq entry status")

	// ErrHandlerRequired is returned when replay is called without a handler
	ErrHandlerRequired = errors.New("handler function is required")
)