package retry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// RetryConfig defines the retry configuration
type RetryConfig struct {
	MaxRetries    int           // Maximum number of retry attempts
	InitialDelay  time.Duration // Initial delay between retries
	MaxDelay      time.Duration // Maximum delay between retries
	Multiplier    float64       // Delay multiplier for exponential backoff
	RetryableErrors []string    // Additional retryable error patterns
}

// DefaultRetryConfig returns the default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:    5,
		InitialDelay:  1 * time.Second,
		MaxDelay:      60 * time.Second,
		Multiplier:    2.0,
	}
}

// RetryError represents an error after retries are exhausted
type RetryError struct {
	LastError  error
	RetryCount int
	Operation  string
}

// Error implements the error interface
func (e *RetryError) Error() string {
	return fmt.Sprintf("%s failed after %d retries: %v", e.Operation, e.RetryCount, e.LastError)
}

// Unwrap returns the underlying error
func (e *RetryError) Unwrap() error {
	return e.LastError
}

// IsRetryable determines if an error is retryable
// Retryable errors include:
// - context deadline exceeded (timeout)
// - database is locked (SQLite SQLITE_BUSY)
// - connection reset
// - SQLITE_BUSY, SQLITE_IOERR
// Not retryable:
// - syntax error
// - permission denied
// - table not found
// - constraint violation
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	errLower := strings.ToLower(errStr)

	// Check for retryable error patterns
	retryablePatterns := []string{
		"context deadline exceeded",
		"deadline exceeded",
		"database is locked",
		"connection reset",
		"connection refused",
		"connection reset by peer",
		"broken pipe",
		"temporary failure",
		"try again",
		"resource temporarily unavailable",
		"too many connections",
		"connection timeout",
		"timeout",
		"i/o timeout",
		"network is unreachable",
		"no route to host",
		"operation timed out",
		"sqlite_busy",
		"sqlite_ioerr",
		"sqlite_locked",
		"sqlite_busy",
		"sql: connection is already closed",
		"driver: bad connection",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errLower, pattern) {
			return true
		}
	}

	// Check for SQLite error codes in error string
	// SQLITE_BUSY (5), SQLITE_IOERR (10), SQLITE_LOCKED (6)
	if strings.Contains(errStr, "SQLITE_BUSY") ||
		strings.Contains(errStr, "SQLITE_IOERR") ||
		strings.Contains(errStr, "SQLITE_LOCKED") ||
		strings.Contains(errStr, "(5)") ||
		strings.Contains(errStr, "(6)") ||
		strings.Contains(errStr, "(10)") {
		return true
	}

	// Non-retryable error patterns
	nonRetryablePatterns := []string{
		"syntax error",
		"permission denied",
		"access denied",
		"table not found",
		"no such table",
		"column not found",
		"no such column",
		"constraint",
		"unique constraint",
		"foreign key constraint",
		"primary key constraint",
		"check constraint",
		"duplicate",
		"duplicate key",
		"duplicate entry",
		"already exists",
		"not null constraint",
		"invalid",
		"malformed",
		"unauthorized",
		"forbidden",
	}

	for _, pattern := range nonRetryablePatterns {
		if strings.Contains(errLower, pattern) {
			return false
		}
	}

	// Default: unknown errors are not retryable to avoid infinite loops
	return false
}

// IsRetryableWithConfig determines if an error is retryable using custom patterns
func IsRetryableWithConfig(err error, cfg RetryConfig) bool {
	if err == nil {
		return false
	}

	// Check standard retryable patterns first
	if IsRetryable(err) {
		return true
	}

	// Check custom retryable patterns
	errStr := strings.ToLower(err.Error())
	for _, pattern := range cfg.RetryableErrors {
		if strings.Contains(errStr, strings.ToLower(pattern)) {
			return true
		}
	}

	return false
}

// Do executes a function with retry logic using exponential backoff
// Returns RetryError if all retries are exhausted
func Do(ctx context.Context, fn func() error, cfg RetryConfig) error {
	return DoWithName(ctx, fn, cfg, "operation")
}

// DoWithName executes a function with retry logic and a descriptive name
func DoWithName(ctx context.Context, fn func() error, cfg RetryConfig, operationName string) error {
	var lastErr error
	retryCount := 0
	delay := cfg.InitialDelay

	for i := 0; i <= cfg.MaxRetries; i++ {
		// Check context before attempting
		if ctx.Err() != nil {
			return &RetryError{
				LastError:  ctx.Err(),
				RetryCount: retryCount,
				Operation:  operationName,
			}
		}

		// Attempt the operation
		err := fn()
		if err == nil {
			// Success
			if retryCount > 0 {
				slog.Info("retry succeeded", "operation", operationName, "retries", retryCount)
			}
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !IsRetryableWithConfig(err, cfg) {
			slog.Warn("retry failed with non-retryable error", "operation", operationName, "error", err)
			return &RetryError{
				LastError:  err,
				RetryCount: retryCount,
				Operation:  operationName,
			}
		}

		// If we've exhausted retries, return error
		if i >= cfg.MaxRetries {
			slog.Warn("retry failed after max retries", "operation", operationName, "max_retries", cfg.MaxRetries, "error", err)
			return &RetryError{
				LastError:  err,
				RetryCount: retryCount,
				Operation:  operationName,
			}
		}

		retryCount++

		// Calculate delay with exponential backoff
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}

		slog.Warn("retry failed",
			"operation", operationName,
			"attempt", retryCount,
			"max_retries", cfg.MaxRetries,
			"delay", delay,
			"error", err)

		// Wait before retry
		select {
		case <-ctx.Done():
			return &RetryError{
				LastError:  ctx.Err(),
				RetryCount: retryCount,
				Operation:  operationName,
			}
		case <-time.After(delay):
		}

		// Increase delay for next attempt
		delay = time.Duration(float64(delay) * cfg.Multiplier)
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
	}

	return &RetryError{
		LastError:  lastErr,
		RetryCount: retryCount,
		Operation:  operationName,
	}
}

// DoWithResult executes a function that returns a result with retry logic
func DoWithResult[T any](ctx context.Context, fn func() (T, error), cfg RetryConfig, operationName string) (T, error) {
	var result T
	err := DoWithName(ctx, func() error {
		r, e := fn()
		if e != nil {
			return e
		}
		result = r
		return nil
	}, cfg, operationName)
	return result, err
}

// calculateDelay calculates the delay for a given retry attempt
func calculateDelay(attempt int, cfg RetryConfig) time.Duration {
	delay := cfg.InitialDelay
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * cfg.Multiplier)
		if delay > cfg.MaxDelay {
			return cfg.MaxDelay
		}
	}
	return delay
}

// GetRetryDelay returns the delay for a specific retry attempt number
func GetRetryDelay(attempt int, cfg RetryConfig) time.Duration {
	return calculateDelay(attempt, cfg)
}