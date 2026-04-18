package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// Retryable errors
		{"context deadline exceeded", context.DeadlineExceeded, true},
		{"database is locked", errors.New("database is locked"), true},
		{"connection reset", errors.New("connection reset by peer"), true},
		{"connection refused", errors.New("connection refused"), true},
		{"broken pipe", errors.New("broken pipe"), true},
		{"timeout", errors.New("operation timed out"), true},
		{"i/o timeout", errors.New("i/o timeout"), true},
		{"SQLITE_BUSY", errors.New("SQLITE_BUSY"), true},
		{"SQLITE_IOERR", errors.New("SQLITE_IOERR"), true},
		{"SQLITE_LOCKED", errors.New("SQLITE_LOCKED"), true},
		{"sqlite error code 5", errors.New("disk I/O error (5)"), true},
		{"sqlite error code 6", errors.New("database table is locked (6)"), true},
		{"sqlite error code 10", errors.New("disk I/O error (10)"), true},
		{"driver bad connection", errors.New("driver: bad connection"), true},
		{"connection already closed", errors.New("sql: connection is already closed"), true},
		{"network unreachable", errors.New("network is unreachable"), true},
		{"no route to host", errors.New("no route to host"), true},
		{"temporary failure", errors.New("temporary failure"), true},

		// Non-retryable errors
		{"syntax error", errors.New("syntax error near SELECT"), false},
		{"permission denied", errors.New("permission denied for table"), false},
		{"access denied", errors.New("access denied"), false},
		{"table not found", errors.New("table not found: users"), false},
		{"no such table", errors.New("no such table: users"), false},
		{"column not found", errors.New("column not found: id"), false},
		{"unique constraint", errors.New("unique constraint violation"), false},
		{"foreign key constraint", errors.New("foreign key constraint fails"), false},
		{"primary key constraint", errors.New("primary key constraint violation"), false},
		{"duplicate key", errors.New("duplicate key value violates unique constraint"), false},
		{"duplicate entry", errors.New("duplicate entry '123' for key"), false},
		{"already exists", errors.New("table already exists"), false},
		{"not null constraint", errors.New("not null constraint violated"), false},
		{"check constraint", errors.New("check constraint failed"), false},
		{"invalid", errors.New("invalid input syntax"), false},
		{"malformed", errors.New("malformed packet"), false},
		{"unauthorized", errors.New("unauthorized access"), false},
		{"forbidden", errors.New("forbidden operation"), false},

		// Edge cases
		{"nil error", nil, false},
		{"unknown error", errors.New("some unknown error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryable(tt.err)
			if result != tt.expected {
				t.Errorf("IsRetryable(%v) = %v, expected %v", tt.err, result, tt.expected)
			}
		})
	}
}

func TestIsRetryableWithConfig(t *testing.T) {
	cfg := RetryConfig{
		RetryableErrors: []string{"custom error pattern", "special failure"},
	}

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"standard retryable", errors.New("connection timeout"), true},
		{"custom pattern match", errors.New("custom error pattern occurred"), true},
		{"another custom pattern", errors.New("special failure detected"), true},
		{"non-retryable", errors.New("syntax error"), false},
		{"nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableWithConfig(tt.err, cfg)
			if result != tt.expected {
				t.Errorf("IsRetryableWithConfig(%v) = %v, expected %v", tt.err, result, tt.expected)
			}
		})
	}
}

func TestDo_Success(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.InitialDelay = 10 * time.Millisecond // Faster for tests

	attempts := 0
	err := Do(context.Background(), func() error {
		attempts++
		return nil // Success on first attempt
	}, cfg)

	if err != nil {
		t.Errorf("Do() returned error: %v", err)
	}
	if attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", attempts)
	}
}

func TestDo_SuccessAfterRetry(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.InitialDelay = 10 * time.Millisecond
	cfg.MaxDelay = 100 * time.Millisecond

	attempts := 0
	err := Do(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return errors.New("connection timeout") // Retryable error
		}
		return nil // Success on 3rd attempt
	}, cfg)

	if err != nil {
		t.Errorf("Do() returned error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestDo_MaxRetriesExhausted(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:   3,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2.0,
	}

	attempts := 0
	err := Do(context.Background(), func() error {
		attempts++
		return errors.New("connection timeout") // Always fails with retryable error
	}, cfg)

	if err == nil {
		t.Error("Do() should have returned error")
	}

	// Check it's a RetryError
	retryErr, ok := err.(*RetryError)
	if !ok {
		t.Errorf("Expected RetryError, got %T", err)
	}
	if retryErr.RetryCount != 3 {
		t.Errorf("Expected 3 retries, got %d", retryErr.RetryCount)
	}
	if attempts != 4 { // 1 initial + 3 retries
		t.Errorf("Expected 4 attempts (1 + 3 retries), got %d", attempts)
	}
}

func TestDo_NonRetryableError(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.InitialDelay = 10 * time.Millisecond

	attempts := 0
	err := Do(context.Background(), func() error {
		attempts++
		return errors.New("syntax error") // Non-retryable
	}, cfg)

	if err == nil {
		t.Error("Do() should have returned error")
	}

	retryErr, ok := err.(*RetryError)
	if !ok {
		t.Errorf("Expected RetryError, got %T", err)
	}
	if retryErr.RetryCount != 0 {
		t.Errorf("Expected 0 retries for non-retryable error, got %d", retryErr.RetryCount)
	}
	if attempts != 1 {
		t.Errorf("Expected 1 attempt for non-retryable error, got %d", attempts)
	}
}

func TestDo_ContextCancellation(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.InitialDelay = 100 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	attempts := 0
	err := Do(ctx, func() error {
		attempts++
		return errors.New("connection timeout")
	}, cfg)

	if err == nil {
		t.Error("Do() should have returned error due to context cancellation")
	}

	retryErr, ok := err.(*RetryError)
	if !ok {
		t.Errorf("Expected RetryError, got %T", err)
	}
	// The last error should be context error
	if !errors.Is(retryErr.LastError, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", retryErr.LastError)
	}
}

func TestDo_ContextCancelDuringWait(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:   5,
		InitialDelay: 1 * time.Second, // Long delay
		MaxDelay:     60 * time.Second,
		Multiplier:   2.0,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short time
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	attempts := 0
	err := Do(ctx, func() error {
		attempts++
		return errors.New("connection timeout")
	}, cfg)

	if err == nil {
		t.Error("Do() should have returned error")
	}

	retryErr, ok := err.(*RetryError)
	if !ok {
		t.Errorf("Expected RetryError, got %T", err)
	}
	if !errors.Is(retryErr.LastError, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", retryErr.LastError)
	}
}

func TestDo_ExponentialBackoff(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:   3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		Multiplier:   2.0,
	}

	start := time.Now()
	attempts := 0
	_ = Do(context.Background(), func() error {
		attempts++
		return errors.New("connection timeout")
	}, cfg)

	duration := time.Since(start)

	// Expected delays: 0 (initial), 100ms, 200ms, 400ms = 700ms total delay
	// Allow some tolerance for timing
	expectedMin := 600 * time.Millisecond
	expectedMax := 900 * time.Millisecond

	if duration < expectedMin || duration > expectedMax {
		t.Errorf("Duration %v outside expected range %v - %v", duration, expectedMin, expectedMax)
	}
}

func TestDo_MaxDelayCap(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:   10,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     50 * time.Millisecond, // Cap at 50ms
		Multiplier:   4.0,                   // Would exceed cap quickly
	}

	attempts := 0
	start := time.Now()
	_ = Do(context.Background(), func() error {
		attempts++
		return errors.New("connection timeout")
	}, cfg)

	duration := time.Since(start)

	// Even with multiplier 4.0, delays should be capped at 50ms
	// Initial delay 10ms, then capped at 50ms for remaining retries
	// 0 + 10 + 50 + 50 + 50 + 50 + 50 + 50 + 50 + 50 = 460ms max
	// Actually: first attempt has no delay, then delays between retries
	// 10ms + min(40ms, 50ms) = 40ms + min(160ms, 50ms) = 50ms + ...
	// Let's just verify it doesn't take forever
	maxExpected := 700 * time.Millisecond // Allow tolerance

	if duration > maxExpected {
		t.Errorf("Duration %v exceeded max expected %v (max delay not being capped?)", duration, maxExpected)
	}
}

func TestRetryError_Error(t *testing.T) {
	innerErr := errors.New("connection failed")
	retryErr := &RetryError{
		LastError:  innerErr,
		RetryCount: 5,
		Operation:  "write",
	}

	expected := "write failed after 5 retries: connection failed"
	if retryErr.Error() != expected {
		t.Errorf("Error() = %s, expected %s", retryErr.Error(), expected)
	}
}

func TestRetryError_Unwrap(t *testing.T) {
	innerErr := errors.New("connection failed")
	retryErr := &RetryError{
		LastError:  innerErr,
		RetryCount: 5,
		Operation:  "write",
	}

	unwrapped := errors.Unwrap(retryErr)
	if unwrapped != innerErr {
		t.Errorf("Unwrap() = %v, expected %v", unwrapped, innerErr)
	}

	// Test errors.Is
	if !errors.Is(retryErr, innerErr) {
		t.Error("errors.Is should match inner error")
	}
}

func TestDefaultRetryConfig(t *testing.T) {
	cfg := DefaultRetryConfig()

	if cfg.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, expected 5", cfg.MaxRetries)
	}
	if cfg.InitialDelay != 1*time.Second {
		t.Errorf("InitialDelay = %v, expected 1s", cfg.InitialDelay)
	}
	if cfg.MaxDelay != 60*time.Second {
		t.Errorf("MaxDelay = %v, expected 60s", cfg.MaxDelay)
	}
	if cfg.Multiplier != 2.0 {
		t.Errorf("Multiplier = %f, expected 2.0", cfg.Multiplier)
	}
}

func TestGetRetryDelay(t *testing.T) {
	cfg := RetryConfig{
		InitialDelay: 1 * time.Second,
		MaxDelay:     60 * time.Second,
		Multiplier:   2.0,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 60 * time.Second},  // Capped
		{10, 60 * time.Second}, // Still capped
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempt), func(t *testing.T) {
			delay := GetRetryDelay(tt.attempt, cfg)
			if delay != tt.expected {
				t.Errorf("GetRetryDelay(%d) = %v, expected %v", tt.attempt, delay, tt.expected)
			}
		})
	}
}

func TestDoWithResult(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.InitialDelay = 10 * time.Millisecond

	attempts := 0
	result, err := DoWithResult(context.Background(), func() (string, error) {
		attempts++
		if attempts < 2 {
			return "", errors.New("connection timeout")
		}
		return "success", nil
	}, cfg, "get_data")

	if err != nil {
		t.Errorf("DoWithResult() returned error: %v", err)
	}
	if result != "success" {
		t.Errorf("result = %s, expected 'success'", result)
	}
	if attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", attempts)
	}
}

func TestDoWithResult_Failure(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:   2,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2.0,
	}

	result, err := DoWithResult(context.Background(), func() (string, error) {
		return "", errors.New("syntax error") // Non-retryable
	}, cfg, "get_data")

	if err == nil {
		t.Error("DoWithResult() should have returned error")
	}
	if result != "" {
		t.Errorf("result should be empty string on error, got %s", result)
	}
}

func TestDoWithName(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.InitialDelay = 10 * time.Millisecond

	err := DoWithName(context.Background(), func() error {
		return errors.New("syntax error")
	}, cfg, "custom_operation")

	if err == nil {
		t.Error("DoWithName() should have returned error")
	}

	retryErr, ok := err.(*RetryError)
	if !ok {
		t.Errorf("Expected RetryError, got %T", err)
	}
	if retryErr.Operation != "custom_operation" {
		t.Errorf("Operation = %s, expected 'custom_operation'", retryErr.Operation)
	}
}
