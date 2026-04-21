package core

import (
	"context"
	"testing"
	"time"
)

func TestStateManager_New(t *testing.T) {
	sm := NewStateManager()
	if sm.Current() != StateIdle {
		t.Errorf("expected initial state to be idle, got %s", sm.Current())
	}
	if !sm.IsIdle() {
		t.Errorf("expected IsIdle() to be true")
	}
}

func TestStateManager_StartStop(t *testing.T) {
	sm := NewStateManager()

	// Test successful start
	ctx, cancel := context.WithCancel(context.Background())
	err := sm.Start(StatePolling, cancel)
	if err != nil {
		t.Errorf("unexpected error starting polling: %v", err)
	}
	if sm.Current() != StatePolling {
		t.Errorf("expected state to be polling, got %s", sm.Current())
	}
	if sm.IsIdle() {
		t.Errorf("expected IsIdle() to be false")
	}

	// Test cannot start when not idle
	err = sm.Start(StateReplay, nil)
	if err == nil {
		t.Errorf("expected error when starting replay while polling")
	}

	// Test stop
	sm.Stop()
	if sm.Current() != StateIdle {
		t.Errorf("expected state to be idle after stop, got %s", sm.Current())
	}
	if !sm.IsIdle() {
		t.Errorf("expected IsIdle() to be true after stop")
	}

	// Verify context was cancelled
	select {
	case <-ctx.Done():
		// Good - context was cancelled
	case <-time.After(100 * time.Millisecond):
		t.Errorf("expected context to be cancelled after Stop()")
	}
}

func TestStateManager_CanStart(t *testing.T) {
	sm := NewStateManager()

	// Can start when idle
	if !sm.CanStart(StatePolling) {
		t.Errorf("expected CanStart(polling) to be true when idle")
	}
	if !sm.CanStart(StateReplay) {
		t.Errorf("expected CanStart(replay) to be true when idle")
	}

	// Cannot start idle (invalid request)
	if sm.CanStart(StateIdle) {
		t.Errorf("expected CanStart(idle) to be false")
	}

	// Cannot start when not idle
	sm.Start(StatePolling, nil)
	if sm.CanStart(StateReplay) {
		t.Errorf("expected CanStart(replay) to be false when polling")
	}
	sm.Stop()
}

func TestStateManager_Metadata(t *testing.T) {
	sm := NewStateManager()

	// Start operation
	sm.Start(StateReplay, nil)

	// Set metadata
	sm.SetMetadata("processed", 10)
	sm.SetMetadata("total", 100)

	// Get metadata
	metadata := sm.Metadata()
	if metadata["processed"] != 10 {
		t.Errorf("expected processed=10, got %v", metadata["processed"])
	}
	if metadata["total"] != 100 {
		t.Errorf("expected total=100, got %v", metadata["total"])
	}

	// Metadata is a copy - modifying it doesn't affect internal state
	metadata["processed"] = 999
	if sm.Metadata()["processed"] != 10 {
		t.Errorf("internal metadata should not be affected by external modification")
	}

	// Stop clears metadata
	sm.Stop()
	metadata = sm.Metadata()
	if len(metadata) != 0 {
		t.Errorf("expected metadata to be empty after stop, got %v", metadata)
	}
}

func TestStateManager_StopMultipleTimes(t *testing.T) {
	sm := NewStateManager()

	sm.Start(StatePolling, nil)

	// Stop multiple times should be safe
	sm.Stop()
	sm.Stop()
	sm.Stop()

	if sm.Current() != StateIdle {
		t.Errorf("expected state to remain idle after multiple stops")
	}
}

func TestStateManager_StopWhenIdle(t *testing.T) {
	sm := NewStateManager()

	// Stop when idle should be safe
	sm.Stop()

	if sm.Current() != StateIdle {
		t.Errorf("expected state to remain idle")
	}
}