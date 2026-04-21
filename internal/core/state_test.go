package core

import (
	"testing"
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

func TestStateManager_Set(t *testing.T) {
	sm := NewStateManager()

	// Set to polling
	sm.Set(StatePolling)
	if sm.Current() != StatePolling {
		t.Errorf("expected state to be polling, got %s", sm.Current())
	}
	if sm.IsIdle() {
		t.Errorf("expected IsIdle() to be false")
	}

	// Set to replay (take over)
	sm.Set(StateReplay)
	if sm.Current() != StateReplay {
		t.Errorf("expected state to be replay, got %s", sm.Current())
	}

	// Set to idle
	sm.Set(StateIdle)
	if sm.Current() != StateIdle {
		t.Errorf("expected state to be idle, got %s", sm.Current())
	}
	if !sm.IsIdle() {
		t.Errorf("expected IsIdle() to be true")
	}
}

func TestStateManager_SetNoChange(t *testing.T) {
	sm := NewStateManager()

	// Set to same state should do nothing
	sm.Set(StateIdle)
	if sm.Current() != StateIdle {
		t.Errorf("expected state to remain idle")
	}

	sm.Set(StatePolling)
	sm.Set(StatePolling) // No change
	if sm.Current() != StatePolling {
		t.Errorf("expected state to remain polling")
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
	sm.Set(StatePolling)
	if sm.CanStart(StateReplay) {
		t.Errorf("expected CanStart(replay) to be false when polling")
	}
	sm.Set(StateIdle)
}

func TestStateManager_Metadata(t *testing.T) {
	sm := NewStateManager()

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

	// Set to idle clears metadata
	sm.Set(StateIdle)
	metadata = sm.Metadata()
	if len(metadata) != 0 {
		t.Errorf("expected metadata to be empty after idle, got %v", metadata)
	}
}

func TestStateManager_MetadataDuringOperation(t *testing.T) {
	sm := NewStateManager()

	// Simulate replay operation
	sm.Set(StateReplay)

	sm.SetMetadata("processed", 50)
	sm.SetMetadata("total", 100)

	metadata := sm.Metadata()
	if metadata["processed"] != 50 {
		t.Errorf("expected processed=50, got %v", metadata["processed"])
	}

	// After operation completes, set to polling
	sm.Set(StatePolling)

	// Metadata should NOT be cleared (only idle clears it)
	metadata = sm.Metadata()
	if metadata["processed"] != 50 {
		t.Errorf("metadata should persist during polling state, got %v", metadata["processed"])
	}
}