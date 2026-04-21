package core

import (
	"log/slog"
	"sync"
)

// RunState represents the current running state of the system.
type RunState string

const (
	// StateIdle means no operation is running.
	StateIdle RunState = "idle"
	// StatePolling means CDC Poller is running.
	StatePolling RunState = "polling"
	// StateReplay means Replay service is running.
	StateReplay RunState = "replay"
	// StateSnapshot means Snapshot service is running (reserved for future).
	StateSnapshot RunState = "snapshot"
)

// StateManager coordinates the running state of Poller, Replay, and Snapshot.
// It acts as a simple state marker - operations check state and pause themselves.
// State is stored in memory and resets to idle on restart.
type StateManager struct {
	mu       sync.RWMutex
	state    RunState
	metadata map[string]any
}

// NewStateManager creates a new StateManager with initial state idle.
func NewStateManager() *StateManager {
	return &StateManager{
		state:    StateIdle,
		metadata: make(map[string]any),
	}
}

// Current returns the current running state.
func (sm *StateManager) Current() RunState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.state
}

// IsIdle checks if the system is in idle state.
func (sm *StateManager) IsIdle() bool {
	return sm.Current() == StateIdle
}

// CanStart checks if a new operation can be started.
// Returns true only if current state is idle and requested state is not idle.
// Note: For operations that want to "take over", use Set() directly.
func (sm *StateManager) CanStart(want RunState) bool {
	return sm.IsIdle() && want != StateIdle
}

// Set directly sets the state without checking.
// Used by operations that want to "take over" (e.g., Replay when Poller is running).
// Other operations will detect the state change and pause themselves.
// StateIdle always clears metadata. Non-idle state transitions also clear metadata.
func (sm *StateManager) Set(state RunState) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	old := sm.state

	// StateIdle always clears metadata (even if already idle)
	if state == StateIdle {
		sm.metadata = make(map[string]any)
	} else if old != state {
		// Non-idle state transition also clears metadata to prevent stale data
		sm.metadata = make(map[string]any)
	}

	if old == state {
		return // No state change
	}

	sm.state = state
	slog.Info("state transition", "from", old, "to", state)
}

// Metadata returns a copy of the current running metadata.
// Used for progress tracking (processed count, total count, etc).
func (sm *StateManager) Metadata() map[string]any {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make(map[string]any, len(sm.metadata))
	for k, v := range sm.metadata {
		result[k] = v
	}
	return result
}

// SetMetadata updates a metadata key-value pair.
// Used during operation to report progress.
func (sm *StateManager) SetMetadata(key string, value any) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.metadata[key] = value
}