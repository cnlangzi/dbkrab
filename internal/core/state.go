package core

import (
	"context"
	"fmt"
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
// It ensures mutual exclusion - only one operation can run at a time.
// State is stored in memory and resets to idle on restart.
type StateManager struct {
	mu       sync.RWMutex
	state    RunState
	cancel   context.CancelFunc
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
func (sm *StateManager) CanStart(want RunState) bool {
	return sm.IsIdle() && want != StateIdle
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

// Start begins a new operation. Returns error if current state is not idle.
// The cancel function will be called when Stop() is invoked.
func (sm *StateManager) Start(what RunState, cancel context.CancelFunc) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.state != StateIdle {
		return fmt.Errorf("cannot start %s: current state is %s", what, sm.state)
	}

	sm.state = what
	sm.cancel = cancel
	sm.metadata = make(map[string]any)

	slog.Info("state transition", "from", StateIdle, "to", what)
	return nil
}

// Stop cancels the current operation and resets state to idle.
// Can be called from any state. Safe to call multiple times.
func (sm *StateManager) Stop() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.cancel != nil {
		sm.cancel()
		sm.cancel = nil
	}

	old := sm.state
	sm.state = StateIdle
	sm.metadata = make(map[string]any)

	if old != StateIdle {
		slog.Info("state transition", "from", old, "to", StateIdle)
	}
}

// SetMetadata updates a metadata key-value pair.
// Used during operation to report progress.
func (sm *StateManager) SetMetadata(key string, value any) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.metadata[key] = value
}