package wasm

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// Instance represents a loaded WASM plugin instance
type Instance struct {
	path string
	mu   sync.Mutex

	// For now, we use a simple function-based approach
	// In production, this would use wasmtime or wasmer
	handler func(*core.Transaction) error
}

// NewInstance loads a WASM plugin from file
func NewInstance(path string) (*Instance, error) {
	// TODO: Implement actual WASM loading with wasmtime/wasmer
	// For now, we return a stub that can be extended

	// Check if file exists (will be replaced with actual WASM loading)
	// data, err := os.ReadFile(path)
	// if err != nil {
	//     return nil, fmt.Errorf("read wasm file: %w", err)
	// }

	return &Instance{
		path: path,
	}, nil
}

// Init calls the plugin's Init function
func (w *Instance) Init(config string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: Call WASM Init function
	// For now, we just store the config
	_ = config

	return nil
}

// Handle calls the plugin's Handle function with a transaction
func (w *Instance) Handle(tx *core.Transaction) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: Call WASM Handle function
	// For now, if a custom handler is set, use it
	if w.handler != nil {
		return w.handler(tx)
	}

	// Default: just log the transaction
	txJSON, _ := json.Marshal(tx)
	fmt.Printf("[WASM Plugin %s] Transaction: %s\n", w.path, string(txJSON))

	return nil
}

// Close calls the plugin's Close function
func (w *Instance) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: Call WASM Close function
	// Cleanup resources

	return nil
}

// SetHandler sets a custom Go handler (for native plugins)
// This allows mixing native Go plugins with WASM plugins
func (w *Instance) SetHandler(h func(*core.Transaction) error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handler = h
}

// Path returns the plugin file path
func (w *Instance) Path() string {
	return w.path
}
