package offset

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Offset stores the LSN position for each table
type Offset struct {
	LSN       string    `json:"lsn"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Store manages LSN offsets for all tables
type Store struct {
	path    string
	offsets map[string]Offset
	mu      sync.RWMutex
}

// NewStore creates a new offset store
func NewStore(path string) *Store {
	return &Store{
		path:    path,
		offsets: make(map[string]Offset),
	}
}

// Load reads offsets from file
func (s *Store) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No existing offsets
		}
		return err
	}

	return json.Unmarshal(data, &s.offsets)
}

// Save writes offsets to file
func (s *Store) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(s.offsets, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.path, data, 0644)
}

// Get returns the LSN for a table
func (s *Store) Get(table string) (Offset, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offset, ok := s.offsets[table]
	return offset, ok
}

// Set updates the LSN for a table
func (s *Store) Set(table string, lsn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.offsets[table] = Offset{
		LSN:       lsn,
		UpdatedAt: time.Now(),
	}

	// Auto-save
	return s.Save()
}

// GetAll returns all offsets
func (s *Store) GetAll() map[string]Offset {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]Offset)
	for k, v := range s.offsets {
		result[k] = v
	}
	return result
}