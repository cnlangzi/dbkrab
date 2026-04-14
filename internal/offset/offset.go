package offset

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ErrStoreClosed is returned when operating on a closed store
var ErrStoreClosed = errors.New("offset store is closed")

// Offset stores the LSN position for each table
type Offset struct {
	LSN        string    `json:"lsn"`
	HasNewData bool      `json:"has_new_data"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// StoreInterface defines the interface for offset storage
type StoreInterface interface {
	Load() error
	Save() error
	Get(table string) (Offset, error)
	Set(table string, lsn string, hasNewData bool) error
	GetAll() (map[string]Offset, error)
}

// Store manages LSN offsets for all tables (JSON file backend)
type Store struct {
	path    string
	offsets map[string]Offset
	mu      sync.RWMutex
}

// Ensure Store implements StoreInterface
var _ StoreInterface = (*Store)(nil)

// NewStore creates a new JSON file offset store
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

	return s.saveWithoutLock()
}

// saveWithoutLock writes offsets to file without acquiring lock (caller must hold lock)
func (s *Store) saveWithoutLock() error {
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
func (s *Store) Get(table string) (Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offset, ok := s.offsets[table]
	if !ok {
		return Offset{}, nil
	}
	return offset, nil
}

// Set updates the LSN for a table
func (s *Store) Set(table string, lsn string, hasNewData bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.offsets[table] = Offset{
		LSN:        lsn,
		HasNewData: hasNewData,
		UpdatedAt:  time.Now(),
	}

	// Auto-save (caller already holds lock, use saveWithoutLock)
	return s.saveWithoutLock()
}

// GetAll returns all offsets
func (s *Store) GetAll() (map[string]Offset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]Offset)
	for k, v := range s.offsets {
		result[k] = v
	}
	return result, nil
}