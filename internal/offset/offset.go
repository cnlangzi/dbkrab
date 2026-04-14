package offset

import (
	"errors"
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