package offset

import (
	"errors"
	"time"
)

// ErrStoreClosed is returned when operating on a closed store
var ErrStoreClosed = errors.New("offset store is closed")

// Offset stores the LSN position for each table
// last_lsn: last LSN from fetched data
// next_lsn: incrementLSN(last_lsn) - next start point
//
// Comparison with globalMaxLSN (fetched at poll start):
//   - next_lsn < globalMaxLSN → new data available, use next_lsn
//   - next_lsn >= globalMaxLSN → no new data
type Offset struct {
	LastLSN   string    `json:"last_lsn"`   // Last LSN from fetched data
	NextLSN   string    `json:"next_lsn"`   // incrementLSN(last_lsn) - next start point
	UpdatedAt time.Time `json:"updated_at"`
}

// StoreInterface defines the interface for offset storage
type StoreInterface interface {
	Load() error
	Save() error
	Get(table string) (Offset, error)
	Set(table string, lastLSN string, nextLSN string) error
	GetAll() (map[string]Offset, error)
}