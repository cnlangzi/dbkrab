package offset

import (
	"errors"
	"time"
)

// ErrStoreClosed is returned when operating on a closed store
var ErrStoreClosed = errors.New("offset store is closed")

// Offset stores the LSN position for each table
// last_lsn: last LSN from fetched data
// next_lsn: incrementLSN(last_lsn) - pre-computed next start point (cached, not for comparison)
// max_lsn: GetMaxLSN() at save time - used to determine if new data exists
type Offset struct {
	LastLSN   string    `json:"last_lsn"`   // Last LSN from fetched data
	NextLSN   string    `json:"next_lsn"`   // incrementLSN(last_lsn) - cached next start point
	MaxLSN    string    `json:"max_lsn"`    // GetMaxLSN() at save time
	UpdatedAt time.Time `json:"updated_at"`
}

// StoreInterface defines the interface for offset storage
type StoreInterface interface {
	Load() error
	Save() error
	Get(table string) (Offset, error)
	Set(table string, lastLSN string, nextLSN string, maxLSN string) error
	GetAll() (map[string]Offset, error)
}