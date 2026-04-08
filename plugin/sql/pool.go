package sql

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite"
)

// Pool manages SQLite connections for a business sink database.
// It provides read/write separation: writes go through a single connection
// to avoid SQLITE_BUSY errors, while reads use a pool for concurrent access.
type Pool struct {
	name string // sink name, used for directory path
	path string // resolved base path (e.g., ./data/sinks/business)

	writeDB *sql.DB // single write connection
	readDB  *sql.DB // read connection pool (WAL concurrent reads)
	mu      sync.RWMutex
	closed  atomic.Bool
}

// NewPool creates a new SQLite connection pool for a sink.
// Path normalization: ./data/sinks/business.db -> ./data/sinks/business
// The actual DB file will be at {path}/{name}.db
func NewPool(name, sqlitePath string) (*Pool, error) {
	// Normalize path: remove .db extension if present to get directory
	basePath := sqlitePath
	if strings.HasSuffix(strings.ToLower(sqlitePath), ".db") {
		basePath = strings.TrimSuffix(sqlitePath, ".db")
	}

	// Resolve to data/sinks/{name} if relative path without prefix
	if !filepath.IsAbs(basePath) && !strings.HasPrefix(basePath, "./") && !strings.HasPrefix(basePath, "../") {
		basePath = filepath.Join("./data/sinks", name)
	}

	// Create directory if not exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Create migrations directory
	migrationsDir := filepath.Join(basePath, "migrations")
	if err := os.MkdirAll(migrationsDir, 0755); err != nil {
		return nil, fmt.Errorf("create migrations directory: %w", err)
	}

	dbPath := filepath.Join(basePath, name+".db")

	pool := &Pool{
		name: name,
		path: basePath,
	}

	// Initialize write connection with single connection to avoid SQLITE_BUSY
	writeDSN := buildDSN(dbPath, true)
	writeDB, err := sql.Open("sqlite", writeDSN)
	if err != nil {
		return nil, fmt.Errorf("open write database: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	pool.writeDB = writeDB

	// Initialize read connection with WAL for concurrent reads
	readDSN := buildDSN(dbPath, false)
	readDB, err := sql.Open("sqlite", readDSN)
	if err != nil {
		writeDB.Close()
		return nil, fmt.Errorf("open read database: %w", err)
	}
	// Read pool can have multiple connections for concurrent reads
	readDB.SetMaxOpenConns(10)
	readDB.SetMaxIdleConns(5)
	pool.readDB = readDB

	return pool, nil
}

// buildDSN builds SQLite DSN with appropriate settings
func buildDSN(dbPath string, write bool) string {
	params := url.Values{}
	params.Add("_journal_mode", "WAL")
	params.Add("_synchronous", "NORMAL")
	params.Add("_busy_timeout", "5000")
	params.Add("_pragma", "temp_store(MEMORY)")
	params.Add("_pragma", "cache_size(-100000)")
	params.Add("_pragma", "mmap_size(1000000000)")
	params.Add("cache", "shared")

	// For write connection, disable WAL checkpointing to avoid busy errors
	if write {
		params.Add("_pragma", "wal_checkpoint(TRUNCATE)")
	}

	return dbPath + "?" + params.Encode()
}

// Write returns the write database connection (single-threaded)
func (p *Pool) Write() *sql.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.writeDB
}

// Read returns the read database connection (concurrent reads)
func (p *Pool) Read() *sql.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.readDB
}

// Path returns the base path for this sink
func (p *Pool) Path() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.path
}

// Name returns the sink name
func (p *Pool) Name() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.name
}

// MigrationsPath returns the migrations directory path
func (p *Pool) MigrationsPath() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return filepath.Join(p.path, "migrations")
}

// Close closes all database connections
func (p *Pool) Close() error {
	// Use atomic swap to avoid deadlock with Read/Write using RLock
	if p.closed.Swap(true) {
		return nil // already closed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	if p.writeDB != nil {
		if err := p.writeDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close write db: %w", err))
		}
	}
	if p.readDB != nil {
		if err := p.readDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close read db: %w", err))
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// PoolManager manages multiple sink pools
type PoolManager struct {
	pools    map[string]*Pool
	mu       sync.RWMutex
	skillsDir string
}

// NewPoolManager creates a new pool manager
func NewPoolManager(skillsDir string) *PoolManager {
	return &PoolManager{
		pools:     make(map[string]*Pool),
		skillsDir: skillsDir,
	}
}

// GetPool returns an existing pool or creates a new one for the given skill
func (m *PoolManager) GetPool(skillName string, sqlitePath string) (*Pool, error) {
	// Normalize path to get consistent key
	key := normalizePoolKey(skillName, sqlitePath)

	m.mu.RLock()
	if pool, ok := m.pools[key]; ok {
		m.mu.RUnlock()
		return pool, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if pool, ok := m.pools[key]; ok {
		return pool, nil
	}

	pool, err := NewPool(skillName, sqlitePath)
	if err != nil {
		return nil, err
	}

	m.pools[key] = pool
	return pool, nil
}

// normalizePoolKey creates a consistent key for pool lookup
func normalizePoolKey(name, sqlitePath string) string {
	// If sqlitePath is provided, use it as part of the key
	if sqlitePath != "" {
		basePath := sqlitePath
		if strings.HasSuffix(strings.ToLower(sqlitePath), ".db") {
			basePath = strings.TrimSuffix(sqlitePath, ".db")
		}
		return fmt.Sprintf("%s:%s", name, basePath)
	}
	// Default: use skill name only
	return name
}

// Close closes all pools
func (m *PoolManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, pool := range m.pools {
		if err := pool.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// ListPools returns all managed pool names
func (m *PoolManager) ListPools() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.pools))
	for name := range m.pools {
		names = append(names, name)
	}
	return names
}
