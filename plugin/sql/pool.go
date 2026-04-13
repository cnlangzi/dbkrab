package sql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cnlangzi/sqlite"
)

// Pool wraps a cnlangzi/sqlite.DB for a business sink database.
// cnlangzi/sqlite provides read/write separation, buffered batch writes,
// and automatic WAL checkpoint management.
type Pool struct {
	name string    // sink name, used for directory path
	path string    // resolved base path (e.g., ./data/sinks/business)
	db   *sqlite.DB
	mu   sync.RWMutex
	closed atomic.Bool
}

// NewPool creates a new SQLite connection pool for a sink.
// Path normalization: ./data/sinks/business.db -> ./data/sinks/business
// The actual DB file will be at {path}/{name}.db
func NewPool(ctx context.Context, name, sqlitePath string) (*Pool, error) {
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

	// Open database using cnlangzi/sqlite
	db, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	pool := &Pool{
		name: name,
		path: basePath,
		db:   db,
	}

	return pool, nil
}

// DB returns the underlying sqlite.DB for direct access if needed
func (p *Pool) DB() *sqlite.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.db
}

// Write returns the sqlite.DB for write operations (buffered batch writes)
func (p *Pool) Write() *sqlite.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.db
}

// WriteTx starts a buffered transaction for atomic writes.
// The Tx buffers all statements until Commit is called.
func (p *Pool) WriteTx(ctx context.Context) (*sqlite.Tx, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.db.Writer.BeginTx(ctx, nil)
}

// Read returns the sqlite.DB for read operations (direct reads)
func (p *Pool) Read() *sqlite.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.db
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

// Close closes the database connection
func (p *Pool) Close() error {
	if p.closed.Swap(true) {
		return nil // already closed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Close()
}

// PoolManager manages multiple sink pools
type PoolManager struct {
	pools     map[string]*Pool
	mu        sync.RWMutex
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
func (m *PoolManager) GetPool(ctx context.Context, skillName string, sqlitePath string) (*Pool, error) {
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

	pool, err := NewPool(ctx, skillName, sqlitePath)
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
