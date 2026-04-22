package snapshot

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/offset"
	"github.com/cnlangzi/dbkrab/plugin"
)

// SnapshotProgress holds the current state of a snapshot operation.
type SnapshotProgress struct {
	State        string `json:"state"`         // "idle", "running", "completed", "failed"
	Tables       int    `json:"tables"`        // Total table count
	Processed     int    `json:"processed"`    // Completed tables count
	CurrentTable  string `json:"current_table"` // Currently processing table name
	CurrentRows   int    `json:"current_rows"`  // Rows processed in current table
	CurrentTotal  int    `json:"current_total"` // Total rows in current table
	Error         string `json:"error"`         // Error message if failed
	StartedAt     string `json:"started_at"`    // Start time
	CompletedAt   string `json:"completed_at"`  // Completion time
}

// SnapshotService manages snapshot operations with singleton behavior.
type SnapshotService struct {
	mu           sync.RWMutex
	running      bool
	stateManager *core.StateManager
	querier      *Querier
	offsetStore  offset.StoreInterface
	pluginMgr    *plugin.Manager
	db           *sql.DB
	progress     SnapshotProgress
	cancelFunc   context.CancelFunc
}

// NewSnapshotService creates a new SnapshotService.
func NewSnapshotService(
	stateManager *core.StateManager,
	db *sql.DB,
	timezone *time.Location,
	offsetStore offset.StoreInterface,
	pluginMgr *plugin.Manager,
) *SnapshotService {
	querier := NewQuerier(db, timezone, nil)
	return &SnapshotService{
		stateManager: stateManager,
		querier:      querier,
		offsetStore:  offsetStore,
		pluginMgr:    pluginMgr,
		db:           db,
		progress:     SnapshotProgress{State: "idle"},
	}
}

// GetProgress returns the current snapshot progress.
func (s *SnapshotService) GetProgress() SnapshotProgress {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.progress
}

// Start starts a snapshot for all CDC-enabled tables.
// Returns error if snapshot is already running.
func (s *SnapshotService) Start(ctx context.Context, tables []CDCTable) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("snapshot already running")
	}
	s.running = true
	s.mu.Unlock()

	// Set state to snapshot (poller will detect and skip)
	s.stateManager.Set(core.StateSnapshot)

	// Initialize progress
	s.mu.Lock()
	s.progress = SnapshotProgress{
		State:    "running",
		Tables:   len(tables),
		Processed: 0,
		StartedAt: time.Now().Format(time.RFC3339),
	}
	s.mu.Unlock()

	// Create cancellable context detached from HTTP/2 request context
	_, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancelFunc = cancel
	s.mu.Unlock()

	// Run snapshot in background
	go s.runSnapshot(context.Background(), tables)

	return nil
}

// runSnapshot executes the snapshot operation in a goroutine.
func (s *SnapshotService) runSnapshot(ctx context.Context, tables []CDCTable) {
	defer func() {
		s.mu.Lock()
		s.running = false
		s.cancelFunc = nil
		s.mu.Unlock()
		s.stateManager.Set(core.StateIdle)
	}()

	slog.Info("snapshot: starting", "table_count", len(tables))

	// Step 1: Clear all sink tables
	if err := s.clearSinkTables(ctx, tables); err != nil {
		s.mu.Lock()
		s.progress.State = "failed"
		s.progress.Error = fmt.Sprintf("clear sink tables: %v", err)
		s.mu.Unlock()
		slog.Error("snapshot: failed to clear sink tables", "error", err)
		return
	}

	// Step 2: Run snapshot for each table
	for i, table := range tables {
		// Check for cancellation
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.progress.State = "failed"
			s.progress.Error = "cancelled"
			s.mu.Unlock()
			return
		default:
		}

		s.mu.Lock()
		s.progress.CurrentTable = fmt.Sprintf("%s.%s", table.Schema, table.Name)
		s.mu.Unlock()

		slog.Info("snapshot: processing table", "table", s.progress.CurrentTable, "progress", i+1, "/", len(tables))

		// Create handler for this table
		handler := NewSnapshotHandler(s.pluginMgr, table.Name)

		// Run snapshot for this table
		startLSN, err := s.querier.Run(ctx, table.Schema, table.Name, handler)
		if err != nil {
			s.mu.Lock()
			s.progress.State = "failed"
			s.progress.Error = fmt.Sprintf("table %s: %v", s.progress.CurrentTable, err)
			s.mu.Unlock()
			slog.Error("snapshot: table failed", "table", s.progress.CurrentTable, "error", err)
			return
		}

		// Calculate next LSN for CDC to resume from
		nextLSN, err := s.querier.IncrementLSN(ctx, startLSN)
		if err != nil {
			s.mu.Lock()
			s.progress.State = "failed"
			s.progress.Error = fmt.Sprintf("increment LSN for %s: %v", s.progress.CurrentTable, err)
			s.mu.Unlock()
			return
		}

		// Update offset store
		fullTableName := fmt.Sprintf("%s.%s", table.Schema, table.Name)
		startLSNStr := hex.EncodeToString(startLSN)
		nextLSNStr := hex.EncodeToString(nextLSN)

		if err := s.offsetStore.Set(fullTableName, startLSNStr, nextLSNStr); err != nil {
			s.mu.Lock()
			s.progress.State = "failed"
			s.progress.Error = fmt.Sprintf("set offset for %s: %v", fullTableName, err)
			s.mu.Unlock()
			return
		}

		if err := s.offsetStore.Flush(); err != nil {
			s.mu.Lock()
			s.progress.State = "failed"
			s.progress.Error = fmt.Sprintf("flush offset: %v", err)
			s.mu.Unlock()
			return
		}

		// Update progress
		s.mu.Lock()
		s.progress.Processed = i + 1
		s.progress.CurrentRows = 0
		s.progress.CurrentTotal = 0
		s.mu.Unlock()

		slog.Info("snapshot: table completed", "table", fullTableName, "start_lsn", startLSNStr, "next_lsn", nextLSNStr)
	}

	// Mark as completed
	s.mu.Lock()
	s.progress.State = "completed"
	s.progress.CompletedAt = time.Now().Format(time.RFC3339)
	s.mu.Unlock()

	slog.Info("snapshot: all tables completed")
}

// clearSinkTables truncates all sink tables for the given CDC tables.
func (s *SnapshotService) clearSinkTables(ctx context.Context, tables []CDCTable) error {
	slog.Info("snapshot: clearing sink tables")

	if s.pluginMgr == nil {
		return fmt.Errorf("snapshot plugin manager not initialized")
	}

	// Get all sink names from plugin manager (which exposes sinker operations)
	sinkNames := s.pluginMgr.ListDatabases()
	if len(sinkNames) == 0 {
		slog.Warn("snapshot: no sinks configured, skipping clear")
		return nil
	}

	// For each sink, truncate all tables
	for _, sinkName := range sinkNames {
		sink, err := s.pluginMgr.GetSinker(sinkName)
		if err != nil {
			return fmt.Errorf("snapshot: failed to get sinker %s: %w", sinkName, err)
		}

		// Get tables in this sink
		sinkTables, err := s.pluginMgr.QueryTables(sinkName)
		if err != nil {
			return fmt.Errorf("snapshot: failed to query tables for sink %s: %w", sinkName, err)
		}

		// Truncate each table
		for _, tableName := range sinkTables {
			query := fmt.Sprintf("DELETE FROM %s", tableName)
			if err := sink.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("snapshot: failed to clear table %s in sink %s: %w", tableName, sinkName, err)
			}
			slog.Info("snapshot: cleared table", "sink", sinkName, "table", tableName)
		}
	}

	return nil
}

// SnapshotHandler handles snapshot batches by writing to sink through plugin manager.
type SnapshotHandler struct {
	pluginMgr *plugin.Manager
	tableName string
}

// NewSnapshotHandler creates a new SnapshotHandler.
func NewSnapshotHandler(pluginMgr *plugin.Manager, tableName string) *SnapshotHandler {
	return &SnapshotHandler{
		pluginMgr: pluginMgr,
		tableName: tableName,
	}
}

// HandleBatch processes a batch of changes from snapshot through the plugin pipeline.
// This routes changes through skills so that sink metadata (e.g., PrimaryKey) is properly set.
func (h *SnapshotHandler) HandleBatch(ctx context.Context, changes []core.Change) error {
	if len(changes) == 0 {
		return nil
	}

	if h.pluginMgr == nil {
		return fmt.Errorf("snapshot plugin manager not initialized")
	}

	// Create batch context for observability
	batchCtx := core.NewBatchContext()

	// Route changes through plugin.Manager.Handle which processes them through skills
	// and then writes to sinks with proper SinkConfig (including PrimaryKey)
	if err := h.pluginMgr.Handle(ctx, changes, batchCtx); err != nil {
		return fmt.Errorf("plugin handle: %w", err)
	}

	return nil
}

// CDCTable represents a CDC-enabled table.
type CDCTable struct {
	Schema string
	Name   string
}

// GetCDCTables returns the list of CDC-enabled tables from config.
func GetCDCTables(cfg *config.Config) []CDCTable {
	tables := make([]CDCTable, 0, len(cfg.Tables))
	for i, t := range cfg.Tables {
		parts := strings.SplitN(t, ".", 2)
		if len(parts) != 2 {
			slog.Warn("snapshot: skipped malformed table entry", "index", i, "entry", t)
			continue
		}
		tables = append(tables, CDCTable{
			Schema: parts[0],
			Name:   parts[1],
		})
	}
	return tables
}