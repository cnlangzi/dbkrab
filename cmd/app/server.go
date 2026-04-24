package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/cdc"
	"github.com/cnlangzi/dbkrab/internal/cdcadmin"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/internal/monitor"
	"github.com/cnlangzi/dbkrab/internal/offset"
	"github.com/cnlangzi/dbkrab/internal/replay"
	"github.com/cnlangzi/dbkrab/internal/sinker"
	"github.com/cnlangzi/dbkrab/internal/snapshot"
	"github.com/cnlangzi/dbkrab/internal/store"
	"github.com/cnlangzi/dbkrab/plugin"
	"github.com/yaitoo/xun"
)

//go:embed dashboard
var dashboardFS embed.FS

// getDashboardFS returns a FS with pages/layouts at root for xun
func getDashboardFS() fs.FS {
	sub, err := fs.Sub(dashboardFS, "dashboard")
	if err != nil {
		return dashboardFS
	}
	return sub
}

// PollMetricsProvider interface for accessing CDC poll metrics
type PollMetricsProvider interface {
	GetMetrics() map[string]interface{}
}

// Server provides HTTP API for plugin and DLQ management
type Server struct {
	manager         *plugin.Manager
	dlq             *dlq.DLQ
	cdcAdmin        *cdcadmin.Admin
	store           store.Store
	stateManager    *core.StateManager // Global state coordinator for Poller/Replay/Snapshot
	sinkerManager   *sinker.Manager
	monitorDB       *monitor.DB
	port            int
	app             *xun.App
	mux             *http.ServeMux
	configPath      string
	config          *config.Config
	configWatcher   *config.Watcher
	sinksRoot       *os.Root            // Secure root for sinks directory access
	skillsPath      string              // Path to SQL skills directory from config
	metricsProvider PollMetricsProvider // Provides CDC poll metrics
	runtime         *core.Runtime       // Runtime for capturer management
	poller          interface{}         // CDC poller (deprecated, use Runtime/Capturer)
}

// NewServer creates a new API server with all features
func NewServer(
	manager *plugin.Manager,
	dlqStore *dlq.DLQ,
	cdcAdmin *cdcadmin.Admin,
	store store.Store,
	sinkerMgr *sinker.Manager,
	monitorDB *monitor.DB,
	port int,
	configPath string,
	cfg *config.Config,
	watcher *config.Watcher,
	stateManager *core.StateManager,
	poller interface{},
	offsetStore offset.StoreInterface,
	db *sql.DB,
	runtime *core.Runtime,
) *Server {
	// Get skills path from config, default to ./skills/sql if not configured
	skillsPath := cfg.Plugins.SQL.Path
	if skillsPath == "" {
		skillsPath = "./skills/sql"
	}

	return &Server{
		manager:       manager,
		dlq:           dlqStore,
		cdcAdmin:      cdcAdmin,
		stateManager:  stateManager,
		store:         store,
		sinkerManager: sinkerMgr,
		monitorDB:     monitorDB,
		port:          port,
		configPath:    configPath,
		config:        cfg,
		configWatcher: watcher,
		skillsPath:    skillsPath,
		poller:        poller,
		runtime:       runtime,
	}
}

// Start starts the API server with xun framework
func (s *Server) Start() error {
	// Create a mux to use with xun
	s.mux = http.NewServeMux()

	// Create xun app with our mux and template filesystem
	// Use Sub FS so pages/layouts are at root level for xun
	// Note: xun automatically serves files from "public/" directory
	dashboardSubFS := getDashboardFS()
	s.app = xun.New(
		xun.WithFsys(dashboardSubFS),
		xun.WithMux(s.mux),
		xun.WithHandlerViewers(&xun.JsonViewer{}, &xun.HtmlViewer{}),
		xun.WithBuildAssetURL(func(name string) bool {
			// Enable asset hashing for JS and CSS files
			return strings.HasSuffix(name, ".js") || strings.HasSuffix(name, ".css")
		}),
	)

	// Register routes
	s.registerAPIRoutes()
	s.registerPageRoutes()

	// Initialize sinks root for secure file access
	// os.Root provides safe access to files within a directory tree
	if s.config != nil && len(s.config.Sinks) > 0 {
		var err error
		s.sinksRoot, err = os.OpenRoot(s.config.Sinks.BasePath())
		if err != nil {
			slog.Warn("failed to open sinks root, sinks API will be limited", "error", err, "path", s.config.Sinks.BasePath())
		}
	}

	// Start xun (this finalizes route registration)
	s.app.Start()

	// Create http server using our mux
	srv := &http.Server{
		Addr:         ":" + strconv.Itoa(s.port),
		Handler:      s.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return srv.ListenAndServe()
}

// Stop stops the API server
func (s *Server) Stop() error {
	if s.app != nil {
		s.app.Close()
	}
	return nil
}

// registerAPIRoutes registers API routes with JsonViewer
func (s *Server) registerAPIRoutes() {
	api := s.app.Group("/api")
	api.Get("/plugins", s.handlePlugins, xun.WithViewer(&xun.JsonViewer{}))
	api.Get("/plugins/{name}", s.handlePluginGet, xun.WithViewer(&xun.JsonViewer{}))
	api.Delete("/plugins/{name}", s.handlePluginDelete, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/plugins/{name}/reload", s.handlePluginReload, xun.WithViewer(&xun.JsonViewer{}))

	// Skills management routes
	api.Get("/skills/list", s.handleSkillsList, xun.WithViewer(&xun.JsonViewer{}))
	api.Get("/skills/files", s.handleSkillsFiles, xun.WithViewer(&xun.JsonViewer{}))
	api.Get("/skills/files/html", s.handleSkillsFilesHTML) // HTML fragment for HTMX
	api.Get("/skills/{id}", s.handleSkillGet, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/skills", s.handleSkillCreate, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/skills/{id}/save", s.handleSkillSave, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/skills/{id}/save/html", s.handleSkillSaveHTML) // HTML fragment for htmx
	api.Delete("/skills/{id}", s.handleSkillDelete, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/skills/validate", s.handleSkillValidate, xun.WithViewer(&xun.JsonViewer{}))
	api.Get("/skills/file/{path...}", s.handleSkillFileGet, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/skills/file/{path}/save", s.handleSkillFileSave, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/skills/folder", s.handleFolderCreate, xun.WithViewer(&xun.JsonViewer{}))

	if s.dlq != nil {
		api.Get("/dlq/list", s.handleDLQList, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/dlq/stats", s.handleDLQStats, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/dlq/{id}", s.handleDLQGet, xun.WithViewer(&xun.JsonViewer{}))
		api.Post("/dlq/{id}/replay", s.handleDLQReplay, xun.WithViewer(&xun.JsonViewer{}))
		api.Post("/dlq/{id}/ignore", s.handleDLQIgnore, xun.WithViewer(&xun.JsonViewer{}))
		api.Delete("/dlq/{id}", s.handleDLQDelete, xun.WithViewer(&xun.JsonViewer{}))
	}

	// CDC administration routes
	if s.cdcAdmin != nil {
		api.Get("/cdc/tables", s.handleCDCTables, xun.WithViewer(&xun.JsonViewer{}))
		api.Post("/cdc/config", s.handleCDCConfig, xun.WithViewer(&xun.JsonViewer{}))
	}

	// CDC changes routes
	if s.store != nil {
		api.Get("/cdc/changes", s.handleCDCChanges, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/cdc/status", s.handleCDCStatus, xun.WithViewer(&xun.JsonViewer{}))
		// Replay routes
		api.Post("/replay", s.handleReplay, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/replay/status", s.handleReplayStatus, xun.WithViewer(&xun.JsonViewer{}))
		slog.Info("CDC changes/status routes registered")
	} else {
		slog.Warn("CDC changes/status routes skipped - store is nil")
	}

	// Snapshot routes
	if s.runtime != nil {
		api.Post("/snapshot/start", s.handleSnapshotStart, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/snapshot/status", s.handleSnapshotStatus, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/snapshot/tables", s.handleSnapshotTables, xun.WithViewer(&xun.JsonViewer{}))
		slog.Info("Snapshot routes registered")
	}

	// CDC gap monitoring routes
	if s.cdcAdmin != nil && s.store != nil {
		api.Get("/cdc/gap", s.handleCDCGap, xun.WithViewer(&xun.JsonViewer{}))
		slog.Info("CDC gap monitoring route registered")
	} else {
		slog.Warn("CDC gap monitoring route skipped - cdcAdmin or sink is nil")
	}

	// Sinks management routes
	api.Get("/sinks/list", s.handleSinksList, xun.WithViewer(&xun.JsonViewer{}))
	api.Get("/sinks/{id}/tables", s.handleSinkTables, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/sinks/{id}/query", s.handleSinkQuery, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/sinks/{id}/migrate", s.handleSinkMigrate, xun.WithViewer(&xun.JsonViewer{}))

	api.Get("/health", s.handleHealth, xun.WithViewer(&xun.JsonViewer{}))
	api.Get("/overview", s.handleOverview)

	// Monitor routes
	if s.monitorDB != nil {
		api.Get("/monitor/batches", s.handleMonitorBatches, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/monitor/skills", s.handleMonitorSkills, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/monitor/sinks", s.handleMonitorSinks, xun.WithViewer(&xun.JsonViewer{}))
	}
}

// registerPageRoutes registers page routes
func (s *Server) registerPageRoutes() {
	// Pages are auto-registered by xun from pages/ directory
	// Skills edit page uses JS to load data (same pattern as sinks page)
	// This is simpler and more reliable with xun's auto-routing
}

// handlePlugins handles GET /api/plugins
func (s *Server) handlePlugins(c *xun.Context) error {
	resp := s.manager.HandleAPI("list", nil)
	return c.View(resp)
}

// handlePluginGet handles GET /api/plugins/{name}
func (s *Server) handlePluginGet(c *xun.Context) error {
	name := c.Request.PathValue("name")
	resp := s.manager.HandleAPI("get", map[string]any{"name": name})
	if !resp.Success {
		c.WriteStatus(http.StatusNotFound)
	}
	return c.View(resp)
}

// handlePluginDelete handles DELETE /api/plugins/{name}
func (s *Server) handlePluginDelete(c *xun.Context) error {
	name := c.Request.PathValue("name")
	resp := s.manager.HandleAPI("unload", map[string]any{"name": name})
	return c.View(resp)
}

// handlePluginReload handles POST /api/plugins/{name}/reload
func (s *Server) handlePluginReload(c *xun.Context) error {
	name := c.Request.PathValue("name")
	resp := s.manager.HandleAPI("reload", map[string]any{"name": name})
	return c.View(resp)
}

// handleDLQList handles GET /api/dlq/list
func (s *Server) handleDLQList(c *xun.Context) error {
	if s.dlq == nil {
		return c.View(map[string]any{"success": false, "error": "DLQ not initialized"})
	}

	status := c.Request.URL.Query().Get("status")
	entries, err := s.dlq.List(status)
	if err != nil {
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}

	return c.View(map[string]any{"success": true, "count": len(entries), "entries": entries})
}

// handleDLQStats handles GET /api/dlq/stats
func (s *Server) handleDLQStats(c *xun.Context) error {
	stats, err := s.getDLQStats()
	if err != nil {
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}
	return c.View(map[string]any{"success": true, "stats": stats})
}

// handleDLQGet handles GET /api/dlq/{id}
func (s *Server) handleDLQGet(c *xun.Context) error {
	if s.dlq == nil {
		return c.View(map[string]any{"success": false, "error": "DLQ not initialized"})
	}

	id, err := strconv.ParseInt(c.Request.PathValue("id"), 10, 64)
	if err != nil {
		c.WriteStatus(http.StatusBadRequest)
		return c.View(map[string]any{"success": false, "error": "invalid entry ID"})
	}

	entry, err := s.dlq.Get(id)
	if err != nil {
		c.WriteStatus(http.StatusNotFound)
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}

	return c.View(map[string]any{"success": true, "entry": entry})
}

// handleDLQReplay handles POST /api/dlq/{id}/replay
func (s *Server) handleDLQReplay(c *xun.Context) error {
	if s.dlq == nil {
		return c.View(map[string]any{"success": false, "error": "DLQ not initialized"})
	}

	id, err := strconv.ParseInt(c.Request.PathValue("id"), 10, 64)
	if err != nil {
		c.WriteStatus(http.StatusBadRequest)
		return c.View(map[string]any{"success": false, "error": "invalid entry ID"})
	}

	err = s.dlq.Replay(context.Background(), id, func(*dlq.DLQEntry) error { return nil })
	if err != nil {
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}

	return c.View(map[string]any{"success": true, "message": "Entry replayed"})
}

// handleDLQDelete handles DELETE /api/dlq/{id}
func (s *Server) handleDLQDelete(c *xun.Context) error {
	if s.dlq == nil {
		return c.View(map[string]any{"success": false, "error": "DLQ not initialized"})
	}

	id, err := strconv.ParseInt(c.Request.PathValue("id"), 10, 64)
	if err != nil {
		c.WriteStatus(http.StatusBadRequest)
		return c.View(map[string]any{"success": false, "error": "invalid entry ID"})
	}

	err = s.dlq.Delete(id)
	if err != nil {
		c.WriteStatus(http.StatusNotFound)
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}

	return c.View(map[string]any{"success": true, "message": "Entry deleted"})
}

// handleHealth handles GET /health
func (s *Server) handleHealth(c *xun.Context) error {
	c.WriteHeader("Content-Type", "text/plain")
	c.WriteStatus(http.StatusOK)
	return c.View(map[string]any{"status": "OK"})
}

// getDLQStats returns formatted DLQ stats
func (s *Server) getDLQStats() (map[string]int, error) {
	if s.dlq == nil {
		return map[string]int{"pending": 0, "resolved": 0, "ignored": 0}, nil
	}

	rawStats, err := s.dlq.Stats()
	if err != nil {
		return nil, err
	}

	stats := map[string]int{"pending": 0, "resolved": 0, "ignored": 0}
	for status, count := range rawStats {
		stats[string(status)] = count
	}
	return stats, nil
}

// handleDLQIgnore handles POST /api/dlq/:id/ignore
func (s *Server) handleDLQIgnore(c *xun.Context) error {
	if s.dlq == nil {
		return c.View(map[string]any{"success": false, "error": "DLQ not initialized"})
	}

	id, err := strconv.ParseInt(c.Request.PathValue("id"), 10, 64)
	if err != nil {
		c.WriteStatus(http.StatusBadRequest)
		return c.View(map[string]any{"success": false, "error": "invalid entry ID"})
	}

	// Read note from form data
	if err := c.Request.ParseForm(); err != nil {
		return c.View(map[string]any{"success": false, "error": "failed to parse form"})
	}
	note := c.Request.FormValue("note")

	err = s.dlq.Ignore(id, note)
	if err != nil {
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}

	return c.View(map[string]any{"success": true, "message": "Entry ignored"})
}

// handleCDCTables handles GET /api/cdc/tables
func (s *Server) handleCDCTables(c *xun.Context) error {
	if s.cdcAdmin == nil {
		return c.View(map[string]any{"success": false, "error": "CDC admin not initialized"})
	}

	// Get tracked tables from config
	var trackedTables []string
	if s.configWatcher != nil {
		cfg := s.configWatcher.Get()
		trackedTables = cfg.Tables
	}

	tables, err := s.cdcAdmin.ListTables(trackedTables)
	if err != nil {
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}

	return c.View(map[string]any{"success": true, "count": len(tables), "tables": tables})
}

// handleCDCConfig handles POST /api/cdc/config
func (s *Server) handleCDCConfig(c *xun.Context) error {
	if s.cdcAdmin == nil {
		return c.View(map[string]any{"success": false, "error": "CDC admin not initialized"})
	}

	// Parse request body
	var req struct {
		Tables []string `json:"tables"`
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return c.View(map[string]any{"success": false, "error": "failed to read request body"})
	}
	defer func() { _ = c.Request.Body.Close() }()

	if err := json.Unmarshal(body, &req); err != nil {
		return c.View(map[string]any{"success": false, "error": "invalid request body"})
	}

	var enabled []string
	var skipped []string
	var errors []string

	// Enable CDC for tables that are checked but CDC not yet enabled in database
	for _, table := range req.Tables {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) != 2 {
			errors = append(errors, fmt.Sprintf("invalid table format: %s (expected schema.table)", table))
			continue
		}
		schema, name := parts[0], parts[1]

		cdcEnabled, err := s.cdcAdmin.GetCDCStatus(schema, name)
		if err != nil {
			errors = append(errors, fmt.Sprintf("check CDC status for %s: %v", table, err))
			continue
		}

		if !cdcEnabled {
			// Enable CDC in database
			if err := s.cdcAdmin.EnableCDC(schema, name); err != nil {
				errors = append(errors, fmt.Sprintf("enable CDC for %s: %v", table, err))
				continue
			}
			enabled = append(enabled, table)
		} else {
			skipped = append(skipped, table)
		}
	}

	// Save tables to config file
	// The config watcher will detect the file change and reload automatically
	if s.configPath != "" {
		// Get current config from watcher (most up-to-date)
		currentCfg := s.configWatcher.Get()

		// Update tables and save to file
		currentCfg.Tables = req.Tables

		if err := config.Save(s.configPath, currentCfg); err != nil {
			slog.Error("failed to save config file", "error", err, "path", s.configPath)
			// Don't fail the request, just log the error
		} else {
			slog.Info("config saved", "path", s.configPath, "tables", req.Tables)
		}
	}

	// If there were errors, return them
	if len(errors) > 0 {
		return c.View(map[string]any{
			"success": false,
			"error":   strings.Join(errors, "; "),
			"enabled": enabled,
			"skipped": skipped,
		})
	}

	return c.View(map[string]any{
		"success": true,
		"message": "CDC configuration updated",
		"enabled": enabled,
		"skipped": skipped,
		"tables":  req.Tables,
	})
}

// handleCDCChanges handles GET /api/cdc/changes
// Query params: limit (default 100), table, operation, transaction_id
func (s *Server) handleCDCChanges(c *xun.Context) error {
	limit := 100
	if l := c.Request.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	tableName := c.Request.URL.Query().Get("table")
	operation := c.Request.URL.Query().Get("operation")
	txID := c.Request.URL.Query().Get("transaction_id")

	logs, err := s.store.GetChangesWithFilter(limit, tableName, operation, txID)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.View(map[string]any{
		"success": true,
		"count":   len(logs),
		"logs":    logs,
	})
}

// handleCDCStatus handles GET /api/cdc/status
// Returns poller state: last_poll_time, last_lsn, total_changes
// Also includes metrics block when metrics provider is available
func (s *Server) handleCDCStatus(c *xun.Context) error {
	if s.store == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "store not initialized",
		})
	}

	state, err := s.store.GetPollerState()
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   err.Error(),
		})
	}

	response := map[string]any{
		"success": true,
		"state":   state,
	}

	// Include metrics if metrics provider is available
	if s.metricsProvider != nil {
		response["metrics"] = s.metricsProvider.GetMetrics()
	}

	return c.View(response)
}

// handleReplay handles POST /api/replay - starts replay (long-running, async)
func (s *Server) handleReplay(c *xun.Context) error {
	if s.runtime == nil {
		return c.View(map[string]any{"success": false, "error": "runtime not initialized"})
	}

	// Reset capturer state so a fresh replay can start (handles repeated invocations)
	s.runtime.Replay().(*replay.ReplayCapturer).Restart()

	s.runtime.SwitchTo(core.CapturerReplay)

	return c.View(map[string]any{"success": true, "message": "replay started", "state": "replay"})
}

// handleReplayStatus handles GET /api/replay/status
func (s *Server) handleReplayStatus(c *xun.Context) error {
	if s.runtime == nil {
		return c.View(map[string]any{"success": false, "error": "runtime not initialized"})
	}

	replayCapturer := s.runtime.Replay().(*replay.ReplayCapturer)
	progress := replayCapturer.Progress()

	// Derive a state string that the frontend can use directly.
	// "replay"     — in progress
	// "completed"  — finished successfully
	// "stopped"    — manually stopped
	// "idle"       — not yet started
	state := "idle"
	if progress.Stopped {
		state = "stopped"
	} else if progress.Started && !progress.Completed {
		state = "replay"
	} else if progress.Started && progress.Completed {
		state = "completed"
	}

	return c.View(map[string]any{
		"success":   true,
		"total":     progress.TotalLSNs,
		"processed": progress.ProcessedLSNs,
		"started":   progress.Started,
		"completed": progress.Completed,
		"stopped":   progress.Stopped,
		"state":     state,
	})
}

// Returns GAP status for all tracked tables including LSN info, lag bytes, and duration
func (s *Server) handleCDCGap(c *xun.Context) error {
	if s.cdcAdmin == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "CDC admin not initialized",
		})
	}

	if s.store == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "store not initialized",
		})
	}

	// Get tracked tables from config
	var trackedTables []string
	if s.configWatcher != nil {
		cfg := s.configWatcher.Get()
		trackedTables = cfg.Tables
	}

	// Get gap detector thresholds from config
	warnLagBytes := int64(100 * 1024 * 1024)  // Default 100MB
	warnLagDuration := 1 * time.Hour          // Default 1 hour
	critLagBytes := int64(1024 * 1024 * 1024) // Default 1GB
	critLagDuration := 6 * time.Hour          // Default 6 hours

	if s.configWatcher != nil {
		cfg := s.configWatcher.Get()
		if cfg.CDC.Gap.Enabled {
			if cfg.CDC.Gap.WarningLagBytes > 0 {
				warnLagBytes = cfg.CDC.Gap.WarningLagBytes
			}
			if cfg.CDC.Gap.WarningLagDuration != "" {
				if dur, err := time.ParseDuration(cfg.CDC.Gap.WarningLagDuration); err == nil {
					warnLagDuration = dur
				}
			}
			if cfg.CDC.Gap.CriticalLagBytes > 0 {
				critLagBytes = cfg.CDC.Gap.CriticalLagBytes
			}
			if cfg.CDC.Gap.CriticalLagDuration != "" {
				if dur, err := time.ParseDuration(cfg.CDC.Gap.CriticalLagDuration); err == nil {
					critLagDuration = dur
				}
			}
		}
	}

	// Get MSSQL database connection from cdcAdmin (GapDetector requires MSSQL for CDC functions)
	db, err := s.cdcAdmin.Connect()
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("connect to MSSQL: %v", err),
		})
	}
	defer db.Close() //nolint:errcheck

	// Create gap detector with MSSQL connection
	gapDetector := cdc.NewGapDetector(db)
	ctx := context.Background()

	// Get CDC max LSN once (shared across all tables)
	maxLSN, err := gapDetector.GetMaxLSN(ctx)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("get max LSN: %v", err),
		})
	}

	// Collect gap info for all tracked tables
	gapInfos := make([]map[string]any, 0, len(trackedTables))

	for _, table := range trackedTables {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) != 2 {
			continue
		}
		schema, name := parts[0], parts[1]
		captureInstance := fmt.Sprintf("%s_%s", schema, name)

		// Get current LSN from poller state (approximation - in real scenario would track per-table)
		// For now, use the global last_lsn from poller state
		state, err := s.store.GetPollerState()
		if err != nil {
			slog.Warn("failed to get poller state", "table", table, "error", err)
			continue
		}

		var currentLSN []byte
		if lsnStr, ok := state["last_lsn"].(string); ok && lsnStr != "" {
			// Convert hex string back to bytes if needed
			// For now, we'll query the actual current LSN from CDC
			currentLSN = maxLSN // Use max as approximation
		}

		// Check gap for this table
		gapInfo, err := gapDetector.CheckGap(ctx, table, captureInstance, currentLSN)
		if err != nil {
			slog.Warn("failed to check gap", "table", table, "error", err)
			continue
		}

		// Determine status
		status := "healthy"
		statusColor := "success"
		if gapInfo.HasGap {
			status = "critical"
			statusColor = "error"
		} else if gapInfo.IsGapCritical(critLagBytes, critLagDuration) {
			status = "critical"
			statusColor = "error"
		} else if gapInfo.IsGapWarning(warnLagBytes, warnLagDuration) {
			status = "warning"
			statusColor = "warning"
		}

		gapInfos = append(gapInfos, map[string]any{
			"table":             table,
			"schema":            schema,
			"name":              name,
			"capture_instance":  captureInstance,
			"current_lsn":       formatLSN(gapInfo.CurrentLSN),
			"min_lsn":           formatLSN(gapInfo.MinLSN),
			"max_lsn":           formatLSN(gapInfo.MaxLSN),
			"has_gap":           gapInfo.HasGap,
			"lag_bytes":         gapInfo.LagBytes,
			"lag_duration":      gapInfo.LagDuration.String(),
			"lag_duration_secs": int64(gapInfo.LagDuration.Seconds()),
			"status":            status,
			"status_color":      statusColor,
			"checked_at":        gapInfo.CheckedAt,
		})
	}

	return c.View(map[string]any{
		"success": true,
		"count":   len(gapInfos),
		"tables":  gapInfos,
		"max_lsn": formatLSN(maxLSN),
		"thresholds": map[string]any{
			"warning_lag_bytes":     warnLagBytes,
			"warning_lag_duration":  warnLagDuration.String(),
			"critical_lag_bytes":    critLagBytes,
			"critical_lag_duration": critLagDuration.String(),
		},
	})
}

// formatLSN converts LSN bytes to hexadecimal string
func formatLSN(lsn []byte) string {
	if len(lsn) == 0 {
		return "0x00000000:00000000"
	}
	// SQL Server LSN is 10 bytes: VLF offset (4 bytes) + log block offset (2 bytes) + slot offset (4 bytes)
	// Format as 0xXXXXXXXX:XXXXXXXX
	if len(lsn) >= 8 {
		return fmt.Sprintf("0x%02X%02X%02X%02X:%02X%02X%02X%02X",
			lsn[0], lsn[1], lsn[2], lsn[3],
			lsn[4], lsn[5], lsn[6], lsn[7])
	}
	// Fallback for shorter LSNs
	var sb strings.Builder
	sb.WriteString("0x")
	for _, b := range lsn {
		fmt.Fprintf(&sb, "%02X", b)
	}
	return sb.String()
}

// formatBytes formats bytes into human-readable format
func formatBytes(bytes int64) string {
	if bytes <= 0 {
		return "0 B"
	}
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// formatDuration formats duration string
func formatDuration(dur string) string {
	if dur == "" {
		return "N/A"
	}
	d, err := time.ParseDuration(dur)
	if err != nil {
		return dur
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}

// renderOverviewHTML renders the overview metrics as HTML
func renderOverviewHTML(m OverviewMetrics) string {
	var sb strings.Builder

	// Determine health status classes
	var healthBgClass, healthTextClass, healthTitle string
	switch m.HealthStatus {
	case "warning":
		healthBgClass = "bg-warning/20"
		healthTextClass = "text-warning"
		healthTitle = "System Warning"
	case "error":
		healthBgClass = "bg-error/20"
		healthTextClass = "text-error"
		healthTitle = "System Error"
	default:
		healthBgClass = "bg-success/20"
		healthTextClass = "text-success"
		healthTitle = "System Healthy"
	}

	// Determine CDC status classes
	var cdcTextClass, cdcStatusText string
	switch m.CDCStatus {
	case "inactive":
		cdcTextClass = "text-warning"
		cdcStatusText = "Inactive"
	case "error":
		cdcTextClass = "text-error"
		cdcStatusText = "Error"
	default:
		cdcTextClass = "text-success"
		cdcStatusText = "Active"
	}

	sb.WriteString(`<div class="space-y-6">`)

	// Health Status Card
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border">`)
	sb.WriteString(`<div class="flex items-center gap-4">`)
	sb.WriteString(`<div class="flex items-center justify-center w-12 h-12 rounded-full ` + healthBgClass + `">`)
	switch m.HealthStatus {
	case "healthy":
		sb.WriteString(`<svg class="w-6 h-6 text-success" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>`)
	case "warning":
		sb.WriteString(`<svg class="w-6 h-6 text-warning" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg>`)
	default:
		sb.WriteString(`<svg class="w-6 h-6 text-error" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>`)
	}
	sb.WriteString(`</div><div>`)
	sb.WriteString(`<h3 class="text-xl font-semibold ` + healthTextClass + `">` + healthTitle + `</h3>`)
	sb.WriteString(`<p class="text-textMuted">` + m.HealthMessage + `</p>`)
	sb.WriteString(`</div></div></div>`)

	// CDC Sync Status Card
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border">`)
	sb.WriteString(`<h3 class="text-lg font-semibold text-text mb-4">CDC Sync Status</h3>`)
	sb.WriteString(`<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">`)
	sb.WriteString(`<div><p class="text-textMuted text-sm">Status</p><p class="text-lg font-semibold ` + cdcTextClass + `">` + cdcStatusText + `</p></div>`)
	fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">Tables Tracked</p><p class="text-lg font-semibold text-text">%d</p></div>`, m.TablesTracked)
	fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">Lag</p><p class="text-lg font-semibold text-text">%s</p></div>`, formatBytes(m.CDCLagBytes))
	fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">Last Sync</p><p class="text-sm font-medium text-text"><span class="local-time" data-utc="%s">%s</span></p></div>`, m.LastSyncTime, m.LastSyncTime)
	sb.WriteString(`</div>`)
	if m.CDCMessage != "" {
		sb.WriteString(`<p class="text-xs text-textMuted mt-3">` + m.CDCMessage + `</p>`)
	}
	sb.WriteString(`</div>`)

	// GAP Monitoring Summary
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border">`)
	sb.WriteString(`<h3 class="text-lg font-semibold text-text mb-4">GAP Monitoring</h3>`)
	sb.WriteString(`<div class="grid grid-cols-1 sm:grid-cols-3 gap-4">`)
	fmt.Fprintf(&sb, `<div class="p-4 rounded-lg bg-success/10 border border-success/20"><p class="text-textMuted text-sm">Healthy Tables</p><p class="text-2xl font-bold text-success">%d</p></div>`, m.GAPHealthyTables)
	fmt.Fprintf(&sb, `<div class="p-4 rounded-lg bg-error/10 border border-error/20"><p class="text-textMuted text-sm">Issues</p><p class="text-2xl font-bold text-error">%d</p></div>`, m.GAPIssueTables)
	sb.WriteString(`<div class="p-4 rounded-lg bg-surfaceHover border border-border">`)
	sb.WriteString(`<p class="text-textMuted text-sm">Max Lag</p>`)
	sb.WriteString(`<p class="text-lg font-semibold text-text">` + formatBytes(m.GAPMaxLagBytes) + `</p>`)
	if m.GAPMaxLagDuration != "" {
		sb.WriteString(`<p class="text-xs text-textMuted mt-1">` + formatDuration(m.GAPMaxLagDuration) + `</p>`)
	}
	sb.WriteString(`</div></div></div>`)

	// TPS Monitoring Card
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border hover:border-border/60 transition-all">`)
	sb.WriteString(`<h3 class="text-lg font-semibold text-text mb-4">TPS Monitoring</h3>`)
	sb.WriteString(`<div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">`)
	fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">Current TPS</p><p class="text-xl font-bold text-primary">%.1f</p></div>`, m.LastSyncTPS)
	fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">Avg TPS (1m)</p><p class="text-xl font-bold text-success">%.1f</p></div>`, m.AvgTPS1m)
	fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">Last Duration</p><p class="text-xl font-bold text-text">%d ms</p></div>`, m.LastSyncDurationMs)
	fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">DLQ Count</p><p class="text-xl font-bold text-error">%d</p></div>`, m.LastDLQCount)
	if m.LastPollTime != "" {
		fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">Last Poll</p><p class="text-sm font-medium text-text"><span class="local-time" data-utc="%s">%s</span></p></div>`, m.LastPollTime, m.LastPollTime)
	} else {
		sb.WriteString(`<div><p class="text-textMuted text-sm">Last Poll</p><p class="text-sm font-medium text-textMuted">N/A</p></div>`)
	}
	sb.WriteString(`</div></div>`)

	// DLQ Stats Cards
	sb.WriteString(`<div class="grid grid-cols-1 sm:grid-cols-3 gap-4 md:gap-6">`)
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-error/20 hover:border-error/40 transition-all">`)
	sb.WriteString(`<div class="flex items-center justify-between mb-2"><p class="text-textMuted text-sm font-medium">DLQ Pending</p>`)
	sb.WriteString(`<div class="w-8 h-8 rounded-lg bg-error/20 flex items-center justify-center"><svg class="w-4 h-4 text-error" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg></div></div>`)
	fmt.Fprintf(&sb, `<p class="text-3xl font-bold text-error">%d</p></div>`, m.DLQPending)

	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-success/20 hover:border-success/40 transition-all">`)
	sb.WriteString(`<div class="flex items-center justify-between mb-2"><p class="text-textMuted text-sm font-medium">DLQ Resolved</p>`)
	sb.WriteString(`<div class="w-8 h-8 rounded-lg bg-success/20 flex items-center justify-center"><svg class="w-4 h-4 text-success" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg></div></div>`)
	fmt.Fprintf(&sb, `<p class="text-3xl font-bold text-success">%d</p></div>`, m.DLQResolved)

	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border hover:border-border/60 transition-all">`)
	sb.WriteString(`<div class="flex items-center justify-between mb-2"><p class="text-textMuted text-sm font-medium">DLQ Ignored</p>`)
	sb.WriteString(`<div class="w-8 h-8 rounded-lg bg-surfaceHover flex items-center justify-center"><svg class="w-4 h-4 text-textMuted" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4"/></svg></div></div>`)
	fmt.Fprintf(&sb, `<p class="text-3xl font-bold text-textMuted">%d</p></div>`, m.DLQIgnored)
	sb.WriteString(`</div>`)

	// Plugin Status Card
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border">`)
	sb.WriteString(`<h3 class="text-lg font-semibold text-text mb-4">Plugins</h3>`)
	sb.WriteString(`<div class="grid grid-cols-1 sm:grid-cols-2 gap-4">`)
	fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">Active</p><p class="text-2xl font-bold text-primary">%d</p></div>`, m.PluginsActive)
	fmt.Fprintf(&sb, `<div><p class="text-textMuted text-sm">SQL Plugins</p><p class="text-2xl font-bold text-success">%d</p></div>`, m.PluginsSQL)
	sb.WriteString(`</div></div>`)

	sb.WriteString(`</div>`)

	return sb.String()
}

// handleOverview handles GET /api/overview - returns HTML fragment with dashboard overview
func (s *Server) handleOverview(c *xun.Context) error {
	// Collect all metrics
	metrics := s.collectOverviewMetrics()

	// Return HTML string directly
	html := renderOverviewHTML(metrics)
	c.WriteHeader("Content-Type", "text/html; charset=utf-8")
	_, err := c.Response.Write([]byte(html))
	return err
}

// OverviewMetrics contains all dashboard overview metrics
type OverviewMetrics struct {
	// Health
	HealthStatus  string
	HealthMessage string

	// CDC Sync Status
	CDCStatus      string // "active", "inactive", "error"
	CDCMessage     string
	CDCLagBytes    int64
	CDCLagDuration string
	TablesTracked  int

	// GAP Monitoring
	GAPHealthyTables  int
	GAPIssueTables    int
	GAPMaxLagBytes    int64
	GAPMaxLagDuration string

	// DLQ Stats
	DLQPending  int
	DLQResolved int
	DLQIgnored  int

	// Plugin Status
	PluginsActive int
	PluginsSQL    int

	// System Info
	Uptime       string
	LastSyncTime string

	// TPS Monitoring (from Poller.GetMetrics)
	LastSyncTPS        float64
	AvgTPS1m           float64
	LastSyncDurationMs int64
	LastDLQCount       int64
	LastPollTime       string
}

// collectOverviewMetrics collects all metrics for the dashboard overview
func (s *Server) collectOverviewMetrics() OverviewMetrics {
	metrics := OverviewMetrics{
		HealthStatus:  "healthy",
		HealthMessage: "All services running",
	}

	// Collect CDC status
	if s.store != nil {
		state, err := s.store.GetPollerState()
		if err != nil {
			metrics.CDCStatus = "error"
			metrics.CDCMessage = "Failed to get poller state"
		} else {
			metrics.CDCStatus = "active"
			metrics.CDCMessage = "CDC polling active"

			// Get last sync time (stored as string in DB)
			if lastPollStr, ok := state["last_poll_time"].(string); ok && lastPollStr != "" {
				// Parse the timestamp string and format for display
				if lastPoll, err := time.Parse("2006-01-02 15:04:05.999999999", lastPollStr); err == nil {
					metrics.LastSyncTime = lastPoll.Format("2006-01-02 15:04:05")
				} else {
					metrics.LastSyncTime = lastPollStr
				}
			}

			// Count tracked tables
			if s.configWatcher != nil {
				cfg := s.configWatcher.Get()
				metrics.TablesTracked = len(cfg.Tables)
			}
		}
	} else {
		metrics.CDCStatus = "inactive"
		metrics.CDCMessage = "CDC not initialized"
	}

	// Collect GAP metrics
	if s.cdcAdmin != nil && s.store != nil && s.configWatcher != nil {
		cfg := s.configWatcher.Get()
		trackedTables := cfg.Tables

		// Get thresholds
		warnLagBytes := int64(100 * 1024 * 1024)
		warnLagDuration := 1 * time.Hour
		critLagBytes := int64(1024 * 1024 * 1024)
		critLagDuration := 6 * time.Hour

		if cfg.CDC.Gap.Enabled {
			if cfg.CDC.Gap.WarningLagBytes > 0 {
				warnLagBytes = cfg.CDC.Gap.WarningLagBytes
			}
			if cfg.CDC.Gap.WarningLagDuration != "" {
				if dur, err := time.ParseDuration(cfg.CDC.Gap.WarningLagDuration); err == nil {
					warnLagDuration = dur
				}
			}
			if cfg.CDC.Gap.CriticalLagBytes > 0 {
				critLagBytes = cfg.CDC.Gap.CriticalLagBytes
			}
			if cfg.CDC.Gap.CriticalLagDuration != "" {
				if dur, err := time.ParseDuration(cfg.CDC.Gap.CriticalLagDuration); err == nil {
					critLagDuration = dur
				}
			}
		}

		// Connect to database for gap detection
		db, err := s.cdcAdmin.Connect()
		if err == nil {
			defer db.Close() //nolint:errcheck
			gapDetector := cdc.NewGapDetector(db)
			ctx := context.Background()

			maxLSN, err := gapDetector.GetMaxLSN(ctx)
			if err == nil {
				state, err := s.store.GetPollerState()
				if err == nil {
					var currentLSN []byte
					if lsnStr, ok := state["last_lsn"].(string); ok && lsnStr != "" {
						currentLSN = maxLSN
					}

					healthyCount := 0
					issueCount := 0
					maxLagBytes := int64(0)
					var maxLagDuration time.Duration

					for _, table := range trackedTables {
						parts := strings.SplitN(table, ".", 2)
						if len(parts) != 2 {
							continue
						}
						schema, name := parts[0], parts[1]
						captureInstance := fmt.Sprintf("%s_%s", schema, name)

						gapInfo, err := gapDetector.CheckGap(ctx, table, captureInstance, currentLSN)
						if err != nil {
							continue
						}

						if gapInfo.HasGap || gapInfo.IsGapCritical(critLagBytes, critLagDuration) {
							issueCount++
						} else if gapInfo.IsGapWarning(warnLagBytes, warnLagDuration) {
							issueCount++
						} else {
							healthyCount++
						}

						if gapInfo.LagBytes > maxLagBytes {
							maxLagBytes = gapInfo.LagBytes
						}
						if gapInfo.LagDuration > maxLagDuration {
							maxLagDuration = gapInfo.LagDuration
						}
					}

					metrics.GAPHealthyTables = healthyCount
					metrics.GAPIssueTables = issueCount
					metrics.GAPMaxLagBytes = maxLagBytes
					metrics.GAPMaxLagDuration = maxLagDuration.String()
				}
			}
		}
	}

	// Collect DLQ stats
	if s.dlq != nil {
		rawStats, err := s.dlq.Stats()
		if err == nil {
			metrics.DLQPending = rawStats[dlq.StatusPending]
			metrics.DLQResolved = rawStats[dlq.StatusResolved]
			metrics.DLQIgnored = rawStats[dlq.StatusIgnored]
		}
	}

	// Collect plugin stats
	if s.manager != nil {
		plugins := s.manager.List()
		metrics.PluginsActive = len(plugins)
		for _, p := range plugins {
			if p.Type == "sql" {
				metrics.PluginsSQL++
			}
		}
	}

	// Collect TPS metrics from metrics provider
	if s.metricsProvider != nil {
		m := s.metricsProvider.GetMetrics()
		if v, ok := m["last_sync_tps"].(float64); ok {
			metrics.LastSyncTPS = v
		}
		if v, ok := m["avg_tps_1m"].(float64); ok {
			metrics.AvgTPS1m = v
		}
		if v, ok := m["last_sync_duration_ms"].(int64); ok {
			metrics.LastSyncDurationMs = v
		}
		if v, ok := m["last_dlq_count"].(int64); ok {
			metrics.LastDLQCount = v
		}
		if v, ok := m["last_poll_time"].(string); ok && v != "" {
			metrics.LastPollTime = v
		}
	}

	return metrics
}

// handleSinksList handles GET /api/sinks/list
// Returns sinks configured in config.yaml with file status (exists/size/modTime)
func (s *Server) handleSinksList(c *xun.Context) error {
	var sinks []map[string]any

	// Get sinks from config (sinks and plugins are same-level in config)
	if s.config != nil && len(s.config.Sinks) > 0 {
		for _, dbCfg := range s.config.Sinks {
			sink := map[string]any{
				"id":         dbCfg.Id,
				"name":       dbCfg.Name,
				"type":       dbCfg.Type,
				"file":       dbCfg.DSN,
				"configured": true,
			}

			// Check if database file exists
			if info, err := os.Stat(dbCfg.DSN); err == nil {
				sink["exists"] = true
				sink["size"] = info.Size()
				sink["sizeHuman"] = formatFileSize(info.Size())
				sink["modTime"] = info.ModTime().Format(time.RFC3339)
			} else {
				sink["exists"] = false
				sink["sizeHuman"] = "N/A"
			}

			sinks = append(sinks, sink)
		}
	}

	// Ensure we return empty array, not null
	if sinks == nil {
		sinks = []map[string]any{}
	}

	return c.View(map[string]any{
		"success": true,
		"count":   len(sinks),
		"sinks":   sinks,
	})
}

// getSinkById finds the sink by ID from config
func (s *Server) getSinkById(id string) (*config.SinkConfig, error) {
	for i := range s.config.Sinks {
		if s.config.Sinks[i].Id == id {
			return &s.config.Sinks[i], nil
		}
	}
	return nil, fmt.Errorf("sink not found: %s", id)
}

// handleSinkTables handles GET /api/sinks/{id}/tables
func (s *Server) handleSinkTables(c *xun.Context) error {
	id := c.Request.PathValue("id")
	if id == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "sink id is required",
		})
	}

	// Find sink by ID
	sinkCfg, err := s.getSinkById(id)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Use sinker manager to query tables
	tables, err := s.sinkerManager.QueryTables(sinkCfg.Name)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("failed to query tables: %v", err),
		})
	}

	// Ensure we return empty array, not null
	if tables == nil {
		tables = []string{}
	}

	return c.View(map[string]any{
		"success": true,
		"count":   len(tables),
		"tables":  tables,
	})
}

// handleSinkQuery handles POST /api/sinks/{id}/query
func (s *Server) handleSinkQuery(c *xun.Context) error {
	id := c.Request.PathValue("id")
	if id == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "sink id is required",
		})
	}

	// Find sink by ID
	sinkCfg, err := s.getSinkById(id)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Parse request body
	var req struct {
		Query string `json:"query"`
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "failed to read request body",
		})
	}
	defer func() { _ = c.Request.Body.Close() }()

	if err := json.Unmarshal(body, &req); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "invalid request body",
		})
	}

	query := strings.TrimSpace(req.Query)
	if query == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "query is required",
		})
	}

	// Only allow SELECT statements
	queryUpper := strings.ToUpper(query)
	if !strings.HasPrefix(queryUpper, "SELECT") {
		return c.View(map[string]any{
			"success": false,
			"error":   "only SELECT statements are allowed",
		})
	}

	// Use sinker manager to execute query
	columns, queryResults, err := s.sinkerManager.Query(sinkCfg.Name, query)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("query execution failed: %v", err),
		})
	}

	return c.View(map[string]any{
		"success": true,
		"columns": columns,
		"rows":    queryResults,
		"count":   len(queryResults),
	})
}

// handleSinkMigrate handles POST /api/sinks/{id}/migrate
func (s *Server) handleSinkMigrate(c *xun.Context) error {
	id := c.Request.PathValue("id")
	if id == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "sink id is required",
		})
	}

	// Find sink by ID
	sinkCfg, err := s.getSinkById(id)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Get sinker instance
	sinker, err := s.sinkerManager.GetSinker(sinkCfg.Name)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("failed to get sinker: %v", err),
		})
	}

	// Run migration
	if err := sinker.Migrate(context.Background()); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("migration failed: %v", err),
		})
	}

	return c.View(map[string]any{
		"success": true,
		"message": "migration completed successfully",
	})
}

func formatFileSize(size int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case size >= GB:
		return fmt.Sprintf("%.2f GB", float64(size)/float64(GB))
	case size >= MB:
		return fmt.Sprintf("%.2f MB", float64(size)/float64(MB))
	case size >= KB:
		return fmt.Sprintf("%.2f KB", float64(size)/float64(KB))
	default:
		return fmt.Sprintf("%d B", size)
	}
}

// handleMonitorBatches handles GET /api/monitor/batches
func (s *Server) handleMonitorBatches(c *xun.Context) error {
	if s.monitorDB == nil {
		return c.View(map[string]any{"success": false, "error": "monitor DB not initialized"})
	}

	limit := 50
	if l := c.Request.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	since := time.Now().AddDate(0, 0, -7) // Default: last 7 days
	if s := c.Request.URL.Query().Get("since"); s != "" {
		// Try parsing as RFC3339 first
		if parsed, err := time.Parse(time.RFC3339, s); err == nil {
			since = parsed
		} else if days, err := strconv.Atoi(s); err == nil && days > 0 {
			// Fallback: treat as number of days
			since = time.Now().AddDate(0, 0, -days)
		}
	}

	logs, err := s.monitorDB.ListBatchLogs(limit, since)
	if err != nil {
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}

	return c.View(map[string]any{"success": true, "logs": logs})
}

// handleMonitorSkills handles GET /api/monitor/skills
func (s *Server) handleMonitorSkills(c *xun.Context) error {
	if s.monitorDB == nil {
		return c.View(map[string]any{"success": false, "error": "monitor DB not initialized"})
	}

	limit := 50
	if l := c.Request.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	skillID := c.Request.URL.Query().Get("skill_id")

	logs, err := s.monitorDB.ListSkillLogs(skillID, "", limit)
	if err != nil {
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}

	return c.View(map[string]any{"success": true, "logs": logs})
}

// handleMonitorSinks handles GET /api/monitor/sinks
func (s *Server) handleMonitorSinks(c *xun.Context) error {
	if s.monitorDB == nil {
		return c.View(map[string]any{"success": false, "error": "monitor DB not initialized"})
	}

	limit := 50
	if l := c.Request.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	sinkName := c.Request.URL.Query().Get("sink_name")
	database := c.Request.URL.Query().Get("database")

	logs, err := s.monitorDB.ListSinkLogs(sinkName, database, limit)
	if err != nil {
		return c.View(map[string]any{"success": false, "error": err.Error()})
	}

	return c.View(map[string]any{"success": true, "logs": logs})
}

// handleSnapshotStart handles POST /api/snapshot/start - starts a full table snapshot
func (s *Server) handleSnapshotStart(c *xun.Context) error {
	if s.runtime == nil {
		return c.View(map[string]any{"success": false, "error": "runtime not initialized"})
	}

	if s.configWatcher == nil {
		return c.View(map[string]any{"success": false, "error": "config not available"})
	}

	cfg := s.configWatcher.Get()
	if len(cfg.Tables) == 0 {
		return c.View(map[string]any{"success": false, "error": "no CDC tables configured"})
	}

	// Clear all sink tables before snapshot so data is written fresh
	if s.manager != nil {
		ctx := c.Request.Context()
		for _, sinkName := range s.manager.ListDatabases() {
			sink, err := s.manager.GetSinker(sinkName)
			if err != nil {
				slog.Warn("snapshot: failed to get sinker, skipping", "sink", sinkName, "error", err)
				continue
			}
			if err := sink.Reset(ctx); err != nil {
				slog.Warn("snapshot: failed to reset sink, continuing", "sink", sinkName, "error", err)
			}
		}
	}

	// Reset the capturer for a fresh run and switch Runtime to it
	s.runtime.Snapshot().(*snapshot.SnapshotCapturer).Restart()
	s.runtime.SwitchTo(core.CapturerSnapshot)

	return c.View(map[string]any{"success": true, "message": "snapshot started", "state": "running"})
}

// handleSnapshotStatus handles GET /api/snapshot/status - returns current snapshot progress
func (s *Server) handleSnapshotStatus(c *xun.Context) error {
	if s.runtime == nil {
		return c.View(map[string]any{"success": false, "error": "runtime not initialized"})
	}

	progress := s.runtime.Snapshot().(*snapshot.SnapshotCapturer).Progress()

	state := "idle"
	if progress.Stopped {
		state = "stopped"
	} else if progress.Started && !progress.Completed {
		state = "running"
	} else if progress.Completed {
		state = "completed"
	}

	return c.View(map[string]any{
		"success":       true,
		"state":         state,
		"total":         progress.TotalTables,
		"processed":     progress.ProcessedTables,
		"current_table": progress.CurrentTable,
		"started":       progress.Started,
		"completed":     progress.Completed,
		"stopped":       progress.Stopped,
		"error":         progress.Error,
	})
}

// handleSnapshotTables handles GET /api/snapshot/tables - returns list of CDC tables
func (s *Server) handleSnapshotTables(c *xun.Context) error {
	if s.configWatcher == nil {
		return c.View(map[string]any{"success": false, "error": "config not available"})
	}

	cfg := s.configWatcher.Get()
	tables := make([]map[string]string, 0, len(cfg.Tables))
	for _, t := range cfg.Tables {
		parts := strings.SplitN(t, ".", 2)
		if len(parts) == 2 {
			tables = append(tables, map[string]string{
				"schema": parts[0],
				"name":   parts[1],
				"full":   t,
			})
		}
	}

	return c.View(map[string]any{"success": true, "count": len(tables), "tables": tables})
}
