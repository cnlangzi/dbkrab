package api

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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
	"github.com/cnlangzi/dbkrab/internal/cdc"
	"github.com/cnlangzi/dbkrab/internal/cdcadmin"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/plugin"
	"github.com/cnlangzi/dbkrab/app/sqlite"
	"github.com/yaitoo/xun"
)

//go:embed all:dashboard
var dashboardFS embed.FS

// getDashboardFS returns a FS with pages/layouts at root for xun
func getDashboardFS() fs.FS {
	sub, err := fs.Sub(dashboardFS, "dashboard")
	if err != nil {
		return dashboardFS
	}
	return sub
}

// Server provides HTTP API for plugin and DLQ management
type Server struct {
	manager     *plugin.Manager
	dlq         *dlq.DLQ
	cdcAdmin    *cdcadmin.Admin
	store       *app.Store
	port        int
	app         *xun.App
	mux         *http.ServeMux
	configPath  string
	config      *config.Config
	configWatcher *config.Watcher
	sinksRoot   *os.Root // Secure root for sinks directory access
	skillsPath  string // Path to SQL skills directory from config
}

// NewServer creates a new API server
func NewServer(manager *plugin.Manager, port int) *Server {
	return &Server{
		manager: manager,
		port:    port,
	}
}

// NewServerWithDLQ creates a new API server with DLQ support
func NewServerWithDLQ(manager *plugin.Manager, dlqStore *dlq.DLQ, port int) *Server {
	return &Server{
		manager: manager,
		dlq:     dlqStore,
		port:    port,
	}
}

// NewServerWithCDC creates a new API server with CDC admin support
func NewServerWithCDC(manager *plugin.Manager, dlqStore *dlq.DLQ, cdcAdmin *cdcadmin.Admin, store *app.Store, port int, configPath string, cfg *config.Config, watcher *config.Watcher) *Server {
	// Get skills path from config, default to ./skills/sql if not configured
	skillsPath := cfg.Plugins.SQL.Path
	if skillsPath == "" {
		skillsPath = "./skills/sql"
	}
	
	return &Server{
		manager:       manager,
		dlq:           dlqStore,
		cdcAdmin:      cdcAdmin,
		store:         store,
		port:          port,
		configPath:    configPath,
		config:        cfg,
		configWatcher: watcher,
		skillsPath:    skillsPath,
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
	if s.config != nil && len(s.config.Sinks.Databases) > 0 {
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

	// CDC logs routes
	if s.store != nil {
		api.Get("/cdc/logs", s.handleCDCLogs, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/cdc/status", s.handleCDCStatus, xun.WithViewer(&xun.JsonViewer{}))
		slog.Info("CDC logs/status routes registered")
	} else {
		slog.Warn("CDC logs/status routes skipped - store is nil")
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
	api.Get("/sinks/{name}/tables", s.handleSinkTables, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/sinks/{name}/query", s.handleSinkQuery, xun.WithViewer(&xun.JsonViewer{}))

	api.Get("/health", s.handleHealth, xun.WithViewer(&xun.JsonViewer{}))
	api.Get("/overview", s.handleOverview)
}

// registerPageRoutes registers page routes
func (s *Server) registerPageRoutes() {
	// Pages are auto-registered by xun from pages/ directory
	
	// Register skills pages explicitly
	s.app.Get("/skills", s.handleSkillsPage)
	s.app.Get("/skills/new", s.handleSkillsNewPage)
	s.app.Get("/skills/edit/{id}", s.handleSkillsEditPage)
	
	// Register sinks pages explicitly
	s.app.Get("/sinks", s.handleSinksPage)
	s.app.Get("/sinks/{name}", s.handleSinkQueryPage)
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

// handleCDCLogs handles GET /api/cdc/logs
// Query params: limit (default 100), table, operation, transaction_id
func (s *Server) handleCDCLogs(c *xun.Context) error {
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

	return c.View(map[string]any{
		"success": true,
		"state":   state,
	})
}

// handleCDCGap handles GET /api/cdc/gap
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
	warnLagBytes := int64(100 * 1024 * 1024)      // Default 100MB
	warnLagDuration := 1 * time.Hour              // Default 1 hour
	critLagBytes := int64(1024 * 1024 * 1024)     // Default 1GB
	critLagDuration := 6 * time.Hour              // Default 6 hours

	if s.configWatcher != nil {
		cfg := s.configWatcher.Get()
		if cfg.CDCProtection.Enabled {
			if cfg.CDCProtection.WarningLagBytes > 0 {
				warnLagBytes = cfg.CDCProtection.WarningLagBytes
			}
			if cfg.CDCProtection.WarningLagDuration != "" {
				if dur, err := time.ParseDuration(cfg.CDCProtection.WarningLagDuration); err == nil {
					warnLagDuration = dur
				}
			}
			if cfg.CDCProtection.CriticalLagBytes > 0 {
				critLagBytes = cfg.CDCProtection.CriticalLagBytes
			}
			if cfg.CDCProtection.CriticalLagDuration != "" {
				if dur, err := time.ParseDuration(cfg.CDCProtection.CriticalLagDuration); err == nil {
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
		"success":     true,
		"count":       len(gapInfos),
		"tables":      gapInfos,
		"max_lsn":     formatLSN(maxLSN),
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
		sb.WriteString(fmt.Sprintf("%02X", b))
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
	healthBgClass := "bg-success/20"
	healthTextClass := "text-success"
	healthTitle := "System Healthy"
	if m.HealthStatus == "warning" {
		healthBgClass = "bg-warning/20"
		healthTextClass = "text-warning"
		healthTitle = "System Warning"
	} else if m.HealthStatus == "error" {
		healthBgClass = "bg-error/20"
		healthTextClass = "text-error"
		healthTitle = "System Error"
	}
	
	// Determine CDC status classes
	cdcTextClass := "text-success"
	cdcStatusText := "Active"
	if m.CDCStatus == "inactive" {
		cdcTextClass = "text-warning"
		cdcStatusText = "Inactive"
	} else if m.CDCStatus == "error" {
		cdcTextClass = "text-error"
		cdcStatusText = "Error"
	}
	
	sb.WriteString(`<div class="space-y-6">`)
	
	// Health Status Card
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border">`)
	sb.WriteString(`<div class="flex items-center gap-4">`)
	sb.WriteString(`<div class="flex items-center justify-center w-12 h-12 rounded-full ` + healthBgClass + `">`)
	if m.HealthStatus == "healthy" {
		sb.WriteString(`<svg class="w-6 h-6 text-success" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>`)
	} else if m.HealthStatus == "warning" {
		sb.WriteString(`<svg class="w-6 h-6 text-warning" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg>`)
	} else {
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
	sb.WriteString(fmt.Sprintf(`<div><p class="text-textMuted text-sm">Tables Tracked</p><p class="text-lg font-semibold text-text">%d</p></div>`, m.TablesTracked))
	sb.WriteString(fmt.Sprintf(`<div><p class="text-textMuted text-sm">Lag</p><p class="text-lg font-semibold text-text">%s</p></div>`, formatBytes(m.CDCLagBytes)))
	sb.WriteString(fmt.Sprintf(`<div><p class="text-textMuted text-sm">Last Sync</p><p class="text-sm font-medium text-text"><span class="local-time" data-utc="%s">%s</span></p></div>`, m.LastSyncTime, m.LastSyncTime))
	sb.WriteString(`</div>`)
	if m.CDCMessage != "" {
		sb.WriteString(`<p class="text-xs text-textMuted mt-3">` + m.CDCMessage + `</p>`)
	}
	sb.WriteString(`</div>`)
	
	// GAP Monitoring Summary
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border">`)
	sb.WriteString(`<h3 class="text-lg font-semibold text-text mb-4">GAP Monitoring</h3>`)
	sb.WriteString(`<div class="grid grid-cols-1 sm:grid-cols-3 gap-4">`)
	sb.WriteString(fmt.Sprintf(`<div class="p-4 rounded-lg bg-success/10 border border-success/20"><p class="text-textMuted text-sm">Healthy Tables</p><p class="text-2xl font-bold text-success">%d</p></div>`, m.GAPHealthyTables))
	sb.WriteString(fmt.Sprintf(`<div class="p-4 rounded-lg bg-error/10 border border-error/20"><p class="text-textMuted text-sm">Issues</p><p class="text-2xl font-bold text-error">%d</p></div>`, m.GAPIssueTables))
	sb.WriteString(`<div class="p-4 rounded-lg bg-surfaceHover border border-border">`)
	sb.WriteString(`<p class="text-textMuted text-sm">Max Lag</p>`)
	sb.WriteString(`<p class="text-lg font-semibold text-text">` + formatBytes(m.GAPMaxLagBytes) + `</p>`)
	if m.GAPMaxLagDuration != "" {
		sb.WriteString(`<p class="text-xs text-textMuted mt-1">` + formatDuration(m.GAPMaxLagDuration) + `</p>`)
	}
	sb.WriteString(`</div></div></div>`)
	
	// DLQ Stats Cards
	sb.WriteString(`<div class="grid grid-cols-1 sm:grid-cols-3 gap-4 md:gap-6">`)
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-error/20 hover:border-error/40 transition-all">`)
	sb.WriteString(`<div class="flex items-center justify-between mb-2"><p class="text-textMuted text-sm font-medium">DLQ Pending</p>`)
	sb.WriteString(`<div class="w-8 h-8 rounded-lg bg-error/20 flex items-center justify-center"><svg class="w-4 h-4 text-error" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg></div></div>`)
	sb.WriteString(fmt.Sprintf(`<p class="text-3xl font-bold text-error">%d</p></div>`, m.DLQPending))
	
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-success/20 hover:border-success/40 transition-all">`)
	sb.WriteString(`<div class="flex items-center justify-between mb-2"><p class="text-textMuted text-sm font-medium">DLQ Resolved</p>`)
	sb.WriteString(`<div class="w-8 h-8 rounded-lg bg-success/20 flex items-center justify-center"><svg class="w-4 h-4 text-success" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg></div></div>`)
	sb.WriteString(fmt.Sprintf(`<p class="text-3xl font-bold text-success">%d</p></div>`, m.DLQResolved))
	
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border hover:border-border/60 transition-all">`)
	sb.WriteString(`<div class="flex items-center justify-between mb-2"><p class="text-textMuted text-sm font-medium">DLQ Ignored</p>`)
	sb.WriteString(`<div class="w-8 h-8 rounded-lg bg-surfaceHover flex items-center justify-center"><svg class="w-4 h-4 text-textMuted" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4"/></svg></div></div>`)
	sb.WriteString(fmt.Sprintf(`<p class="text-3xl font-bold text-textMuted">%d</p></div>`, m.DLQIgnored))
	sb.WriteString(`</div>`)
	
	// Plugin Status Card
	sb.WriteString(`<div class="bg-surface rounded-xl shadow-lg p-6 border border-border">`)
	sb.WriteString(`<h3 class="text-lg font-semibold text-text mb-4">Plugins</h3>`)
	sb.WriteString(`<div class="grid grid-cols-1 sm:grid-cols-2 gap-4">`)
	sb.WriteString(fmt.Sprintf(`<div><p class="text-textMuted text-sm">Active</p><p class="text-2xl font-bold text-primary">%d</p></div>`, m.PluginsActive))
	sb.WriteString(fmt.Sprintf(`<div><p class="text-textMuted text-sm">SQL Plugins</p><p class="text-2xl font-bold text-success">%d</p></div>`, m.PluginsSQL))
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
	HealthStatus string
	HealthMessage string
	
	// CDC Sync Status
	CDCStatus string // "active", "inactive", "error"
	CDCMessage string
	CDCLagBytes int64
	CDCLagDuration string
	TablesTracked int
	
	// GAP Monitoring
	GAPHealthyTables int
	GAPIssueTables int
	GAPMaxLagBytes int64
	GAPMaxLagDuration string
	
	// DLQ Stats
	DLQPending int
	DLQResolved int
	DLQIgnored int
	
	// Plugin Status
	PluginsActive int
	PluginsSQL int
	
	// System Info
	Uptime string
	LastSyncTime string
}

// collectOverviewMetrics collects all metrics for the dashboard overview
func (s *Server) collectOverviewMetrics() OverviewMetrics {
	metrics := OverviewMetrics{
		HealthStatus: "healthy",
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
			
			// Get last sync time
			if lastPoll, ok := state["last_poll_time"].(time.Time); ok {
				metrics.LastSyncTime = lastPoll.Format("2006-01-02 15:04:05")
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
		
		if cfg.CDCProtection.Enabled {
			if cfg.CDCProtection.WarningLagBytes > 0 {
				warnLagBytes = cfg.CDCProtection.WarningLagBytes
			}
			if cfg.CDCProtection.WarningLagDuration != "" {
				if dur, err := time.ParseDuration(cfg.CDCProtection.WarningLagDuration); err == nil {
					warnLagDuration = dur
				}
			}
			if cfg.CDCProtection.CriticalLagBytes > 0 {
				critLagBytes = cfg.CDCProtection.CriticalLagBytes
			}
			if cfg.CDCProtection.CriticalLagDuration != "" {
				if dur, err := time.ParseDuration(cfg.CDCProtection.CriticalLagDuration); err == nil {
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
	
	return metrics
}

// handleSinksList handles GET /api/sinks/list
func (s *Server) handleSinksList(c *xun.Context) error {
	// Check if sinks root is available
	if s.sinksRoot == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "sinks directory not available",
		})
	}
	
	entries, err := fs.ReadDir(s.sinksRoot.FS(), ".")
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("failed to read sinks directory: %v", err),
		})
	}
	
	var sinks []map[string]any
	for _, entry := range entries {
		if entry.IsDir() {
			// Check for .db files inside the directory
			dbFiles, err := fs.ReadDir(s.sinksRoot.FS(), entry.Name())
			if err != nil {
				continue
			}
			for _, dbFile := range dbFiles {
				if strings.HasSuffix(strings.ToLower(dbFile.Name()), ".db") {
					filePath := filepath.Join(entry.Name(), dbFile.Name())
					info, err := s.sinksRoot.Stat(filePath)
					if err != nil {
						continue
					}
					sinks = append(sinks, map[string]any{
						"name":      entry.Name(),
						"file":      dbFile.Name(),
						"path":      filePath,
						"size":      info.Size(),
						"sizeHuman": formatFileSize(info.Size()),
						"modTime":   info.ModTime().Format(time.RFC3339),
					})
				}
			}
		} else if strings.HasSuffix(strings.ToLower(entry.Name()), ".db") {
			// Direct .db file in sinks directory
			info, err := s.sinksRoot.Stat(entry.Name())
			if err != nil {
				continue
			}
			name := strings.TrimSuffix(entry.Name(), ".db")
			sinks = append(sinks, map[string]any{
				"name":      name,
				"file":      entry.Name(),
				"path":      entry.Name(),
				"size":      info.Size(),
				"sizeHuman": formatFileSize(info.Size()),
				"modTime":   info.ModTime().Format(time.RFC3339),
			})
		}
	}
	
	return c.View(map[string]any{
		"success": true,
		"count":   len(sinks),
		"sinks":   sinks,
	})
}

// handleSinkTables handles GET /api/sinks/{name}/tables
func (s *Server) handleSinkTables(c *xun.Context) error {
	name := c.Request.PathValue("name")
	if name == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "sink name is required",
		})
	}
	
	// Check if sinks root is available
	if s.sinksRoot == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "sinks directory not available",
		})
	}
	
	// Validate name to prevent path traversal
	// os.Root methods automatically reject paths that try to escape the root
	if !filepath.IsLocal(name) {
		return c.View(map[string]any{
			"success": false,
			"error":   "invalid sink name",
		})
	}
	
	// Try to find the database file using os.Root for secure access
	// First try: {name}/{name}.db
	dbRelPath := filepath.Join(name, name+".db")
	if _, err := s.sinksRoot.Stat(dbRelPath); err != nil {
		// Second try: {name}.db
		dbRelPath = name + ".db"
		if _, err := s.sinksRoot.Stat(dbRelPath); err != nil {
			return c.View(map[string]any{
				"success": false,
				"error":   fmt.Sprintf("sink database not found: %s", name),
			})
		}
	}
	
	// Build full path for SQLite driver
	dbPath := filepath.Join(s.config.Sinks.BasePath(), dbRelPath)
	
	// Open read-only connection
	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("failed to open database: %v", err),
		})
	}
	defer func() { _ = db.Close() }()
	
	// Query tables from sqlite_master
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("failed to query tables: %v", err),
		})
	}
	defer func() { _ = rows.Close() }()
	
	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		// Skip internal SQLite tables
		if !strings.HasPrefix(tableName, "sqlite_") {
			tables = append(tables, tableName)
		}
	}
	
	return c.View(map[string]any{
		"success": true,
		"count":   len(tables),
		"tables":  tables,
	})
}

// handleSinkQuery handles POST /api/sinks/{name}/query
func (s *Server) handleSinkQuery(c *xun.Context) error {
	name := c.Request.PathValue("name")
	if name == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "sink name is required",
		})
	}
	
	// Check if sinks root is available
	if s.sinksRoot == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "sinks directory not available",
		})
	}
	
	// Validate name to prevent path traversal
	// os.Root methods automatically reject paths that try to escape the root
	if !filepath.IsLocal(name) {
		return c.View(map[string]any{
			"success": false,
			"error":   "invalid sink name",
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
	
	// Try to find the database file using os.Root for secure access
	// First try: {name}/{name}.db
	dbRelPath := filepath.Join(name, name+".db")
	if _, err := s.sinksRoot.Stat(dbRelPath); err != nil {
		// Second try: {name}.db
		dbRelPath = name + ".db"
		if _, err := s.sinksRoot.Stat(dbRelPath); err != nil {
			return c.View(map[string]any{
				"success": false,
				"error":   fmt.Sprintf("sink database not found: %s", name),
			})
		}
	}
	
	// Build full path for SQLite driver
	dbPath := filepath.Join(s.config.Sinks.BasePath(), dbRelPath)
	
	// Open read-only connection
	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("failed to open database: %v", err),
		})
	}
	defer func() { _ = db.Close() }()
	
	// Execute query with limit
	limitQuery := query
	if !strings.Contains(strings.ToUpper(query), "LIMIT") {
		limitQuery = fmt.Sprintf("%s LIMIT 1000", query)
	}
	
	rows, err := db.Query(limitQuery)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("query execution failed: %v", err),
		})
	}
	defer func() { _ = rows.Close() }()
	
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   fmt.Sprintf("failed to get columns: %v", err),
		})
	}
	
	// Fetch results
	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		
		if err := rows.Scan(valuePtrs...); err != nil {
			return c.View(map[string]any{
				"success": false,
				"error":   fmt.Sprintf("failed to scan row: %v", err),
			})
		}
		
		rowMap := make(map[string]any)
		for i, col := range columns {
			rowMap[col] = values[i]
		}
		results = append(results, rowMap)
	}
	
	return c.View(map[string]any{
		"success": true,
		"columns": columns,
		"rows":    results,
		"count":   len(results),
	})
}

// handleSinksPage handles GET /sinks
func (s *Server) handleSinksPage(c *xun.Context) error {
	return c.View(map[string]any{
		"activeTab": "sinks",
		"title":     "Sinks",
	})
}

// handleSinkQueryPage handles GET /sinks/{name}
func (s *Server) handleSinkQueryPage(c *xun.Context) error {
	name := c.Request.PathValue("name")
	return c.View(map[string]any{
		"activeTab": "sinks",
		"title":     fmt.Sprintf("Sink: %s", name),
		"sinkName":  name,
	})
}

// formatFileSize formats file size in human-readable format
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
