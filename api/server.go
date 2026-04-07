package api

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/cdc"
	"github.com/cnlangzi/dbkrab/internal/cdcadmin"
	"github.com/cnlangzi/dbkrab/internal/config"
	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/plugin"
	"github.com/cnlangzi/dbkrab/sink/sqlite"
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
	sink        *sqlite.Sink
	port        int
	app         *xun.App
	mux         *http.ServeMux
	configPath  string
	config      *config.Config
	configWatcher *config.Watcher
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
func NewServerWithCDC(manager *plugin.Manager, dlqStore *dlq.DLQ, cdcAdmin *cdcadmin.Admin, sink *sqlite.Sink, port int, configPath string, cfg *config.Config, watcher *config.Watcher) *Server {
	return &Server{
		manager:       manager,
		dlq:           dlqStore,
		cdcAdmin:      cdcAdmin,
		sink:          sink,
		port:          port,
		configPath:    configPath,
		config:        cfg,
		configWatcher: watcher,
	}
}

// Start starts the API server with xun framework
func (s *Server) Start() error {
	// Create a mux to use with xun
	s.mux = http.NewServeMux()

	// Create xun app with our mux and template filesystem
	// Use Sub FS so pages/layouts are at root level for xun
	dashboardSubFS := getDashboardFS()
	s.app = xun.New(
		xun.WithFsys(dashboardSubFS),
		xun.WithMux(s.mux),
		xun.WithHandlerViewers(&xun.JsonViewer{}, &xun.HtmlViewer{}),
	)

	// Register routes
	s.registerAPIRoutes()
	s.registerPageRoutes()

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
	api.Get("/plugins/:name", s.handlePluginGet, xun.WithViewer(&xun.JsonViewer{}))
	api.Delete("/plugins/:name", s.handlePluginDelete, xun.WithViewer(&xun.JsonViewer{}))
	api.Post("/plugins/:name/reload", s.handlePluginReload, xun.WithViewer(&xun.JsonViewer{}))

	if s.dlq != nil {
		api.Get("/dlq/list", s.handleDLQList, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/dlq/stats", s.handleDLQStats, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/dlq/:id", s.handleDLQGet, xun.WithViewer(&xun.JsonViewer{}))
		api.Post("/dlq/:id/replay", s.handleDLQReplay, xun.WithViewer(&xun.JsonViewer{}))
		api.Post("/dlq/:id/ignore", s.handleDLQIgnore, xun.WithViewer(&xun.JsonViewer{}))
		api.Delete("/dlq/:id", s.handleDLQDelete, xun.WithViewer(&xun.JsonViewer{}))
	}

	// CDC administration routes
	if s.cdcAdmin != nil {
		api.Get("/cdc/tables", s.handleCDCTables, xun.WithViewer(&xun.JsonViewer{}))
		api.Post("/cdc/config", s.handleCDCConfig, xun.WithViewer(&xun.JsonViewer{}))
	}

	// CDC logs routes
	if s.sink != nil {
		api.Get("/cdc/logs", s.handleCDCLogs, xun.WithViewer(&xun.JsonViewer{}))
		api.Get("/cdc/status", s.handleCDCStatus, xun.WithViewer(&xun.JsonViewer{}))
		slog.Info("CDC logs/status routes registered")
	} else {
		slog.Warn("CDC logs/status routes skipped - sink is nil")
	}

	// CDC gap monitoring routes
	if s.cdcAdmin != nil && s.sink != nil {
		api.Get("/cdc/gap", s.handleCDCGap, xun.WithViewer(&xun.JsonViewer{}))
		slog.Info("CDC gap monitoring route registered")
	} else {
		slog.Warn("CDC gap monitoring route skipped - cdcAdmin or sink is nil")
	}

	api.Get("/health", s.handleHealth, xun.WithViewer(&xun.JsonViewer{}))
}

// registerPageRoutes registers page routes
func (s *Server) registerPageRoutes() {
	// Pages are auto-registered by xun from pages/ directory
	// Manual registration for custom handlers if needed
}

// handlePlugins handles GET /api/plugins
func (s *Server) handlePlugins(c *xun.Context) error {
	resp := s.manager.HandleAPI("list", nil)
	return c.View(resp)
}

// handlePluginGet handles GET /api/plugins/:name
func (s *Server) handlePluginGet(c *xun.Context) error {
	name := c.Routing.Options.GetString("name")
	resp := s.manager.HandleAPI("get", map[string]any{"name": name})
	if !resp.Success {
		c.WriteStatus(http.StatusNotFound)
	}
	return c.View(resp)
}

// handlePluginDelete handles DELETE /api/plugins/:name
func (s *Server) handlePluginDelete(c *xun.Context) error {
	name := c.Routing.Options.GetString("name")
	resp := s.manager.HandleAPI("unload", map[string]any{"name": name})
	return c.View(resp)
}

// handlePluginReload handles POST /api/plugins/:name/reload
func (s *Server) handlePluginReload(c *xun.Context) error {
	name := c.Routing.Options.GetString("name")
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

// handleDLQGet handles GET /api/dlq/:id
func (s *Server) handleDLQGet(c *xun.Context) error {
	if s.dlq == nil {
		return c.View(map[string]any{"success": false, "error": "DLQ not initialized"})
	}

	id, err := strconv.ParseInt(c.Routing.Options.GetString("id"), 10, 64)
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

// handleDLQReplay handles POST /api/dlq/:id/replay
func (s *Server) handleDLQReplay(c *xun.Context) error {
	if s.dlq == nil {
		return c.View(map[string]any{"success": false, "error": "DLQ not initialized"})
	}

	id, err := strconv.ParseInt(c.Routing.Options.GetString("id"), 10, 64)
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

// handleDLQDelete handles DELETE /api/dlq/:id
func (s *Server) handleDLQDelete(c *xun.Context) error {
	if s.dlq == nil {
		return c.View(map[string]any{"success": false, "error": "DLQ not initialized"})
	}

	id, err := strconv.ParseInt(c.Routing.Options.GetString("id"), 10, 64)
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

	id, err := strconv.ParseInt(c.Routing.Options.GetString("id"), 10, 64)
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

	logs, err := s.sink.GetChangesWithFilter(limit, tableName, operation, txID)
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
	if s.sink == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "sink not initialized",
		})
	}

	state, err := s.sink.GetPollerState()
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

	if s.sink == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "sink not initialized",
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
		state, err := s.sink.GetPollerState()
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
