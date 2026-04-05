package api

import (
	"context"
	"embed"
	"io/fs"
	"net/http"
	"strconv"
	"time"

	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/plugin"
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
	manager *plugin.Manager
	dlq     *dlq.DLQ
	port    int
	app     *xun.App
	mux     *http.ServeMux
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
	)

	// Register API routes
	s.registerAPIRoutes()

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

	api.Get("/health", s.handleHealth, xun.WithViewer(&xun.JsonViewer{}))
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
