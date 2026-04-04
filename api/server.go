package api

import (
	"context"
	"embed"
	"encoding/json"
	"html/template"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/plugin"
)

//go:embed dashboard/templates
var templatesFS embed.FS

// Server provides HTTP API for plugin and DLQ management
type Server struct {
	manager   *plugin.Manager
	dlq       *dlq.DLQ
	port      int
	server    *http.Server
	templates *template.Template
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

// Start starts the API server
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Page routes
	mux.HandleFunc("GET /", s.handleDashboard)
	mux.HandleFunc("GET /dlq", s.handleDLQPage)
	mux.HandleFunc("GET /plugins", s.handlePluginsPage)

	// API routes
	mux.HandleFunc("GET /api/plugins", s.handlePlugins)
	mux.HandleFunc("GET /api/plugins/", s.handlePluginGet)
	mux.HandleFunc("DELETE /api/plugins/", s.handlePluginDelete)
	mux.HandleFunc("POST /api/plugins/", s.handlePluginReload)

	if s.dlq != nil {
		mux.HandleFunc("GET /api/dlq/list", s.handleDLQList)
		mux.HandleFunc("GET /api/dlq/stats", s.handleDLQStats)
		mux.HandleFunc("GET /api/dlq/", s.handleDLQGet)
		mux.HandleFunc("POST /api/dlq/", s.handleDLQReplay)
		mux.HandleFunc("POST /api/dlq/", s.handleDLQIgnore)
		mux.HandleFunc("DELETE /api/dlq/", s.handleDLQDelete)
	}

	mux.HandleFunc("GET /health", s.handleHealth)

	s.server = &http.Server{
		Addr:         ":" + strconv.Itoa(s.port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s.server.ListenAndServe()
}

// Stop stops the API server
func (s *Server) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

// renderTemplate renders a template with the given data
func (s *Server) renderTemplate(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Use xun's template engine indirectly via standard html/template
	tmpl := template.New("").Funcs(template.FuncMap{
		"eq":      func(a, b any) bool { return a == b },
		"index":   func(m map[string]any, key string) any { return m[key] },
		"len":     func(v any) int { return reflect.ValueOf(v).Len() },
		"datetime": func(t time.Time) string { return t.Format("2006-01-02 15:04:05") },
	})
	// Parse templates from embed.FS
	var err error
	s.templates, err = tmpl.ParseGlob("dashboard/templates/**/*.html")
	if err != nil {
		http.Error(w, "template error: "+err.Error(), 500)
		return
	}
	if err := s.templates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

// handleDashboard renders the main dashboard page
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	stats, _ := s.getDLQStats()
	plugins := s.manager.HandleAPI("list", nil)

	s.renderTemplate(w, "index.html", map[string]any{
		"title":     "Dashboard",
		"stats":     stats,
		"plugins":   plugins,
		"health":    "healthy",
		"activeTab": "overview",
	})
}

// handleDLQPage renders the DLQ management page
func (s *Server) handleDLQPage(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.renderTemplate(w, "dlq.html", map[string]any{
			"title":   "Error",
			"message": "DLQ not initialized",
		})
		return
	}

	entries, err := s.dlq.List("")
	if err != nil {
		s.renderTemplate(w, "dlq.html", map[string]any{
			"title":   "Error",
			"message": err.Error(),
		})
		return
	}

	s.renderTemplate(w, "dlq.html", map[string]any{
		"title":     "Dead Letter Queue",
		"entries":   entries,
		"activeTab": "dlq",
	})
}

// handlePluginsPage renders the plugins management page
func (s *Server) handlePluginsPage(w http.ResponseWriter, r *http.Request) {
	plugins := s.manager.HandleAPI("list", nil)

	s.renderTemplate(w, "plugins.html", map[string]any{
		"title":     "Plugins",
		"plugins":   plugins,
		"activeTab": "plugins",
	})
}

// handlePlugins handles GET /api/plugins
func (s *Server) handlePlugins(w http.ResponseWriter, r *http.Request) {
	resp := s.manager.HandleAPI("list", nil)
	s.writeJSON(w, resp)
}

// handlePluginGet handles GET /api/plugins/:name
func (s *Server) handlePluginGet(w http.ResponseWriter, r *http.Request) {
	name := extractPathParam(r.URL.Path, "/api/plugins/")
	resp := s.manager.HandleAPI("get", map[string]any{"name": name})
	if !resp.Success {
		w.WriteHeader(http.StatusNotFound)
	}
	s.writeJSON(w, resp)
}

// handlePluginDelete handles DELETE /api/plugins/:name
func (s *Server) handlePluginDelete(w http.ResponseWriter, r *http.Request) {
	name := extractPathParam(r.URL.Path, "/api/plugins/")
	resp := s.manager.HandleAPI("unload", map[string]any{"name": name})
	s.writeJSON(w, resp)
}

// handlePluginReload handles POST /api/plugins/:name/reload
func (s *Server) handlePluginReload(w http.ResponseWriter, r *http.Request) {
	name := extractPathParam(r.URL.Path, "/api/plugins/")
	resp := s.manager.HandleAPI("reload", map[string]any{"name": name})
	s.writeJSON(w, resp)
}

// handleDLQList handles GET /api/dlq/list
func (s *Server) handleDLQList(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.writeJSON(w, map[string]any{"success": false, "error": "DLQ not initialized"})
		return
	}

	status := r.URL.Query().Get("status")
	entries, err := s.dlq.List(status)
	if err != nil {
		s.writeJSON(w, map[string]any{"success": false, "error": err.Error()})
		return
	}

	s.writeJSON(w, map[string]any{"success": true, "count": len(entries), "entries": entries})
}

// handleDLQStats handles GET /api/dlq/stats
func (s *Server) handleDLQStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.getDLQStats()
	if err != nil {
		s.writeJSON(w, map[string]any{"success": false, "error": err.Error()})
		return
	}
	s.writeJSON(w, map[string]any{"success": true, "stats": stats})
}

// handleDLQGet handles GET /api/dlq/:id
func (s *Server) handleDLQGet(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.writeJSON(w, map[string]any{"success": false, "error": "DLQ not initialized"})
		return
	}

	id, err := strconv.ParseInt(extractPathParam(r.URL.Path, "/api/dlq/"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		s.writeJSON(w, map[string]any{"success": false, "error": "invalid entry ID"})
		return
	}

	entry, err := s.dlq.Get(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		s.writeJSON(w, map[string]any{"success": false, "error": err.Error()})
		return
	}

	s.writeJSON(w, map[string]any{"success": true, "entry": entry})
}

// handleDLQReplay handles POST /api/dlq/:id/replay
func (s *Server) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.writeJSON(w, map[string]any{"success": false, "error": "DLQ not initialized"})
		return
	}

	id, err := strconv.ParseInt(extractPathParam(r.URL.Path, "/api/dlq/"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		s.writeJSON(w, map[string]any{"success": false, "error": "invalid entry ID"})
		return
	}

	err = s.dlq.Replay(context.Background(), id, func(*dlq.DLQEntry) error { return nil })
	if err != nil {
		s.writeJSON(w, map[string]any{"success": false, "error": err.Error()})
		return
	}

	s.writeJSON(w, map[string]any{"success": true, "message": "Entry replayed"})
}

// handleDLQIgnore handles POST /api/dlq/:id/ignore
func (s *Server) handleDLQIgnore(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.writeJSON(w, map[string]any{"success": false, "error": "DLQ not initialized"})
		return
	}

	id, err := strconv.ParseInt(extractPathParam(r.URL.Path, "/api/dlq/"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		s.writeJSON(w, map[string]any{"success": false, "error": "invalid entry ID"})
		return
	}

	body, _ := io.ReadAll(r.Body)
	var params struct{ Note string }
	json.Unmarshal(body, &params)

	err = s.dlq.Ignore(id, params.Note)
	if err != nil {
		s.writeJSON(w, map[string]any{"success": false, "error": err.Error()})
		return
	}

	s.writeJSON(w, map[string]any{"success": true, "message": "Entry ignored"})
}

// handleDLQDelete handles DELETE /api/dlq/:id
func (s *Server) handleDLQDelete(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.writeJSON(w, map[string]any{"success": false, "error": "DLQ not initialized"})
		return
	}

	id, err := strconv.ParseInt(extractPathParam(r.URL.Path, "/api/dlq/"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		s.writeJSON(w, map[string]any{"success": false, "error": "invalid entry ID"})
		return
	}

	err = s.dlq.Delete(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		s.writeJSON(w, map[string]any{"success": false, "error": err.Error()})
		return
	}

	s.writeJSON(w, map[string]any{"success": true, "message": "Entry deleted"})
}

// handleHealth handles GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
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

// writeJSON writes JSON response
func (s *Server) writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// extractPathParam extracts the path parameter from a URL path
func extractPathParam(path, prefix string) string {
	// Handle both /api/dlq/123 and /api/plugins/name/reload formats
	path = trimPrefix(path, prefix)
	// Remove trailing /replay or /ignore if present
	path = trimSuffix(path, "/replay")
	path = trimSuffix(path, "/ignore")
	return path
}

func trimPrefix(s, prefix string) string {
	if len(s) > len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

func trimSuffix(s, suffix string) string {
	if len(s) > len(suffix) && s[len(s)-len(suffix):] == suffix {
		return s[:len(s)-len(suffix)]
	}
	return s
}
