package api

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/plugin"
)

//go:embed dashboard/templates/*.html
var templatesFS embed.FS

// Server provides HTTP API for plugin and DLQ management
type Server struct {
	manager   *plugin.Manager
	dlq       *dlq.DLQ
	port      int
	server    *http.Server
	templates *template.Template
}

// PageData holds template data
type PageData struct {
	Title     string
	Stats     map[string]int
	Plugins   plugin.APIResponse
	Entries   []*dlq.DLQEntry
	Health    string
	ActiveTab string
	Message   string
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
	// Parse templates
	tmpl, err := template.New("").ParseFS(templatesFS, "dashboard/templates/*.html")
	if err != nil {
		return fmt.Errorf("parse templates: %w", err)
	}
	s.templates = tmpl

	mux := http.NewServeMux()

	// Dashboard pages (SSR)
	mux.HandleFunc("/", s.handleDashboard)
	mux.HandleFunc("/dlq", s.handleDLQPage)
	mux.HandleFunc("/plugins", s.handlePluginsPage)

	// API endpoints
	mux.HandleFunc("/api/plugins", s.handlePlugins)
	mux.HandleFunc("/api/plugins/", s.handlePluginAction)
	mux.HandleFunc("/api/dlq/list", s.handleDLQList)
	mux.HandleFunc("/api/dlq/", s.handleDLQAction)
	mux.HandleFunc("/api/dlq/stats", s.handleDLQStats)
	mux.HandleFunc("/health", s.handleHealth)

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
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
func (s *Server) renderTemplate(w http.ResponseWriter, name string, data PageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl := s.templates.Lookup(name)
	if tmpl == nil {
		http.Error(w, "template not found: "+name, http.StatusInternalServerError)
		return
	}
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleDashboard renders the main dashboard page
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	stats, _ := s.getDLQStats()
	plugins := s.manager.HandleAPI("list", nil)

	s.renderTemplate(w, "index.html", PageData{
		Title:     "Dashboard",
		Stats:     stats,
		Plugins:   plugins,
		Health:    "healthy",
		ActiveTab: "overview",
	})
}

// handleDLQPage renders the DLQ management page
func (s *Server) handleDLQPage(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.renderTemplate(w, "dlq.html", PageData{
			Title:   "Error",
			Message: "DLQ not initialized",
		})
		return
	}

	entries, err := s.dlq.List("")
	if err != nil {
		s.renderTemplate(w, "dlq.html", PageData{
			Title:   "Error",
			Message: err.Error(),
		})
		return
	}

	s.renderTemplate(w, "dlq.html", PageData{
		Title:     "Dead Letter Queue",
		Entries:   entries,
		ActiveTab: "dlq",
	})
}

// handlePluginsPage renders the plugins management page
func (s *Server) handlePluginsPage(w http.ResponseWriter, r *http.Request) {
	plugins := s.manager.HandleAPI("list", nil)

	s.renderTemplate(w, "plugins.html", PageData{
		Title:     "Plugins",
		Plugins:   plugins,
		ActiveTab: "plugins",
	})
}

// handlePlugins handles GET /api/plugins (list)
func (s *Server) handlePlugins(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := s.manager.HandleAPI("list", nil)
	s.writeJSON(w, resp)
}

// handlePluginAction handles plugin actions
func (s *Server) handlePluginAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/plugins/")
	if path == "" {
		http.Error(w, "plugin name required", http.StatusBadRequest)
		return
	}

	var name string
	var action string

	if strings.HasSuffix(path, "/reload") {
		name = strings.TrimSuffix(path, "/reload")
		action = "reload"
	} else {
		name = path
	}

	params := map[string]interface{}{"name": name}

	switch r.Method {
	case "GET":
		resp := s.manager.HandleAPI("get", params)
		if !resp.Success {
			w.WriteHeader(http.StatusNotFound)
		}
		s.writeJSON(w, resp)

	case "DELETE":
		resp := s.manager.HandleAPI("unload", params)
		s.writeJSON(w, resp)

	case "POST":
		if action == "reload" {
			resp := s.manager.HandleAPI("reload", params)
			s.writeJSON(w, resp)
		} else {
			http.Error(w, "unknown action", http.StatusBadRequest)
		}

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleDLQList handles GET /api/dlq/list
func (s *Server) handleDLQList(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.writeJSON(w, map[string]interface{}{
			"success": false,
			"error":   "DLQ not initialized",
		})
		return
	}

	if r.Method != "GET" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := r.URL.Query().Get("status")
	entries, err := s.dlq.List(status)
	if err != nil {
		s.writeJSON(w, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	s.writeJSON(w, map[string]interface{}{
		"success": true,
		"count":   len(entries),
		"entries": entries,
	})
}

// handleDLQAction handles DLQ actions
func (s *Server) handleDLQAction(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.writeJSON(w, map[string]interface{}{
			"success": false,
			"error":   "DLQ not initialized",
		})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/dlq/")
	if path == "" {
		http.Error(w, "entry ID required", http.StatusBadRequest)
		return
	}

	var action string
	var idStr string

	if strings.HasSuffix(path, "/replay") {
		action = "replay"
		idStr = strings.TrimSuffix(path, "/replay")
	} else if strings.HasSuffix(path, "/ignore") {
		action = "ignore"
		idStr = strings.TrimSuffix(path, "/ignore")
	} else {
		idStr = path
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid entry ID", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		entry, err := s.dlq.Get(id)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			s.writeJSON(w, map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
			return
		}
		s.writeJSON(w, map[string]interface{}{
			"success": true,
			"entry":   entry,
		})

	case "POST":
		if action == "replay" {
			err := s.dlq.Replay(r.Context(), id, func(entry *dlq.DLQEntry) error {
				return nil
			})
			if err != nil {
				s.writeJSON(w, map[string]interface{}{
					"success": false,
					"error":   err.Error(),
				})
				return
			}
			s.writeJSON(w, map[string]interface{}{
				"success": true,
				"message": "Entry replayed successfully",
			})
		} else if action == "ignore" {
			body, _ := io.ReadAll(r.Body)
			var params struct {
				Note string `json:"note"`
			}
			json.Unmarshal(body, &params)

			err = s.dlq.Ignore(id, params.Note)
			if err != nil {
				s.writeJSON(w, map[string]interface{}{
					"success": false,
					"error":   err.Error(),
				})
				return
			}
			s.writeJSON(w, map[string]interface{}{
				"success": true,
				"message": "Entry ignored",
			})
		} else {
			w.WriteHeader(http.StatusBadRequest)
			s.writeJSON(w, map[string]interface{}{
				"success": false,
				"error":   "unknown action",
			})
		}

	case "DELETE":
		err := s.dlq.Delete(id)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			s.writeJSON(w, map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
			return
		}
		s.writeJSON(w, map[string]interface{}{
			"success": true,
			"message": "Entry deleted",
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleDLQStats handles GET /api/dlq/stats
func (s *Server) handleDLQStats(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.writeJSON(w, map[string]interface{}{
			"success": false,
			"error":   "DLQ not initialized",
		})
		return
	}

	if r.Method != "GET" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats, err := s.getDLQStats()
	if err != nil {
		s.writeJSON(w, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	s.writeJSON(w, map[string]interface{}{
		"success": true,
		"stats":   stats,
	})
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

// handleHealth handles health check
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// writeJSON writes JSON response
func (s *Server) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// Helper functions for templates
func add(a, b int) int { return a + b }
func sub(a, b int) int { return a - b }
func index(m map[string]int, key string) int { return m[key] }
func lenSlice(v interface{}) int {
	switch v := v.(type) {
	case []any:
		return len(v)
	default:
		return 0
	}
}
func join(sep string, elems []string) string {
	return strings.Join(elems, sep)
}
func base(v string) string {
	return filepath.Base(v)
}
