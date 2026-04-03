package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cnlangzi/dbkrab/plugin"
)

// Server provides HTTP API for plugin management
type Server struct {
	manager  *plugin.Manager
	port     int
	server   *http.Server
}

// NewServer creates a new API server
func NewServer(manager *plugin.Manager, port int) *Server {
	return &Server{
		manager: manager,
		port:    port,
	}
}

// Start starts the API server
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Plugin management endpoints
	mux.HandleFunc("/api/plugins", s.handlePlugins)
	mux.HandleFunc("/api/plugins/", s.handlePluginAction)
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

// handlePlugins handles GET /api/plugins (list) and POST /api/plugins (load)
func (s *Server) handlePlugins(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		// List plugins
		resp := s.manager.HandleAPI("list", nil)
		s.writeJSON(w, resp)

	case "POST":
		// Load a new plugin
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s.writeJSON(w, plugin.APIResponse{Success: false, Error: "read body failed"})
			return
		}

		var params map[string]interface{}
		if err := json.Unmarshal(body, &params); err != nil {
			s.writeJSON(w, plugin.APIResponse{Success: false, Error: "invalid json"})
			return
		}

		resp := s.manager.HandleAPI("load", params)
		s.writeJSON(w, resp)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePluginAction handles plugin-specific actions
// GET /api/plugins/{name} - get info
// DELETE /api/plugins/{name} - unload
// POST /api/plugins/{name}/reload - reload
func (s *Server) handlePluginAction(w http.ResponseWriter, r *http.Request) {
	// Extract plugin name from path
	path := r.URL.Path[len("/api/plugins/"):]
	if path == "" {
		http.Error(w, "plugin name required", http.StatusBadRequest)
		return
	}

	// Check for sub-action
	var action string
	var name string

	if len(path) > 8 && path[len(path)-7:] == "/reload" {
		action = "reload"
		name = path[:len(path)-7]
	} else {
		name = path
	}

	params := map[string]interface{}{"name": name}

	switch r.Method {
	case "GET":
		// Get plugin info - list and find
		resp := s.manager.HandleAPI("list", nil)
		s.writeJSON(w, resp)

	case "DELETE":
		// Unload plugin
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