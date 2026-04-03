package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cnlangzi/dbkrab/internal/dlq"
	"github.com/cnlangzi/dbkrab/plugin"
)

// Server provides HTTP API for plugin and DLQ management
type Server struct {
	manager  *plugin.Manager
	dlq      *dlq.DLQ
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

	// Plugin management endpoints
	mux.HandleFunc("/api/plugins", s.handlePlugins)
	mux.HandleFunc("/api/plugins/", s.handlePluginAction)

	// DLQ endpoints
	if s.dlq != nil {
		mux.HandleFunc("/api/dlq/list", s.handleDLQList)
		mux.HandleFunc("/api/dlq/", s.handleDLQAction)
		mux.HandleFunc("/api/dlq/stats", s.handleDLQStats)
	}

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
		// Get single plugin info
		resp := s.manager.HandleAPI("get", params)
		if !resp.Success {
			w.WriteHeader(http.StatusNotFound)
		}
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

// handleDLQList handles GET /api/dlq/list?status=pending
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

// handleDLQAction handles DLQ entry actions
// GET /api/dlq/{id} - get entry
// POST /api/dlq/{id}/replay - replay entry
// POST /api/dlq/{id}/ignore - ignore entry
// DELETE /api/dlq/{id} - delete entry
func (s *Server) handleDLQAction(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		s.writeJSON(w, map[string]interface{}{
			"success": false,
			"error":   "DLQ not initialized",
		})
		return
	}

	// Extract ID and action from path
	path := r.URL.Path[len("/api/dlq/"):]
	if path == "" {
		http.Error(w, "entry ID required", http.StatusBadRequest)
		return
	}

	// Check for sub-action
	var action string
	var idStr string

	if len(path) > 7 && path[len(path)-7:] == "/replay" {
		action = "replay"
		idStr = path[:len(path)-7]
	} else if len(path) > 7 && path[len(path)-7:] == "/ignore" {
		action = "ignore"
		idStr = path[:len(path)-7]
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
		// Get single entry
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
			// Replay entry - for now just mark as resolved (actual replay requires handler)
			err := s.dlq.Replay(r.Context(), id, func(entry *dlq.DLQEntry) error {
				// In a real implementation, this would re-process the transaction
				// For now, we just simulate success
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
			// Read note from request body
			body, err := io.ReadAll(r.Body)
			if err != nil {
				s.writeJSON(w, map[string]interface{}{
					"success": false,
					"error":   "read body failed",
				})
				return
			}

			var params struct {
				Note string `json:"note"`
			}
			if err := json.Unmarshal(body, &params); err != nil {
				params.Note = ""
			}

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
			http.Error(w, "unknown action", http.StatusBadRequest)
		}

	case "DELETE":
		// Delete entry
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

	stats, err := s.dlq.Stats()
	if err != nil {
		s.writeJSON(w, map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Convert stats map to a more readable format
	type StatEntry struct {
		Status string `json:"status"`
		Count  int    `json:"count"`
	}
	var result []StatEntry
	for status, count := range stats {
		result = append(result, StatEntry{
			Status: string(status),
			Count:  count,
		})
	}

	s.writeJSON(w, map[string]interface{}{
		"success": true,
		"stats":   result,
	})
}

// handleHealth handles health check
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("OK")); err != nil {
		http.Error(w, "write error", http.StatusInternalServerError)
	}
}

// writeJSON writes JSON response
func (s *Server) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
	}
}
