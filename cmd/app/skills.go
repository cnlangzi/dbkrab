package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/cnlangzi/dbkrab/internal/json"
	"github.com/cnlangzi/dbkrab/plugin/sql"
	"github.com/yaitoo/xun"
	"gopkg.in/yaml.v3"
)

// SkillFileInfo represents a file in the skills directory
type SkillFileInfo struct {
	Name    string `json:"name"`
	Path    string `json:"path"`
	Type    string `json:"type"` // "file" or "dir"
	Size    int64  `json:"size,omitempty"`
	ModTime string `json:"mod_time,omitempty"`
	IsSQL   bool   `json:"is_sql,omitempty"`
	IsYAML  bool   `json:"is_yaml,omitempty"`
}

// SkillListResponse represents the response for skill list
type SkillListResponse struct {
	Success bool        `json:"success"`
	Skills  []SkillInfo `json:"skills"`
	Count   int         `json:"count"`
	Error   string      `json:"error,omitempty"`
}

// SkillInfo contains skill metadata
type SkillInfo struct {
	Name        string   `json:"name"`
	Id          string   `json:"id"`   // SHA256(file)[:12]
	File        string   `json:"file"` // Relative path from config.plugins.sql.path
	Description string   `json:"description"`
	Tables      []string `json:"tables"`
	Status      string   `json:"status"` // loaded, error, not_loaded
	Version     string   `json:"version"`
	LoadTime    string   `json:"load_time,omitempty"`
	Error       string   `json:"error,omitempty"`
}

// CreateSkillRequest represents a request to create a new skill
type CreateSkillRequest struct {
	Name    string `json:"name"`    // File name (without .yml suffix)
	Content string `json:"content"` // YAML content
}

// CreateFolderRequest represents a request to create a new folder
type CreateFolderRequest struct {
	Name string `json:"name"`
}

// SaveSkillRequest represents a request to save a skill
type SaveSkillRequest struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

// ValidateSkillRequest represents a request to validate skill content
type ValidateSkillRequest struct {
	Content string `json:"content"`
	Type    string `json:"type"` // "yaml" or "sql"
}

// ValidateSkillResponse represents the response for validation
type ValidateSkillResponse struct {
	Success  bool     `json:"success"`
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// handleSkillsList handles GET /api/skills/list
func (s *Server) handleSkillsList(c *xun.Context) error {
	if s.manager == nil {
		return c.View(SkillListResponse{
			Success: false,
			Error:   "Plugin manager not initialized",
		})
	}

	plugins := s.manager.List()
	skills := make([]SkillInfo, 0, len(plugins))

	for _, p := range plugins {
		if p.Type != "sql" {
			continue
		}

		// Get skill from memory via HandleAPI
		resp := s.manager.HandleAPI("get_skill", map[string]any{"id": p.Id})
		if !resp.Success {
			continue
		}

		skill, ok := resp.Data.(*sql.Skill)
		if !ok {
			continue
		}

		skillInfo := SkillInfo{
			Name:        skill.Name,
			Id:          skill.Id,
			File:        skill.File,
			Description: skill.Description,
			Tables:      skill.On,
		}

		// Set status based on Error field
		if skill.Error != "" {
			skillInfo.Status = "error"
			skillInfo.Error = skill.Error
		} else {
			skillInfo.Status = "loaded"
		}

		// Set load time from LastLoadedAt (if available)
		if !skill.LastLoadedAt.IsZero() {
			skillInfo.LoadTime = skill.LastLoadedAt.Format("2006-01-02 15:04:05")
		}

		skills = append(skills, skillInfo)
	}

	// Sort by name
	sort.Slice(skills, func(i, j int) bool {
		return skills[i].Name < skills[j].Name
	})

	return c.View(SkillListResponse{
		Success: true,
		Skills:  skills,
		Count:   len(skills),
	})
}

// handleSkillsFilesHTML handles GET /api/skills/files/html
// Returns HTML fragment for HTMX (like handleOverview pattern)
func (s *Server) handleSkillsFilesHTML(c *xun.Context) error {
	skillsDir := "skills"

	entries, err := os.ReadDir(skillsDir)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Failed to read skills directory: " + err.Error(),
		})
	}

	files := make([]SkillFileInfo, 0)

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		fileInfo := SkillFileInfo{
			Name:    entry.Name(),
			Path:    filepath.Join(skillsDir, entry.Name()),
			ModTime: info.ModTime().Format(time.RFC3339),
		}

		if entry.IsDir() {
			fileInfo.Type = "dir"
		} else {
			fileInfo.Type = "file"
			fileInfo.Size = info.Size()

			if strings.HasSuffix(entry.Name(), ".sql") {
				fileInfo.IsSQL = true
			} else if strings.HasSuffix(entry.Name(), ".yml") || strings.HasSuffix(entry.Name(), ".yaml") {
				fileInfo.IsYAML = true
			}
		}

		files = append(files, fileInfo)
	}

	// Sort: directories first, then files, alphabetically
	sort.Slice(files, func(i, j int) bool {
		if files[i].Type != files[j].Type {
			return files[i].Type == "dir"
		}
		return files[i].Name < files[j].Name
	})

	// Render HTML fragment
	html := renderSkillsFilesHTML(files)
	c.WriteHeader("Content-Type", "text/html; charset=utf-8")
	_, err = c.Response.Write([]byte(html))
	return err
}

// renderSkillsFilesHTML renders file list as HTML fragment
func renderSkillsFilesHTML(files []SkillFileInfo) string {
	if len(files) == 0 {
		return `<div class="p-8 text-center">
			<svg class="w-16 h-16 mx-auto mb-4 text-textMuted opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
				<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"/>
			</svg>
			<p class="text-textMuted">No files</p>
		</div>`
	}

	var html strings.Builder
	for _, file := range files {
		icon := "📄"
		typeLabel := "File"
		typeClass := "bg-surfaceHover text-textMuted"

		if file.Type == "dir" {
			icon = "📁"
			typeLabel = "Folder"
			typeClass = "bg-primary/20 text-primary"
		} else if file.IsSQL {
			icon = "📜"
			typeLabel = "SQL"
			typeClass = "bg-success/20 text-success"
		} else if file.IsYAML {
			icon = "📝"
			typeLabel = "YAML"
			typeClass = "bg-warning/20 text-warning"
		}

		sizeInfo := ""
		if file.Type == "file" {
			sizeInfo = fmt.Sprintf("<p class=\"text-xs text-textMuted\">%d bytes</p>", file.Size)
		}

		fmt.Fprintf(&html, `
		<div class="p-4 hover:bg-surfaceHover transition-colors">
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-3 min-w-0 flex-1">
					<span class="text-2xl flex-shrink-0">%s</span>
					<div class="min-w-0 flex-1">
						<p class="font-medium text-text truncate">%s</p>
						%s
					</div>
				</div>
				<span class="px-2 py-1 rounded text-xs font-medium %s flex-shrink-0 ml-2">%s</span>
			</div>
		</div>`, icon, file.Name, sizeInfo, typeClass, typeLabel)
	}

	return html.String()
}

// Returns JSON for API clients
func (s *Server) handleSkillsFiles(c *xun.Context) error {
	skillsDir := "skills"

	entries, err := os.ReadDir(skillsDir)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Failed to read skills directory: " + err.Error(),
		})
	}

	files := make([]SkillFileInfo, 0)

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		fileInfo := SkillFileInfo{
			Name:    entry.Name(),
			Path:    filepath.Join(skillsDir, entry.Name()),
			ModTime: info.ModTime().Format(time.RFC3339),
		}

		if entry.IsDir() {
			fileInfo.Type = "dir"
		} else {
			fileInfo.Type = "file"
			fileInfo.Size = info.Size()

			if strings.HasSuffix(entry.Name(), ".sql") {
				fileInfo.IsSQL = true
			} else if strings.HasSuffix(entry.Name(), ".yml") || strings.HasSuffix(entry.Name(), ".yaml") {
				fileInfo.IsYAML = true
			}
		}

		files = append(files, fileInfo)
	}

	// Sort: directories first, then files, alphabetically
	sort.Slice(files, func(i, j int) bool {
		if files[i].Type != files[j].Type {
			return files[i].Type == "dir"
		}
		return files[i].Name < files[j].Name
	})

	return c.View(map[string]any{
		"success": true,
		"files":   files,
		"count":   len(files),
	})
}

// handleSkillGet handles GET /api/skills/{id}
// The id parameter is the skill ID (SHA256(file)[:12])
func (s *Server) handleSkillGet(c *xun.Context) error {
	id := c.Request.PathValue("id")
	if id == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "Skill ID required",
		})
	}

	if s.manager == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Plugin manager not initialized",
		})
	}

	// Get skill from memory via HandleAPI
	resp := s.manager.HandleAPI("get_skill", map[string]any{"id": id})
	if !resp.Success {
		return c.View(map[string]any{
			"success": false,
			"error":   resp.Error,
		})
	}

	skill, ok := resp.Data.(*sql.Skill)
	if !ok {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid skill data",
		})
	}

	// Calculate version hash from raw content
	hash := sha256.Sum256([]byte(skill.Raw))
	version := hex.EncodeToString(hash[:])

	return c.View(map[string]any{
		"success": true,
		"name":    skill.Name,
		"file":    skill.File,
		"id":      skill.Id,
		"content": skill.Raw,
		"version": version,
	})
}

// handleSkillCreate handles POST /api/skills
func (s *Server) handleSkillCreate(c *xun.Context) error {
	var req CreateSkillRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	// Validate name
	if req.Name == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "File name is required",
		})
	}

	// Validate name format (alphanumeric, underscore, hyphen, must start with letter)
	if !isValidSkillName(req.Name) {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid file name. Use only letters, numbers, underscores, and hyphens. Must start with a letter.",
		})
	}

	// Validate YAML content
	if req.Content == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "YAML content is required",
		})
	}

	// Parse and validate YAML syntax
	var skill sql.Skill
	if err := yaml.Unmarshal([]byte(req.Content), &skill); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "YAML syntax error: " + err.Error(),
		})
	}

	// Validate required fields
	if skill.Name == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "Field 'name' is required in YAML",
		})
	}

	if len(skill.On) == 0 {
		return c.View(map[string]any{
			"success": false,
			"error":   "Field 'on' must contain at least one table",
		})
	}

	// Validate sinks configuration
	if err := skill.ValidateSinks(); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Sinks configuration error: " + err.Error(),
		})
	}

	// Check if skill file already exists
	sqlPath := "skills/sql" // default
	if s.config != nil && s.config.Plugins.SQL.Path != "" {
		sqlPath = s.config.Plugins.SQL.Path
	}
	skillPath := filepath.Join(sqlPath, req.Name+".yml")
	if _, err := os.Stat(skillPath); err == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Skill file already exists: " + req.Name + ".yml",
		})
	}

	// Write skill file
	if err := os.WriteFile(skillPath, []byte(req.Content), 0644); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Failed to create skill file: " + err.Error(),
		})
	}

	// Skill will be auto-reloaded by the internal file watcher (StartWatch)
	if s.manager != nil {
		slog.Info("Skill created, file watcher will auto-reload", "skill", req.Name, "file", skillPath)
	}

	return c.View(map[string]any{
		"success": true,
		"message": "Skill created successfully",
		"name":    req.Name,
	})
}

// handleSkillSave handles POST /api/skills/{id}/save
// The id parameter is the skill ID (SHA256(file)[:12])
func (s *Server) handleSkillSave(c *xun.Context) error {
	id := c.Request.PathValue("id")
	if id == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "Skill ID required",
		})
	}

	if s.manager == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Plugin manager not initialized",
		})
	}

	// Get existing skill from memory to find file path
	resp := s.manager.HandleAPI("get_skill", map[string]any{"id": id})
	if !resp.Success {
		return c.View(map[string]any{
			"success": false,
			"error":   resp.Error,
		})
	}

	skill, ok := resp.Data.(*sql.Skill)
	if !ok {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid skill data",
		})
	}

	filePath := skill.File

	var req SaveSkillRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	// Validate YAML syntax before saving
	var newSkill sql.Skill
	if err := yaml.Unmarshal([]byte(req.Content), &newSkill); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid YAML syntax: " + err.Error(),
		})
	}

	// Validate required fields
	if newSkill.Name == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "Skill name is required in YAML",
		})
	}

	if len(newSkill.On) == 0 {
		return c.View(map[string]any{
			"success": false,
			"error":   "At least one table must be specified in 'on' field",
		})
	}

	// Security: ensure path is within skills/sql directory
	// filePath is relative to config.plugins.sql.path (e.g., "cost.yml")
	sqlPath := "skills/sql" // default
	if s.config != nil && s.config.Plugins.SQL.Path != "" {
		sqlPath = s.config.Plugins.SQL.Path
	}
	skillPath := filepath.Join(sqlPath, filePath)
	cleanPath := filepath.Clean(skillPath)
	cleanSqlPath := filepath.Clean(sqlPath)
	if !strings.HasPrefix(cleanPath, cleanSqlPath) {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid path: must be within skills/sql directory",
		})
	}

	// Write skill file
	if err := os.WriteFile(skillPath, []byte(req.Content), 0644); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Failed to save skill: " + err.Error(),
		})
	}

	// Skill will be auto-reloaded by the internal file watcher (StartWatch)
	slog.Info("Skill saved, file watcher will auto-reload", "id", id, "file", filePath)

	return c.View(map[string]any{
		"success": true,
		"message": "Skill saved successfully",
	})
}

// handleSkillDelete handles DELETE /api/skills/{id}
// The id parameter is the skill ID (SHA256(file)[:12])
func (s *Server) handleSkillDelete(c *xun.Context) error {
	id := c.Request.PathValue("id")
	if id == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "Skill ID required",
		})
	}

	if s.manager == nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Plugin manager not initialized",
		})
	}

	// Get existing skill from memory to find file path
	resp := s.manager.HandleAPI("get_skill", map[string]any{"id": id})
	if !resp.Success {
		return c.View(map[string]any{
			"success": false,
			"error":   resp.Error,
		})
	}

	skill, ok := resp.Data.(*sql.Skill)
	if !ok {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid skill data",
		})
	}

	filePath := skill.File

	// Security: ensure path is within skills/sql directory
	sqlPath := "skills/sql" // default
	if s.config != nil && s.config.Plugins.SQL.Path != "" {
		sqlPath = s.config.Plugins.SQL.Path
	}
	skillPath := filepath.Join(sqlPath, filePath)
	cleanPath := filepath.Clean(skillPath)
	cleanSqlPath := filepath.Clean(sqlPath)
	if !strings.HasPrefix(cleanPath, cleanSqlPath) {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid path: must be within skills/sql directory",
		})
	}

	// Check if file exists
	if _, err := os.Stat(skillPath); os.IsNotExist(err) {
		return c.View(map[string]any{
			"success": false,
			"error":   "Skill file not found",
		})
	}

	// Delete skill file
	if err := os.Remove(skillPath); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Failed to delete skill: " + err.Error(),
		})
	}

	slog.Info("Skill file deleted, will be removed from memory by file watcher", "id", id)

	return c.View(map[string]any{
		"success": true,
		"message": "Skill deleted successfully",
	})
}

// handleSkillValidate handles POST /api/skills/validate
func (s *Server) handleSkillValidate(c *xun.Context) error {
	var req ValidateSkillRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	response := ValidateSkillResponse{
		Success:  true,
		Valid:    true,
		Errors:   []string{},
		Warnings: []string{},
	}

	switch req.Type {
	case "yaml":
		// Validate YAML syntax
		var skill sql.Skill
		if err := yaml.Unmarshal([]byte(req.Content), &skill); err != nil {
			response.Valid = false
			response.Errors = append(response.Errors, "YAML syntax error: "+err.Error())
			break
		}

		// Validate required fields
		if skill.Name == "" {
			response.Valid = false
			response.Errors = append(response.Errors, "Field 'name' is required")
		}

		if len(skill.On) == 0 {
			response.Valid = false
			response.Errors = append(response.Errors, "Field 'on' must contain at least one table")
		}

		// Validate SQL syntax in sinks
		for _, sink := range skill.Sinks {
			if sink.SQL != "" {
				// Basic SQL validation - check for common syntax issues
				if err := validateSQLBasic(sink.SQL); err != nil {
					response.Errors = append(response.Errors, fmt.Sprintf("Sink SQL error: %s", err.Error()))
					response.Valid = false
				}
			}
		}

		// Validate sinks configuration
		if err := skill.ValidateSinks(); err != nil {
			response.Valid = false
			response.Errors = append(response.Errors, fmt.Sprintf("Sinks configuration error: %s", err.Error()))
		}

		// Add warnings for potential issues
		if skill.Description == "" {
			response.Warnings = append(response.Warnings, "Consider adding a description")
		}

	case "sql":
		// Validate SQL syntax (basic validation, no DB connection)
		if err := validateSQLBasic(req.Content); err != nil {
			response.Valid = false
			response.Errors = append(response.Errors, err.Error())
		}

	default:
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid validation type. Must be 'yaml' or 'sql'",
		})
	}

	return c.View(response)
}

// handleSkillFileGet handles GET /api/skills/file/{path...}
func (s *Server) handleSkillFileGet(c *xun.Context) error {
	filePath := c.Request.PathValue("path")
	if filePath == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "File path required",
		})
	}

	// Security: prevent path traversal
	// filePath should be like "sql/cost.yml" (includes subdirectory)
	sqlPath := "skills" // base skills directory
	cleanPath := filepath.Clean(filepath.Join(sqlPath, filePath))
	if !strings.HasPrefix(cleanPath, filepath.Clean(sqlPath)) {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid path: must be within skills directory",
		})
	}

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Failed to read file: " + err.Error(),
		})
	}

	return c.View(map[string]any{
		"success": true,
		"content": string(data),
		"path":    filePath,
	})
}

// handleSkillFileSave handles POST /api/skills/file/{path}/save
func (s *Server) handleSkillFileSave(c *xun.Context) error {
	filePath := c.Request.PathValue("path")
	if filePath == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "File path required",
		})
	}

	var req SaveSkillRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	// Security: prevent path traversal
	// filePath should be like "sql/cost.yml" (includes subdirectory)
	skillsDir := "skills" // base skills directory
	cleanPath := filepath.Clean(filepath.Join(skillsDir, filePath))
	if !strings.HasPrefix(cleanPath, filepath.Clean(skillsDir)) {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid path: must be within skills directory",
		})
	}

	// Validate SQL syntax if it's a .sql file
	if strings.HasSuffix(filePath, ".sql") {
		if err := validateSQLBasic(req.Content); err != nil {
			return c.View(map[string]any{
				"success": false,
				"error":   "Invalid SQL syntax: " + err.Error(),
			})
		}
	}

	// Write file
	if err := os.WriteFile(cleanPath, []byte(req.Content), 0644); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Failed to save file: " + err.Error(),
		})
	}

	return c.View(map[string]any{
		"success": true,
		"message": "File saved successfully",
	})
}

// handleFolderCreate handles POST /api/skills/folder
func (s *Server) handleFolderCreate(c *xun.Context) error {
	var req CreateFolderRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	if req.Name == "" {
		return c.View(map[string]any{
			"success": false,
			"error":   "Folder name is required",
		})
	}

	// Validate folder name
	if !isValidSkillName(req.Name) {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid folder name",
		})
	}

	// Security: ensure path is within skills directory
	// This allows creating subdirectories within skills
	skillsDir := "skills"
	folderPath := filepath.Join(skillsDir, req.Name)
	cleanPath := filepath.Clean(folderPath)
	if !strings.HasPrefix(cleanPath, filepath.Clean(skillsDir)) {
		return c.View(map[string]any{
			"success": false,
			"error":   "Invalid path: must be within skills directory",
		})
	}

	// Create folder
	if err := os.MkdirAll(cleanPath, 0755); err != nil {
		return c.View(map[string]any{
			"success": false,
			"error":   "Failed to create folder: " + err.Error(),
		})
	}

	return c.View(map[string]any{
		"success": true,
		"message": "Folder created successfully",
	})
}

// handleSkillSaveHTML handles POST /skills/{id}/save/html - HTML fragment for htmx
func (s *Server) handleSkillSaveHTML(c *xun.Context) error {
	id := c.Request.PathValue("id")
	if id == "" {
		return writeHTML(c, "<p class='text-error'>Skill ID required</p>")
	}

	if s.manager == nil {
		return writeHTML(c, "<p class='text-error'>Plugin manager not initialized</p>")
	}

	// Get content from form data
	content := c.Request.FormValue("content")
	if content == "" {
		return writeHTML(c, "<p class='text-error'>Content is required</p>")
	}

	// Get existing skill
	resp := s.manager.HandleAPI("get_skill", map[string]any{"id": id})
	if !resp.Success {
		return writeHTML(c, "<p class='text-error'>Skill not found: "+resp.Error+"</p>")
	}

	skill, ok := resp.Data.(*sql.Skill)
	if !ok {
		return writeHTML(c, "<p class='text-error'>Invalid skill data</p>")
	}

	filePath := skill.File

	// Validate YAML
	var newSkill sql.Skill
	if err := yaml.Unmarshal([]byte(content), &newSkill); err != nil {
		return writeHTML(c, "<p class='text-error'>Invalid YAML: "+err.Error()+"</p>")
	}

	if newSkill.Name == "" {
		return writeHTML(c, "<p class='text-error'>Skill name required</p>")
	}

	if len(newSkill.On) == 0 {
		return writeHTML(c, "<p class='text-error'>At least one table required</p>")
	}

	// Security: ensure path is within skills/sql directory
	sqlPath := "skills/sql" // default
	if s.config != nil && s.config.Plugins.SQL.Path != "" {
		sqlPath = s.config.Plugins.SQL.Path
	}
	skillPath := filepath.Join(sqlPath, filePath)
	cleanPath := filepath.Clean(skillPath)
	cleanSqlPath := filepath.Clean(sqlPath)
	if !strings.HasPrefix(cleanPath, cleanSqlPath) {
		return writeHTML(c, "<p class='text-error'>Invalid path</p>")
	}

	// Write file
	if err := os.WriteFile(skillPath, []byte(content), 0644); err != nil {
		return writeHTML(c, "<p class='text-error'>Failed to save: "+err.Error()+"</p>")
	}

	slog.Info("Skill saved via HTML endpoint", "id", id, "file", filePath)

	return writeHTML(c, "<p class='text-success'>✓ Saved! Redirecting...</p><script>setTimeout(function(){window.location.href='/skills';},1500);</script>")
}

// writeHTML writes HTML fragment to response
func writeHTML(c *xun.Context, html string) error {
	c.Response.Header().Set("Content-Type", "text/html; charset=utf-8")
	c.Response.WriteHeader(http.StatusOK)
	//nolint:errcheck
	c.Response.Write([]byte(html))
	return nil
}

// isValidSkillName validates skill/folder names
func isValidSkillName(name string) bool {
	if name == "" {
		return false
	}

	// Must start with a letter
	if !unicode.IsLetter(rune(name[0])) {
		return false
	}

	// Only allow letters, numbers, underscores, and hyphens
	for _, c := range name {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' && c != '-' {
			return false
		}
	}

	return true
}

// validateSQLBasic performs basic SQL validation without a database connection
func validateSQLBasic(sqlStr string) error {
	if strings.TrimSpace(sqlStr) == "" {
		return fmt.Errorf("SQL cannot be empty")
	}

	// Basic checks
	sqlUpper := strings.ToUpper(strings.TrimSpace(sqlStr))

	// Check for common SQL statement types
	validStarts := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "WITH"}
	hasValidStart := false
	for _, start := range validStarts {
		if strings.HasPrefix(sqlUpper, start) {
			hasValidStart = true
			break
		}
	}

	if !hasValidStart {
		return fmt.Errorf("SQL must start with a valid statement (SELECT, INSERT, UPDATE, DELETE, etc.)")
	}

	// Check for balanced parentheses
	openParens := strings.Count(sqlStr, "(")
	closeParens := strings.Count(sqlStr, ")")
	if openParens != closeParens {
		return fmt.Errorf("unbalanced parentheses: %d open, %d close", openParens, closeParens)
	}

	return nil
}
