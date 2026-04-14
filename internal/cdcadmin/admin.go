package cdcadmin

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/cnlangzi/dbkrab/internal/config"
	_ "github.com/denisenkom/go-mssqldb"
)

// Admin provides CDC administration operations
type Admin struct {
	cfg *config.MSSQLConfig
}

// NewAdmin creates a new CDC admin instance
func NewAdmin(cfg *config.MSSQLConfig) *Admin {
	return &Admin{cfg: cfg}
}

// TableInfo represents a table with CDC status
type TableInfo struct {
	Schema      string `json:"schema"`
	Name        string `json:"name"`
	CDCEnabled  bool   `json:"cdc_enabled"`
	Tracked     bool   `json:"tracked"` // Whether table is in config.tables
}

// DatabaseInfo represents a database with its tables
type DatabaseInfo struct {
	Name   string       `json:"name"`
	Tables []TableInfo  `json:"tables"`
}

// Connect creates a new database connection
func (a *Admin) Connect() (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"server=%s;port=%d;user id=%s;password=%s;database=%s;encrypt=disable",
		a.cfg.Host,
		a.cfg.Port,
		a.cfg.User,
		a.cfg.Password,
		a.cfg.Database,
	)
	return sql.Open("sqlserver", connStr)
}

// ListTables returns all user tables in the database with CDC status
// trackedTables is the list of tables from config file to mark as Tracked
func (a *Admin) ListTables(trackedTables []string) ([]TableInfo, error) {
	db, err := a.Connect()
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	query := `
		SELECT 
			s.name AS schema_name,
			t.name AS table_name,
			CASE WHEN c.name IS NOT NULL THEN 1 ELSE 0 END AS is_cdc_enabled
		FROM sys.tables t
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		LEFT JOIN sys.tables c ON t.object_id = c.object_id AND c.is_tracked_by_cdc = 1
		WHERE t.type = 'U' AND s.name NOT IN ('sys', 'cdc')
		ORDER BY s.name, t.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// Create a set of tracked tables for fast lookup
	trackedSet := make(map[string]bool)
	for _, t := range trackedTables {
		trackedSet[strings.ToLower(t)] = true
	}

	var tables []TableInfo
	for rows.Next() {
		var ti TableInfo
		var cdcEnabled int
		if err := rows.Scan(&ti.Schema, &ti.Name, &cdcEnabled); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		ti.CDCEnabled = cdcEnabled == 1
		// Check if this table is in the tracked list (case-insensitive)
		fullTable := fmt.Sprintf("%s.%s", ti.Schema, ti.Name)
		ti.Tracked = trackedSet[strings.ToLower(fullTable)]
		tables = append(tables, ti)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	return tables, nil
}

// GetCDCStatus returns CDC status for a specific table
func (a *Admin) GetCDCStatus(schema, table string) (bool, error) {
	db, err := a.Connect()
	if err != nil {
		return false, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	query := `
		SELECT is_tracked_by_cdc 
		FROM sys.tables 
		WHERE object_id = OBJECT_ID(@p1, 'U')
	`

	var isTracked bool
	err = db.QueryRow(query, schema+"."+table).Scan(&isTracked)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, fmt.Errorf("table not found: %s.%s", schema, table)
		}
		return false, fmt.Errorf("query: %w", err)
	}

	return isTracked, nil
}

// EnableCDC enables CDC for a specific table
func (a *Admin) EnableCDC(schema, table string) error {
	db, err := a.Connect()
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Check if CDC is enabled at database level
	var isCDCEnabled bool
	err = db.QueryRow("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()").Scan(&isCDCEnabled)
	if err != nil {
		return fmt.Errorf("check database CDC: %w", err)
	}
	if !isCDCEnabled {
		return fmt.Errorf("CDC is not enabled at database level")
	}

	// Enable CDC for the table
	// Note: This requires db_owner role
	// supports_net_changes = 1 ensures UPDATE operations only return one record
	// (operation=4 UPDATE_AFTER), not two records (operation=3 UPDATE_BEFORE and 4)
	query := `EXEC sys.sp_cdc_enable_table 
		@source_schema = @p1, 
		@source_name = @p2, 
		@role_name = NULL, 
		@supports_net_changes = 1`

	_, err = db.Exec(query, schema, table)
	if err != nil {
		return fmt.Errorf("enable CDC: %w", err)
	}

	return nil
}

// DisableCDC disables CDC for a specific table
func (a *Admin) DisableCDC(schema, table string) error {
	db, err := a.Connect()
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	query := `EXEC sys.sp_cdc_disable_table 
		@source_schema = @p1, 
		@source_name = @p2, 
		@capture_instance = @p3`

	captureInstance := schema + "_" + table
	_, err = db.Exec(query, schema, table, captureInstance)
	if err != nil {
		return fmt.Errorf("disable CDC: %w", err)
	}

	return nil
}

// UpdateTrackedTables updates the list of tracked tables in config
func (a *Admin) UpdateTrackedTables(tables []string) error {
	// This will be handled by config hotreload
	// The API will write to config file and trigger reload
	return nil
}

// CDCStatus represents the CDC status of a table
type CDCStatus struct {
	Schema          string
	Table           string
	CaptureInstance string
	CDCEnabled      bool
	NeedsEnable     bool
	EnableError     string // Error message if enabling CDC failed
}

// CheckAndEnableCDC checks if CDC is enabled for configured tables and enables it if needed
// Returns status for each table and any errors encountered
func (a *Admin) CheckAndEnableCDC(configuredTables []string) ([]CDCStatus, error) {
	db, err := a.Connect()
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	// First check if CDC is enabled at database level
	var isCDCEnabled bool
	err = db.QueryRow("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()").Scan(&isCDCEnabled)
	if err != nil {
		return nil, fmt.Errorf("check database CDC status: %w", err)
	}

	// Check if user has db_owner role
	var isDBOwner bool
	err = db.QueryRow("SELECT IS_MEMBER('db_owner')").Scan(&isDBOwner)
	if err != nil {
		slog.Warn("failed to check db_owner role", "error", err)
	}

	if !isCDCEnabled {
		// Enable CDC at database level
		_, err = db.Exec("EXEC sys.sp_cdc_enable_db")
		if err != nil {
			return nil, fmt.Errorf("enable database CDC: %w", err)
		}
	}

	var statuses []CDCStatus
	var enableErrors []string

	for _, table := range configuredTables {
		schema, tableName := parseTableName(table)
		captureInstance := schema + "_" + tableName

		status := CDCStatus{
			Schema:          schema,
			Table:           tableName,
			CaptureInstance: captureInstance,
		}

		// Check if CDC is enabled for this table and whether supports_net_changes = 1
		checkQuery := `
			SELECT supports_net_changes
			FROM cdc.change_tables
			WHERE capture_instance = @p1
		`
		var supportsNetChanges int
		err = db.QueryRow(checkQuery, captureInstance).Scan(&supportsNetChanges)
		if err == sql.ErrNoRows {
			// Capture instance does not exist yet
			status.CDCEnabled = false
			status.NeedsEnable = true
		} else if err != nil {
			status.CDCEnabled = false
			status.NeedsEnable = true
		} else {
			status.CDCEnabled = true
			// Force recreate if supports_net_changes = 0
			status.NeedsEnable = supportsNetChanges != 1
		}

		// Enable (or recreate) CDC if needed
		if status.NeedsEnable {
			// If capture instance already exists but has wrong config, disable it first
			if status.CDCEnabled {
				slog.Info("recreating CDC capture instance to enable supports_net_changes=1",
					"table", schema+"."+tableName,
					"capture_instance", captureInstance)
				disableQuery := `
					EXEC sys.sp_cdc_disable_table
						@source_schema = @schema,
						@source_name = @table,
						@capture_instance = @capture_instance
				`
				_, disableErr := db.Exec(disableQuery,
					sql.Named("schema", schema),
					sql.Named("table", tableName),
					sql.Named("capture_instance", captureInstance))
				if disableErr != nil {
					enableErrors = append(enableErrors, fmt.Sprintf("%s.%s disable: %v", schema, tableName, disableErr))
					status.EnableError = disableErr.Error()
					statuses = append(statuses, status)
					continue
				}
			}

			enableQuery := `
				EXEC sys.sp_cdc_enable_table
					@source_schema = @schema,
					@source_name = @table,
					@role_name = NULL,
					@supports_net_changes = 1
			`
			_, err = db.Exec(enableQuery, sql.Named("schema", schema), sql.Named("table", tableName))
			if err != nil {
				// Record error but continue checking other tables
				enableErrors = append(enableErrors, fmt.Sprintf("%s.%s: %v", schema, tableName, err))
				status.EnableError = err.Error()
			} else {
				status.CDCEnabled = true
				status.NeedsEnable = false
			}
		}

		statuses = append(statuses, status)
	}

	// Log permission status
	if len(enableErrors) > 0 && !isDBOwner {
		slog.Warn("CDC auto-enable failed - user does not have db_owner role", 
			"is_db_owner", isDBOwner,
			"errors", strings.Join(enableErrors, "; "))
	}

	// If there were enable errors, return them as a combined message
	if len(enableErrors) > 0 {
		return statuses, fmt.Errorf("CDC enable errors: %s", strings.Join(enableErrors, "; "))
	}

	return statuses, nil
}

// parseTableName extracts schema and table name from "schema.table" format
func parseTableName(fullName string) (string, string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "dbo", parts[0]
}

// CDCJobInfo represents CDC job configuration
// DefaultRetentionMinutes is the recommended CDC retention (7 days = 10080 minutes)
const DefaultRetentionMinutes = 10080

type CDCJobInfo struct {
	JobType        string
	JobName        string
	Retention      int // in minutes
	PollingInterval int
}

// CheckAndSetCDCRetention checks CDC job retention and sets it to 7 days if needed
// Returns current retention value and any changes made
func (a *Admin) CheckAndSetCDCRetention() (int, error) {
	db, err := a.Connect()
	if err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Get current CDC job configuration
	rows, err := db.Query("EXEC sp_cdc_help_jobs")
	if err != nil {
		return 0, fmt.Errorf("query CDC jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var currentRetention int
	var jobName string
	
	// Find cleanup job
	for rows.Next() {
		var jobID []byte
		var jobType, name string
		var maxtrans, maxscans int
		var continuous bool
		var pollinginterval, retention, threshold int
		
		err = rows.Scan(&jobID, &jobType, &name, &maxtrans, &maxscans, 
			&continuous, &pollinginterval, &retention, &threshold)
		if err != nil {
			return 0, fmt.Errorf("scan job info: %w", err)
		}
		
		if jobType == "cleanup" {
			currentRetention = retention
			jobName = name
			break
		}
	}

	// Check if retention needs to be updated
	if currentRetention != DefaultRetentionMinutes {
		slog.Info("updating CDC cleanup job retention",
			"current", currentRetention,
			"recommended", DefaultRetentionMinutes,
			"job_name", jobName)
		
		_, err = db.Exec(`
			EXEC sp_cdc_change_job 
				@job_type = N'cleanup', 
				@retention = @retention
		`, sql.Named("retention", DefaultRetentionMinutes))
		if err != nil {
			return currentRetention, fmt.Errorf("update CDC retention: %w", err)
		}
		
		slog.Info("CDC cleanup job retention updated successfully",
			"old_retention_minutes", currentRetention,
			"new_retention_minutes", DefaultRetentionMinutes)
		return DefaultRetentionMinutes, nil
	}

	slog.Info("CDC cleanup job retention already configured correctly",
		"retention_minutes", currentRetention,
		"retention_days", float64(currentRetention)/1440)

	return currentRetention, nil
}
