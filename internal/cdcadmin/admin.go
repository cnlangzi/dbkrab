package cdcadmin

import (
	"database/sql"
	"fmt"

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
func (a *Admin) ListTables() ([]TableInfo, error) {
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

	var tables []TableInfo
	for rows.Next() {
		var ti TableInfo
		var cdcEnabled int
		if err := rows.Scan(&ti.Schema, &ti.Name, &cdcEnabled); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		ti.CDCEnabled = cdcEnabled == 1
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
	query := `EXEC sys.sp_cdc_enable_table 
		@source_schema = @p1, 
		@source_name = @p2, 
		@role_name = NULL, 
		@supports_net_changes = 0`

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
