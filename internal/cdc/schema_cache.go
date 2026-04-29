package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"log/slog"
)

// validTableName validates table name to prevent SQL injection
var validTableName = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// TableSchema represents primary key information for a table
type TableSchema struct {
	TableName string
	Columns  []string // Primary key columns in order
}

// TableSchemaCache caches primary key information from MSSQL
type TableSchemaCache struct {
	mu     sync.RWMutex
	schema map[string]TableSchema // keyed by "schema.table"
	db     *sql.DB
}

// NewTableSchemaCache creates a new schema cache
func NewTableSchemaCache(db *sql.DB) *TableSchemaCache {
	return &TableSchemaCache{
		db:     db,
		schema: make(map[string]TableSchema),
	}
}

// Load fetches primary key information for the given tables
// tableNames should be in "schema.table" or "table" format
func (c *TableSchemaCache) Load(ctx context.Context, tableNames []string) error {
	if len(tableNames) == 0 {
		return nil
	}

	// Convert all table names to schema.table format
	fullNames := make([]string, len(tableNames))
	for i, name := range tableNames {
		schema, table := ParseTableName(name)
		fullNames[i] = fmt.Sprintf("%s.%s", schema, table)
	}

	// Build query with IN clause
	placeholders := make([]string, len(fullNames))
	args := make([]interface{}, len(fullNames))
	for i, name := range fullNames {
		// Validate table name
		if !validTableName.MatchString(name) {
			return fmt.Errorf("invalid table name: %s", name)
		}
		placeholders[i] = "?"
		args[i] = name
	}

	query := fmt.Sprintf(`
		SELECT t.name AS table_name, c.name AS column_name
		FROM sys.tables t
		INNER JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.key_ordinal = 1
		INNER JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		WHERE i.is_primary_key = 1
		AND t.name IN (%s)
		ORDER BY t.name, ic.key_ordinal
	`, strings.Join(placeholders, ","))

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query primary keys: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Warn("TableSchemaCache: rows.Close error", "error", err)
		}
	}()

	// Group results by table
	results := make(map[string]TableSchema)
	for rows.Next() {
		var tableName, columnName string
		if err := rows.Scan(&tableName, &columnName); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}

		if _, ok := results[tableName]; !ok {
			results[tableName] = TableSchema{
				TableName: tableName,
				Columns:   []string{},
			}
		}
		schema := results[tableName]
		schema.Columns = append(schema.Columns, columnName)
		results[tableName] = schema
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}

	// Update cache
	c.mu.Lock()
	defer c.mu.Unlock()

	// Build map of tableName to schema prefix from fullNames lookup
	tableToSchema := make(map[string]string)
	for _, fullName := range fullNames {
		schema, table := ParseTableName(fullName)
		tableToSchema[table] = schema
	}

	// Clear and rebuild, using full schema.table format as key
	c.schema = make(map[string]TableSchema)
	for _, s := range results {
		// Use schema.table format as key to avoid collision across schemas
		schemaPref := tableToSchema[s.TableName]
		if schemaPref == "" {
			schemaPref = "dbo" // default
		}
		key := fmt.Sprintf("%s.%s", schemaPref, s.TableName)
		c.schema[key] = s
	}

	slog.Info("TableSchemaCache: loaded primary keys",
		"tables", len(results),
		"table_names", fullNames)

	return nil
}

// Get returns the primary key columns for a table
// tableName can be in "schema.table" or just "table" format
func (c *TableSchemaCache) Get(tableName string) ([]string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Normalize to tableName format
	schema, table := ParseTableName(tableName)
	key := table // Try table name first

	// Check if we have it as-is
	if s, ok := c.schema[key]; ok {
		return s.Columns, true
	}

	// Try with schema prefix
	key = fmt.Sprintf("%s.%s", schema, table)
	if s, ok := c.schema[key]; ok {
		return s.Columns, true
	}

	// Also try looking through all entries
	for _, s := range c.schema {
		if s.TableName == table || s.TableName == tableName {
			return s.Columns, true
		}
	}

	return nil, false
}

// GetSchema returns the full TableSchema for a table
func (c *TableSchemaCache) GetSchema(tableName string) (TableSchema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, table := ParseTableName(tableName)
	key := table

	if s, ok := c.schema[key]; ok {
		return s, true
	}

	key = fmt.Sprintf("%s.%s", schema, table)
	if s, ok := c.schema[key]; ok {
		return s, true
	}

	for _, s := range c.schema {
		if s.TableName == table || s.TableName == tableName {
			return s, true
		}
	}

	return TableSchema{}, false
}

// ColumnCount returns the number of primary key columns for a table
func (c *TableSchemaCache) ColumnCount(tableName string) int {
	cols, ok := c.Get(tableName)
	if !ok {
		return 0
	}
	return len(cols)
}

// All returns all cached schemas
func (c *TableSchemaCache) All() map[string]TableSchema {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy
	result := make(map[string]TableSchema, len(c.schema))
	for k, v := range c.schema {
		cols := make([]string, len(v.Columns))
		copy(cols, v.Columns)
		result[k] = TableSchema{
			TableName: v.TableName,
			Columns:   cols,
		}
	}
	return result
}