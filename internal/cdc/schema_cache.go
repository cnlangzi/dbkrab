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
// Supports both "table" and "schema.table" formats
var validTableName = regexp.MustCompile(`^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)?$`)

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

	// Convert all table names to schema.table format and track schema/table separately
	type tableInfo struct {
		schema string
		table  string
	}
	infos := make([]tableInfo, len(tableNames))
	fullNames := make([]string, len(tableNames))

	for i, name := range tableNames {
		schema, table := ParseTableName(name)
		infos[i] = tableInfo{schema: schema, table: table}
		fullNames[i] = fmt.Sprintf("%s.%s", schema, table)

		// Validate table name (supports both "table" and "schema.table")
		if !validTableName.MatchString(fullNames[i]) {
			return fmt.Errorf("invalid table name: %s", fullNames[i])
		}
	}

	// Build separate lists for schema and table names
	schemas := make([]string, len(infos))
	tables := make([]string, len(infos))
	for i, info := range infos {
		schemas[i] = info.schema
		tables[i] = info.table
	}

	// Build query with proper schema.table pair filtering
	// Each entry in tables has corresponding schema in same index position
	tablePlaceholders := make([]string, len(tables))
	args := make([]interface{}, 0, len(tables))

	for i := range tables {
		tablePlaceholders[i] = "(schema_name(t.schema_id) = ? AND t.name = ?)"
		args = append(args, schemas[i], tables[i])
	}

	query := fmt.Sprintf(`
		SELECT t.name AS table_name, c.name AS column_name, schema_name(t.schema_id) AS table_schema
		FROM sys.tables t
		INNER JOIN sys.index_columns ic ON ic.object_id = t.object_id
		INNER JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		WHERE i.is_primary_key = 1
		AND (%s)
		ORDER BY t.name, ic.key_ordinal
	`, strings.Join(tablePlaceholders, " OR "))

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query primary keys: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Warn("TableSchemaCache: rows.Close error", "error", err)
		}
	}()

	// Group results by table, collecting all key columns
	results := make(map[string][]string)
	for rows.Next() {
		var tableName, columnName, tableSchema string
		if err := rows.Scan(&tableName, &columnName, &tableSchema); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		// Use schema.table as key
		key := fmt.Sprintf("%s.%s", tableSchema, tableName)
		results[key] = append(results[key], columnName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}

	// Update cache
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear and rebuild
	c.schema = make(map[string]TableSchema)
	for key, cols := range results {
		// Parse key to get table name
		parts := strings.SplitN(key, ".", 2)
		tableName := key
		if len(parts) == 2 {
			tableName = parts[1]
		}
		c.schema[key] = TableSchema{
			TableName: tableName,
			Columns:  cols,
		}
	}

	slog.Info("TableSchemaCache: loaded primary keys",
		"tables", len(results),
		"schemas", schemas,
		"table_names", tables)

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