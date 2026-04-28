package sql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/yaitoo/sqle"
	"github.com/yaitoo/sqle/migrate"
)

// SQLiteSink writes business data to a dedicated SQLite file per skill.
// It uses connection pooling for read/write separation and supports migrations.
type SQLiteSink struct {
	pool  *Pool
	skill *Skill
}

// NewSQLiteSink creates a new SQLite sink for a skill
func NewSQLiteSink(skill *Skill, pool *Pool) *SQLiteSink {
	return &SQLiteSink{
		pool:  pool,
		skill: skill,
	}
}

// Write writes transformed DataSets to the sink database
func (s *SQLiteSink) Write(ctx context.Context, ops []core.Sink) error {
	if len(ops) == 0 {
		return nil
	}

	tx, err := s.pool.WriteTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, op := range ops {
		switch op.OpType {
		case core.OpInsert:
			if err := s.insertInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("insert %s: %w", op.Config.Output, err)
			}
		case core.OpUpdateAfter:
			if err := s.updateInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("update %s: %w", op.Config.Output, err)
			}
		case core.OpDelete:
			if err := s.deleteInTx(tx, op.Config, op.DataSet); err != nil {
				return fmt.Errorf("delete %s: %w", op.Config.Output, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// colType infers a SQLite type name from a Go value.
func colType(v any) string {
	if v == nil {
		return "TEXT"
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "REAL"
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return "BLOB"
		}
		return "TEXT"
	case reflect.Bool:
		return "INTEGER"
	case reflect.Struct:
		if rv.Type() == reflect.TypeOf(time.Time{}) {
			return "DATETIME"
		}
		return "TEXT"
	default:
		return "TEXT"
	}
}

// sampleTypes returns a slice of SQLite type names, one per column.
// It scans rows in order and uses the first non-nil value for each column.
func sampleTypes(columns []string, rows [][]any) []string {
	types := make([]string, len(columns))
	for _, row := range rows {
		for colIdx := range columns {
			if types[colIdx] == "" && row[colIdx] != nil {
				types[colIdx] = colType(row[colIdx])
			}
		}
	}
	// Default all remaining (all nil) columns to TEXT
	for i := range types {
		if types[i] == "" {
			types[i] = "TEXT"
		}
	}
	return types
}

// txExecutor allows both *sql.Tx and *sqlite.Tx to be used interchangeably
type txExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// insertInTx inserts DataSet into table
func (s *SQLiteSink) insertInTx(tx txExecutor, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Infer SQLite types from data
	colTypes := sampleTypes(ds.Columns, ds.Rows)

	// Ensure table exists
	if err := s.ensureTable(tx, config.Output, ds.Columns, colTypes); err != nil {
		return err
	}

	for _, row := range ds.Rows {
		// Build INSERT SQL based on OnConflict strategy
		var sqlStr string
		var args []any

		switch config.OnConflict {
		case "overwrite":
			// Use proper UPSERT: INSERT ... ON CONFLICT(pk) DO UPDATE SET col = excluded.col
			// This updates only the columns provided, not the entire row
			escapedCols := make([]string, len(ds.Columns))
			for i, col := range ds.Columns {
				escapedCols[i] = fmt.Sprintf("[%s]", col)
			}
			placeholders := make([]string, len(ds.Columns))
			for i := range ds.Columns {
				placeholders[i] = "?"
			}
			colList := strings.Join(escapedCols, ", ")
			valuesList := strings.Join(placeholders, ", ")

			// Build SET clause with excluded. prefix for non-PK columns
			pk := config.PrimaryKey
			var setClauses []string
			for _, col := range ds.Columns {
				if col == pk {
					continue
				}
				setClauses = append(setClauses, fmt.Sprintf("[%s] = excluded.[%s]", col, col))
			}

			if len(setClauses) == 0 {
				// Only PK provided - no columns to update on conflict
				sqlStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
					config.Output, colList, valuesList)
			} else {
				setClause := strings.Join(setClauses, ", ")
				pkEscaped := fmt.Sprintf("[%s]", pk)
				sqlStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s",
					config.Output, colList, valuesList, pkEscaped, setClause)
			}
			args = row
		case "skip":
			// INSERT OR IGNORE
			escapedCols := make([]string, len(ds.Columns))
			for i, col := range ds.Columns {
				escapedCols[i] = fmt.Sprintf("[%s]", col)
			}
			placeholders := make([]string, len(ds.Columns))
			for i := range ds.Columns {
				placeholders[i] = "?"
			}
			sqlStr = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedCols, ", "),
				strings.Join(placeholders, ", "))
			args = row
		default:
			// Default to INSERT with ON CONFLICT clause
			escapedCols := make([]string, len(ds.Columns))
			for i, col := range ds.Columns {
				escapedCols[i] = fmt.Sprintf("[%s]", col)
			}
			placeholders := make([]string, len(ds.Columns))
			for i := range ds.Columns {
				placeholders[i] = "?"
			}
			sqlStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				config.Output,
				strings.Join(escapedCols, ", "),
				strings.Join(placeholders, ", "))
			args = row
		}

		_, err := tx.Exec(sqlStr, args...)
		if err != nil {
			return fmt.Errorf("execute: %w", err)
		}
	}

	return nil
}

// updateInTx updates records in table
func (s *SQLiteSink) updateInTx(tx txExecutor, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Find PK index
	pkIndex := -1
	for i, col := range ds.Columns {
		if col == config.PrimaryKey {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return fmt.Errorf("primary key %s not found", config.PrimaryKey)
	}

	for _, row := range ds.Rows {
		// Build UPDATE SQL
		var setClauses []string
		var values []any
		for i, col := range ds.Columns {
			if col == config.PrimaryKey {
				continue
			}
			setClauses = append(setClauses, fmt.Sprintf("[%s] = ?", col))
			values = append(values, row[i])
		}

		pkValue := row[pkIndex]
		sqlStr := fmt.Sprintf("UPDATE %s SET %s WHERE [%s] = ?",
			config.Output,
			strings.Join(setClauses, ", "),
			config.PrimaryKey)
		values = append(values, pkValue)

		_, err := tx.Exec(sqlStr, values...)
		if err != nil {
			return fmt.Errorf("execute: %w", err)
		}
	}

	return nil
}

// deleteInTx deletes records from table
func (s *SQLiteSink) deleteInTx(tx txExecutor, config core.SinkConfig, ds *core.DataSet) error {
	if ds == nil || len(ds.Rows) == 0 {
		return nil
	}

	// Find PK index
	pkIndex := -1
	for i, col := range ds.Columns {
		if col == config.PrimaryKey {
			pkIndex = i
			break
		}
	}

	if pkIndex == -1 {
		return fmt.Errorf("primary key %s not found", config.PrimaryKey)
	}

	pkValues := make([]any, len(ds.Rows))
	for i, row := range ds.Rows {
		pkValues[i] = row[pkIndex]
	}

	// Build IN clause
	placeholders := make([]string, len(ds.Rows))
	for i := range ds.Rows {
		placeholders[i] = "?"
	}

	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE [%s] IN (%s)",
		config.Output,
		config.PrimaryKey,
		strings.Join(placeholders, ", "))

	_, err := tx.Exec(sqlStr, pkValues...)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}

	return nil
}

// ensureTable ensures the table exists with the given columns and types
func (s *SQLiteSink) ensureTable(tx txExecutor, table string, columns []string, colTypes []string) error {
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = fmt.Sprintf("[%s] %s", col, colTypes[i])
	}

	sqlStr := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		table,
		strings.Join(escapedCols, ", "))

	_, err := tx.Exec(sqlStr)
	return err
}

// RunMigrations runs any pending migrations using sqle/migrate
func (s *SQLiteSink) RunMigrations() error {
	migrationsDir := s.pool.MigrationsPath()

	// Check if migrations directory exists
	if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
		return nil // No migrations directory, skip
	}

	// Create sqle.DB from the pool's underlying write connection
	sqleDB := sqle.Open(s.pool.Write().Writer.DB)

	migrator := migrate.New(sqleDB)
	moduleName := "dbkrab-sink-" + s.skill.Name
	if err := migrator.Discover(os.DirFS(migrationsDir), migrate.WithModule(moduleName)); err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	if err := migrator.Migrate(context.Background()); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	return nil
}

// Close closes the sink
func (s *SQLiteSink) Close() error {
	return s.pool.Close()
}

// Query executes a read query against the sink database
func (s *SQLiteSink) Query(query string, args ...any) (*sql.Rows, error) {
	return s.pool.Read().Query(query, args...)
}

// QueryRow executes a read query that returns a single row
func (s *SQLiteSink) QueryRow(query string, args ...any) *sql.Row {
	return s.pool.Read().QueryRow(query, args...)
}

// GetPool returns the underlying pool
func (s *SQLiteSink) GetPool() *Pool {
	return s.pool
}

// LastModified returns the last modification time of the sink database
func (s *SQLiteSink) LastModified() (time.Time, error) {
	dbPath := filepath.Join(s.pool.Path(), s.pool.Name()+".db")
	info, err := os.Stat(dbPath)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}
