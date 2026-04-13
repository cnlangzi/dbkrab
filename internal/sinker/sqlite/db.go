package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cnlangzi/sqlite"
	"github.com/yaitoo/sqle"
	"github.com/yaitoo/sqle/migrate"

	_ "github.com/mattn/go-sqlite3"
)

// DB is a wrapper around github.com/cnlangzi/sqlite.DB
// providing read/write separation and migration support.
type DB = sqlite.DB

// NewSinkerDB creates a new SQLite DB with read/write separation and migration support.
// ModuleName is hardcoded to "dbkrab".
func NewSinkerDB(ctx context.Context, dsn string, migrations string) (*DB, error) {
	if dsn == "" {
		dsn = ":memory:"
	}

	inmemory := strings.HasPrefix(dsn, ":memory:")

	db, err := sqlite.Open(ctx, dsn)
	if err != nil {
		return nil, err
	}

	// Run migrations if Migrations is provided
	if migrations != "" {
		sqleDB := sqle.Open(db.Writer.DB)
		migrator := migrate.New(sqleDB)
		if err := migrator.Discover(os.DirFS(migrations), migrate.WithModule("dbkrab")); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("load migrations: %w", err)
		}
		if err := migrator.Init(context.Background()); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("init migrations: %w", err)
		}
		if err := migrator.Migrate(context.Background()); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("run migrations: %w", err)
		}
	} else if !inmemory {
		// No migration path provided for file-based database - fail fast
		return nil, fmt.Errorf("migration path is required for sinker SQLite databases: please provide migrations in config")
	}

	return db, nil
}

// Execer is an interface for executing queries
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// InsertInTx inserts rows into a table within a transaction.
func InsertInTx(tx Execer, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		QuoteIdent(table),
		QuoteIdentList(columns),
		strings.Join(placeholders, ","))

	for _, row := range rows {
		if _, err := tx.Exec(query, row...); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
	}

	return nil
}

// UpdateInTx updates rows in a table within a transaction.
func UpdateInTx(tx Execer, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	setClause := make([]string, len(columns))
	for i, col := range columns {
		setClause[i] = fmt.Sprintf("%s = ?", QuoteIdent(col))
	}

	query := fmt.Sprintf("UPDATE %s SET %s", QuoteIdent(table), strings.Join(setClause, ","))

	for _, row := range rows {
		if _, err := tx.Exec(query, row...); err != nil {
			return fmt.Errorf("update: %w", err)
		}
	}

	return nil
}

// DeleteInTx deletes rows from a table within a transaction.
func DeleteInTx(tx Execer, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	if len(columns) == 0 {
		return errors.New("delete requires at least one column")
	}

	whereClause := make([]string, len(columns))
	for i, col := range columns {
		whereClause[i] = fmt.Sprintf("%s = ?", QuoteIdent(col))
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		QuoteIdent(table),
		strings.Join(whereClause, " AND "))

	for _, row := range rows {
		if _, err := tx.Exec(query, row...); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}

	return nil
}

func QuoteIdent(s string) string {
	var buf strings.Builder
	buf.Grow(len(s) + 2)
	buf.WriteByte('`')
	for _, c := range s {
		buf.WriteRune(c)
		if c == '`' {
			buf.WriteByte('`')
		}
	}
	buf.WriteByte('`')
	return buf.String()
}

func QuoteIdentList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = QuoteIdent(col)
	}
	return strings.Join(quoted, ",")
}
