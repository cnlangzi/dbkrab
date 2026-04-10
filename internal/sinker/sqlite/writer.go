package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/sqliteutil"
	pkgSqlite "github.com/cnlangzi/dbkrab/pkg/sqlite"
)

// Sinker implements sinker.Sinker for SQLite.
type Sinker struct {
	name  string
	db    *pkgSqlite.DB
	mu    sync.Mutex
	closed bool
}

// NewSinker creates a new SQLite sinker.
func NewSinker(name string, cfg pkgSqlite.Config) (*Sinker, error) {
	db, err := pkgSqlite.New(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("create sqlite db: %w", err)
	}

	return &Sinker{
		name: name,
		db:   db,
	}, nil
}

// DatabaseName returns the database name.
func (s *Sinker) DatabaseName() string {
	return s.name
}

// DatabaseType returns the database type.
func (s *Sinker) DatabaseType() string {
	return "sqlite"
}

// Write writes a batch of sink operations to the database.
func (s *Sinker) Write(ctx context.Context, ops []core.Sink) error {
	if len(ops) == 0 {
		return nil
	}

	slog.Debug("SQLiteSinker.Write: starting batch write",
		"database", s.name,
		"operations", len(ops))

	tx, err := s.db.Writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, op := range ops {
		if err := s.writeOp(ctx, tx, op); err != nil {
			return fmt.Errorf("write op %s: %w", op.Config.Output, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (s *Sinker) writeOp(ctx context.Context, tx *sql.Tx, op core.Sink) error {
	config := core.TableConfig{
		Output:     op.Config.Output,
		PrimaryKey: op.Config.PrimaryKey,
		OnConflict: op.Config.OnConflict,
	}

	switch op.OpType {
	case core.OpInsert:
		return sqliteutil.InsertInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	case core.OpUpdateAfter:
		return sqliteutil.UpdateInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	case core.OpDelete:
		return sqliteutil.DeleteInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	default:
		return fmt.Errorf("unknown operation type: %v", op.OpType)
	}
}

// Close closes the database connection.
func (s *Sinker) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	return s.db.Close()
}
