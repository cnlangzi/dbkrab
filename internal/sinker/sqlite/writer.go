package sqlite

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cnlangzi/dbkrab/internal/core"
	"github.com/cnlangzi/dbkrab/internal/sqliteutil"
)

// Sinker implements sinker.Sinker for SQLite.
type Sinker struct {
	name   string
	db     *DB
	mu     sync.Mutex
	closed bool
}

// NewSinker creates a new SQLite sinker.
func NewSinker(name string, dsn string, migrations string) (*Sinker, error) {
	db, err := NewSinkerDB(context.Background(), dsn, migrations)
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

func (s *Sinker) writeOp(ctx context.Context, tx sqliteutil.TxExec, op core.Sink) error {
	config := sqliteutil.TableConfig{
		Output:     op.Config.Output,
		PrimaryKey: op.Config.PrimaryKey,
		OnConflict: op.Config.OnConflict,
	}

	slog.Debug("SQLiteSinker.writeOp: processing",
		"database", s.name,
		"output", op.Config.Output,
		"opType", op.OpType,
		"opTypeName", op.OpType.String(),
		"primaryKey", op.Config.PrimaryKey,
		"columns", len(op.DataSet.Columns),
		"rows", len(op.DataSet.Rows))

	switch op.OpType {
	case core.OpInsert:
		return sqliteutil.InsertInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	case core.OpUpdateAfter:
		return sqliteutil.UpdateInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	case core.OpDelete:
		return sqliteutil.DeleteInTx(tx, config, op.DataSet.Columns, op.DataSet.Rows)
	default:
		// Defensively handle unknown operations by logging and dropping.
		slog.Error("SQLiteSinker.writeOp: unsupported operation type, dropping",
			"database", s.name,
			"output", op.Config.Output,
			"opType", op.OpType,
			"opTypeName", op.OpType.String(),
			"primaryKey", op.Config.PrimaryKey,
			"columns", len(op.DataSet.Columns),
			"rows", len(op.DataSet.Rows),
			"hint", "supported types are: OpInsert(2), OpUpdateAfter(4), OpDelete(1)")
		return nil
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
