package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestMigration(t *testing.T) {
	// 使用临时目录
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "business.db")
	migrationsDir := filepath.Join(tmpDir, "migrations")

	// 创建 migrations 目录
	os.MkdirAll(migrationsDir, 0755)

	s, err := NewSinker("business", dbPath, migrationsDir)
	if err != nil {
		t.Fatalf("Error creating sinker: %v", err)
	}
	defer s.Close()

	err = s.Migrate(context.Background())
	if err != nil {
		t.Fatalf("Migration error: %v", err)
	}
}
