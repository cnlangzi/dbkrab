package sqlite

import (
	"context"
	"os"
	"testing"
)

func TestMigration(t *testing.T) {
	// 使用 business.db
	os.Remove("/opt/dbkrab/data/sinks/business/business.db")
	os.Remove("/opt/dbkrab/data/sinks/business/business.db-shm")
	os.Remove("/opt/dbkrab/data/sinks/business/business.db-wal")

	s, err := NewSinker("business", "/opt/dbkrab/data/sinks/business/business.db", "/opt/dbkrab/data/sinks/business/migrations")
	if err != nil {
		t.Fatalf("Error creating sinker: %v", err)
	}
	defer s.Close()

	err = s.Migrate(context.Background())
	if err != nil {
		t.Fatalf("Migration error: %v", err)
	}
}
