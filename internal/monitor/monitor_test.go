package monitor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDB_New(t *testing.T) {
	// Create temp directory for test
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Check that file was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("logs.db file was not created")
	}
}

func TestDB_WriteBatchLog(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Test successful pull log
	pullLog := &BatchLog{
		BatchID:      "test-pull-001",
		FetchedRows: 100,
		TxCount:     5,
		DLQCount:    0,
		DurationMs:  50,
		Status:      PullStatusSuccess,
		CreatedAt:   time.Now(),
	}

	if err := monitorDB.WriteBatchLog(pullLog); err != nil {
		t.Fatalf("Failed to write pull log: %v", err)
	}

	// Flush to ensure data is committed before reading
	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Verify the log was written
	logs, err := monitorDB.ListBatchLogs(10, time.Now().AddDate(0, 0, -1))
	if err != nil {
		t.Fatalf("Failed to list pull logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("Expected 1 log, got %d", len(logs))
	}

	if logs[0].BatchID != "test-pull-001" {
		t.Errorf("Expected BatchID test-pull-001, got %s", logs[0].BatchID)
	}

	if logs[0].Status != PullStatusSuccess {
		t.Errorf("Expected status SUCCESS, got %s", logs[0].Status)
	}
}

func TestDB_WriteBatchLog_Partial(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Test partial pull log (with DLQ entries)
	pullLog := &BatchLog{
		BatchID:      "test-pull-002",
		FetchedRows: 100,
		TxCount:     5,
		DLQCount:    2,
		DurationMs:  100,
		Status:      PullStatusPartial,
		CreatedAt:   time.Now(),
	}

	if err := monitorDB.WriteBatchLog(pullLog); err != nil {
		t.Fatalf("Failed to write pull log: %v", err)
	}

	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	logs, err := monitorDB.ListBatchLogs(10, time.Now().AddDate(0, 0, -1))
	if err != nil {
		t.Fatalf("Failed to list pull logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("Expected 1 log, got %d", len(logs))
	}

	if logs[0].Status != PullStatusPartial {
		t.Errorf("Expected status PARTIAL, got %s", logs[0].Status)
	}

	if logs[0].DLQCount != 2 {
		t.Errorf("Expected DLQCount 2, got %d", logs[0].DLQCount)
	}
}

func TestDB_WriteBatchLog_Failed(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Test failed pull log
	pullLog := &BatchLog{
		BatchID:      "test-pull-003",
		FetchedRows: 0,
		TxCount:     0,
		DLQCount:    0,
		DurationMs:  10,
		Status:      PullStatusFailed,
		CreatedAt:   time.Now(),
	}

	if err := monitorDB.WriteBatchLog(pullLog); err != nil {
		t.Fatalf("Failed to write pull log: %v", err)
	}

	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	logs, err := monitorDB.ListBatchLogs(10, time.Now().AddDate(0, 0, -1))
	if err != nil {
		t.Fatalf("Failed to list pull logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("Expected 1 log, got %d", len(logs))
	}

	if logs[0].Status != PullStatusFailed {
		t.Errorf("Expected status FAILED, got %s", logs[0].Status)
	}
}

func TestDB_WriteSkillLog(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// First write a pull log
	pullLog := &BatchLog{
		BatchID:      "test-pull-004",
		FetchedRows: 100,
		TxCount:     5,
		DLQCount:    0,
		DurationMs:  50,
		Status:      PullStatusSuccess,
		CreatedAt:   time.Now(),
	}
	if err := monitorDB.WriteBatchLog(pullLog); err != nil {
		t.Fatalf("Failed to write pull log: %v", err)
	}

	// Test skill log - executed
	skillLog := &SkillLog{
		BatchID:        "test-pull-004",
		SkillID:       "skill-001",
		SkillName:     "test-skill",
		Operation:     "INSERT",
		RowsProcessed: 50,
		Status:        SkillStatusExecuted,
		DurationMs:    10,
		CreatedAt:     time.Now(),
	}

	if err := monitorDB.WriteSkillLog(skillLog); err != nil {
		t.Fatalf("Failed to write skill log: %v", err)
	}

	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	logs, err := monitorDB.ListSkillLogs("", "test-pull-004", 10)
	if err != nil {
		t.Fatalf("Failed to list skill logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("Expected 1 skill log, got %d", len(logs))
	}

	if logs[0].SkillName != "test-skill" {
		t.Errorf("Expected skill name test-skill, got %s", logs[0].SkillName)
	}

	if logs[0].Status != SkillStatusExecuted {
		t.Errorf("Expected status EXECUTED, got %s", logs[0].Status)
	}
}

func TestDB_WriteSkillLog_Skip(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Write pull log
	pullLog := &BatchLog{
		BatchID:      "test-pull-005",
		FetchedRows: 100,
		TxCount:     5,
		DLQCount:    0,
		DurationMs:  50,
		Status:      PullStatusSuccess,
		CreatedAt:   time.Now(),
	}
	if err := monitorDB.WriteBatchLog(pullLog); err != nil {
		t.Fatalf("Failed to write pull log: %v", err)
	}

	// Test skill log - skipped
	skillLog := &SkillLog{
		BatchID:        "test-pull-005",
		SkillID:       "skill-002",
		SkillName:     "test-skip-skill",
		Operation:     "UPDATE",
		RowsProcessed: 0,
		Status:        SkillStatusSkip,
		DurationMs:    1,
		CreatedAt:     time.Now(),
	}

	if err := monitorDB.WriteSkillLog(skillLog); err != nil {
		t.Fatalf("Failed to write skill log: %v", err)
	}

	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	logs, err := monitorDB.ListSkillLogs("", "test-pull-005", 10)
	if err != nil {
		t.Fatalf("Failed to list skill logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("Expected 1 skill log, got %d", len(logs))
	}

	if logs[0].Status != SkillStatusSkip {
		t.Errorf("Expected status SKIP, got %s", logs[0].Status)
	}
}

func TestDB_WriteSkillLog_Error(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Write pull log
	pullLog := &BatchLog{
		BatchID:      "test-pull-006",
		FetchedRows: 100,
		TxCount:     5,
		DLQCount:    1,
		DurationMs:  100,
		Status:      PullStatusPartial,
		CreatedAt:   time.Now(),
	}
	if err := monitorDB.WriteBatchLog(pullLog); err != nil {
		t.Fatalf("Failed to write pull log: %v", err)
	}

	// Test skill log - error
	skillLog := &SkillLog{
		BatchID:        "test-pull-006",
		SkillID:       "skill-003",
		SkillName:     "test-error-skill",
		Operation:     "DELETE",
		RowsProcessed: 0,
		Status:        SkillStatusError,
		ErrorMessage:  "connection timeout",
		DurationMs:    5000,
		CreatedAt:     time.Now(),
	}

	if err := monitorDB.WriteSkillLog(skillLog); err != nil {
		t.Fatalf("Failed to write skill log: %v", err)
	}

	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	logs, err := monitorDB.ListSkillLogs("", "test-pull-006", 10)
	if err != nil {
		t.Fatalf("Failed to list skill logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("Expected 1 skill log, got %d", len(logs))
	}

	if logs[0].Status != SkillStatusError {
		t.Errorf("Expected status ERROR, got %s", logs[0].Status)
	}

	if logs[0].ErrorMessage != "connection timeout" {
		t.Errorf("Expected error message 'connection timeout', got %s", logs[0].ErrorMessage)
	}
}

func TestDB_WriteSinkLog(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Write pull log
	pullLog := &BatchLog{
		BatchID:      "test-pull-007",
		FetchedRows: 100,
		TxCount:     5,
		DLQCount:    0,
		DurationMs:  50,
		Status:      PullStatusSuccess,
		CreatedAt:   time.Now(),
	}
	if err := monitorDB.WriteBatchLog(pullLog); err != nil {
		t.Fatalf("Failed to write pull log: %v", err)
	}

	// Test sink log - success
	sinkLog := &SinkLog{
		BatchID:       "test-pull-007",
		SkillName:    "test-skill",
		SinkName:     "business",
		OutputTable:  "orders",
		Operation:    "INSERT",
		RowsWritten:  50,
		Status:       SinkStatusSuccess,
		DurationMs:   20,
		CreatedAt:    time.Now(),
	}

	if err := monitorDB.WriteSinkLog(sinkLog); err != nil {
		t.Fatalf("Failed to write sink log: %v", err)
	}

	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	logs, err := monitorDB.ListSinkLogs("", "test-pull-007", 10)
	if err != nil {
		t.Fatalf("Failed to list sink logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("Expected 1 sink log, got %d", len(logs))
	}

	if logs[0].OutputTable != "orders" {
		t.Errorf("Expected output table orders, got %s", logs[0].OutputTable)
	}

	if logs[0].Status != SinkStatusSuccess {
		t.Errorf("Expected status SUCCESS, got %s", logs[0].Status)
	}
}

func TestDB_WriteSinkLog_Error(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Write pull log
	pullLog := &BatchLog{
		BatchID:      "test-pull-008",
		FetchedRows: 100,
		TxCount:     5,
		DLQCount:    1,
		DurationMs:  200,
		Status:      PullStatusPartial,
		CreatedAt:   time.Now(),
	}
	if err := monitorDB.WriteBatchLog(pullLog); err != nil {
		t.Fatalf("Failed to write pull log: %v", err)
	}

	// Test sink log - error
	sinkLog := &SinkLog{
		BatchID:       "test-pull-008",
		SkillName:    "test-skill",
		SinkName:     "analytics",
		OutputTable:  "events",
		Operation:    "INSERT",
		RowsWritten:  0,
		Status:       SinkStatusError,
		ErrorMessage: "table does not exist",
		DurationMs:   10,
		CreatedAt:    time.Now(),
	}

	if err := monitorDB.WriteSinkLog(sinkLog); err != nil {
		t.Fatalf("Failed to write sink log: %v", err)
	}

	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	logs, err := monitorDB.ListSinkLogs("", "test-pull-008", 10)
	if err != nil {
		t.Fatalf("Failed to list sink logs: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("Expected 1 sink log, got %d", len(logs))
	}

	if logs[0].Status != SinkStatusError {
		t.Errorf("Expected status ERROR, got %s", logs[0].Status)
	}

	if logs[0].ErrorMessage != "table does not exist" {
		t.Errorf("Expected error message 'table does not exist', got %s", logs[0].ErrorMessage)
	}
}

func TestDB_GetBatchLogStats(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Write multiple pull logs with different statuses
	now := time.Now()
	for i := 0; i < 5; i++ {
		status := PullStatusSuccess
		if i == 3 {
			status = PullStatusPartial
		}
		if i == 4 {
			status = PullStatusFailed
		}

		pullLog := &BatchLog{
			BatchID:      string(rune('a' + i)),
			FetchedRows: 100,
			TxCount:     5,
			DLQCount:    0,
			DurationMs:  50,
			Status:      status,
			CreatedAt:   now.Add(time.Duration(i) * time.Minute),
		}
		if err := monitorDB.WriteBatchLog(pullLog); err != nil {
			t.Fatalf("Failed to write pull log: %v", err)
		}
	}

	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Get stats for all time
	stats, err := monitorDB.GetBatchLogStats(now.Add(-10 * time.Minute))
	if err != nil {
		t.Fatalf("Failed to get pull log stats: %v", err)
	}

	if stats["total_pulls"] != 5 {
		t.Errorf("Expected total_pulls 5, got %v", stats["total_pulls"])
	}

	if stats["success_count"] != 3 {
		t.Errorf("Expected success_count 3, got %v", stats["success_count"])
	}

	if stats["partial_count"] != 1 {
		t.Errorf("Expected partial_count 1, got %v", stats["partial_count"])
	}

	if stats["failed_count"] != 1 {
		t.Errorf("Expected failed_count 1, got %v", stats["failed_count"])
	}
}

func TestDB_Closed(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Close the database
	monitorDB.Close()

	// Try to write after close - should fail
	pullLog := &BatchLog{
		BatchID:      "test-pull-closed",
		FetchedRows: 100,
		TxCount:     5,
		DLQCount:    0,
		DurationMs:  50,
		Status:      PullStatusSuccess,
		CreatedAt:   time.Now(),
	}

	if err := monitorDB.WriteBatchLog(pullLog); err != ErrLogsClosed {
		t.Errorf("Expected ErrLogsClosed, got %v", err)
	}
}

func TestDB_Flush(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "logs.db")

	ctx := context.Background()
	monitorDB, err := New(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer monitorDB.Close()

	// Write a log
	pullLog := &BatchLog{
		BatchID:      "test-pull-flush",
		FetchedRows: 100,
		TxCount:     5,
		DLQCount:    0,
		DurationMs:  50,
		Status:      PullStatusSuccess,
		CreatedAt:   time.Now(),
	}
	if err := monitorDB.WriteBatchLog(pullLog); err != nil {
		t.Fatalf("Failed to write pull log: %v", err)
	}

	// Flush
	if err := monitorDB.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Read should work after flush
	logs, err := monitorDB.ListBatchLogs(10, time.Now().AddDate(0, 0, -1))
	if err != nil {
		t.Fatalf("Failed to list pull logs after flush: %v", err)
	}

	if len(logs) != 1 {
		t.Errorf("Expected 1 log after flush, got %d", len(logs))
	}
}