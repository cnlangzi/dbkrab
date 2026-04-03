package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// GapInfo contains information about CDC gap detection
type GapInfo struct {
	Table            string
	CurrentLSN       []byte
	MinLSN           []byte
	MaxLSN           []byte
	HasGap           bool          // true if current_lsn < min_lsn (data loss)
	LagBytes         int64         // max_lsn - current_lsn in bytes
	LagDuration      time.Duration // estimated time lag based on LSN timestamps
	MissingLSNRange  LSNRange      // range of missing LSNs (if HasGap)
	CheckedAt        time.Time
}

// LSNRange represents a range of LSN values
type LSNRange struct {
	Start []byte
	End   []byte
}

// GapDetector detects CDC data gaps and lag
type GapDetector struct {
	db *sql.DB
}

// NewGapDetector creates a new gap detector
func NewGapDetector(db *sql.DB) *GapDetector {
	return &GapDetector{db: db}
}

// CheckGap checks for CDC gaps and lag for a specific table
func (d *GapDetector) CheckGap(ctx context.Context, captureInstance string, currentLSN []byte) (GapInfo, error) {
	gap := GapInfo{
		Table:      captureInstance,
		CurrentLSN: currentLSN,
		CheckedAt:  time.Now(),
	}

	// Get CDC min LSN (cleanup boundary)
	minLSN, err := d.GetMinLSN(ctx, captureInstance)
	if err != nil {
		return gap, fmt.Errorf("get min LSN: %w", err)
	}
	gap.MinLSN = minLSN

	// Get CDC max LSN (latest change)
	maxLSN, err := d.GetMaxLSN(ctx)
	if err != nil {
		return gap, fmt.Errorf("get max LSN: %w", err)
	}
	gap.MaxLSN = maxLSN

	// Check if current LSN is behind min LSN (data loss)
	if len(currentLSN) > 0 && len(minLSN) > 0 {
		cmp := CompareLSN(currentLSN, minLSN)
		if cmp < 0 {
			gap.HasGap = true
			gap.MissingLSNRange = LSNRange{
				Start: currentLSN,
				End:   minLSN,
			}
		}
	}

	// Calculate lag in bytes (max_lsn - current_lsn)
	if len(maxLSN) > 0 && len(currentLSN) > 0 {
		gap.LagBytes = LSNBytesDiff(maxLSN, currentLSN)
	}

	// Estimate lag duration (requires querying LSN time mapping)
	if len(currentLSN) > 0 && len(maxLSN) > 0 {
		currentTime, err := d.GetLSNTime(ctx, currentLSN)
		if err == nil {
			maxTime, err := d.GetLSNTime(ctx, maxLSN)
			if err == nil {
				gap.LagDuration = maxTime.Sub(currentTime)
			}
		}
	}

	return gap, nil
}

// GetMinLSN returns the minimum LSN for a capture instance (cleanup boundary)
func (d *GapDetector) GetMinLSN(ctx context.Context, captureInstance string) ([]byte, error) {
	// Validate capture instance to prevent SQL injection
	if !validCaptureInstance.MatchString(captureInstance) {
		return nil, fmt.Errorf("invalid capture instance name: %s", captureInstance)
	}

	var lsn []byte
	query := fmt.Sprintf("SELECT sys.fn_cdc_get_min_lsn('%s')", captureInstance)
	err := d.db.QueryRowContext(ctx, query).Scan(&lsn)
	return lsn, err
}

// GetMaxLSN returns the current max LSN
func (d *GapDetector) GetMaxLSN(ctx context.Context) ([]byte, error) {
	var lsn []byte
	err := d.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&lsn)
	return lsn, err
}

// GetLSNTime returns the timestamp for a given LSN
func (d *GapDetector) GetLSNTime(ctx context.Context, lsn []byte) (time.Time, error) {
	var t time.Time
	err := d.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_map_lsn_to_time(@lsn)",
		sql.Named("lsn", lsn)).Scan(&t)
	return t, err
}

// CompareLSN compares two LSN values
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func CompareLSN(a, b []byte) int {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	if len(a) == 0 {
		return -1
	}
	if len(b) == 0 {
		return 1
	}

	// LSN is 10 bytes in SQL Server CDC
	// Compare byte by byte from left to right
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}

	// If all compared bytes are equal, longer one is greater
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// LSNBytesDiff calculates the approximate byte difference between two LSNs
// This is a rough estimate based on LSN structure
func LSNBytesDiff(a, b []byte) int64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	// LSN in SQL Server CDC is structured as:
	// - VLF sequence number (4 bytes)
	// - Log block offset (4 bytes)
	// - Slot number (2 bytes)
	// For estimation, we treat it as a big-endian integer
	diff := int64(0)
	multiplier := int64(1)

	// Compare from right to left
	for i := len(a) - 1; i >= 0 && i >= len(b)-10; i-- {
		ai := int64(0)
		bi := int64(0)
		if i >= 0 && i < len(a) {
			ai = int64(a[i])
		}
		if i >= 0 && i < len(b) {
			bi = int64(b[i])
		}
		diff += (ai - bi) * multiplier
		multiplier *= 256
	}

	if diff < 0 {
		diff = -diff
	}
	return diff
}

// IsGapCritical determines if a gap is critical based on thresholds
func (g *GapInfo) IsGapCritical(maxLagBytes int64, maxLagDuration time.Duration) bool {
	if g.HasGap {
		return true // Data loss is always critical
	}
	if g.LagBytes > maxLagBytes {
		return true
	}
	if g.LagDuration > maxLagDuration {
		return true
	}
	return false
}

// IsGapWarning determines if a gap is a warning (not critical but concerning)
func (g *GapInfo) IsGapWarning(warnLagBytes int64, warnLagDuration time.Duration) bool {
	if g.HasGap {
		return false // HasGap is critical, not warning
	}
	if g.LagBytes > warnLagBytes {
		return true
	}
	if g.LagDuration > warnLagDuration {
		return true
	}
	return false
}
