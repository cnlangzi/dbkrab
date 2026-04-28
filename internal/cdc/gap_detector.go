package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"
)

// GapInfo contains information about CDC gap detection
type GapInfo struct {
	Table           string // Original table name (schema.table format)
	CaptureInstance string // CDC capture instance name
	CurrentLSN      []byte
	MinLSN          []byte
	MaxLSN          []byte
	HasGap          bool          // true if current_lsn < min_lsn (data loss)
	LagBytes        int64         // max_lsn - current_lsn in bytes
	LagDuration     time.Duration // estimated time lag based on LSN timestamps
	MissingLSNRange LSNRange      // range of missing LSNs (if HasGap)
	CheckedAt       time.Time
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
// tableName: original table name in schema.table format
// captureInstance: CDC capture instance name
// tableMaxLSN: optional per-table max LSN. If nil, uses global max LSN.
func (d *GapDetector) CheckGap(ctx context.Context, tableName, captureInstance string, currentLSN []byte, tableMaxLSN []byte) (GapInfo, error) {
	gap := GapInfo{
		Table:           tableName,
		CaptureInstance: captureInstance,
		CurrentLSN:      currentLSN,
		CheckedAt:       time.Now(),
	}

	// Get CDC min LSN (cleanup boundary)
	minLSN, err := d.GetMinLSN(ctx, captureInstance)
	if err != nil {
		return gap, fmt.Errorf("get min LSN: %w", err)
	}
	gap.MinLSN = minLSN

	// Get CDC max LSN (latest change) - use per-table if provided, otherwise global
	if len(tableMaxLSN) > 0 {
		gap.MaxLSN = tableMaxLSN
	} else {
		maxLSN, err := d.GetMaxLSN(ctx)
		if err != nil {
			return gap, fmt.Errorf("get max LSN: %w", err)
		}
		gap.MaxLSN = maxLSN
	}

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
	if len(gap.MaxLSN) > 0 && len(currentLSN) > 0 {
		gap.LagBytes = LSNBytesDiff(gap.MaxLSN, currentLSN)
	}

	// Estimate lag duration (requires querying LSN time mapping)
	if len(currentLSN) > 0 && len(gap.MaxLSN) > 0 {
		currentTime, err := d.GetLSNTime(ctx, currentLSN)
		if err == nil {
			maxTime, err := d.GetLSNTime(ctx, gap.MaxLSN)
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

// GetTableMaxLSN returns the max LSN for a specific capture instance (per-table)
// This queries the CDC change table (CT) to get the actual max LSN for each table's changes.
// If the change table is empty or doesn't exist, returns nil (caller should fallback to global max LSN).
func (d *GapDetector) GetTableMaxLSN(ctx context.Context, captureInstance string) ([]byte, error) {
	// Validate capture instance to prevent SQL injection
	if !validCaptureInstance.MatchString(captureInstance) {
		return nil, fmt.Errorf("invalid capture instance name: %s", captureInstance)
	}

	var lsn []byte
	// Query the CDC change table (CT) for the maximum __$start_lsn
	// The CT table name follows the pattern: cdc.<capture_instance>_CT
	ctTable := fmt.Sprintf("cdc.%s_CT", captureInstance)
	query := fmt.Sprintf("SELECT MAX(__$start_lsn) FROM %s", ctTable)
	err := d.db.QueryRowContext(ctx, query).Scan(&lsn)
	if err != nil {
		return nil, fmt.Errorf("query table max LSN from %s: %w", ctTable, err)
	}
	// lsn will be nil if the table is empty (no changes yet)
	return lsn, nil
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

// LSNBytesDiff calculates the approximate byte difference between two LSNs.
// CDC LSNs are 10 bytes; we use only the lower 8 bytes to fit in int64.
// This provides a reasonable approximation for lag detection while avoiding
// overflow issues with full 10-byte values (which can exceed 2^63-1).
// For typical CDC lag scenarios (hours to days of changes), 8 bytes provides
// sufficient precision. If overflow is detected, returns math.MaxInt64.
func LSNBytesDiff(a, b []byte) int64 {
	const lsnLen = 10
	const usableBytes = 8 // Use lower 8 bytes to fit in int64

	// Require full 10-byte LSNs; anything else is treated as "no information".
	if len(a) != lsnLen || len(b) != lsnLen {
		return 0
	}

	diff := int64(0)
	multiplier := int64(1)
	overflow := false

	// Walk from least-significant byte to most-significant,
	// but only use the lower 8 bytes to avoid int64 overflow.
	for i := lsnLen - 1; i >= lsnLen-usableBytes; i-- {
		ai := int64(a[i])
		bi := int64(b[i])
		delta := ai - bi

		// Skip if delta is zero (no contribution to diff)
		if delta == 0 {
			multiplier *= 256
			continue
		}

		// Check for potential overflow before multiplying (only if delta != 0)
		if multiplier > math.MaxInt64/256 {
			overflow = true
			break
		}

		term := delta * multiplier
		// Check for overflow on addition
		if delta > 0 && diff > math.MaxInt64-term {
			overflow = true
			break
		}
		if delta < 0 && diff < math.MinInt64-term {
			overflow = true
			break
		}

		diff += term
		multiplier *= 256
	}

	if overflow {
		return math.MaxInt64
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
