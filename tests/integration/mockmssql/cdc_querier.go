package mockmssql

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cnlangzi/dbkrab/internal/cdc"
)

// CDCQuerier implements core.CDCQuerier interface using mock data
type CDCQuerier struct {
	handler *MockHandler
}

// NewCDCQuerier creates a new mock CDCQuerier
func NewCDCQuerier(handler *MockHandler) *CDCQuerier {
	return &CDCQuerier{handler: handler}
}

// GetMinLSN returns the minimum LSN for a capture instance
func (q *CDCQuerier) GetMinLSN(ctx context.Context, captureInstance string) ([]byte, error) {
	// Return a default min LSN
	return []byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 1}, nil
}

// GetMaxLSN returns the current max LSN
func (q *CDCQuerier) GetMaxLSN(ctx context.Context) ([]byte, error) {
	return q.handler.MaxLSN, nil
}

// IncrementLSN returns the next LSN after the given one
func (q *CDCQuerier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error) {
	if len(lsn) != 10 {
		return nil, fmt.Errorf("invalid LSN length: %d", len(lsn))
	}

	// Simple increment: treat last 4 bytes as counter
	// Convert to big-endian uint32 and increment
	counter := uint32(lsn[6])<<24 | uint32(lsn[7])<<16 | uint32(lsn[8])<<8 | uint32(lsn[9])
	counter++

	result := make([]byte, 10)
	copy(result[:6], lsn[:6])
	result[6] = byte(counter >> 24)
	result[7] = byte(counter >> 16)
	result[8] = byte(counter >> 8)
	result[9] = byte(counter)

	return result, nil
}

// GetChanges queries CDC changes for a table
func (q *CDCQuerier) GetChanges(ctx context.Context, captureInstance string, tableName string, fromLSN []byte, toLSN []byte) ([]cdc.Change, error) {
	// Parse table name from capture instance (format: schema_table)
	actualTableName := tableName
	if idx := len(captureInstance) - len("_") - len(tableName); idx > 0 {
		// captureInstance format: dbo_TestProducts
		if len(captureInstance) > 4 && captureInstance[:4] == "dbo_" {
			actualTableName = captureInstance[4:]
		}
	}

	// Return mock CDC changes based on table
	return q.getMockChanges(actualTableName, fromLSN, toLSN)
}

// getMockChanges returns mock CDC changes for a table
func (q *CDCQuerier) getMockChanges(tableName string, fromLSN []byte, toLSN []byte) ([]cdc.Change, error) {
	// Generate mock changes based on LSN range
	var changes []cdc.Change

	fromStr := hex.EncodeToString(fromLSN)
	toStr := hex.EncodeToString(toLSN)

	// For testing, return sample changes when fromLSN != toLSN
	if fromStr != toStr && fromStr != "" {
		switch tableName {
		case "TestProducts":
			changes = []cdc.Change{
				{
					Table:         "TestProducts",
					TransactionID: "00000000-0000-0000-0000-000000000001",
					LSN:           toLSN,
					Operation:     2, // INSERT
					CommitTime:    q.handler.getCurrentTime(),
					Data: map[string]interface{}{
						"productid":   3,
						"productname": "Test Product",
						"price":       29.99,
						"stock":       50,
					},
				},
			}
		case "TestOrders":
			changes = []cdc.Change{
				{
					Table:         "TestOrders",
					TransactionID: "00000000-0000-0000-0000-000000000001",
					LSN:           toLSN,
					Operation:     2, // INSERT
					CommitTime:    q.handler.getCurrentTime(),
					Data: map[string]interface{}{
						"orderid":       2,
						"productid":     3,
						"quantity":      5,
						"totalamount":   149.95,
					},
				},
			}
		case "TestOrderItems":
			changes = []cdc.Change{
				{
					Table:         "TestOrderItems",
					TransactionID: "00000000-0000-0000-0000-000000000001",
					LSN:           toLSN,
					Operation:     2, // INSERT
					CommitTime:    q.handler.getCurrentTime(),
					Data: map[string]interface{}{
						"orderitemid": 2,
						"orderid":     2,
						"itemname":    "Test Item",
						"itemprice":   29.99,
					},
				},
			}
		}
	}

	return changes, nil
}

func (h *MockHandler) getCurrentTime() time.Time {
	return time.Now()
}