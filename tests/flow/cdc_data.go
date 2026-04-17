package flow

import (
	"time"

	"github.com/cnlangzi/dbkrab/internal/core"
)

// BuildLSN creates a mock LSN from a hex string
func BuildLSN(hexStr string) []byte {
	l := make([]byte, len(hexStr)/2)
	for i := 0; i < len(l); i++ {
		var b byte
		hexCharToByte(hexStr[i*2], hexStr[i*2+1], &b)
		l[i] = b
	}
	return l
}

func hexCharToByte(a, b byte, out *byte) {
	*out = 0
	if a >= '0' && a <= '9' {
		*out |= (a - '0') << 4
	} else if a >= 'a' && a <= 'f' {
		*out |= (a - 'a' + 10) << 4
	} else if a >= 'A' && a <= 'F' {
		*out |= (a - 'A' + 10) << 4
	}
	if b >= '0' && b <= '9' {
		*out |= (b - '0')
	} else if b >= 'a' && b <= 'f' {
		*out |= (b - 'a' + 10)
	} else if b >= 'A' && b <= 'F' {
		*out |= (b - 'A' + 10)
	}
}

// ChangeBuilder helps build mock CDC changes
type ChangeBuilder struct {
	table      string
	txID       string
	lsn        []byte
	operation  core.Operation
	data       map[string]interface{}
	commitTime time.Time
}

// NewChange creates a new change builder
func NewChange(table, txID string) *ChangeBuilder {
	return &ChangeBuilder{
		table:      table,
		txID:       txID,
		lsn:        BuildLSN("0000000001000000"),
		operation:  core.OpInsert,
		data:       make(map[string]interface{}),
		commitTime: time.Now(),
	}
}

// WithLSN sets the LSN
func (b *ChangeBuilder) WithLSN(lsnHex string) *ChangeBuilder {
	b.lsn = BuildLSN(lsnHex)
	return b
}

// WithOperation sets the operation type
func (b *ChangeBuilder) WithOperation(op core.Operation) *ChangeBuilder {
	b.operation = op
	return b
}

// WithData sets the data fields
func (b *ChangeBuilder) WithData(data map[string]interface{}) *ChangeBuilder {
	b.data = data
	return b
}

// WithCommitTime sets the commit time
func (b *ChangeBuilder) WithCommitTime(t time.Time) *ChangeBuilder {
	b.commitTime = t
	return b
}

// Build returns the constructed Change
func (b *ChangeBuilder) Build() core.Change {
	return core.Change{
		Table:         b.table,
		TransactionID: b.txID,
		LSN:           b.lsn,
		Operation:     b.operation,
		Data:          b.data,
		CommitTime:    b.commitTime,
	}
}

// BuildSlice builds a slice of changes
func BuildSlice(changes ...core.Change) []core.Change {
	return changes
}

// MockOrdersChange creates a mock INSERT change for the orders table
func MockOrdersChange(txID string, orderID int64, amount float64, status string) core.Change {
	return NewChange("dbo.orders", txID).
		WithLSN("0000000001000001").
		WithOperation(core.OpInsert).
		WithData(map[string]interface{}{
			"order_id":   orderID,
			"amount":     amount,
			"status":     status,
			"created_at": time.Now(),
		}).
		Build()
}

// MockOrderItemsChange creates a mock INSERT change for the order_items table
func MockOrderItemsChange(txID string, orderID int64, itemID int64, itemName string, price float64) core.Change {
	return NewChange("dbo.order_items", txID).
		WithLSN("0000000001000002").
		WithOperation(core.OpInsert).
		WithData(map[string]interface{}{
			"order_item_id": itemID,
			"order_id":      orderID,
			"item_name":     itemName,
			"price":         price,
		}).
		Build()
}

// MockInventoryChange creates a mock INSERT/UPDATE/DELETE change for the inventory table
func MockInventoryChange(txID string, productID int64, quantity int, operation core.Operation) core.Change {
	return NewChange("dbo.inventory", txID).
		WithLSN("0000000001000003").
		WithOperation(operation).
		WithData(map[string]interface{}{
			"product_id": productID,
			"quantity":   quantity,
		}).
		Build()
}

// MockProductChange creates a mock INSERT change for the products table
func MockProductChange(txID string, productID int64, productName string, price float64) core.Change {
	return NewChange("dbo.products", txID).
		WithLSN("0000000001000001").
		WithOperation(core.OpInsert).
		WithData(map[string]interface{}{
			"product_id":   productID,
			"product_name": productName,
			"price":        price,
		}).
		Build()
}

// SingleTransactionChanges creates changes all in the same transaction
// Useful for building cross-table transaction test data
type TransactionBuilder struct {
	txID       string
	changes    []core.Change
	commitTime time.Time
}

// NewTransaction creates a new transaction builder
func NewTransaction(txID string) *TransactionBuilder {
	return &TransactionBuilder{
		txID:       txID,
		changes:    make([]core.Change, 0),
		commitTime: time.Now(),
	}
}

// Add adds a change to the transaction
func (tb *TransactionBuilder) Add(c core.Change) *TransactionBuilder {
	tb.changes = append(tb.changes, c)
	return tb
}

// AddInsert adds an INSERT change
func (tb *TransactionBuilder) AddInsert(table string, data map[string]interface{}) *TransactionBuilder {
	tb.changes = append(tb.changes, NewChange(table, tb.txID).
		WithLSN("0000000001000000").
		WithOperation(core.OpInsert).
		WithData(data).
		WithCommitTime(tb.commitTime).
		Build())
	return tb
}

// AddUpdate adds an UPDATE change
func (tb *TransactionBuilder) AddUpdate(table string, data map[string]interface{}) *TransactionBuilder {
	tb.changes = append(tb.changes, NewChange(table, tb.txID).
		WithLSN("0000000001000000").
		WithOperation(core.OpUpdateAfter).
		WithData(data).
		WithCommitTime(tb.commitTime).
		Build())
	return tb
}

// AddDelete adds a DELETE change
func (tb *TransactionBuilder) AddDelete(table string, data map[string]interface{}) *TransactionBuilder {
	tb.changes = append(tb.changes, NewChange(table, tb.txID).
		WithLSN("0000000001000000").
		WithOperation(core.OpDelete).
		WithData(data).
		WithCommitTime(tb.commitTime).
		Build())
	return tb
}

// Build returns all changes in the transaction
func (tb *TransactionBuilder) Build() []core.Change {
	return tb.changes
}
