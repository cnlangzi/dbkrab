package mockmssql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
)

// Driver implements database/sql/driver.Driver
type Driver struct {
	handlers map[string]*MockHandler
}

// MockHandler holds mock data and state
type MockHandler struct {
	Tables       map[string]*TableData
	MaxLSN       []byte
	CurrentLSN   []byte
	QueryResults map[string][]MockRow
}

// TableData holds mock table data
type TableData struct {
	Columns []string
	Rows    [][]interface{}
}

// MockRow represents a single row in query results
type MockRow map[string]interface{}

// NewDriver creates a new mock MSSQL driver
func NewDriver() *Driver {
	d := &Driver{
		handlers: make(map[string]*MockHandler),
	}
	d.registerDefaultHandler()
	return d
}

func (d *Driver) registerDefaultHandler() {
	h := &MockHandler{
		Tables:   make(map[string]*TableData),
		MaxLSN:   []byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 1}, // 0x000000000100000001
		CurrentLSN: []byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 1},
		QueryResults: make(map[string][]MockRow),
	}

	// Register default tables
	h.Tables["TestProducts"] = &TableData{
		Columns: []string{"ProductID", "ProductName", "Price", "Stock"},
		Rows: [][]interface{}{
			{1, "Product A", 10.99, 100},
			{2, "Product B", 20.99, 50},
		},
	}

	h.Tables["TestOrders"] = &TableData{
		Columns: []string{"OrderID", "ProductID", "Quantity", "TotalAmount"},
		Rows: [][]interface{}{
			{1, 1, 2, 21.98},
		},
	}

	h.Tables["TestOrderItems"] = &TableData{
		Columns: []string{"OrderItemID", "OrderID", "ItemName", "ItemPrice"},
		Rows: [][]interface{}{
			{1, 1, "Item A", 10.99},
		},
	}

	d.handlers["default"] = h
}

// Open returns a new connection to the mock database
func (d *Driver) Open(name string) (driver.Conn, error) {
	h, ok := d.handlers[name]
	if !ok {
		h = d.handlers["default"]
	}

	return &conn{handler: h}, nil
}

// conn implements driver.Conn
type conn struct {
	handler *MockHandler
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return &tx{conn: c}, nil
}

// Exec implements context.Context executor
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Handle CDC system queries
	query = normalizeQuery(query)

	switch query {
	case "SELECT sys.fn_cdc_get_max_lsn()":
		return &mockResult{rowsAffected: 1}, nil
	case "SELECT sys.fn_cdc_get_min_lsn('dbo_TestProducts')":
		return &mockResult{rowsAffected: 1}, nil
	case "SELECT sys.fn_cdc_get_min_lsn('dbo_TestOrders')":
		return &mockResult{rowsAffected: 1}, nil
	case "SELECT sys.fn_cdc_get_min_lsn('dbo_TestOrderItems')":
		return &mockResult{rowsAffected: 1}, nil
	default:
		return &mockResult{rowsAffected: 0}, nil
	}
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {

	query = normalizeQuery(query)

	// Handle CDC function queries
	switch query {
	case "SELECT sys.fn_cdc_get_max_lsn()":
		return &rows{
			columns: []string{"computed"},
			values:  [][]interface{}{{c.handler.MaxLSN}},
		}, nil
	case "SELECT sys.fn_cdc_get_min_lsn('dbo_TestProducts')":
		return &rows{
			columns: []string{"computed"},
			values:  [][]interface{}{{[]byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 1}}},
		}, nil
	case "SELECT sys.fn_cdc_get_min_lsn('dbo_TestOrders')":
		return &rows{
			columns: []string{"computed"},
			values:  [][]interface{}{{[]byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 1}}},
		}, nil
	case "SELECT sys.fn_cdc_get_min_lsn('dbo_TestOrderItems')":
		return &rows{
			columns: []string{"computed"},
			values:  [][]interface{}{{[]byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 1}}},
		}, nil
	case "SELECT snapshot_isolation_state FROM sys.databases WHERE name = DB_NAME()":
		return &rows{
			columns: []string{"snapshot_isolation_state"},
			values:  [][]interface{}{{1}},
		}, nil
	default:
		// Check for CDC net_changes function
		if len(query) > 30 && query[:30] == "SELECT *, sys.fn_cdc_map_lsn" {
			// Return empty CDC changes for now
			return &rows{
				columns: []string{"__$start_lsn", "__$operation", "__$transaction_id", "ProductID", "ProductName", "Price", "Stock"},
				values:  [][]interface{}{},
			}, nil
		}
	}

	// Default: return empty result
	return &rows{
		columns: []string{},
		values:  [][]interface{}{},
	}, nil
}

// tx implements driver.Tx
type tx struct {
	conn *conn
}

func (t *tx) Commit() error {
	return nil
}

func (t *tx) Rollback() error {
	return nil
}

// stmt implements driver.Stmt
type stmt struct {
	conn  *conn
	query string
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return &mockResult{rowsAffected: 1}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return &rows{
		columns: []string{},
		values:  [][]interface{}{},
	}, nil
}

// rows implements driver.Rows
type rows struct {
	columns []string
	values  [][]interface{}
	pos     int
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.values) {
		return io.EOF
	}

	row := r.values[r.pos]
	for i, val := range row {
		if i >= len(dest) {
			break
		}
		dest[i] = val
	}
	r.pos++
	return nil
}

// mockResult implements driver.Result
type mockResult struct {
	rowsAffected int64
	lastInsertID int64
}

func (r *mockResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *mockResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// normalizeQuery removes common SQL variations for matching
func normalizeQuery(query string) string {
	// Remove extra whitespace
	result := ""
	for i := 0; i < len(query); i++ {
		c := query[i]
		if c == '\t' || c == '\n' || c == '\r' {
			c = ' '
		}
		if c == ' ' && len(result) > 0 && result[len(result)-1] == ' ' {
			continue
		}
		result += string(c)
	}
	return result
}

func init() {
	sql.Register("mockmssql", NewDriver())
}

// HandlerForTest returns the mock handler for testing purposes
func HandlerForTest() *MockHandler {
	d := &Driver{
		handlers: make(map[string]*MockHandler),
	}
	d.registerDefaultHandler()
	return d.handlers["default"]
}