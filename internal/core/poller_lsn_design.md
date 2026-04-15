# Poller LSN Offset Design

## Overview

This document describes the LSN offset management strategy used by the CDC poller to efficiently detect whether there is new data to fetch from MSSQL CDC.

## Problem Statement

MSSQL CDC functions `fn_cdc_get_all_changes_*` and `fn_cdc_get_net_changes_*` return rows where `__$start_lsn >= @from_lsn`. If we store the last processed LSN directly, the next query would re-fetch already processed rows.

**Example:**
- last_lsn stored = `0x64`
- Query with `from_lsn = 0x64` returns rows at LSN `0x64`, `0x65`, `0x66`, etc.
- We process up to `0x66`, then store `next_lsn = incrementLSN(0x66) = 0x67`
- Next query with `from_lsn = 0x67` starts after what we've processed

## Solution: last_lsn vs max_lsn Comparison

We store three pieces of information per table:
1. `last_lsn`: The LSN of the last processed row
2. `next_lsn`: incrementLSN(last_lsn), cached for convenience
3. `max_lsn`: The max LSN in the database when this offset was saved

### Key Insight

- `last_lsn != max_lsn` → New data exists (max has advanced)
- `last_lsn == max_lsn` → No new data (we've caught up)

## MSSQL CDC LSN Functions

| Function | Description |
|----------|-------------|
| `sys.fn_cdc_get_min_lsn(capture_instance)` | Returns the minimum LSN for a CDC capture instance |
| `sys.fn_cdc_get_max_lsn()` | Returns the current maximum LSN in the database |
| `sys.fn_cdc_increment_lsn(lsn)` | Returns the next LSN after the given one |

## GetFromLSN Function

```go
func (p *Poller) GetFromLSN(ctx context.Context, table string, stored offset.Offset, globalMaxLSN LSN) ([]byte, error)
```

### Logic Flow

```
Case 1: Fresh Start (stored.last_lsn == "")
    return GetMinLSN(table), nil

Case 2: last_lsn != globalMaxLSN → New data available
    return stored.next_lsn (as cached fromLSN)

Case 3: last_lsn == globalMaxLSN → No new data
    return nil
```

### Why globalMaxLSN?

`globalMaxLSN` is fetched once at the start of each poll cycle as a consistent snapshot. This ensures all tables see the same max LSN boundary, maintaining cross-table transaction consistency.

Note: The `max_lsn` column was removed from the offset schema (it was stored but never used for comparison after the change to use `globalMaxLSN`).

## Offset Storage Schema

```sql
CREATE TABLE offsets (
    table_name TEXT PRIMARY KEY,
    last_lsn TEXT,
    next_lsn TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
```

### JSON Representation

```json
{
  "last_lsn": "000235d200001f370004",
  "next_lsn": "000235d200001f370005",
  "updated_at": "2026-04-14T08:00:00Z"
}
```

## Poll Cycle

```
1. globalMaxLSN = GetMaxLSN()  // Consistent snapshot for all tables

2. For each table:
   a. stored = offsets.Get(table)
   b. fromLSN, err := GetFromLSN(table, stored, globalMaxLSN)
      - If last_lsn empty → getMinLSN()
      - If last_lsn != max_lsn → use next_lsn
      - If last_lsn == max_lsn → skip (no data)
   c. If fromLSN == nil, continue to next table
   
   d. changes = GetChanges(fromLSN, nil)
   
   e. If changes > 0:
      - last_lsn = last change's LSN
      - next_lsn = incrementLSN(last_lsn)
      - max_lsn = globalMaxLSN
      - offsets.Set(table, last_lsn, next_lsn, max_lsn)
      
      Else (no changes):
      - Keep existing last_lsn, next_lsn
      - max_lsn = globalMaxLSN (updated for next comparison)
      - offsets.Set(table, last_lsn, next_lsn, max_lsn)
```

## Scenarios

| Scenario | last_lsn | globalMaxLSN | next_lsn | Result |
|----------|----------|--------------|----------|--------|
| Fresh start | "" | any | "" | getMinLSN() |
| Has new data | 0x64 | 0x70 | 0x65 | Return 0x65 |
| No new data | 0x70 | 0x70 | 0x71 | Return nil |
| Data just arrived | 0x64 | 0x70 | 0x65 | Return 0x65 |

## Why Store max_lsn?

`max_lsn` enables the `last_lsn != max_lsn` comparison:

- When we save an offset after processing, we capture `max_lsn` at that moment
- On next poll, if `max_lsn` has advanced beyond `last_lsn`, we know new data exists
- This works because MSSQL LSN is monotonically increasing database-wide

## Benefits

1. **Simple comparison**: `last_lsn != max_lsn` is O(1) and exact
2. **Efficient**: No need to call `incrementLSN` + `GetMaxLSN` on every poll when `hasNewData=true`
3. **Cache friendly**: `next_lsn` pre-computed to avoid repeated `incrementLSN` calls
4. **Consistent snapshot**: `globalMaxLSN` ensures cross-table transaction boundaries are consistent

## Implementation Details

### Offset Struct (internal/offset/offset.go)

```go
type Offset struct {
    LastLSN   string    `json:"last_lsn"`
    NextLSN   string    `json:"next_lsn"`
    UpdatedAt time.Time `json:"updated_at"`
}
```

### StoreInterface (internal/offset/offset.go)

```go
type StoreInterface interface {
    Get(table string) (Offset, error)
    Set(table, lastLSN, nextLSN string) error
    GetAll() (map[string]Offset, error)
    Load() error
}
```

### IncrementLSN (internal/cdc/cdc.go)

```go
func (q *Querier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error)
```

Calls `SELECT sys.fn_cdc_increment_lsn(@p1)`.
