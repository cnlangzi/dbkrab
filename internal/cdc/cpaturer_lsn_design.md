# ChangeCapturer LSN Offset Design

## Overview

This document describes the LSN offset management strategy used by ChangeCapturer to efficiently detect whether there is new data to fetch from MSSQL CDC.

## Problem Statement

MSSQL CDC functions `fn_cdc_get_all_changes_*` and `fn_cdc_get_net_changes_*` return rows where `__$start_lsn >= @from_lsn`. If we store the last processed LSN directly, the next query would re-fetch already processed rows.

**Example:**
- last_lsn stored = `0x64`
- Query with `from_lsn = 0x64` returns rows at LSN `0x64`, `0x65`, `0x66`, etc.
- We process up to `0x66`, then store `next_lsn = incrementLSN(0x66) = 0x67`
- Next query with `from_lsn = 0x67` starts after what we've processed

## Solution: last_lsn vs globalMaxLSN Comparison

We store two pieces of information per table:
1. `last_lsn`: The LSN of the last processed row
2. `next_lsn`: incrementLSN(last_lsn), cached for convenience

### Key Insight

- `last_lsn != globalMaxLSN` → New data exists (globalMaxLSN has advanced)
- `last_lsn == globalMaxLSN` → No new data (we've caught up)

`globalMaxLSN` is fetched once at the start of each poll cycle as a consistent snapshot.

## MSSQL CDC LSN Functions

| Function | Description |
|----------|-------------|
| `sys.fn_cdc_get_min_lsn(capture_instance)` | Returns the minimum LSN for a CDC capture instance |
| `sys.fn_cdc_get_max_lsn()` | Returns the current maximum LSN in the database |
| `sys.fn_cdc_increment_lsn(lsn)` | Returns the next LSN after the given one |

## getFromLSN Method

```go
func (c *ChangeCapturer) getFromLSN(ctx context.Context, table string, stored offset.Offset, globalMaxLSN []byte) ([]byte, error)
```

### Logic Flow

```
Case 1: Fresh Start (stored.last_lsn == "")
    return GetMinLSN(captureInstance), nil

Case 2: Invalid stored LSN (hex decode error)
    return GetMinLSN(captureInstance), nil

Case 3: last_lsn != globalMaxLSN → New data available
    if stored.next_lsn != "":
        return stored.next_lsn (as cached fromLSN)
    else:
        return IncrementLSN(last_lsn)

Case 4: last_lsn == globalMaxLSN → No new data
    return nil, nil
```

### Why globalMaxLSN?

`globalMaxLSN` is fetched once at the start of each poll cycle as a consistent snapshot. This ensures all tables see the same max LSN boundary, maintaining cross-table transaction consistency.

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
   b. fromLSN, err := getFromLSN(table, stored, globalMaxLSN)
      - If last_lsn empty → getMinLSN()
      - If last_lsn invalid hex → getMinLSN()
      - If last_lsn != globalMaxLSN → use next_lsn or increment
      - If last_lsn == globalMaxLSN → skip (no data)
   c. If fromLSN == nil, continue to next table

   d. changes = GetChanges(fromLSN, nil)

   e. If changes > 0:
      - last_lsn = last change's LSN
      - next_lsn = incrementLSN(last_lsn)
      - offsets.Set(table, last_lsn, next_lsn)

      Else (no changes):
      - Nothing to update, keep existing offset
```

## Scenarios

| Scenario | last_lsn | globalMaxLSN | next_lsn | Result |
|----------|----------|--------------|----------|--------|
| Fresh start | "" | any | "" | getMinLSN() |
| Invalid hex | "invalid" | any | any | getMinLSN() |
| Has new data | 0x64 | 0x70 | 0x65 | Return 0x65 |
| No new data | 0x70 | 0x70 | 0x71 | Return nil |
| Data just arrived (no cached next_lsn) | 0x64 | 0x70 | "" | Return IncrementLSN(0x64) |

## Benefits

1. **Simple comparison**: `last_lsn != globalMaxLSN` is O(1) and exact
2. **Efficient**: No need to call `incrementLSN` + `GetMaxLSN` on every poll when `hasNewData=true`
3. **Cache friendly**: `next_lsn` pre-computed to avoid repeated `incrementLSN` calls
4. **Consistent snapshot**: `globalMaxLSN` ensures cross-table transaction boundaries are consistent
5. **Cold start resilience**: Invalid stored LSN triggers fresh getMinLSN() rather than failing

## Implementation Details

### CDCQuerier Interface (internal/cdc/capturer.go)

```go
type CDCQuerier interface {
    GetMinLSN(ctx context.Context, captureInstance string) ([]byte, error)
    GetMaxLSN(ctx context.Context) ([]byte, error)
    IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error)
    GetChanges(ctx context.Context, captureInstance string, tableName string, fromLSN []byte, toLSN []byte) ([]Change, error)
}
```

### Offset Struct (internal/offset/offset.go)

```go
type Offset struct {
    LastLSN   string    `json:"last_lsn"`
    NextLSN   string    `json:"next_lsn"`
    UpdatedAt time.Time `json:"updated_at"`
}
```

### OffsetGetter Interface (internal/cdc/capturer.go)

```go
type OffsetGetter interface {
    Get(table string) (offset.Offset, error)
}
```

### LSN Comparison (internal/cdc/capturer.go)

```go
type LSN []byte

func (l LSN) Compare(other []byte) int
```

Byte-by-byte comparison with length-aware ordering.
