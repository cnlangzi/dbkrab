# CDC LSN Offset Management

## Overview

This document describes the LSN offset management strategy used by the CDC poller to efficiently detect whether there is new data to fetch from MSSQL CDC.

## Problem Statement

MSSQL CDC functions `fn_cdc_get_all_changes_*` and `fn_cdc_get_net_changes_*` return rows where `__$start_lsn >= @from_lsn`. This means if we store the last processed LSN directly, the next query would re-fetch the same row.

**Example:**
- stored_lsn = `0x64`
- Query with `from_lsn = 0x64` returns rows at LSN `0x64`, `0x65`, `0x66`, etc.
- If we store `0x66` as the offset (last LSN), next query with `from_lsn = 0x66` returns `0x66`, `0x67`, etc.
- But what if the query returns `0x64` again? We'd re-fetch already processed data.

## Solution: hasNewData Flag

We store two pieces of information per table:
1. `lsn`: The current LSN position
2. `hasNewData`: Boolean flag indicating whether the last poll cycle found new data

### Why This Works

- `hasNewData = true`: Last poll had data, stored LSN is `nextLSN(lastProcessedLSN)`. We can directly use it as `fromLSN`.
- `hasNewData = false`: Last poll had no data, stored LSN is `lastProcessedLSN`. We need to check if new data exists.

## MSSQL CDC LSN Functions

| Function | Description |
|----------|-------------|
| `sys.fn_cdc_get_min_lsn(capture_instance)` | Returns the minimum LSN for a CDC capture instance |
| `sys.fn_cdc_get_max_lsn()` | Returns the current maximum LSN in the database |
| `sys.fn_cdc_increment_lsn(lsn)` | Returns the next LSN after the given one (`lsn + 1`) |
| `fn_cdc_get_all_changes_*` | CDC query: `__$start_lsn >= @from_lsn` |

## GetFromLSN Function

```go
func (p *Poller) GetFromLSN(ctx context.Context, table string, stored offset.Offset) (fromLSN []byte, hasData bool, err error)
```

### Logic Flow

```
Case 1: Fresh Start (stored.LSN == "")
    return GetMinLSN(table), true

Case 2: Resume with hasNewData == true (上次有数据)
    // 存储的是 incrementLSN(lastLSN)，是下一个待处理的 LSN
    return stored.LSN, true

Case 3: Resume with hasNewData == false (上次无数据)
    nextLSN = IncrementLSN(stored.LSN)
    maxLSN = GetMaxLSN()

    if nextLSN > maxLSN:
        return nil, false  // 仍无新数据
    else:
        return nextLSN, true  // 有新数据
```

## updateOffsets Function

This function is called after each poll cycle to update the offset. It sets the `hasNewData` flag based on whether changes were fetched.

```go
func (p *Poller) updateOffsets(ctx context.Context, results []tablePollResult, ...) error {
    for _, r := range results {
        if len(r.changes) == 0 {
            // 无数据：保持 LSN 不变，设置 hasNewData = false
            offsets.Set(table, currentOffset.LSN, false)
        else:
            // 有数据：存储 incrementLSN(lastLSN)，设置 hasNewData = true
            nextLSN = IncrementLSN(lastLSN)
            offsets.Set(table, nextLSN, true)
    }
}
```

### Why Store incrementLSN(lastLSN) When hasNewData = true?

When `hasNewData = true`, we store `incrementLSN(lastLSN)` because:
- MSSQL CDC returns `__$start_lsn >= from_lsn`
- If we stored `lastLSN` directly, the next query would re-fetch rows at `lastLSN`
- By storing `incrementLSN(lastLSN)`, the next query starts after the last processed row

### Why Keep LSN Unchanged When hasNewData = false?

When `hasNewData = false`, we keep the LSN unchanged because:
- The stored LSN is the position of the last processed row
- There was no new data at that position during the last poll
- If there is new data now, `GetFromLSN` will use `IncrementLSN(stored.LSN)` to find it

## Poll Cycle

```
for each table:
    stored = offsets.Get(table)
    fromLSN, hasData, err := GetFromLSN(table, stored)
    if err:
        handle error
        continue
    if !hasData:
        continue  // Skip table, no new data

    changes = GetChanges(fromLSN, nil)
    if len(changes) > 0:
        lastLSN = changes[len(changes)-1].LSN
        nextLSN = IncrementLSN(lastLSN)
        offsets.Set(table, nextLSN, true)  // hasNewData = true
    else:
        offsets.Set(table, stored.LSN, false)  // hasNewData = false, keep LSN
```

## Offset Storage Schema

```sql
CREATE TABLE offsets (
    table_name TEXT PRIMARY KEY,
    lsn TEXT NOT NULL,
    has_new_data INTEGER NOT NULL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
```

### JSON Representation

```json
{
  "lsn": "000235d200001f370004",
  "has_new_data": true,
  "updated_at": "2026-04-14T08:00:00Z"
}
```

## Scenarios

| Scenario | stored.LSN | stored.HasNewData | GetFromLSN Returns |
|----------|------------|-------------------|-------------------|
| Fresh start | "" | false | GetMinLSN(), true |
| Has new data | "0x65" | true | 0x65, true |
| No new data | "0x64" | false | nil, false (nextLSN > maxLSN) |
| New data arrived | "0x64" | false | 0x65, true (nextLSN <= maxLSN) |

## LSN Properties

- **Database-level**: LSN is a global sequence number across the entire database, not per-table
- **Monotonically increasing**: LSN never decreases or reuse
- **Each table advances independently**: Different tables reach maxLSN at different rates

### Implications

- Table A might be at LSN 200 while Table B is still at LSN 100
- When calling `incrementLSN(100)` for Table B, it returns 101
- If `GetMaxLSN()` is 200, then `101 <= 200` means Table B "has data"
- Table B will query and receive all changes from LSN 101 onwards
- This is correct behavior - no data is lost

## Benefits

1. **No duplicate fetches**: The `hasNewData` flag ensures we never re-fetch the same LSN
2. **Efficient detection**: `hasNewData = true` cases skip the `IncrementLSN` + `GetMaxLSN` comparison
3. **Clear semantics**: The flag distinguishes between "no data at this position" vs "data exists but not fetched yet"
4. **Correctness**: LSN comparison is deterministic and exact

## Implementation Details

### Offset Struct (internal/offset/offset.go)

```go
type Offset struct {
    LSN        string    `json:"lsn"`
    HasNewData bool      `json:"has_new_data"`
    UpdatedAt  time.Time `json:"updated_at"`
}
```

### StoreInterface (internal/offset/offset.go)

```go
type StoreInterface interface {
    Load() error
    Save() error
    Get(table string) (Offset, error)
    Set(table string, lsn string, hasNewData bool) error
    GetAll() (map[string]Offset, error)
}
```

### IncrementLSN (internal/cdc/query.go)

```go
func (q *Querier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error) {
    var nextLSN []byte
    err := q.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_increment_lsn(@p1)", lsn).Scan(&nextLSN)
    return nextLSN, err
}
```
