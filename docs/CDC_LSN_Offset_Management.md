# CDC LSN Offset Management

## Overview

This document describes the LSN offset management strategy used by the CDC poller to efficiently detect whether there is new data to fetch from MSSQL CDC.

## MSSQL CDC LSN Functions

| Function | Description |
|----------|-------------|
| `sys.fn_cdc_get_min_lsn(capture_instance)` | Returns the minimum LSN for a CDC capture instance |
| `sys.fn_cdc_get_max_lsn()` | Returns the current maximum LSN in the database |
| `sys.fn_cdc_increment_lsn(lsn)` | Returns the next LSN after the given one (`lsn + 1`) |
| `fn_cdc_get_all_changes_*` | CDC query: `__$start_lsn >= @from_lsn` |

## Core Algorithm

The key insight is that we can determine if there is new data by comparing:

```
hasNewData = (incrementLSN(stored_lsn) <= GetMaxLSN())
```

### Why This Works

- `incrementLSN(stored_lsn)` returns `stored_lsn + 1`, representing the "next LSN after what we've processed"
- `GetMaxLSN()` returns the actual maximum LSN that exists in the database
- If `incrementLSN(stored_lsn) > GetMaxLSN()`: no new data exists (database hasn't advanced past our stored position)
- If `incrementLSN(stored_lsn) <= GetMaxLSN()`: there is new data to fetch

## GetFromLSN Function

```go
func (p *Poller) GetFromLSN(ctx context.Context, table string, stored offset.Offset) (fromLSN []byte, hasData bool, err error)
```

### Logic Flow

```
Case 1: Fresh Start (stored.LSN == "")
    return GetMinLSN(table), true

Case 2: Resume (stored.LSN != "")
    nextLSN = IncrementLSN(stored_lsn)
    maxLSN = GetMaxLSN()

    if nextLSN > maxLSN:
        return nil, false  // No new data
    else:
        return stored_lsn, true  // Has new data
```

## Poll Cycle

```
for each table:
    stored = offsets.Get(table)
    fromLSN, hasData, err := GetFromLSN(table, stored)
    if err != nil:
        handle error
        continue
    if !hasData:
        continue  // Skip table, no new data

    changes = GetChanges(fromLSN, nil)
    if len(changes) > 0:
        lastLSN = changes[len(changes)-1].LSN
        offsets.Set(table, lastLSN)
```

## Scenarios

| Scenario | stored.LSN | incrementLSN(stored) | GetMaxLSN | Result |
|----------|------------|---------------------|-----------|--------|
| Fresh start | "" | - | - | GetMinLSN(), fetch |
| Has new data | "0x64" | 0x65 | 0x70 | Use 0x64, fetch |
| No new data | "0x64" | 0x65 | 0x64 | Skip (no new data) |
| Has data, same LSN | "0x64" | 0x65 | 0x65 | Use 0x64, fetch |

## LSN Properties

- **Database-level**: LSN is a global sequence number across the entire database, not per-table
- **Monotonically increasing**: LSN never decreases or reuse
- **Each table advances independently**: Different tables reach maxLSN at different rates

### Implications

- Table A might be at LSN 200 while Table B is still at LSN 100
- When calling `incrementLSN(100)` for Table B, it returns 101
- If `GetMaxLSN()` is 200, then `101 <= 200` means Table B "has data"
- Table B will query and receive all changes from LSN 100 onwards, including LSN 100 if it has new data there
- This is correct behavior - no data is lost

## Implementation Details

### IncrementLSN (cdc/query.go)

```go
func (q *Querier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error) {
    var nextLSN []byte
    err := q.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_increment_lsn(@p1)", lsn).Scan(&nextLSN)
    return nextLSN, err
}
```

### GetFromLSN (core/poller.go)

```go
func (p *Poller) GetFromLSN(ctx context.Context, table string, stored offset.Offset) (fromLSN []byte, hasData bool, err error) {
    if stored.LSN == "" {
        minLSN, err := p.querier.GetMinLSN(ctx, captureInstance)
        if err != nil {
            return nil, false, fmt.Errorf("get min LSN: %w", err)
        }
        return minLSN, true, nil
    }

    storedLSN, err := ParseLSN(stored.LSN)
    if err != nil {
        return nil, false, fmt.Errorf("parse LSN: %w", err)
    }

    nextLSN, err := p.querier.IncrementLSN(ctx, storedLSN)
    if err != nil {
        return nil, false, fmt.Errorf("increment LSN: %w", err)
    }

    maxLSN, err := p.querier.GetMaxLSN(ctx)
    if err != nil {
        return nil, false, fmt.Errorf("get max LSN: %w", err)
    }

    if LSN(nextLSN).Compare(LSN(maxLSN)) > 0 {
        return nil, false, nil  // No new data
    }
    return storedLSN, true, nil  // Has new data
}
```

## Benefits

1. **No filtering needed**: `GetFromLSN` guarantees `fromLSN` is correct, no need to filter `LSN == fromLSN`
2. **No bloom filter**: Avoids false positives that could cause data loss
3. **No flags**: Uses pure LSN comparison to determine state
4. **Clear semantics**: Three distinct cases (fresh start, has data, no data)
5. **Correctness**: LSN comparison is deterministic and exact
