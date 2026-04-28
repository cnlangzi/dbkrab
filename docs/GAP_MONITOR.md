# GAP Monitor Design

**CDC Data Integrity Monitoring System**

---

## Overview

GAP Monitor 是 CDC 数据完整性监控系统，用于检测：

| 问题类型 | 触发条件 | 风险级别 |
|---------|---------|---------|
| **数据断档 (Gap)** | `current_lsn < min_lsn` | **Critical** - 数据已丢失，无法恢复 |
| **复制延迟 (Lag)** | `lag_bytes > threshold` 或 `lag_duration > threshold` | Warning/Critical - 处理进度落后 |

---

## LSN Fundamentals

### SQL Server CDC LSN Structure

```
LSN = 10 bytes = VLF offset (4B) + log block offset (2B) + slot offset (4B)
      ┌─────────────┬───────────┬─────────────┐
      │ VLF Offset  │ Log Block │ Slot Offset │
      │   4 bytes   │  2 bytes  │   4 bytes   │
      └─────────────┴───────────┴─────────────┘
```

### Key LSN Types

| LSN 类型 | 来源函数 | 含义 | 用途 |
|---------|---------|------|------|
| **Min LSN** | `sys.fn_cdc_get_min_lsn(@capture_instance)` | CDC 清理边界，此值之前的数据已被 cleanup job 清理 | 断档检测基准 |
| **Table Max LSN** | `SELECT MAX(__$start_lsn) FROM cdc.{capture_instance}_CT` | 单表最新变更位置 | Lag 计算基准 |
| **Current LSN** | `offset.db` 的 `offsets` 表 | 本系统上次处理到的位置（per-table tracking） | 断档/延迟起点 |

### Global Max LSN（仅供参考）

```
sys.fn_cdc_get_max_lsn() → 全局数据库最新变更位置
```

**重要**：全局 Max LSN **不参与单表 gap 检测**，仅用于 Dashboard UI 显示"整体进度参考"。

**为什么不用全局 Max LSN 计算单表 Lag？**

- 全局 Max LSN 是整个数据库的最新变更位置，不代表特定表的变更
- 一张表可能很久没有变更，但全局 Max LSN 一直在推进
- 用全局 LSN 计算单表 lag 会产生**错误的延迟警报**

---

## GetTableMaxLSN Design

### 核心设计思路

```
单表 Lag = Table Max LSN - Current LSN
                ↑              ↑
         单表实际变更位置    本系统处理位置
```

**原则**：
1. **只用表级 LSN**：从 `cdc.{capture_instance}_CT` 查询 `MAX(__$start_lsn)`
2. **nil 则跳过**：如果 CT 表为空（无变更或被清理），返回 nil，跳过 gap 检测
3. **不 fallback 全局**：即使 table max LSN 为 nil，也不用全局 max LSN 替代

### 实现逻辑

```go
// GetTableMaxLSN returns the max LSN for a specific capture instance
// Design principle:
//   - Query CDC change table (CT) for actual table max LSN
//   - If CT is empty (no changes or cleaned up), return nil
//   - Caller should SKIP gap detection when nil is returned
//   - NEVER fallback to global max LSN for lag calculation
func (d *GapDetector) GetTableMaxLSN(ctx context.Context, captureInstance string) ([]byte, error) {
    // Validate capture instance to prevent SQL injection
    if !validCaptureInstance.MatchString(captureInstance) {
        return nil, fmt.Errorf("invalid capture instance name: %s", captureInstance)
    }

    var lsn []byte
    ctTable := fmt.Sprintf("cdc.%s_CT", captureInstance)
    query := fmt.Sprintf("SELECT MAX(__$start_lsn) FROM %s", ctTable)
    
    err := d.db.QueryRowContext(ctx, query).Scan(&lsn)
    if err != nil {
        // Query error (CT table might not exist) → return error
        return nil, fmt.Errorf("query table max LSN from %s: %w", ctTable, err)
    }
    
    // lsn == nil means CT table is empty (no changes or cleaned)
    // → return nil, caller should skip gap detection
    return lsn, nil
}
```

### nil 返回值的处理

| 情况 | GetTableMaxLSN 返回 | API 处理 |
|------|---------------------|---------|
| CT 表存在，有变更 | `[]byte` (非 nil) | 正常执行 gap 检测 |
| CT 表存在，但空（无变更） | `nil` | 跳过 gap 检测 |
| CT 表不存在或查询失败 | `error` | warn + continue（跳过） |
| CT 表数据被 CDC cleanup 清理 | `nil` 或可能小于 min_lsn | 跳过 gap 检测 |

---

## Gap Detection Logic

### CheckGap Implementation

```go
// CheckGap checks for CDC gaps and lag for a specific table
// Parameters:
//   - tableName: original table name (schema.table format)
//   - captureInstance: CDC capture instance name
//   - currentLSN: LSN position from offset.db (system's last processed position)
//   - tableMaxLSN: max LSN from CDC change table (actual table's latest change)
//
// tableMaxLSN Behavior:
//   - len(tableMaxLSN) > 0: use it as gap.MaxLSN for lag calculation
//   - len(tableMaxLSN) == 0: skip lag calculation (table has no changes or cleaned)
//   - DO NOT fallback to global GetMaxLSN() - global LSN is NOT per-table accurate
func (d *GapDetector) CheckGap(ctx context.Context, tableName, captureInstance string, 
    currentLSN []byte, tableMaxLSN []byte) (GapInfo, error) {
    
    gap := GapInfo{
        Table:           tableName,
        CaptureInstance: captureInstance,
        CurrentLSN:      currentLSN,
        CheckedAt:       time.Now(),
    }

    // Step 1: Get CDC min LSN (cleanup boundary)
    minLSN, err := d.GetMinLSN(ctx, captureInstance)
    if err != nil {
        return gap, fmt.Errorf("get min LSN: %w", err)
    }
    gap.MinLSN = minLSN

    // Step 2: Set max LSN (ONLY from tableMaxLSN, never fallback to global)
    // If tableMaxLSN is nil/empty, skip lag-related calculations
    if len(tableMaxLSN) > 0 {
        gap.MaxLSN = tableMaxLSN
    }
    // NOTE: Removed fallback to GetMaxLSN() - it's incorrect for per-table lag

    // Step 3: Gap detection (data loss check)
    // current_lsn < min_lsn → data has been cleaned before we processed it
    if len(currentLSN) > 0 && len(minLSN) > 0 {
        if CompareLSN(currentLSN, minLSN) < 0 {
            gap.HasGap = true
            gap.MissingLSNRange = LSNRange{
                Start: currentLSN,
                End:   minLSN,
            }
        }
    }

    // Step 4: Lag calculation (ONLY if max_lsn is available)
    // Skip if tableMaxLSN was nil (no changes or cleaned)
    if len(gap.MaxLSN) > 0 && len(currentLSN) > 0 {
        gap.LagBytes = LSNBytesDiff(gap.MaxLSN, currentLSN)
        
        // Estimate lag duration via LSN → timestamp mapping
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
```

### Gap vs Lag

| 概念 | 条件 | 含义 | 状态 |
|------|------|------|------|
| **Gap (数据断档)** | `current_lsn < min_lsn` | 需要处理的数据已被 CDC cleanup 清理 | **Critical** |
| **Lag (复制延迟)** | `max_lsn - current_lsn > threshold` | 处理进度落后于最新变更 | Warning/Critical |

**关键区别**：
- Gap 检测不需要 max_lsn，只需要 current_lsn vs min_lsn
- Lag 计算需要 max_lsn，如果 tableMaxLSN 为 nil 则跳过 lag 计算

---

## Status Determination

### 状态判定逻辑

```go
// Priority: Gap > Critical Lag > Warning Lag > Healthy
func determineStatus(gapInfo GapInfo, warnBytes, warnDuration, critBytes, critDuration) string {
    // Data gap is always critical (data loss)
    if gapInfo.HasGap {
        return "critical"
    }
    
    // No max_lsn means table has no changes or cleaned - skip lag checks
    if len(gapInfo.MaxLSN) == 0 {
        return "healthy"  // or "no_data" for UI differentiation
    }
    
    // Critical lag threshold
    if gapInfo.LagBytes > critBytes || gapInfo.LagDuration > critDuration {
        return "critical"
    }
    
    // Warning lag threshold
    if gapInfo.LagBytes > warnBytes || gapInfo.LagDuration > warnDuration {
        return "warning"
    }
    
    return "healthy"
}
```

### 状态优先级

1. **HasGap = true** → **Critical**（数据丢失，不可恢复）
2. **LagBytes > CriticalThreshold** → **Critical**
3. **LagDuration > CriticalThreshold** → **Critical**
4. **LagBytes > WarningThreshold** → **Warning**
5. **LagDuration > WarningThreshold** → **Warning**
6. **MaxLSN = nil** → **Healthy**（或 UI 显示 "No Data"）
7. **其他** → **Healthy**

---

## Configuration

### Thresholds (config.yaml)

```yaml
cdc:
  gap:
    enabled: true
    check_interval: "1m"              # Gap check frequency
    warning_lag_bytes: 104857600      # 100MB
    warning_lag_duration: "1h"
    critical_lag_bytes: 1073741824    # 1GB  
    critical_lag_duration: "6h"
    recovery:
      strategy: "manual"              # manual | snapshot | timestamp
```

### Default Values

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `check_interval` | 1m | Gap 检测轮询间隔 |
| `warning_lag_bytes` | 100MB | 延迟字节警告阈值 |
| `warning_lag_duration` | 1h | 延迟时间警告阈值 |
| `critical_lag_bytes` | 1GB | 延迟字节严重阈值 |
| `critical_lag_duration` | 6h | 延迟时间严重阈值 |

---

## Data Sources

### Per-Table Offset Tracking

```
offset.db (SQLite)
    └── offsets table
        ├── table_name (TEXT, PK)
        ├── last_lsn (TEXT)    ← Current LSN for this table
        ├── next_lsn (TEXT)    ← Pre-computed next start point
        └── updated_at (TIMESTAMP)
```

**设计原则**：
- 每表独立 tracking，不同表可能有不同的处理进度
- `last_lsn` 是上次处理到的 LSN（用于 gap/lag 计算）
- `next_lsn` 是 `incrementLSN(last_lsn)` 的缓存（用于下次查询起点）

### API Flow

```
handleCDCGap (server.go)
    │
    ├── 1. Load offsetStore (刷新最新 offset 数据)
    │
    ├── 2. Get global max LSN (仅用于 UI 显示，不参与计算)
    │       → GetMaxLSN()
    │
    ├── 3. For each tracked table:
    │       ├── Get current_lsn from offsetStore.Get(table)
    │       ├── Get table_max_lsn from GetTableMaxLSN(capture_instance)
    │       │   └── If nil: continue (skip this table)
    │       │   └── If error: warn + continue
    │       ├── CheckGap(table, capture_instance, current_lsn, table_max_lsn)
    │       └── Determine status (critical/warning/healthy)
    │
    └── 4. Return JSON response
```

---

## LSN Utility Functions

### CompareLSN

```go
// CompareLSN compares two LSN values byte-by-byte
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func CompareLSN(a, b []byte) int {
    // Handle nil/empty cases
    if len(a) == 0 && len(b) == 0 { return 0 }
    if len(a) == 0 { return -1 }
    if len(b) == 0 { return 1 }
    
    // Compare from most significant byte to least
    for i := 0; i < min(len(a), len(b)); i++ {
        if a[i] < b[i] { return -1 }
        if a[i] > b[i] { return 1 }
    }
    
    // All compared bytes equal, length determines order
    if len(a) < len(b) { return -1 }
    if len(a) > len(b) { return 1 }
    return 0
}
```

### LSNBytesDiff

```go
// LSNBytesDiff calculates approximate byte difference between two LSNs
// Uses only lower 8 bytes to fit in int64 (avoids overflow)
// For typical CDC lag scenarios, 8 bytes provides sufficient precision
func LSNBytesDiff(a, b []byte) int64 {
    const lsnLen = 10
    const usableBytes = 8
    
    if len(a) != lsnLen || len(b) != lsnLen {
        return 0
    }
    
    // Walk from least-significant byte, use only lower 8 bytes
    diff := int64(0)
    multiplier := int64(1)
    
    for i := lsnLen - 1; i >= lsnLen - usableBytes; i-- {
        diff += (int64(a[i]) - int64(b[i])) * multiplier
        multiplier *= 256
    }
    
    if diff < 0 { diff = -diff }
    return diff
}
```

### GetLSNTime

```go
// GetLSNTime maps LSN to transaction commit timestamp
// Uses sys.fn_cdc_map_lsn_to_time() SQL Server function
func (d *GapDetector) GetLSNTime(ctx context.Context, lsn []byte) (time.Time, error) {
    var t time.Time
    err := d.db.QueryRowContext(ctx, 
        "SELECT sys.fn_cdc_map_lsn_to_time(@lsn)",
        sql.Named("lsn", lsn)).Scan(&t)
    return t, err
}
```

---

## Frontend Implementation (gap.html)

### UI Components

| 组件 | 内容 | 数据来源 |
|------|------|---------|
| **Overview Cards** | Max LSN (global), Table Count, Healthy Count, Issues Count | API `/api/cdc/gap` |
| **Threshold Info** | Warning/Critical thresholds display | API response `thresholds` |
| **Table List** | Per-table LSN, lag bytes, lag duration, status | API response `tables[]` |

### Status Badge Colors

```javascript
const statusColors = {
    'critical': 'error',    // Red - data loss or severe lag
    'warning': 'warning',   // Yellow - moderate lag
    'healthy': 'success'    // Green - normal
}
```

### Sorting Logic

```javascript
// Sort by status priority (critical first)
const statusOrder = { 'critical': 0, 'warning': 1, 'healthy': 2 };
sortedTables = tables.sort((a, b) => {
    if (statusOrder[a.status] !== statusOrder[b.status]) {
        return statusOrder[a.status] - statusOrder[b.status];
    }
    return a.table.localeCompare(b.table);
});
```

---

## API Response Schema

```json
{
  "success": true,
  "count": 3,
  "tables": [
    {
      "table": "dbo.orders",
      "schema": "dbo",
      "name": "orders",
      "capture_instance": "dbo_orders",
      "current_lsn": "0x0000002D00000A760066",
      "min_lsn": "0x0000002D00000A000000",
      "max_lsn": "0x0000002D00000B00007A",
      "has_gap": false,
      "lag_bytes": 524288,
      "lag_duration": "2m30s",
      "lag_duration_secs": 150,
      "status": "healthy",
      "status_color": "success",
      "checked_at": "2025-04-28T01:00:00Z"
    }
  ],
  "max_lsn": "0x0000002D00000B00007A",  // Global max (for UI display only)
  "thresholds": {
    "warning_lag_bytes": "104857600",
    "warning_lag_duration": "1h",
    "critical_lag_bytes": "1073741824",
    "critical_lag_duration": "6h"
  }
}
```

---

## Code Review Checklist

### GetTableMaxLSN 修改要点

1. **移除 fallback 注释**：
   - 旧注释：`caller should fallback to global max LSN`
   - 新注释：`caller should SKIP gap detection when nil is returned`

2. **CheckGap 修改要点**：
   - 移除 `else { maxLSN, err := d.GetMaxLSN(ctx) }` fallback 分支
   - 当 `len(tableMaxLSN) == 0` 时，`gap.MaxLSN` 保持为 nil
   - lag 计算时检查 `len(gap.MaxLSN) > 0`，否则跳过

3. **API handleCDCGap 修改要点**：
   - 当 `GetTableMaxLSN` 返回 nil 时，可选择：
     - continue（跳过该表）
     - 或返回 status: "no_data" 让 UI 区分显示
   - 不用 globalMaxLSN 替代 tableMaxLSN

### 单元测试补充

```go
func TestGetTableMaxLSN_EmptyTable(t *testing.T) {
    // CT table exists but empty → should return nil, not error
    lsn, err := detector.GetTableMaxLSN(ctx, "dbo_empty_table")
    assert.NoError(t, err)
    assert.Nil(t, lsn)
}

func TestCheckGap_NoTableMaxLSN(t *testing.T) {
    // When tableMaxLSN is nil, should skip lag calculation
    gap, err := detector.CheckGap(ctx, "dbo.test", "dbo_test", currentLSN, nil)
    assert.NoError(t, err)
    assert.Nil(t, gap.MaxLSN)
    assert.Equal(t, int64(0), gap.LagBytes)
    assert.Equal(t, time.Duration(0), gap.LagDuration)
}
```

---

## Recovery Strategy (When HasGap = true)

| Strategy | 动作 | 适用场景 |
|----------|------|---------|
| **manual** | 手动干预，需运维介入 | 默认策略，安全第一 |
| **snapshot** | 自动触发全量快照同步 | 数据可重建，有明确 PK |
| **timestamp** | 从指定时间点重新拉取 | 有明确的时间边界 |

**当前实现**：`recovery.strategy = "manual"` 是默认值，Gap 检测只报警，不自动触发恢复。

---

## Summary

### Key Design Principles

1. **只用表级 LSN**：`GetTableMaxLSN()` 是单表 lag 计算的唯一来源
2. **nil 则跳过**：CT 表为空时跳过 gap/lag 计算，不 fallback
3. **全局仅供参考**：`GetMaxLSN()` 只用于 UI 显示，不参与单表计算
4. **Gap 优于 Lag**：数据断档永远 Critical，延迟需要阈值判定
5. **Per-table tracking**：每表独立 offset，不同表进度可能不同

### Files to Modify

| File | Change |
|------|--------|
| `internal/cdc/gap_detector.go` | 移除 CheckGap 的 GetMaxLSN fallback 分支 |
| `cmd/app/server.go` | handleCDCGap 处理 nil tableMaxLSN 的逻辑 |
| `internal/cdc/gap_detector.go` | 更新注释，说明正确的设计意图 |