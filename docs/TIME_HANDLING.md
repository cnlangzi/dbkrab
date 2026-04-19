# 统一时间处理方案

## 概述

本文档描述了 dbkrab 项目中统一的时间/日期处理方案，确保从 MSSQL CDC 到 SQLite 存储的整个数据流中时间处理的一致性、正确性和兼容性。

## 设计原则

1. **信任 Driver**: 利用 `mattn/go-sqlite3` driver 的成熟时间处理逻辑
2. **统一 UTC**: 所有时间内部存储和传输使用 UTC
3. **保留精度**: 支持纳秒级时间精度
4. **时区透明**: 仅在边界层（MSSQL 读取）处理时区转换

## 数据流

### 写入路径 (MSSQL → SQLite)

```
MSSQL CDC (DATETIME/SMALLDATETIME)
    ↓
cdc.DateTime.Scan() - 时区转换 (SQL Server 本地时间 → UTC)
    ↓
Go time.Time (UTC)
    ↓
直接传递给 tx.Exec() - 不手动格式化
    ↓
mattn/go-sqlite3 driver 自动序列化
    ↓
SQLite TEXT: "2006-01-02 15:04:05.999999999+00:00"
```

### 读取路径 (SQLite → Go)

```
SQLite TEXT: "2006-01-02 15:04:05.999999999+00:00"
    ↓
mattn/go-sqlite3 driver 自动解析 (列类型为 TIMESTAMP/DATETIME)
    ↓
Go time.Time (UTC)
    ↓
直接使用，无需解析
```

## 关键修改

### 1. `internal/cdc/types.go` - DateTime.Value()

**修改前:**
```go
func (d DateTime) Value() (driver.Value, error) {
    if !d.valid || d.val.IsZero() {
        return time.Time{}, nil  // 零时间存为零值
    }
    return d.val.Format(time.RFC3339Nano), nil  // 手动格式化为字符串
}
```

**修改后:**
```go
func (d DateTime) Value() (driver.Value, error) {
    if !d.valid || d.val.IsZero() {
        return nil, nil  // 无效时间存为 NULL
    }
    return d.val, nil  // 返回 time.Time，driver 自动序列化
}
```

**理由:**
- 让 driver 处理序列化，保留完整精度和时区信息
- 无效时间存为 NULL 而非 `0001-01-01`

---

### 2. `internal/sqliteutil/util.go` - normalizeRowValues()

**修改前:**
```go
func normalizeRowValues(columns []string, row []interface{}) []interface{} {
    normalized := make([]interface{}, len(row))
    for i, v := range row {
        if t, ok := v.(time.Time); ok {
            normalized[i] = t.Format("2006-01-02 15:04:05")  // 丢失纳秒和时区
        } else {
            normalized[i] = v
        }
    }
    return normalized
}
```

**修改后:**
```go
func normalizeRowValues(columns []string, row []interface{}) []interface{} {
    // 直接传递，driver 自动处理 time.Time 序列化
    return row
}
```

**理由:**
- 避免手动格式化导致精度丢失
- 简化代码，减少重复逻辑

---

### 3. `internal/store/sqlite/store.go` - UpdatePollerState()

**修改前:**
```go
_, err := s.db.Exec(`
    UPDATE poller_state
    SET last_poll_time = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = 1
`, lastLSN, fetchedCount, insertedCount)
```

**修改后:**
```go
now := time.Now().UTC()
_, err := s.db.Exec(`
    UPDATE poller_state
    SET last_poll_time = ?,
        updated_at = ?
    WHERE id = 1
`, now, lastLSN, fetchedCount, insertedCount, now)
```

**理由:**
- 统一使用 Go 生成时间，确保格式和精度一致
- 避免混用 `CURRENT_TIMESTAMP` 和 Go time 导致格式不一致

---

### 4. `internal/cdc/query.go` - 读取 commit time

**修改前:**
```go
switch v := val.(type) {
case time.Time:
    commitTime = v
case string:
    if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
        commitTime = parsed
    }
}
```

**修改后:**
```go
if t, ok := val.(time.Time); ok {
    commitTime = t
}
```

**理由:**
- `DateTime.Value()` 现在直接返回 `time.Time`，不需要处理字符串

---

### 5. `internal/sinker/manager.go` - parseDatetime()

**修改前:** 尝试 6 种格式解析
**修改后:** 简化为优先解析 RFC3339Nano 和 SQLite 原生格式

**理由:**
- Driver 会自动解析 TIMESTAMP 列为 `time.Time`
- 仅需处理边缘情况（遗留数据或 TEXT 列）

---

## SQLite 兼容性

### CURRENT_TIMESTAMP 行为

```sql
SELECT CURRENT_TIMESTAMP;
-- 返回："2026-04-19 03:05:22" (TEXT, UTC, 秒级精度)
```

**特性:**
- 返回类型：TEXT
- 格式：`YYYY-MM-DD HH:MM:SS` (19 字符)
- 时区：UTC（不是本地时间）
- 精度：秒级（无毫秒/纳秒）

**与 Driver 序列化格式对比:**

| 来源 | 格式 | 精度 | 时区 |
|------|------|------|------|
| `CURRENT_TIMESTAMP` | `2026-04-19 03:05:22` | 秒 | UTC |
| Driver (Go time.Time) | `2026-04-19 03:05:22.123456789+00:00` | 纳秒 | UTC |

**兼容性:** ✅ 两种格式都能被 driver 正确解析为 `time.Time`

---

## 时区处理

### MSSQL 时区转换

MSSQL CDC 的 `__$commit_time` 字段存在时区问题：
- SQL Server 返回的 `datetime` 没有时区信息
- Go MSSQL driver 错误地将其标记为 UTC
- 实际是 SQL Server 的本地时间（如 UTC+8）

**解决方案:**
```go
// convertTime 重新解释 driver 时间的时区
func (d *DateTime) convertTime(driverTime time.Time) time.Time {
    // 将 driver 的"UTC"时间重新解释为 SQL Server 本地时间
    // 然后转换为真正的 UTC
    return time.Date(
        driverTime.Year(), driverTime.Month(), driverTime.Day(),
        driverTime.Hour(), driverTime.Minute(), driverTime.Second(), driverTime.Nanosecond(),
        d.timezone,  // SQL Server 时区（如 Asia/Shanghai）
    ).UTC()
}
```

**示例:**
```
MSSQL 返回：2026-04-19 12:26:00 (driver 标记为 UTC，实际是 UTC+8)
转换后：   2026-04-19 04:26:00 UTC (正确)
```

---

## 边缘情况处理

| 情况 | 处理方式 |
|------|---------|
| **nil 值** | Scan 设置 `valid=false`，Value 返回 `nil` (SQLite NULL) |
| **零时间** | `time.IsZero()` 检查，Value 返回 `nil` |
| **时区缺失** | 假设源数据库时区，转换为 UTC |
| **纳秒精度** | 使用 RFC3339Nano 格式完整保留 |
| **无效字符串** | 解析失败返回错误，不静默忽略 |

---

## 测试验证

所有相关测试通过：
```bash
go test ./internal/cdc/... ./internal/store/... ./internal/sinker/... -v
```

关键测试：
- ✅ `TestDateTimeScanValue` - 验证时区转换
- ✅ `TestDateTimeZeroValue` - 验证零时间处理
- ✅ `TestSinker_*` - 验证 SQLite 写入
- ✅ `TestStore_*` - 验证 CDC 存储

---

## 迁移指南

### 现有数据

现有数据库中的时间数据无需迁移：
- Driver 能正确解析旧格式（`YYYY-MM-DD HH:MM:SS`）
- 新写入的数据使用更精确的格式
- 两种格式可以共存

### 新代码规范

1. **直接传递 `time.Time`**: 不要手动 Format 为字符串
2. **统一使用 UTC**: `time.Now().UTC()`
3. **避免 `CURRENT_TIMESTAMP`**: 用 Go 生成时间（可选，保留作为 fallback）
4. **依赖列类型**: 确保时间列声明为 `TIMESTAMP` 或 `DATETIME`

---

## 参考

- [mattn/go-sqlite3 时间处理](https://pkg.go.dev/github.com/mattn/go-sqlite3#SQLiteTimestampFormats)
- [SQLite 日期时间函数](https://www.sqlite.org/lang_datefunc.html)
- Go `time` 包文档
