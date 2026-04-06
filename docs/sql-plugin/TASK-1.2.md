# Task 1.2: CDC Parameter Injection

**文件**: `internal/sqlplugin/params.go`  
**阶段**: Phase 1 - MVP  
**依赖**: Task 1.1 (skill.yml Parser)  
**上游调用方**: Task 1.5 (SQL Plugin Engine)

---

## 目标

将 `core.Change` 转换为 SQL 模板参数，为后续 Executor 的 SQL 执行提供数据。

---

## 设计原则

1. **每个 Change 单独处理** — Engine 层循环调用，每次只处理一行 Change
2. **Data 字段优先** — `change.Data` 的值覆盖同名的 CDC 元数据
3. **OpUpdateBefore 忽略** — 直接返回空 map
4. **LSN 转换为 string** — 便于 SQL 模板代入

---

## 核心接口

```go
package sqlplugin

import (
    "github.com/cnlangzi/dbkrab/internal/core"
)

// BuildParams builds CDC parameters from a single Change for SQL template substitution.
// One change at a time (no array handling).
// Returns empty map if change.Operation is OpUpdateBefore.
func BuildParams(change *core.Change) (map[string]any, error)
```

### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `change` | `*core.Change` | 单行 CDC 变更数据 |

### 返回值

| 类型 | 说明 |
|------|------|
| `map[string]any` | SQL 模板参数键值对 |
| `error` | 始终返回 nil（暂不需校验） |

---

## 参数构建规则

### CDC 元数据参数

| 参数名 | 来源 | 示例 |
|--------|------|------|
| `@cdc_lsn` | `change.LSN` (转 string) | `"\x00\x01\xa2\xb"` |
| `@cdc_tx_id` | `change.TransactionID` | `"tx-12345"` |
| `@cdc_table` | `change.Table` | `"orders"` |
| `@cdc_operation` | `change.Operation` (转 int) | `2` |

### 自动生成的表名 ID 参数

| 参数名 | 来源 | 示例 |
|--------|------|------|
| `@{table}_id` | `change.Data[table + "_id"]` | `@orders_id` → `100` |

自动生成规则：`table_name + "_id"`（小写）

例如：
- 表名 `orders` → 参数 `orders_id`
- 表名 `order_items` → 参数 `order_items_id`

### Data 字段参数

`change.Data` 的所有字段直接展开为 `@{field}` 参数。

| 参数名 | 来源 | 示例 |
|--------|------|------|
| `@{任意字段名}` | `change.Data[field]` | `@order_id` → `100` |

---

## 字段优先级

**`change.Data` 覆盖 CDC 元数据**（用于处理用户自定义的同名字段）

```go
// CDC 元数据先写入
params["cdc_lsn"] = string(change.LSN)

// Data 字段后写入，同名覆盖
for k, v := range change.Data {
    params[k] = v
}
```

### 覆盖示例

```go
// 如果 Data 有 cdc_lsn 字段
change.Data = {"cdc_lsn": "user-defined-value", "order_id": 100}

// 最终 params["cdc_lsn"] = "user-defined-value" (Data 覆盖 metadata)
```

---

## OpUpdateBefore 处理

```go
if change.Operation == core.OpUpdateBefore {
    return make(map[string]any), nil
}
```

Engine 层遇到空的 params 时，应跳过该 Change 的处理。

---

## 数据类型

`change.Data` 是 `map[string]interface{}`，字段值可能是：

| Go 类型 | SQL 表达 | 示例 |
|---------|----------|------|
| `int` / `int64` | 数字 | `100` |
| `float64` | 数字 | `99.99` |
| `string` | 单引号字符串 | `'pending'` |
| `bool` | `1` / `0` | `true` → `1` |
| `time.Time` | 格式化为 string | `'2026-04-06 19:00:00'` |
| `[]byte` | hex string | `'\x00\x01'` |
| `nil` | `NULL` | - |

类型转换逻辑由 Task 1.3 Executor 处理。

---

## 单行示例

### 输入

```go
change := core.Change{
    Table:         "orders",
    TransactionID: "tx-12345",
    LSN:           []byte{0x00, 0x01, 0xA2, 0xB},
    Operation:     core.OpInsert,
    Data: map[string]any{
        "order_id": 100,
        "amount":   50.00,
        "status":   "pending",
    },
}
```

### 输出

```go
params := map[string]any{
    // CDC metadata (lower priority)
    "cdc_lsn":       "\x00\x01\xa2\xb",
    "cdc_tx_id":     "tx-12345",
    "cdc_table":     "orders",
    "cdc_operation": 2,

    // Auto-generated table ID (from table name)
    "orders_id": 100,

    // Data fields (higher priority, overwrite metadata if同名)
    "order_id": 100,
    "amount":   50.00,
    "status":   "pending",
}
```

### 对应 SQL 模板

```sql
-- 使用 @order_id（单数）对应 Data["order_id"]
SELECT * FROM orders WHERE order_id = @order_id

-- 使用 @orders_id（自动生成）
SELECT * FROM orders WHERE orders_id = @orders_id
```

---

## OpUpdateBefore 示例

### 输入

```go
change := core.Change{
    Table:         "orders",
    TransactionID: "tx-12345",
    LSN:           []byte{0x00, 0x01, 0xA2, 0xC},
    Operation:     core.OpUpdateBefore,  // 3
    Data: map[string]any{
        "order_id": 100,
        "amount":   50.00,
    },
}
```

### 输出

```go
params := map[string]any{}  // 空 map
```

Engine 层应检测到空 params 并跳过处理。

---

## 验收标准

- [ ] 单个 Change 构建出正确的参数 map
- [ ] `change.Data` 覆盖同名的 CDC 元数据字段
- [ ] LSN 从 `[]byte` 转换为 string
- [ ] `OpUpdateBefore` 返回空 map
- [ ] 自动生成 `{table}_id` 参数
- [ ] 单元测试覆盖正常场景和边界场景

---

## 文件结构

```
internal/sqlplugin/
├── skill.go       # Task 1.1
└── params.go      # Task 1.2 (本任务)
```

---

## 上游依赖

| 文件 | 依赖方 | 用途 |
|------|--------|------|
| `internal/core/transaction.go` | Engine | Change 数据结构定义 |

---

## 下游使用方

| 文件 | 用途 |
|------|------|
| Task 1.5 Engine | `BuildParams` 在循环每个 Change 时调用，结果传入 Executor.Execute |

---

## 单元测试要点

```go
func TestBuildParams_Insert(t *testing.T) { ... }
func TestBuildParams_Update(t *testing.T) { ... }
func TestBuildParams_Delete(t *testing.T) { ... }
func TestBuildParams_OpUpdateBefore_Ignored(t *testing.T) { ... }
func TestBuildParams_DataOverwritesMetadata(t *testing.T) { ... }
func TestBuildParams_AutoGenerateTableID(t *testing.T) { ... }
```

---

## TASKS.md 原始需求 vs 本方案调整

| TASKS.md 描述 | 本方案调整 | 原因 |
|--------------|-----------|------|
| `BuildParams(tx *core.Transaction, op Operation)` | 改为处理单个 `*core.Change` | 无数组场景，Engine 层循环调用 |
| `@{table}_ids` 数组收集 | 移除 | 用户明确：不会出现数组，每个 Change 单独处理 |
| 返回 `{table}_ids` 数组 | 移除 | 同上 |
| `cdc_operation` 为 `int` | 保持 | MSSQL 参数绑定需要具体数值 |

---

## 备注

- 暂不处理 `change.Data` 字段类型到 SQL 类型的转换，该逻辑在 Task 1.3 Executor 中实现
- 暂不处理 `nil` 值的 SQL 表达（由 Executor 决定如何序列化）
- 暂不校验 `change.Data` 的字段完整性，由上游负责
