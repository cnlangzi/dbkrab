# CDC LSN Offset 管理优化方案

## 1. 问题背景

### 1.1 MSSQL CDC LSN 特性

- **LSN 是数据库级别的全局序列号**，不是表级别的
- LSN 是事务日志序列号，格式为 `0x000235c90000263e0001`（文件号:偏移:序列号）
- LSN 严格单调递增，每个新事务分配新的 LSN
- 不同表的 LSN 独立推进，但共享同一个全局序列号空间

### 1.2 MSSQL CDC 函数

| 函数 | 说明 |
|------|------|
| `sys.fn_cdc_get_min_lsn(capture_instance)` | 获取 CDC capture instance 的起始 LSN |
| `sys.fn_cdc_get_max_lsn()` | 获取数据库当前最大 LSN |
| `sys.fn_cdc_increment_lsn(lsn)` | 返回 `lsn + 1` 的 LSN |
| `fn_cdc_get_all_changes_*` | 查询 CDC 变更，`__$start_lsn >= @from_lsn` |

### 1.3 现有实现问题

当前实现在 `poller.go` 中：
- 使用 `startLSN` 直接查询 `GetChanges`
- 依赖过滤 `LSN == startLSN` 的记录来避免重复
- 逻辑分散，需要改进

## 2. 方案设计

### 2.1 核心思想

使用 `incrementLSN` 和 `GetMaxLSN` 的比较来判断是否有新数据，无需 flag。

### 2.2 关键公式

```
hasNewData = (incrementLSN(stored_lsn) <= GetMaxLSN())
```

**解释**：
- `incrementLSN(lsn)` 返回 `lsn + 1`，是"下一个可用的 LSN"
- `GetMaxLSN()` 返回数据库当前实际存在的最大 LSN
- 如果 `incrementLSN(stored_lsn) > GetMaxLSN()`：stored_lsn 之后没有任何新数据
- 如果 `incrementLSN(stored_lsn) <= GetMaxLSN()`：stored_lsn 之后有新数据可以拉取

### 2.3 新增 CDC 函数

在 `internal/cdc/query.go` 中添加：

```go
// IncrementLSN returns the next LSN after the given one
func (q *Querier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error) {
    var nextLSN []byte
    err := q.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_increment_lsn(@p1)", lsn).Scan(&nextLSN)
    return nextLSN, err
}
```

## 3. GetFromLSN 函数设计

### 3.1 函数签名

```go
// GetFromLSN 返回用于 CDC 查询的起始 LSN
// 返回值:
//   - lsn: 起始 LSN
//   - hasData: 是否有新数据可以拉取
//   - err: 错误信息
func (p *Poller) GetFromLSN(ctx context.Context, table string, stored StoredOffset) (lsn []byte, hasData bool, err error)
```

### 3.2 逻辑流程

```
GetFromLSN(table, stored):
    // Case 1: 无存储 offset → Fresh Start
    if stored.LSN == "":
        return GetMinLSN(table), true, nil

    // Case 2: 有存储 offset → Resume
    storedLSN := ParseLSN(stored.LSN)

    // 获取 incrementLSN 和 maxLSN
    nextLSN := IncrementLSN(storedLSN)
    maxLSN := GetMaxLSN()

    // Case 3: incrementLSN > maxLSN → 没有新数据
    if nextLSN > maxLSN:
        return nil, false, nil  // hasData = false

    // Case 4: incrementLSN <= maxLSN → 有新数据
    return storedLSN, true, nil
```

### 3.3 场景覆盖

| 场景 | stored.LSN | incrementLSN(stored) | GetMaxLSN | 结果 |
|------|------------|---------------------|-----------|------|
| Fresh Start | "" | - | - | GetMinLSN(), hasData=true |
| 有新数据 | "0x64" | 0x65 | 0x70 | 0x64, hasData=true |
| 刚好没新数据 | "0x64" | 0x65 | 0x64 | nil, hasData=false |
| 积累了一些新数据 | "0x64" | 0x65 | 0x68 | 0x64, hasData=true |
| 多次 poll 无新数据 | "0x64" | 0x65 | 0x64 | nil, hasData=false |

## 4. Poll 循环优化

### 4.1 新流程

```
PollCycle(tables):
    for each table in tables:
        // 1. 获取 fromLSN
        fromLSN, hasData, err := GetFromLSN(table)
        if err != nil:
            handleError(err)
            continue
        if !hasData:
            // 没有新数据，跳过此表
            continue

        // 2. 拉取数据
        changes := GetChanges(fromLSN, nil)

        // 3. 处理并保存 offset（保存的是 lastLSN，不是 fromLSN）
        if len(changes) > 0:
            lastLSN := changes[len(changes)-1].LSN
            SaveOffset(table, lastLSN)
        else:
            // 有数据但被过滤了，用 fromLSN 的 increment 作为 lastLSN
            // 这不应该发生，因为 hasData=true 说明有新数据
            lastLSN := IncrementLSN(fromLSN)
            SaveOffset(table, lastLSN)
```

### 4.2 关键变化

- **不再需要过滤 `LSN == startLSN` 的记录**，因为 `GetFromLSN` 已经保证返回的 `fromLSN` 是安全的
- **不再需要 bloom filter 或 map 做批次内去重**（如果使用 hash ID 方案的话）
- **Offset 存储的是 lastLSN**，用于下次 `GetFromLSN` 的判断

## 5. Offset 数据结构

### 5.1 现有结构

```go
type StoredOffset struct {
    LSN     string  // 十六进制字符串，如 "000235c90000263e0001"
    Updated time.Time
}
```

### 5.2 新方案保持不变

只需存储 `LSN` 即可，不需要额外 flag。判断逻辑通过 `incrementLSN` 和 `GetMaxLSN` 比较实现。

## 6. 边界情况分析

### 6.1 LSN 是数据库级别

- 表A的 maxLSN=200，表B的 offset=100
- 表B调用 `incrementLSN(100)=101`，`GetMaxLSN()=200`
- `101 <= 200`，所以表B会继续拉取
- 但表B可能实际没有自己的新数据（200是表A产生的）
- **结论**：这是预期行为，表B会拉取但可能返回空

### 6.2 同一 LSN 多表变更

- 事务 TX 在 LSN=100 同时修改表A和表B
- 表A已处理到100，表B从90开始
- 表B的 `incrementLSN(90)=91 <= 200`，会拉取
- LSN=100 的记录会被包含在结果中
- **结论**：正确，表B会收到TX的变更

### 6.3 并发 Poll

- 如果有多个 poller 实例同时运行
- LSN 推进是 MSSQL 事务日志保证的原子操作
- 每个 poller 独立读取独立的 offset store
- **结论**：并发安全

### 6.4 网络中断/重连

- 重连后，`GetMaxLSN()` 会反映最新的实际状态
- 如果有增量数据，会正确识别
- **结论**：无影响

## 7. 实现计划

### 7.1 第一步：添加 IncrementLSN 函数

```go
// internal/cdc/query.go
func (q *Querier) IncrementLSN(ctx context.Context, lsn []byte) ([]byte, error)
```

### 7.2 第二步：实现 GetFromLSN 函数

```go
// internal/core/poller.go
func (p *Poller) GetFromLSN(ctx context.Context, table string, stored StoredOffset) ([]byte, bool, error)
```

### 7.3 第三步：重构 PollTables 函数

- 移除旧的 `stored.LSN != "" && LSN == startLSN` 过滤逻辑
- 使用新的 `GetFromLSN` 流程
- 简化 offset 保存逻辑

### 7.4 第四步：移除旧代码

- 删除 bloom filter 相关代码（如果有）
- 删除 `incrementLSN` 注释中提到的 issue #132 的 workaround

## 8. 优点总结

1. **逻辑更清晰**：用 `incrementLSN` vs `GetMaxLSN` 判断是否有新数据
2. **无需 flag**：不需要存储额外的 hasData 标志
3. **消除重复过滤**：不再需要过滤 `LSN == startLSN`
4. **边界清晰**：Fresh Start vs Resume vs No New Data 三个 case 明确
5. **正确性保证**：
   - 不会漏数据（`hasData=true` 保证有新数据）
   - 不会重复拉取（`hasData=false` 跳过无数据的表）
