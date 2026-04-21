# dbkrab 集成测试

## 概述

集成测试使用 **Mock MSSQL Driver**（`mockmssql`），无需 Docker 或真实的 MSSQL 容器。

Mock driver 模拟了 CDC 查询接口（`fn_cdc_get_max_lsn`、`fn_cdc_get_min_lsn` 等），提供测试数据，与真实 SQLite 存储层配合验证完整数据流。

## 本地运行

### 前提条件

**无需 Docker**，只需 Go 环境。

### 快速运行

```bash
# 运行所有测试（包括集成测试）
make test

# 仅运行集成测试
go test -v -race ./tests/integration/...
```

### Mock Driver 实现

| 文件 | 说明 |
|------|------|
| `mockmssql/driver.go` | Mock MSSQL driver，注册为 `mockmssql` |
| `mockmssql/cdc_querier.go` | 实现 `CDCQuerier` 接口 |

Mock driver 模拟的表：
- `TestProducts` (ProductID, ProductName, Price, Stock)
- `TestOrders` (OrderID, ProductID, Quantity, TotalAmount)
- `TestOrderItems` (OrderItemID, OrderID, ItemName, ItemPrice)

## 测试用例

| 测试 | 说明 |
|------|------|
| `TestMockMSSQLDriver` | 验证 Mock driver 基础查询功能 |
| `TestMockCDCQuerier` | 验证 Mock CDCQuerier 接口 |
| `TestMockCDCQuerierWithChanges` | 验证 CDC 变更数据返回 |
| `TestIntegrationWithSQLite` | Mock MSSQL + 真实 SQLite 全链路集成 |
| `TestMockMSQLWithRealPoller` | 验证与 Poller 组件接口兼容性 |
| `TestCDCChangeTypes` | 验证 CDC 操作类型处理 |
| `TestSQLiteStoreWrites` | 验证 SQLite 存储写入 |

## CI/CD

GitHub Actions 和 pre-commit hook 都会自动运行集成测试：

```bash
# CI workflow (PR)
go test -v -race ./...

# Pre-commit hook
make vet && make test && make lint
```

无需额外配置，所有测试在提交 PR 或本地 commit 时自动执行。
