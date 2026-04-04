# dbkrab 集成测试

## 本地运行

### 前提条件

**必须安装 Docker**

```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
# 重新登录或运行: newgrp docker

# 验证
docker --version
```

### 快速运行（推荐）

```bash
cd tests/integration
./run-local.sh
```

脚本会：
1. 自动检查 Docker 是否安装
2. 启动 MSSQL Server 2022 容器（Developer Edition）
3. 等待 SQL Server 就绪
4. 运行初始化脚本（启用 CDC）
5. 执行集成测试
6. 询问是否清理容器

### 手动运行

```bash
# 1. 启动 MSSQL 容器
docker run -d \
  --name dbkrab-test-mssql \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD=Test1234! \
  -e MSSQL_PID=Developer \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

# 2. 等待启动（约 30 秒）
docker logs -f dbkrab-test-mssql

# 3. 运行初始化脚本
docker exec -i dbkrab-test-mssql /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'Test1234!' \
  < tests/integration/setup.sql

# 4. 运行测试
cd tests/integration
MSSQL_HOST=localhost \
MSSQL_PORT=1433 \
MSSQL_USER=sa \
MSSQL_PASSWORD=Test1234! \
MSSQL_DATABASE=dbkrab_test \
go test -v -race -timeout=5m ./...

# 5. 清理容器
docker stop dbkrab-test-mssql && docker rm dbkrab-test-mssql
```

## MSSQL 版本说明

| 版本 | 容器镜像 | CDC 支持 | 限制 | 推荐用途 |
|------|---------|---------|------|---------|
| **Developer Edition** | `mssql/server:2022-latest` | ✅ 完整 | 无（功能同 Enterprise） | 测试/开发 ✅ |
| **Express Edition** | `mssql/server:2019-GA-ubuntu-18.04` | ✅ 完整 | 数据库最大 4GB | 生产轻量部署 |
| **Enterprise Edition** | 需自带 License | ✅ 完整 | 无 | 生产环境 |

**测试环境推荐 Developer Edition**：
- 免费
- 功能完整（同 Enterprise）
- 无数据库大小限制
- CDC 功能与生产环境一致

## 系统要求

| 资源 | 要求 | 说明 |
|------|------|------|
| 内存 | 最少 2GB | MSSQL 容器占用约 1.5-2GB |
| 磁盘 | 最少 5GB | 容器 + 测试数据 |
| Docker | 20.10+ | 支持 Linux/macOS/Windows |

## 故障排查

### 容器启动失败

```bash
# 查看日志
docker logs dbkrab-test-mssql

# 检查端口占用
sudo lsof -i :1433
```

### 测试连接失败

```bash
# 手动测试连接
docker exec -it dbkrab-test-mssql /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'Test1234!' -Q "SELECT 1"
```

### CDC 未启用

```bash
# 检查数据库 CDC 状态
docker exec -i dbkrab-test-mssql /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'Test1234!' \
  -Q "SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'dbkrab_test'"
```

## 测试用例

| 测试 | 说明 |
|------|------|
| `TestCDCEnabled` | 验证 CDC 在数据库和表上启用 |
| `TestChangeCapture` | 验证 INSERT/UPDATE 被 CDC 捕获 |
| `TestCrossTableTransaction` | 验证跨表事务完整性 |
| `TestLSNProgression` | 验证 LSN 正确递进 |
| `TestConnectionRecovery` | 验证连接处理 |
| `TestDataDirectory` | 验证数据目录可写 |

## CI/CD

GitHub Actions 会在 PR 时自动运行集成测试：
- 使用 MSSQL service container
- 自动执行 setup.sql
- 运行所有集成测试

无需本地运行即可提交 PR，但建议本地验证后再推送。
