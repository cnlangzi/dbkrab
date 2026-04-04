#!/bin/bash
# 本地集成测试快速运行脚本
# 不需要 docker-compose，直接启动 MSSQL 容器

set -e

echo "=== dbkrab 本地集成测试 ==="

# 检查 Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装"
    echo ""
    echo "安装命令:"
    echo "  curl -fsSL https://get.docker.com | sudo sh"
    echo "  sudo usermod -aG docker \$USER"
    echo "  # 然后重新登录或运行: newgrp docker"
    exit 1
fi

# 检查 MSSQL 容器是否已运行
if docker ps --format '{{.Names}}' | grep -q dbkrab-test-mssql; then
    echo "✅ MSSQL 容器已运行"
else
    echo "🚀 启动 MSSQL 容器..."
    docker run -d \
        --name dbkrab-test-mssql \
        -e ACCEPT_EULA=Y \
        -e MSSQL_SA_PASSWORD=Test1234! \
        -e MSSQL_PID=Developer \
        -p 1433:1433 \
        mcr.microsoft.com/mssql/server:2022-latest
    
    echo "⏳ 等待 SQL Server 启动..."
    for i in {1..30}; do
        if docker exec dbkrab-test-mssql /opt/mssql-tools/bin/sqlcmd \
            -S localhost -U sa -P 'Test1234!' -Q "SELECT 1" &>/dev/null; then
            echo "✅ SQL Server 就绪"
            break
        fi
        echo "  等待中... ($i/30)"
        sleep 5
    done
fi

# 初始化测试数据库
echo "📦 初始化测试数据库（CDC 配置）..."
docker exec dbkrab-test-mssql /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'Test1234!' \
    -i /workspace/tests/integration/setup.sql \
    2>/dev/null || \
docker exec -i dbkrab-test-mssql /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P 'Test1234!' \
    < tests/integration/setup.sql

# 运行测试
echo "🧪 运行集成测试..."
cd tests/integration
MSSQL_HOST=localhost \
MSSQL_PORT=1433 \
MSSQL_USER=sa \
MSSQL_PASSWORD=Test1234! \
MSSQL_DATABASE=dbkrab_test \
go test -v -race -timeout=5m ./...

TEST_EXIT_CODE=$?

# 清理（可选）
echo ""
read -p "是否停止 MSSQL 容器？(y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker stop dbkrab-test-mssql
    docker rm dbkrab-test-mssql
    echo "✅ 容器已清理"
fi

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "=== 集成测试 PASSED ==="
else
    echo "=== 集成测试 FAILED (exit code: $TEST_EXIT_CODE) ==="
fi

exit $TEST_EXIT_CODE
