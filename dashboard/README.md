# dbkrab Dashboard

运维 Dashboard - 用于监控和管理 dbkrab CDC 系统

## 功能

### Overview（总览）
- 系统健康状态检查
- DLQ 统计（Pending/Resolved/Ignored）
- 活跃插件数量
- 快速刷新

### Dead Letter Queue（死信队列）
- 查看所有 DLQ 条目
- 按状态筛选
- 查看详情（包括变更数据）
- 重放（Replay）失败事务
- 忽略（Ignore）条目（需填写原因）
- 删除条目

### Plugins（插件管理）
- 查看已加载插件
- 查看插件配置（监控表、版本等）
- 重载插件
- 卸载插件

## 开发模式

```bash
cd dashboard

# 安装依赖
npm install

# 启动开发服务器（端口 3001，代理到 API 3000）
npm run dev
```

开发服务器会：
- 运行在 http://localhost:3001
- 自动代理 `/api/*` 请求到 `http://localhost:3000`
- 支持热重载

## 生产构建

```bash
# 构建静态文件
npm run build

# 构建产物在 dashboard/dist/
# Go API 会嵌入这些文件并在 3000 端口服务
```

## API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/health` | GET | 健康检查 |
| `/api/plugins` | GET | 列出所有插件 |
| `/api/plugins` | POST | 加载新插件 |
| `/api/plugins/{name}` | GET | 获取插件详情 |
| `/api/plugins/{name}` | DELETE | 卸载插件 |
| `/api/plugins/{name}/reload` | POST | 重载插件 |
| `/api/dlq/list` | GET | 列出 DLQ 条目 |
| `/api/dlq/{id}` | GET | 获取 DLQ 条目详情 |
| `/api/dlq/{id}/replay` | POST | 重放 DLQ 条目 |
| `/api/dlq/{id}/ignore` | POST | 忽略 DLQ 条目 |
| `/api/dlq/{id}` | DELETE | 删除 DLQ 条目 |
| `/api/dlq/stats` | GET | DLQ 统计 |

## 技术栈

- **Frontend**: React 18 + Vite
- **UI**: Ant Design 5
- **Hooks**: ahooks
- **Backend**: Go 1.26 (embed FS)

## 端口

- **开发模式**: 3001 (Dashboard) + 3000 (API)
- **生产模式**: 3000 (API + 静态文件)
