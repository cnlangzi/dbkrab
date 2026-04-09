# AGENTS.md - 开发规范

## 日志规范

### 使用 slog 而非 fmt

项目使用 `log/slog` 进行结构化日志记录，**禁止使用 `fmt.Print` 系列进行日志输出**。

### 日志级别

| 级别 | 使用场景 |
|------|----------|
| `slog.Debug` | 调试信息，仅开发时使用 |
| `slog.Info` | 正常操作信息，如连接成功、初始化完成 |
| `slog.Warn` | 警告信息，如可恢复的错误、异常状态 |
| `slog.Error` | 错误信息，如操作失败、连接断开 |

### 正确用法

```go
import "log/slog"

// 基础用法
slog.Info("message", "key", value)
slog.Error("operation failed", "error", err)

// 常见示例
slog.Info("connected to MSSQL", "host", host, "database", dbName)
slog.Warn("failed to close connection", "error", closeErr)
slog.Error("failed to load skill", "file", filePath, "error", err)
```

### 禁止用法

```go
// ❌ 禁止使用 fmt.Println / fmt.Printf / fmt.Sprint
fmt.Println("message")
fmt.Printf("value: %s", v)

// ❌ 禁止使用 log.Print / log.Printf
log.Print("message")
log.Printf("error: %v", err)
```

### 错误处理中的日志

```go
// ❌ 不要只打印不返回
if err != nil {
    fmt.Printf("error: %v\n", err) // 错误被忽略
}

// ✅ 正确：记录并返回
if err != nil {
    slog.Error("operation failed", "error", err)
    return fmt.Errorf("operation: %w", err)
}

// ✅ 正确：记录并继续（适用于可恢复错误）
if err != nil {
    slog.Warn("skipping item", "id", id, "error", err)
    continue
}
```

### defer Close() 错误处理

文件/资源关闭时的错误应记录到日志：

```go
// ✅ 正确
defer func() {
    if closeErr := file.Close(); closeErr != nil {
        slog.Warn("failed to close file", "path", filePath, "error", closeErr)
    }
}()

// ❌ 错误：忽略 defer close 错误
defer file.Close()

// ❌ 错误：silent ignore
defer func() { _ = file.Close() }()
```

### 上下文信息

日志应包含足够的上下文信息，便于排查问题：

```go
// ✅ 包含上下文
slog.Error("skill file parse failed",
    "file", "/path/to/skill.yml",
    "line", 42,
    "error", parseErr,
)

// ❌ 缺少上下文
slog.Error("parse failed")
```

---

## 代码规范

### 错误处理

1. 错误应向上传播，不应无声忽略
2. 使用 `fmt.Errorf("context: %w", err)` 包装错误
3. 第三方库的 close/error 回调必须处理并记录

### 线程安全

- 共享状态使用 `sync.RWMutex` 保护
- 读操作用 `RLock()`，写操作用 `Lock()`
- 导出字段若非线程安全，应在文档中说明
