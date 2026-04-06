# Task 1.1: skill.yml Parser

**文件**: `internal/sqlplugin/skill.go`  
**阶段**: Phase 1 - MVP  
**前置任务**: 无  
**下游调用方**: Task 1.5 (SQL Plugin Engine)

---

## 目标

将 `skill.yml` 配置文件解析为 Go 结构体，并加载关联的 SQL 文件内容。

---

## 核心接口

```go
package sqlplugin

// LoadSkill loads and parses a skill.yml from the given path
func LoadSkill(path string) (*Skill, error)
```

### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `path` | `string` | skill.yml 文件的绝对路径或相对路径 |

### 返回值

| 类型 | 说明 |
|------|------|
| `*Skill` | 解析后的 skill 配置结构体 |
| `error` | 解析错误或文件读取错误 |

---

## 数据结构

### Operation 常量

```go
type Operation int

const (
    OpDelete Operation = 1
    OpInsert Operation = 2
    OpUpdate Operation = 4
)
```

**注意**：`OpUpdate` 对应 `core.OpUpdateAfter(4)`，不处理 `OpUpdateBefore(3)`。

### Stage

```go
type Stage struct {
    Name      string `yaml:"name"`        // 阶段名称
    SQLFile   string `yaml:"sql_file"`    // SQL 文件路径（相对路径）
    SQL       string `yaml:"-"`           // 已加载的 SQL 内容（非 YAML 字段）
    TempTable string `yaml:"temp_table"`  // 输出临时表名
}
```

- `SQLFile`: 相对于 skill.yml 所在目录的路径
- `SQL`: 启动时读取文件内容填充，SQL 模板引擎直接使用此字段

### SinkConfig

```go
type SinkConfig struct {
    Name       string `yaml:"name"`        // sink 名称
    On         string `yaml:"on"`          // 表过滤器，为空表示所有表
    SQL        string `yaml:"sql"`         // SQL 模板字符串
    SQLFile    string `yaml:"sql_file"`    // SQL 文件路径（二选一）
    Output     string `yaml:"output"`      // 目标 SQLite 表名（必填）
    PrimaryKey string `yaml:"primary_key"` // 主键列名（必填，用于 upsert）
}
```

**校验规则**：
- `Output` 必填，未设置则报错
- `PrimaryKey` 必填，未设置则报错
- `SQL` 和 `SQLFile` 二选一，至少设置一个

### Skill

```go
type Skill struct {
    Name        string                      `yaml:"name"`
    Description string                      `yaml:"description"`
    On          []string                    `yaml:"on"`          // 监听的表列表
    Stages      []Stage                     `yaml:"stages"`
    Sinks       map[Operation][]SinkConfig  `yaml:"sinks"`
}
```

---

## LoadSkill 流程

```
1. os.ReadFile(skill.yml)
       ↓
2. yaml.Unmarshal → Skill 结构体（SQL 字段为原始 YAML 值）
       ↓
3. 校验 SinkConfig.Output、PrimaryKey 必填
       ↓
4. 遍历 Stages，对每个 SQLFile:
       ↓
   4.1 filepath.Join(skillDir, sqlFile) → 绝对路径
       ↓
   4.2 os.ReadFile(sqlContent)
       ↓
   4.3 stage.SQL = sqlContent
       ↓
5. 返回填充好的 *Skill
```

---

## 辅助方法

### GetSinks

```go
func (s *Skill) GetSinks(op Operation) []SinkConfig
```

返回指定操作类型的 sink 配置列表。

### MatchTable

```go
func (s *Skill) MatchTable(table string) bool
```

检查该 skill 是否监听指定表。

- `On` 为空 → 返回 `true`（监听所有表）
- `On` 包含表名 → 返回 `true`
- 表名不在列表 → 返回 `false`

---

## 目录结构假设

```
sql_plugins/sync_orders/
├── skill.yml       # LoadSkill("sql_plugins/sync_orders/skill.yml")
└── stages/
    └── enrich.sql  # 通过 skill.yml 中的 sql_file: "stages/enrich.sql" 引用

# skillDir = "sql_plugins/sync_orders"
# sqlFile = "stages/enrich.sql"
# → 绝对路径 = "sql_plugins/sync_orders/stages/enrich.sql"
```

---

## 文件结构

```
internal/sqlplugin/
├── skill.go       # Task 1.1 (本任务)
└── params.go      # Task 1.2
```

---

## 验收标准

- [ ] 解析单表配置（`on: [orders]`）
- [ ] 解析 `sinks.insert/update/delete` 三个操作
- [ ] `sql_file` 路径正确拼接并读取
- [ ] `on` 字段（表过滤器）正确解析
- [ ] `Output` 和 `PrimaryKey` 必填校验
- [ ] `SQL` 和 `SQLFile` 二选一校验
- [ ] 单元测试覆盖正常场景和校验失败场景

---

## 单元测试要点

```go
func TestLoadSkill_Basic(t *testing.T) { ... }
func TestLoadSkill_SQLFileLoaded(t *testing.T) { ... }
func TestLoadSkill_AllOperations(t *testing.T) { ... }
func TestLoadSkill_SinkOutputRequired(t *testing.T) { ... }
func TestLoadSkill_SinkPrimaryKeyRequired(t *testing.T) { ... }
func TestLoadSkill_SinkSQLOrSQLFileRequired(t *testing.T) { ... }
func TestSkill_MatchTable(t *testing.T) { ... }
func TestSkill_GetSinks(t *testing.T) { ... }
```

---

## TASKS.md 原始需求 vs 本方案

| TASKS.md 描述 | 本方案调整 | 原因 |
|--------------|-----------|------|
| `Stage.TempTable` 字段 | 保留但不使用 | 阶段输出表名由 SQL 内 `INTO #temp` 指定，此字段可作为校验或文档用途 |
| 返回 error 列表 | 改为单条 error | 简化错误处理，调用方逐条处理即可 |
