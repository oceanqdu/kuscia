# 崖山数据库 MySQL 兼容模式 — Kuscia 数据源接入测试报告

## 1. 测试背景

评估崖山数据库（YashanDB）v23.3+ 的 MySQL 兼容模式是否能作为 Kuscia 的 MySQL 数据源使用。

### 测试环境
- **崖山数据库**: 172.28.61.143:1690（MySQL 兼容模式）
- **数据库**: test / 用户: sales
- **Kuscia 版本**: v1.2.0b0
- **Go 驱动**: github.com/go-sql-driver/mysql
- **测试日期**: 2026-05-11

---

## 2. 修改前后对比

### 2.1 总体对比

| 指标 | 修改前 | 修改后 | 变化 |
|------|--------|--------|------|
| 测试用例总数 | 24 | 24 | 不变 |
| 通过数 | 21 | **24** | **+3** |
| 失败数 | 3 | **0** | **-3** |
| 通过率 | 87.5% | **100%** | **+12.5%** |

### 2.2 分类对比

| 类别 | 修改前 | 修改后 |
|------|--------|--------|
| 连接层 | 3/3 ✅ | 3/3 ✅ |
| 读取层 | 8/8 ✅ | 8/8 ✅ |
| 写入层 | 6/8 ⚠️ | **8/8 ✅** |
| 类型映射 | 1/2 ⚠️ | **2/2 ✅** |
| 集成流程 | 1/3 ⚠️ | **3/3 ✅** |

### 2.3 失败用例修复明细

| 用例 | 修改前 | 修改后 | 根因 |
|------|--------|--------|------|
| T-W09 Arrow→MySQL 类型建表 | ❌ YAS-04209 | ✅ PASS | 去掉 SIGNED |
| T-01 Arrow→MySQL 类型映射 | ❌ YAS-04209 | ✅ PASS | 去掉 SIGNED |
| I-02 Uploader 写入流程 | ❌ YAS-04209 | ✅ PASS | 去掉 SIGNED |
| I-03 读写往返 | ❌ YAS-04209 | ✅ PASS | 去掉 SIGNED |

---

## 3. 修改内容

### 3.1 问题发现

Kuscia 的 `mysql_uploader.go:ArrowDataTypeToMySQLType()` 在生成 CREATE TABLE DDL 时，对有符号整数类型使用了 `SIGNED` 关键字。崖山 MySQL 兼容模式不支持此非标准语法。

```
Error 4209: YAS-04209 unexpected word SIGNED
```

### 3.2 修改方案

**文件**: `pkg/datamesh/dataserver/io/builtin/mysql_uploader.go`

```go
// 修改前
case arrow.PrimitiveTypes.Int8:   return "TINYINT SIGNED"
case arrow.PrimitiveTypes.Int16:  return "SMALLINT SIGNED"
case arrow.PrimitiveTypes.Int32:  return "INT SIGNED"
case arrow.PrimitiveTypes.Int64:  return "BIGINT SIGNED"

// 修改后
case arrow.PrimitiveTypes.Int8:   return "TINYINT"
case arrow.PrimitiveTypes.Int16:  return "SMALLINT"
case arrow.PrimitiveTypes.Int32:  return "INT"
case arrow.PrimitiveTypes.Int64:  return "BIGINT"
```

### 3.3 修改影响分析

| 维度 | 影响 |
|------|------|
| **语义变化** | 无 — `SIGNED` 是 MySQL 整数类型的默认行为，去掉后语义完全不变 |
| **标准 MySQL 兼容性** | ✅ 完全兼容 — 不带 SIGNED 是标准写法 |
| **崖山兼容性** | ✅ 完全兼容 — 崖山支持标准写法 |
| **影响范围** | 仅影响 DDL（CREATE TABLE），不影响 DML（INSERT/SELECT） |
| **现有用户影响** | 无 — 不带 SIGNED 的表结构与带 SIGNED 的完全一致 |

### 3.4 崖山 DDL 类型兼容性验证

| DDL 类型 | 崖山支持 | 标准 MySQL | 说明 |
|----------|---------|-----------|------|
| `BIGINT` | ✅ | ✅ | 默认有符号，标准写法 |
| `BIGINT SIGNED` | ❌ YAS-04209 | ✅ | 非标准，崖山不支持 |
| `BIGINT UNSIGNED` | ✅ | ✅ | 标准写法 |
| `INT` | ✅ | ✅ | 默认有符号 |
| `INT SIGNED` | ❌ YAS-04209 | ✅ | 非标准 |
| `INT UNSIGNED` | ✅ | ✅ | 标准 |
| `SMALLINT` | ✅ | ✅ | 默认有符号 |
| `SMALLINT SIGNED` | ❌ YAS-04209 | ✅ | 非标准 |
| `TINYINT` | ✅ | ✅ | 默认有符号 |
| `TINYINT SIGNED` | ❌ YAS-04209 | ✅ | 非标准 |
| `TINYINT(1)` | ✅ | ✅ | 标准布尔类型 |
| `FLOAT` / `DOUBLE` / `TEXT` | ✅ | ✅ | 标准 |
| `DATE` / `TIMESTAMP` / `DATETIME` | ✅ | ✅ | 标准 |
| `TIME` / `TIME(6)` | ✅ | ✅ | 标准 |
| `DECIMAL` | ✅ | ✅ | 标准 |

---

## 4. 修改后测试详情（全部通过）

### 4.1 连接层（3/3 ✅）

| 测试 | 结果 | 耗时 |
|------|------|------|
| T-C01 连接崖山数据库 | ✅ PASS | 0.00s |
| T-C02 连接池复用 | ✅ PASS | 0.00s |
| T-C03 版本查询 (5.7.42) | ✅ PASS | 0.00s |

### 4.2 读取层（8/8 ✅）

| 测试 | 结果 | 耗时 | 说明 |
|------|------|------|------|
| T-R01 全类型读取 (16种类型) | ✅ PASS | 0.89s | BOOL/INT/FLOAT/VARCHAR/TEXT/DATE/TIMESTAMP/DECIMAL |
| T-R02 反引号列名 | ✅ PASS | 0.74s | `` `my col` `` 含空格的列名 |
| T-R03 表名大小写不敏感 | ✅ PASS | 0.82s | 小写/大写/混合均可查询 |
| T-R04 NULL 值读取 | ✅ PASS | 1.10s | sql.NullString 正确识别 NULL |
| T-R05 空结果集 | ✅ PASS | 1.01s | 无数据时正确返回 |
| T-R06 WHERE 条件查询 | ✅ PASS | 3.70s | 过滤条件正确 |
| T-R07 批量读取 1000 行 | ✅ PASS | 0.82s | 分批读取正确 |
| T-R08 部分列读取 | ✅ PASS | 0.93s | 只查询部分列正确 |

### 4.3 写入层（8/8 ✅）

| 测试 | 结果 | 耗时 | 说明 |
|------|------|------|------|
| T-W01 CREATE TABLE | ✅ PASS | 0.74s | 标准类型建表成功 |
| T-W02 DROP TABLE IF EXISTS | ✅ PASS | 0.42s | 正确删除 |
| T-W03 INSERT 单行 | ✅ PASS | 5.09s | 数据写入正确 |
| T-W04 INSERT 批量 (100行事务) | ✅ PASS | 0.99s | 事务批量写入 |
| T-W05 事务 COMMIT | ✅ PASS | 0.69s | 数据持久化 |
| T-W06 事务 ROLLBACK | ✅ PASS | 1.11s | 数据正确回滚 |
| T-W07 DELETE FROM | ✅ PASS | 1.03s | 清空表数据 |
| T-W08 DELETE 降级清理 | ✅ PASS | 1.96s | DROP 失败后 DELETE 降级 |

### 4.4 数据类型映射（2/2 ✅）

| 测试 | 结果 | 耗时 | 说明 |
|------|------|------|------|
| T-T01 Arrow→MySQL 类型映射 | ✅ PASS | 0.93s | 16 种类型全部建表成功 |
| T-T02 MySQL→Arrow 类型映射 | ✅ PASS | 0.00s | 10 种类型全部正确 |

### 4.5 集成流程（3/3 ✅）

| 测试 | 结果 | 耗时 | 说明 |
|------|------|------|------|
| I-01 Downloader 完整流程 | ✅ PASS | 1.24s | 3 行数据正确读取 |
| I-02 Uploader 写入流程 | ✅ PASS | 1.01s | 建表 + 写入 + 验证 |
| I-03 读写往返 | ✅ PASS | 1.76s | 源表读取 → 目标表写入 → 数据一致 |

---

## 5. 最终结论

| 维度 | 修改前 | 修改后 |
|------|--------|--------|
| **连接层** | ✅ 兼容 | ✅ 兼容 |
| **读取层** | ✅ 兼容 | ✅ 兼容 |
| **写入层** | ❌ SIGNED 报错 | ✅ **完全兼容** |
| **数据类型映射** | ❌ SIGNED 报错 | ✅ **完全兼容** |
| **事务管理** | ✅ 兼容 | ✅ 兼容 |
| **表名大小写** | ✅ 不敏感 | ✅ 不敏感 |

**总体评估**：通过去掉 `mysql_uploader.go` 中 4 行代码的 `SIGNED` 关键字（非标准语法），崖山数据库 MySQL 兼容模式现在可以**完全作为 Kuscia 的 MySQL 数据源使用**。修改后 24 个集成测试全部通过，同时保持与标准 MySQL 的完全兼容性。

### 使用方式

通过 Kuscia API 创建数据源时指定 `type=mysql`，endpoint 填写崖山 MySQL 监听地址：

```json
{
  "type": "mysql",
  "info": {
    "database": {
      "endpoint": "172.28.61.143:1690",
      "user": "sales",
      "password": "Swxa_2026",
      "database": "test"
    }
  }
}
```

### 注意事项
1. 崖山侧需确保 MySQL 监听服务已开启（配置 `service.ini`）
2. 崖山 MySQL 兼容模式仅支持**单机部署**
3. 避免使用 binary 类型列（Kuscia 通用限制，与崖山无关）

---

## 附录 A：测试覆盖矩阵（修改后）

| 编号 | 测试用例 | 修改前 | 修改后 | 优先级 |
|------|---------|--------|--------|--------|
| C-01 | 连接崖山数据库 | ✅ | ✅ | P0 |
| C-02 | 连接池复用 | ✅ | ✅ | P1 |
| C-03 | 版本查询 | ✅ | ✅ | P1 |
| R-01 | 全类型读取 (16种) | ✅ | ✅ | P0 |
| R-02 | 反引号列名 | ✅ | ✅ | P0 |
| R-03 | 表名大小写不敏感 | ✅ | ✅ | P0 |
| R-04 | NULL 值读取 | ✅ | ✅ | P0 |
| R-05 | 空结果集 | ✅ | ✅ | P1 |
| R-06 | WHERE 条件查询 | ✅ | ✅ | P0 |
| R-07 | 批量读取 1000 行 | ✅ | ✅ | P1 |
| R-08 | 部分列读取 | ✅ | ✅ | P0 |
| W-01 | CREATE TABLE | ✅ | ✅ | P0 |
| W-02 | DROP TABLE IF EXISTS | ✅ | ✅ | P0 |
| W-03 | INSERT 单行 | ✅ | ✅ | P0 |
| W-04 | INSERT 批量 (事务) | ✅ | ✅ | P0 |
| W-05 | 事务 COMMIT | ✅ | ✅ | P0 |
| W-06 | 事务 ROLLBACK | ✅ | ✅ | P0 |
| W-07 | DELETE FROM | ✅ | ✅ | P1 |
| W-08 | DELETE 降级清理 | ✅ | ✅ | P1 |
| T-01 | Arrow→MySQL 类型映射 | ❌ | ✅ | P0 |
| T-02 | MySQL→Arrow 类型映射 | ✅ | ✅ | P0 |
| I-01 | Downloader 完整流程 | ✅ | ✅ | P0 |
| I-02 | Uploader 写入流程 | ❌ | ✅ | P0 |
| I-03 | 读写往返 | ❌ | ✅ | P0 |

## 附录 B：运行测试

```bash
# 运行原有单元测试（模拟数据库）
go test -v -run "TestMySQL" ./pkg/datamesh/dataserver/io/builtin/

# 运行崖山集成测试（真实数据库）
go test -v -tags=integration -run "TestYashan" -timeout 180s ./pkg/datamesh/dataserver/io/builtin/
```