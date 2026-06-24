# 用 Rust 重写 PostgreSQL：面向 AI Agent 的线程模型数据库

> 副标题：构建一个 PG 协议兼容的 Agent-Native 数据库，而非重复造轮子
>
> 日期：2026 年 6 月 10 日

---

## 一、动机：为什么要重写 PG

两个核心目标：

1. **内存安全性**：PG 用 C 实现（~150 万行），面临所有 C 语言的内存安全风险——缓冲区溢出、use-after-free、整数溢出。Rust 在编译期消除这些问题。
2. **进程模型→线程模型**：PG 每连接 `fork()` 一个 backend 进程，对 AI Agent 场景（数百~数千长连接、高频小查询、需要共享上下文）极其不利。线程模型可以：
   - 降低千连接内存开销 **5–10×**（进程模型 10–15 GB → 线程模型 1–2 GB）
   - 减少连接建立延迟 **~10×**（同机 TCP 2 ms+ → 线程池预热 + 零拷贝协议解析 ~200 μs）
   - 让所有缓存原生共享，而非仅靠 shared_buffers
   - 数据库内建连接多路复用，不再依赖外部 PgBouncer

Rust 恰好解决了线程模型的核心恐惧（数据竞争）——编译期的 `Send`/`Sync` trait 可以替代进程隔离，在语言层面提供并发稳定性保证。

---

## 二、Agent 后端需要数据库做什么

Agent 对数据库的需求和传统 Web 应用不同：

| Agent 需求 | 当前 PG 的方案 | 理想方案 |
|-----------|--------------|---------|
| 持久化 Agent 状态（对话历史、任务队列、工具结果） | JSONB 列 | 原生 JSON/Document 存储 + 版本化 |
| 向量检索（RAG 知识库、记忆检索） | pgvector 扩展 | 原生向量索引（HNSW/DiskANN），与事务引擎统一 |
| 实时事件推送（Agent A 完成→通知 Agent B） | LISTEN/NOTIFY | 基于 WAL 变更捕获的推送，支持 predicate-based subscription |
| 多租户隔离（不同用户/Agent 的数据安全） | Schema 级隔离 | 原生 tenant 概念，带 quota、rate limiting、RBAC |
| 时间旅行/审计（Agent 说了什么、做了什么变更） | 应用层日志 + 定时快照 | MVCC 直接暴露时间维度作为查询接口 |
| 函数调用作为一等公民（Agent 调用 DB 不是"查询"而是"工具调用"） | PL/pgSQL 函数 | 原生支持函数注册 + 权限控制，让 Agent Framework 直接通过 wire protocol 调用 |
| 极低延迟 | 同机 TCP ~0.5–2 ms（端到端） | 目标同机 TCP <200 μs，线程模型 + Rust |

---

## 三、技术路线：三层策略

完整重写 PG 不现实（30 年积累、150 万行 C、数千贡献者）。务实的方法是三层分工：

### Layer 1：直接用现成的 Rust 组件（不要重写）

| PG 组件 | 可用的 Rust 替代 | 边界与注意事项 |
|---------|-----------------|---------------|
| SQL 解析 + 查询引擎框架 | **Apache DataFusion**（最成熟的 Rust 查询引擎，Arrow 生态） | DataFusion 面向**分析型（AP）**设计（列存、向量化）。作为 OLTP 引擎需要深度定制：点查 latency 优化、索引回表、行存迭代器、事务调度层需自研 |
| 存储格式 | **行存页格式**（B+Tree / LSM-Tree）；分析层可选 **Parquet** 列存投影；Arrow 作为内存交换格式 | Parquet 是不可变列存，**不适合 OLTP 主存储**（随机单行读写性能极差），仅用于只读归档或分析投影 |
| 向量索引 | Faiss（C++ FFI）或 Rust 原生向量库 | 向量索引（HNSW）与事务引擎的集成是难点：插入/删除的并发控制、事务回滚时向量索引的回滚、WAL 一致性，需自研协调层 |
| 分布式共识 | raft-rs（TiKV 同款） | 成熟，可直接复用 |
| PG Wire Protocol | 自行实现（~3000 行可覆盖 psql/SQLAlchemy/Prisma 兼容） | 协议解析层可用 Rust 零拷贝实现 |

### Layer 2：重点自研三个差异化核心

| 自研组件 | 原因 |
|---------|------|
| **线程模型存储引擎** | 对 PG 最大的架构改进，没有现成的。需自研行存页管理、Buffer Pool、io_uring 异步 I/O（见成熟度说明） |
| **MVCC 引擎** | PG 的 XID 回卷和 VACUUM 是 MVCC 的必然代价，但进程模型让并发控制和缓存共享变得笨拙。线程模型下可用更轻量的细粒度锁结构实现多版本并发 |
| **Agent-native 接口层** | 工具注册、状态机、实时订阅——不是传统数据库功能，是 Agent 持久化运行时 |

#### Agent API 具体示例

Agent 通过标准 SQL 连接注册并调用工具，工具成为数据库的一等公民：

```sql
-- Agent 注册外部工具
REGISTER TOOL 'send_email' AS
  INPUT (to TEXT, subject TEXT, body TEXT)
  HANDLER 'https://api.internal/v1/email'
  TIMEOUT 5s
  RETRIES 3;

-- Agent 查询时，数据库内嵌调用工具并返回结果
SELECT * FROM tool_call('send_email', 'user@example.com', 'Hello', 'Task completed');

-- 订阅状态变更（predicate-based WAL streaming）
SUBSCRIBE TO state_changes
  WHERE agent_id = 'agent-42' AND status = 'completed'
  WITH (push = true, debounce_ms = 100);
```

### Layer 3：渐进式兼容

| 阶段 | 内容 |
|------|------|
| **Phase 1** | PG wire protocol + 线程引擎 + 基础 DDL/DML → 能跑 psql、SQLAlchemy、Prisma |
| **Phase 2** | 完整的 PG SQL 方言兼容（通过 DataFusion SQL planner + 自研 OLTP 定制）+ 原生向量 + 实时推送 |
| **Phase 3** | Agent 专用能力（多租户、时间旅行查询、工具调用接口、Workflow 引擎） |

---

## 四、架构草图

```
                     PG Wire Protocol (psql / SQLAlchemy / Prisma)
                                    │
┌───────────────────────────────────┼───────────────────────────────┐
│                        Connection Multiplexer                     │
│                  (tokio async runtime, 线程池调度)                  │
├────────────┬──────────────┬──────────────┬───────────────────────┤
│ SQL Parser │ Query Planner│   Executor   │  Agent API (tools/state)
│ (DataFusion│ (DataFusion  │  (自研,      │  - 函数注册/调用
│  SQL)      │  优化器)     │  Arrow batch)│  - 工作流状态机
│            │              │  行存迭代器   │  - 实时订阅 (WAL-based)
├────────────┴──────────────┴──────────────┴───────────────────────┤
│                     MVCC Storage Engine (自研, Rust)              │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  ┌──────────┐ │
│  │ Page     │  │ WAL      │  │ Lock Manager     │  │ Vector   │ │
│  │ Cache    │  │ (io_uring│  │ (细粒度 latch    │  │ Index    │ │
│  │ (shared) │  │  async,  │  │  + Lock-free     │  │ (原生    │ │
│  │          │  │  实验性) │  │  读路径)         │  │  HNSW)   │ │
│  └──────────┘  └──────────┘  └──────────────────┘  └──────────┘ │
├──────────────────────────────────────────────────────────────────┤
│                        Multi-Tenant Isolation                     │
│         (per-tenant schema, quota, rate limiting, RBAC)           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 五、补充设计维度

以下维度虽未体现在核心架构中，但对生产可用性至关重要：

### 事务隔离

Agent 场景的事务特征：
- **默认**：Read Committed（RC），适合大多数 Agent 读-决策-写循环
- **强一致性**：Serializable Snapshot Isolation（SSI），用于多 Agent 协作、竞争资源场景
- 线程模型下，SSI 可借助共享内存直接访问写入集（Write Set），避免 PG 的 SIREAD 锁在 shared_buffers 上的争用，使冲突检测路径更短

### 备份、恢复与 PITR

Neon 的核心价值之一是分支（Branching）+ 时间点恢复（PITR）。本架构继承并简化这一能力：
- **Copy-on-Write 存储层**：内存中 Page 遵循 MVCC（原地更新 + 旧版本保留）。后台 checkpoint 将脏 Page 序列化为不可变对象写入新层，旧层保留为逻辑快照——即 **CoW 发生在 checkpoint 落盘时机**，而非每次写入。Update 操作路径：内存中修改 Page → WAL 记录 → 后台 checkpoint 时以 CoW 方式落盘
- **分支 = 零成本**：创建新 Tenant/Branch 只需引用已有层，无需复制数据
- **PITR = 层回滚**：基于 LSN（Log Sequence Number）直接挂载历史层
- **归档**：冷层自动压缩为 Parquet，推送到对象存储（S3）

### 扩展机制

PG 的强大在于扩展生态（PostGIS、pgvector、TimescaleDB）。新数据库的扩展模型建议：
- **Rust Crate 插件**：扩展以动态链接库（`cdylib`）或 WASM 模块加载，通过 Rust trait 接口注册自定义类型、索引访问方法、FDW
- **WASM UDF**：用户自定义函数用 WASM 沙箱运行，保证安全性（类似 AWS Lambda 的隔离模型）
- **工具注册协议**：Agent 工具本质上是一种「特殊扩展」，复用同一套注册/发现/鉴权机制

### 向量索引与事务协调

原生向量索引不是「把 pgvector 的 C 代码改写成 Rust」这么简单：
- **插入并发**：HNSW 图的并发插入需要图级锁或乐观重试，否则图结构可能损坏
- **事务回滚**：向量索引的插入必须参与 MVCC——事务回滚时，向量索引中的对应条目也需回滚
- **WAL 一致性**：向量索引的 checkpoint 必须和行存的 LSN 对齐，崩溃恢复后保持一致
- **建议方案**：向量索引作为「二级索引」维护，条目持有指向行数据的 `TID + XID`，由 MVCC 引擎统一垃圾回收

### 明确不做

明确边界是架构设计的一部分——以下能力当前阶段**主动排除**，保持工程聚焦：

| 不做 | 原因 |
|------|------|
| 不做列存 OLAP 引擎 | DataFusion + Parquet 归档已覆盖分析投影需求，不造 ClickHouse |
| 不做分布式 | Phase 1-3 专注单机线程模型 + CoW 分支；分布式在 raft-rs 就绪后单独评估，存算分离架构为过渡方案 |
| 不做 PG 扩展二进制兼容 | C 扩展生态（PostGIS、TimescaleDB、Citus）无法直接平移，用 Rust trait + WASM 重建扩展生态 |

---

## 六、可行性评估

### 为什么值得做

- 进程→线程的架构升级是 PG 本身做不到的（30 年的 fork 模型尾大不掉）
- Agent 正在成为数据库的新一类"超级用户"，而 PG 从未为此设计
- Rust + 线程模型让"又安全又高效"成为可能
- DataFusion 等现成组件让你不需要从零写 SQL 引擎

### 核心风险

| 风险 | 说明 |
|------|------|
| **存储引擎正确性** | MVCC、崩溃恢复、WAL 的正确性验证需要多年打磨——这是数据库最大的坑 |
| **SQL 方言长尾** | PG 的 SQL 兼容是巨大的长尾工作，不是功能多寡而是细节 |
| **性能基准** | PG 经过 30 年调优，追赶性能需要大量工程投入 |
| **生态兼容** | 大量 PG 扩展（PostGIS、TimescaleDB、Citus）无法直接复用；需建立新的 Rust/WASM 扩展生态 |
| **团队要求** | 这不是一个"学 Rust"级别的项目，需要数据库内核经验 |
| **io_uring 成熟度** | tokio-uring 目前仍是实验性 crate，生产环境需充分压测，或有回退到 epoll/AIO 的预案 |
| **向量索引事务协调** | HNSW/MVCC/WAL 的三方一致性是已知难题，非简单重写可解决 |

### 参考案例

| 项目 | 路径 | 启示 |
|------|------|------|
| **Limbo**（Rust 重写 SQLite） | 全量重写，渐进替代 | 文件格式兼容 + DST 测试是关键 |
| **TiKV**（Rust 分布式 KV） | 新架构，协议兼容 | CNCF 唯一 Rust 毕业项目，证明可行 |
| **Neon**（Serverless PG） | 不改 PG 核心，改存储层 | 用 Rust 重写存储但保留 PG 查询引擎；存算分离是务实路径 |
| **Sled vs RocksDB** | 工程成熟度不够 | 纯 Rust 存储引擎在 compaction、崩溃恢复验证、性能调优上追不上多年积累的 C++；**非语言限制，而是工程积累差距** |

---

## 七、结论

不要以"重写 PG"为目标——那会陷入功能完备性的无底洞。应该以"构建 Agent-Native 数据库"为目标，PG 只是协议兼容层。

核心技术决策：
1. **DataFusion** 承担 SQL Parser + Planner 框架，OLTP 执行层深度定制
2. **自研线程模型存储引擎**（行存 B+Tree/LSM + MVCC + 细粒度并发控制）
3. **PG wire protocol** 作为生态入口，兼容现有驱动和 ORM
4. **Rust 类型系统**（`Send`/`Sync`/Ownership）替代进程隔离，提供编译期安全
5. **Copy-on-Write 存储层** 原生支持分支、PITR、冷热分层
6. **WASM + Rust trait** 作为扩展机制，替代 PG 的 C 扩展生态

这条路同时避开了 PG C 代码的内存安全问题，也避开了 PG 进程模型的架构负债，面向 Agent 场景建立了真正的差异化能力。
