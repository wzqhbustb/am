# Agent-Native 数据库规划文档（初稿）

> 用 Rust 重新实现一套面向 AI Agent 场景的数据库系统。

---

## 一、背景与动机

在 AI Agent 正在吞噬和重构整个软件系统的进程中，Postgres 已经成为 Agent 后端数据库的事实标准。但与此同时，PG 在 Agent 场景下存在明显不足：

- 进程模型导致高并发 Agent 连接成本极高；
- 对向量、语义、多模态等现代 AI 负载的支持属于"后装"式；
- C 语言实现的内存安全问题长期存在；
- Agent 使用数据库的方式（工具调用、记忆、观测、协作）与传统应用差异巨大。

因此，目标是构建一套**面向 AI Agent 场景、支持传统 TP 型数据库核心功能、同时原生解决上述问题**的新型数据库系统。

---

## 二、核心设计目标

### 1. 解决 Postgres 的进程模型问题

Postgres 的进程模型（每个连接一个 backend 进程）在短连接、高并发 Agent 场景下非常重。新系统应使用线程来快速响应 Agent 对数据库的增删改查操作。

关键不是"用线程代替进程"，而是构建一个能支持**无共享或共享内存上的轻量任务调度**的运行时。

候选路线：

| 路线 | 代表 | 优势 | 风险 |
|---|---|---|---|
| 线程池 + 共享内存 | 类似现代 OLTP | 低延迟、高并发 | 需重写 buffer pool、锁、WAL |
| async/await 运行时 | tokio + per-core 架构 | 海量连接、轻量任务 | 数据库内核 async 化挑战很大 |
| 混合模型 | 连接用线程/协程，执行引擎用 MPP | 灵活 | 复杂度最高 |

**初步建议**：采用当前 Rust 数据库的主流模式——

- **网络 / 连接层**：使用 tokio async runtime 处理连接多路复用、协议解析、idle 等待等 IO 密集型工作；
- **执行 / 存储层**：使用同步线程池（如 `tokio::task::spawn_blocking` 或自研工作线程池）处理事务执行、buffer pool、WAL 写入等 CPU/IO 混合工作。

这样既能获得 async 在高并发连接上的优势，又避免把 MVCC、锁、页缓存等内核逻辑强行 async 化。

---

### 2. Agent Native：面向 Agent 使用数据库的场景

"Agent Native" 是产品定位，不只是功能堆砌。Agent 使用数据库的典型模式：

- **工具调用（Tool Use）**：Agent 通过 Function Calling 读写数据库，每次调用都是短事务；
- **记忆（Memory）**：长期记忆需要向量 + 语义检索 + 时间序列；
- **观测（Observation）**：Agent 执行步骤、数据库内部状态、数据变更来源需要被记录、回放、审计，且对 LLM 可理解；
- **多 Agent 协作**：多个 Agent 可能同时读写同一张表，甚至互相触发。

数据库应原生支持：

- 结构化 + 非结构化混合存储（标量、向量、文本块、JSON、blob 同表）；
- 事务内触发外部事件（如行更新后通知另一个 Agent）；
- 查询结果默认对 Agent 友好（自动 JSON 序列化、返回 schema 元数据、支持流式返回），降低 LLM 解析成本；
- Agent ID / Session ID / Trace ID 作为一等公民；
- 可观测性数据结构化输出（JSON 执行计划、行级 provenance、query trace），方便 Agent 自我诊断和调试。

### 2.1 数据模型示例

一个具体的 DDL 比十页原则描述更能说明"Agent Native"长什么样：

```sql
CREATE TABLE agent_memory (
    id          TEXT PRIMARY KEY,
    content     TEXT,
    embedding   VECTOR(1536),
    metadata    JSONB,
    tags        TEXT[],
    created_by  AGENT_ID,       -- 一等公民：Agent 身份
    session_id  TRACE_ID,       -- 一等公民：会话追踪
    created_at  TIMESTAMP DEFAULT now()
) WITH (
    vector_index = 'hnsw',
    provenance   = true
);
```

这个示例体现了几个核心设计：

- `VECTOR(1536)` 是原生向量类型，不是扩展；
- `JSONB` 和数组类型与标量、向量共存于同一表；
- `AGENT_ID` 和 `TRACE_ID` 是内置类型，自动参与 provenance 和 trace；
- `WITH (vector_index = 'hnsw', provenance = true)` 声明式地启用向量索引和行级溯源。

---

### 3. 用 Rust 解决 C 语言的内存安全问题

Rust 的收益不仅是内存安全：

- **并发安全**：借用检查器在编译期约束数据所有权，对线程模型数据库极其重要；
- **生态**：`tokio`、`arrow-rs`、`datafusion`、`parquet-rs`、`lance`、`rust-iceberg` 等库正在成熟；
- **可维护性**：对长周期基础设施项目，Rust 的抽象能力比 C 更适合大型团队协作。

注意：Rust 写数据库内核仍然很新，底层优化（自定义内存分配、NUMA 感知、CPU cache 优化）需要额外功力。

---

### 4. 计算层：从标量检索到统一检索

除了标量检索，需要对模糊查询、语义检索、向量检索有更好的支持。

目标是构建一个**统一的执行引擎**，处理：

| 查询类型 | 技术 | 适合场景 |
|---|---|---|
| 精确匹配 / 范围 | B+Tree / LSM | TP 事务 |
| 全文 / 模糊 | 倒排索引、Trigram、BM25 | 文本搜索 |
| 语义 | Embedding + 向量索引（HNSW、IVF） | 相似含义 |
| 多模态 | 跨模态 Embedding、CLIP 等 | 图搜文、文搜图 |

建议架构：**计算层基于 Arrow 格式 + DataFusion 或自研执行器**，索引层支持插件化。

一个有前景的方向是把 LanceDB 的索引理念和 pgvector 的经验结合，做一个原生的"混合检索引擎"，而不是在 Postgres 外面再挂一个向量数据库。

---

### 5. 内置 AI（LLM）能力

具体用 AI 做什么仍在探索。建议分阶段：

**第一阶段：查询优化助手**
- 自动索引推荐
- 查询重写建议
- 慢查询根因分析

**第二阶段：自然语言到查询（NL2SQL / NL2API）**
- Agent 用自然语言描述需求，数据库返回执行计划或结果
- 比传统 BI 的 NL2SQL 更自然，因为 Agent 本身就是 LLM 驱动

**第三阶段：自治数据库**
- 自动调参
- 自动分区
- 异常检测和自愈

**初期原则**：LLM 先作为 advisory 角色，不影响核心事务路径。不要一开始就碰"自治执行"。

---

### 6. 存储层：多模态存储 + ACID

多模态存储和 ACID 不矛盾，关键是**分层存储 + 元数据与数据分离**：

**热数据主路径（OLTP 核心）**：
- 使用自研行存页格式或 LSM-Tree；
- 支持随机单行读写、UPDATE/DELETE、点查；
- 通过 MVCC + WAL 保证 ACID。

**冷数据 / 归档 / 分析**：
- 使用 Parquet 作为不可变列存格式，用于分析投影和低成本归档；
- 使用 Iceberg 作为表格式 / catalog，支持版本化快照。

**向量与多模态数据**：
- 使用 Lance 作为向量、图像、嵌套数据的持久化格式；
- 大对象（图像、音频、视频）存在对象存储，表中只存引用 + embedding；
- 小对象 / 向量 / 文本块可以内联存储。

**元数据层**：事务型，保证 ACID，存 schema、索引、事务状态。

**Copy-on-Write 快照（CoW Snapshots）**：
- 磁盘上的数据文件采用不可变层设计；
- 后台 checkpoint 将内存脏页以 CoW 方式写入新文件，旧文件保留为逻辑快照；
- 通过引用已有文件实现低成本快照，支持时间点恢复（PITR）；
- 旧快照可压缩为 Parquet 推送到对象存储，作为冷归档。

注意：Phase 0-2 先聚焦单机和 checkpoint 级快照，不引入多 Tenant / 多分支的复杂语义，那是后续多租户和协作场景才需要的能力。

注意：Parquet / Lance 是不可变列存格式，**不适合作为热数据的随机更新主存储**，它们定位为冷数据归档和向量/多模态存储。

---

### 7. 行业生态：拥抱开放表格式和文件格式

Iceberg、Lance、Parquet 代表"数据格式统一化"的趋势。新数据库不应是另一个数据孤岛，而应是一个**高性能执行引擎 + 事务层**，下面按场景选择开放格式：

| 数据类型 | 格式 | 原因 |
|---|---|---|
| 热 OLTP 数据 | 自研行存页 / LSM | 支持随机更新、低延迟点查 |
| 冷数据归档 | Parquet | 高压缩率、生态互通 |
| 向量 / 多模态 | Lance | 原生支持向量和嵌套数据 |
| 表格式 / Catalog | Iceberg | 版本化、开放生态 |
| 内存交换 | Arrow | 零拷贝、生态标准 |

好处：

- 和 Lakehouse 生态互通（Spark、DuckDB、Polars 可直接读取）；
- 多模态友好；
- 存储成本可控。

---

## 三、竞品格局与生态位

当前规划不能只对 Postgres 出拳，必须清楚其他玩家已经走到哪一步，以及自己的不可替代性在哪。

### 3.1 竞品矩阵

| 竞品 | 核心能力 | 优势 | 不足 | 本项目差异 |
|---|---|---|---|---|
| **Postgres + pgvector** | 成熟 TP + 向量扩展 | 生态最丰富、工具链最全 | 进程模型重、向量是后装扩展、多模态能力弱、VACUUM 负担 | 线程模型 + 原生向量索引 + 多模态一体化 |
| **Neon** | Serverless PG、存算分离 | 用存算分离解决连接数和成本问题，PG 兼容 | 计算节点仍是 PG 进程模型；page server 只做存储不做计算优化；向量/多模态/AI 能力需额外扩展 | 自研内核，从线程模型和 Agent 场景出发重新设计 |
| **LanceDB** | Rust + Lance + 向量检索 | 向量/多模态原生、嵌入式、与现代 AI 工作流贴合 | 事务能力弱，不是 TP 数据库，无并发 ACID | 在 LanceDB 擅长的向量/多模态之上，补足完整 TP 事务 |
| **DuckDB** | 嵌入式 OLAP、Arrow + Parquet | 分析性能极强、Python/R 生态好 | 不适合 OLTP 随机更新、无高并发事务 | 重点在 TP 而非 AP，但共享 Arrow/Parquet 生态 |
| **sqlite-vec** | 轻量嵌入式向量 | 极小、易嵌入、无部署成本 | 无并发事务、无扩展性、不适合服务端 | 企业级并发 + 事务 + 服务端部署 |
| **libSQL (Turso)** | Rust fork of SQLite + 向量 + 同步复制 | SQLite 兼容、Serverless、边缘部署 | 仍是 SQLite 内核（单写、锁模型）；向量/多模态非原生 | PG 协议 + 线程模型 + 原生向量多模态 |
| **Limbo** | Rust 重写 SQLite | 激进地用 Rust 重写 SQLite，探索线程模型 | 早期项目，生态和成熟度远不及 SQLite | 类似的技术路线验证，但目标是 Server-grade 而非嵌入式 |
| **Databricks / Snowflake** | 云数仓、Lakehouse、AI 函数 | 大数据处理、BI、AI 集成 | 高延迟、非 TP、成本高、不适合 Agent 实时调用 | 低延迟 TP + AI 辅助，面向在线 Agent 负载 |
| **CockroachDB / TiDB / Yugabyte** | 分布式 NewSQL | 强扩展、强一致、高可用 | 向量/多模态非原生、单机延迟高于专用 OLTP | 先做单机低延迟 + 向量多模态，再考虑分布式 |
| **TiDB Serverless / PlanetScale** | Serverless MySQL | 连接成本低、自动扩缩容 | 向量/多模态弱、PG 生态不兼容 | PG 协议兼容 + 向量多模态原生 |

### 3.2 核心差异化

单独看每个维度，都有人在做：

- **TP 能力**：Postgres、NewSQL 已经很强；
- **向量检索**：LanceDB、pgvector、专用向量数据库已经很强；
- **多模态**：Lance、专用对象存储已经很强；
- **AI 辅助**：Snowflake、Databricks 已经在做。

但把 **TP + 向量 + 多模态 + AI 辅助** 四个维度**统一在一个系统内**，并且保证 ACID、低延迟、PG 协议兼容，目前没有人真正做好。

这就是本项目的生态位：**Agent 场景下的统一数据平台**——不是替换 Postgres，不是复制 LanceDB，也不是做一个更快的 DuckDB，而是让 Agent 能在一个数据库里同时完成事务状态更新、记忆向量检索、多模态内容管理和 AI 辅助查询优化。

### 3.3 竞争壁垒假设

如果竞品跟进，可能的反击路径：

- **LanceDB 把事务做完善**：LanceDB 更偏向 AI 数据湖，补齐完整 TP 事务需要重写存储引擎，路径很长；
- **Neon 把向量做多模态做好**：Neon 内核仍是 PG，受限于 PG 的进程模型和扩展机制，难以做到真正原生；
- **Postgres 改进进程模型**：社区巨大惯性，近年内不可能发生；
- **DuckDB 加 TP 能力**：DuckDB 设计目标是 OLAP，加 TP 会与其核心架构冲突。

因此，本项目的窗口期在于：**用 Rust 自研内核，从线程模型和 Agent Native 语义出发，把四个维度做统一**。

---

## 四、分阶段实现路径

不建议一开始就写完整数据库。可按以下顺序推进：

### Phase 0：概念验证（4-8 周）

- 实现 **PostgreSQL wire protocol** 协议层，支持 psql、SQLAlchemy、Prisma 等标准客户端连接；
- 基于 Arrow + DataFusion + Lance，做单节点"Agent 数据库原型"；
- 在存储层使用 **SQLite 或 RocksDB 作为临时存储**，利用其已有的原子写入 + WAL 保证单语句 auto-commit 的原子性；
- 支持 JSON 文档 + 向量 + 标量 + 单语句 auto-commit；
- 验证：能用 psql / Python driver 连上来执行基础 SQL + 向量检索。

> Phase 0 不实现完整多语句事务（BEGIN/COMMIT/ROLLBACK），但单语句必须具备原子性：崩溃后要么全成功要么全失败，不会留下半行脏数据。向量索引和行存的更新也必须是单语句原子的。

> 把 wire protocol 放在最前面，是因为 Agent 生态（LangChain、LlamaIndex、SQLAlchemy 等）几乎都假设后端是 Postgres 兼容的。协议兼容是生态入口，越早做代价越小。

### Phase 1a：自研存储引擎（3-6 个月）

- 替换 SQLite/RocksDB 临时存储，自研行存页管理 + Buffer Pool；
- 实现 MVCC + WAL + 崩溃恢复；
- 支持 Snapshot Isolation 级别的事务；
- 线程模型替代进程模型（网络层 tokio async，执行/存储层同步线程池）。

### Phase 1b：原生向量 + 完整 PG 兼容（3-6 个月）

- 原生向量索引（HNSW）与事务协调；
- 完整 PG 协议兼容（更完整的 SQL 方言、类型系统、系统表）；
- 可观测性能力落地（JSON EXPLAIN、provenance、query trace）。

### Phase 2：扩展性与生态集成（12-24 个月）

- 分布式事务 / 分片；
- 和 Iceberg / Lance 生态深度集成；
- 内置 LLM 查询优化。

### Phase 3：云原生与 Agent 框架（24 个月+）

- 云原生 / Serverless 形态；
- Agent 框架深度集成（LangChain、LlamaIndex、AutoGen 等）；
- 自然语言查询、自治调优等高级 AI 能力。

---

## 五、关键设计维度

以下维度虽未体现在核心功能列表中，但对正确性和工程聚焦至关重要。

### 5.1 事务隔离级别

Agent 场景的事务特征：

- **默认 Read Committed（RC）**：适合大多数 Agent 读-决策-写循环；
- **Serializable Snapshot Isolation（SSI）**：用于多 Agent 协作、竞争资源场景；
- 线程模型下，SSI 可借助共享内存直接访问写入集，避免 PG 的 SIREAD 锁在 shared_buffers 上的争用。

### 5.2 崩溃恢复

WAL 设计是存储引擎的核心：

- 每条事务变更先写 WAL，再修改内存页；
- checkpoint 定期将脏页刷盘；
- 崩溃后通过 WAL replay 恢复到一致状态；
- 向量索引、元数据 catalog 与行存共享同一个 WAL 日志流和 LSN 序列，确保三者在 checkpoint 和崩溃恢复时处于同一逻辑时间点。

### 5.3 向量索引与事务的协调

原生向量索引不是"把 pgvector 的 C 代码改写成 Rust"这么简单：

- **插入并发**：HNSW 图的并发插入需要图级锁或乐观重试；
- **事务回滚**：向量索引的插入必须参与 MVCC，事务回滚时索引条目也需回滚；
- **WAL 一致性**：向量索引的 checkpoint 必须和行存 LSN 对齐；
- **图结构回滚的复杂性**：HNSW 是图结构而非 B+Tree。插入新节点时需要重建图的局部连接，如果事务回滚，这些已建立的图连接需要反向拆除，比 B+Tree 的"插入 → 回滚 → 标记 dead tuple"复杂得多；
- **建议方案（简化版）**：向量索引先只支持 READ COMMITTED 级别的可见性——查询只看已提交数据，忽略 in-flight 的插入。未提交插入的图更新暂存在 per-transaction 的 delta 里，提交时原子合并。这比直接支持完整的 SI/SSI 简单很多，且能满足大多数 Agent 场景；
- **长期方案**：向量索引作为二级索引维护，条目持有指向行数据的 TID + XID，由 MVCC 引擎统一垃圾回收。

### 5.4 可观测性

Agent 场景下，数据库需要被 Agent 观测、理解和调试。传统 EXPLAIN 和慢查询日志对 LLM 不够友好，应提供结构化、可消费的观测能力：

- **JSON 执行计划**：`EXPLAIN (FORMAT JSON)` 默认返回结构化执行计划，包含耗时、扫描行数、索引使用、算子详情，方便 LLM 直接解析；
- **行级 provenance**：每一行记录写入者身份（Agent ID、Session ID、Trace ID、事务 ID），支持 `SELECT * FROM table WITH provenance` 查询；
- **Query Trace**：记录每条 SQL 的完整生命周期（解析、规划、执行、提交），支持按 Trace ID 聚合和回放；
- **内置审计日志**：数据变更（谁、什么时候、改了什么）作为一等能力，可直接被 Agent 用于自我反思和错误定位。

### 5.5 明确不做

明确边界是架构设计的一部分：

| 不做 | 原因 |
|---|---|
| 不做列存 OLAP 引擎 | DataFusion + Parquet 归档已覆盖分析投影需求 |
| 不做分布式（Phase 0-2） | 先专注单机线程模型 + CoW 快照，分布式单独评估 |
| 不做 PG C 扩展二进制兼容 | 用 Rust trait + WASM 重建扩展生态 |
| 不做内置 LLM 模型 | LLM 先作为外部服务，降低部署和运维复杂度 |

---

## 六、关键决策建议

1. **Wire Protocol**：兼容 PostgreSQL 协议，从 Phase 0 开始就是生态入口。

2. **存储格式分层**：
   - 热数据：自研行存页 / LSM；
   - 冷数据归档：Parquet；
   - 向量 / 多模态：Lance；
   - 表格式：Iceberg；
   - 内存交换：Arrow。

3. **查询引擎**：初期用 DataFusion，后期性能不够再自研执行器。

4. **线程模型**：网络/连接层 tokio async，执行/存储层同步线程池。

5. **AI 集成**：LLM 先作为外部服务调用，从查询优化助手开始，不进入核心事务路径。

---

## 七、总结

这个项目的本质是在做 **"AI Agent 时代的 PostgreSQL"**。当前 Agent 基础设施确实是"把 Postgres、向量数据库、对象存储、缓存拼在一起"，架构脆弱。

七个方向都是正确的，但建议**先聚焦最小可用产品**：

> 一个支持基础 SQL + 向量检索 + ACID 的 Rust 单节点数据库，兼容 PostgreSQL 协议，能被 psql / Python driver 直接连接。

跑通这个 MVP，再逐步替换内核、扩展分布式能力、深化生态集成。

---

## 八、下一步输出物

规划阶段到此告一段落，后续需要产出以下具体交付物：

1. **系统架构图**：覆盖网络层、SQL 层、执行层、MVCC 存储引擎、向量索引、可观测性模块的交互关系；
2. **Phase 0 技术选型清单**：wire protocol 实现方案、DataFusion 集成方式、临时存储（SQLite/RocksDB）选择、Lance 使用范围；
3. **Agent Native API / 查询语言 Spec**：工具注册协议、AGENT_ID / TRACE_ID 语义、JSON 执行计划格式、provenance 查询语法；
4. **存储引擎设计文档**：行存页格式或 LSM 选择、MVCC 版本链、WAL 格式、checkpoint 与 CoW 快照机制。
