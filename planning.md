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

### 2.3 Agent Native 数据库的范式融合

如果存在一个"AI Agent 的单一数据库"，它需要在**数据模型、查询语义、事务保证、性能特征、AI 原生接口**五个维度上实现范式融合。这不是简单的"Postgres + 插件"，而是需要在**存储引擎层面**重新设计：单一存储引擎通过多模态索引同时服务关系、向量、文档、图、时序、KV、全文等数据形态，避免 Agent 的写入路径分裂为多个同步事务。

基于 §2.1.2 的能力清单，我们将长期能力映射到阶段：

| 能力 | 目标阶段 | 说明 |
|---|---|---|
| 关系 + 向量 + 文档/半结构化 | Phase 0–1a | 内核 day 1 支持 |
| 原生 HNSW + 多向量空间 | Phase 1a–1b | 单文档多 embedding、加权融合 |
| 全文检索（BM25 + 中文分词） | Phase 1b | 与向量/结构化混合检索 |
| ACID + MVCC + 检查点（SAVEPOINT） | Phase 1a | Agent 工作流中断可恢复 |
| HTAP（行存 TP + 列存 AP 统一引擎） | Phase 2 | 与 TP 共享 WAL/MVCC/LSN，不引入独立 OLAP 引擎 |
| 事件溯源 / 数据血缘 | Phase 2 | 在 provenance 基础上扩展 |
| 图模型（属性图/Cypher 或 GQL） | Phase 2+ | 内核原生支持，用于 Agent 推理链、工具依赖 |
| 时序 + KV | Phase 2+ | Agent 行为轨迹、会话状态/队列 |
| 自动向量化 | Phase 1b/2 | `GENERATED ALWAYS AS embed` 语法 |
| 向量压缩（SQ/PQ） | Phase 2 | 亿级向量场景 |
| MCP Server / LangGraph / LlamaIndex | Phase 1b/2/3 | Agent 生态协议集成 |
| 多租户 RLS + 动态脱敏 | Phase 2 | 云原生/合规 |
| Serverless + COW 秒级分支 | Phase 2+ | Agent 沙箱 / speculative branching |

以下能力**明确不在内核主路径**：内置 LLM 推理、分布式事务（Phase 2 前）。HTAP 不是"在外部再挂一个 OLAP 引擎"，而是通过统一自研存储引擎同时支持 TP 行存和 AP 列存投影/扫描，共享 WAL/MVCC/LSN；图模型作为内核原生能力在 Phase 2+ 引入，而非通过外部扩展或 WASM 模拟。性能目标随开发深入和基准测试逐步量化，当前只保留方向性参考。

---

### 3. 用 Rust 解决 C 语言的内存安全问题

Rust 的收益不仅是内存安全：

- **并发安全**：借用检查器在编译期约束数据所有权，对线程模型数据库极其重要；
- **生态**：`tokio`、`arrow-rs`、`datafusion`、`parquet-rs`、`hnsw-rs` / `instant-distance`、`rust-iceberg` 等库正在成熟；
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

一个有前景的方向是把现代向量数据库（pgvector / LanceDB / Qdrant 等）的索引经验与原生混合检索引擎结合，做一个原生的"混合检索引擎"，而不是在 Postgres 外面再挂一个向量数据库。

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

### 6. 存储层：自研引擎统一服务 + 元数据与数据分离

存储层只承担一件事：**自研行存引擎统一服务所有结构化负载**（OLTP 行 + 向量 + 元数据），外部格式只承担导入导出和联邦查询。

**热数据主路径（OLTP + 向量，Phase 0 起）**：
- 自研行存页格式：Phase 0 v0（append-only WAL + 固定页 + 单写者）→ Phase 1a（行级 MVCC + Buffer Pool）；
- 自研 native HNSW：Phase 1a（RC-only + per-tx delta）→ Phase 1b（SI/SSI 升级为 TID + XID 二级索引）；
- 行存 / 向量索引 / 元数据 catalog **共享同一 LSN 序列和 WAL 流**，三者在 checkpoint 和崩溃恢复时天然一致；
- 多模态大对象（图像 / 音频 / 视频）走对象存储（S3 / MinIO / 本地 FS），表中存 BYTEA / TEXT 引用列 + embedding 列。

**HTAP 主路径（Phase 2）**：
- 统一自研引擎同时服务 TP 行存与 AP 列存投影/扫描；
- AP 查询与 TP 共享同一套 WAL/MVCC/LSN，保证分析结果的事务一致性；
- Parquet / Iceberg 仅用于冷归档与外部互操作，不作为热数据 AP 存储。

**元数据层**：与行存共享同一事务层，ACID 由自研引擎统一保证，不单独建存储。

**Copy-on-Write 快照（CoW Snapshots，Phase 2+）**：
- 磁盘上的数据文件采用不可变层设计；
- 后台 checkpoint 将内存脏页以 CoW 方式写入新文件，旧文件保留为逻辑快照；
- 通过引用已有文件实现低成本快照，支持时间点恢复（PITR）；
- 旧快照可导出为 Parquet 推送到对象存储，作为冷归档。

注意：Phase 0-2 先聚焦单机和 checkpoint 级快照，不引入多 Tenant / 多分支的复杂语义，那是后续多租户和协作场景才需要的能力。

注意：**不在内核内引入 Parquet / Lance / Iceberg 作为存储格式**。它们的角色分别是：
- Parquet — import / export 格式（DataFusion 直接读取，外部 Spark / DuckDB / Polars 可消费导出文件）；
- Iceberg — Phase 2 之后再评估是否需要做多引擎互操作；
- Lance — 完全不引入，避免依赖竞品生态。

---

### 7. 行业生态：协议兼容 + 格式互操作

新数据库不应是另一个数据孤岛，但「不成为数据孤岛」**不等于「内核采用异构存储」**。生态接入分两层：

**协议层（必须，Phase 0 起）**：
- PostgreSQL wire protocol 兼容 → 客户端生态（psql / SQLAlchemy / Prisma / LangChain / LlamaIndex 等）
- 标准 SQL 方言 → BI 工具生态（DBeaver / DataGrip）

**格式层（互操作，非存储，Phase 0 起）**：
- import：原生支持读取 Parquet / CSV / JSON（DataFusion 直接驱动），无需 ETL
- export：原生支持导出 Parquet / CSV / JSON，便于外部 Spark / DuckDB / Polars 消费
- 多模态大对象：与对象存储（S3 / MinIO / 本地 FS）互通
- 内存交换：Arrow 零拷贝（DataFusion 内部 + 跨进程 + 客户端返回）

**不在内核承担的角色**：
- 不做独立 OLAP 数仓（HTAP 由统一引擎自带）
- 不做 Lakehouse 表格式（Iceberg 集成 Phase 2 后再评估）
- 不内嵌向量文件格式（Phase 0 向量由 in-memory HNSW 原型承担，不依赖任何外部向量文件格式）

好处：

- 客户端生态零迁移成本接入
- 数据可双向流动，外部工具能消费我们的导出
- 内核职责单一，所有结构化数据走自研引擎，外部格式不会渗透到 catalog / TableProvider

---

## 三、竞品格局与生态位

当前规划不能只对 Postgres 出拳，必须清楚其他玩家已经走到哪一步，以及自己的不可替代性在哪。

### 3.1 竞品矩阵

| 竞品 | 核心能力 | 优势 | 不足 | 本项目差异 |
|---|---|---|---|---|
| **Postgres + pgvector** | 成熟 TP + 向量扩展 | 生态最丰富、工具链最全 | 进程模型重、向量是后装扩展、多模态能力弱、VACUUM 负担 | 线程模型 + 原生向量索引 + 多模态一体化 |
| **Neon** | Serverless PG、存算分离 | 用存算分离解决连接数和成本问题，PG 兼容 | 计算节点仍是 PG 进程模型；page server 只做存储不做计算优化；向量/多模态/AI 能力需额外扩展 | 自研内核，从线程模型和 Agent 场景出发重新设计 |
| **LanceDB** | Rust + Lance + 向量检索 | 向量/多模态原生、嵌入式、与现代 AI 工作流贴合 | 事务能力弱，不是 TP 数据库，无并发 ACID | 在 LanceDB 擅长的向量/多模态之上，补足完整 TP 事务 |
| **DuckDB** | 嵌入式 OLAP、Arrow + Parquet | 分析性能极强、Python/R 生态好 | 不适合 OLTP 随机更新、无高并发事务 | 重点在 TP + HTAP，但共享 Arrow/Parquet 生态 |
| **sqlite-vec** | 轻量嵌入式向量 | 极小、易嵌入、无部署成本 | 无并发事务、无扩展性、不适合服务端 | 企业级并发 + 事务 + 服务端部署 |
| **libSQL (Turso)** | Rust fork of SQLite + 向量 + 同步复制 | SQLite 兼容、Serverless、边缘部署 | 仍是 SQLite 内核（单写、锁模型）；向量/多模态非原生 | PG 协议 + 线程模型 + 原生向量多模态 |
| **Limbo** | Rust 重写 SQLite | 激进地用 Rust 重写 SQLite，探索线程模型 | 早期项目，生态和成熟度远不及 SQLite | 类似的技术路线验证，但目标是 Server-grade 而非嵌入式 |
| **Databricks / Snowflake** | 云数仓、Lakehouse、AI 函数 | 大数据处理、BI、AI 集成 | 高延迟、非 TP、成本高、不适合 Agent 实时调用 | 低延迟 TP + HTAP + AI 辅助，面向在线 Agent 负载 |
| **CockroachDB / TiDB / Yugabyte** | 分布式 NewSQL | 强扩展、强一致、高可用 | 向量/多模态非原生、单机延迟高于专用 OLTP | 先做单机低延迟 + 向量多模态，再考虑分布式 |
| **TiDB Serverless / PlanetScale** | Serverless MySQL | 连接成本低、自动扩缩容 | 向量/多模态弱、PG 生态不兼容 | PG 协议兼容 + 向量多模态原生 |

### 3.2 核心差异化

单独看每个维度，都有人在做：

- **TP 能力**：Postgres、NewSQL 已经很强；
- **向量检索**：LanceDB、pgvector、专用向量数据库已经很强；
- **多模态**：专用对象存储已经很强；
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

不建议一开始就写完整数据库。整个实现路径分两个维度：

- **横向**：每个 Phase 同时推进协议/SQL 层、执行层、存储引擎层、索引层、可观测层；
- **纵向**：存储引擎内部按组件逐步演进，从 **v0 最小引擎 → MVCC 引擎 → HTAP 引擎**。

### 4.1 存储引擎总体架构与组件

为了使实现路径可追踪，先把存储引擎拆成以下组件。每个 Phase 明确升级哪些组件，而不是笼统地写"自研存储引擎"。

| 组件 | Phase 0 | Phase 1a | Phase 1b | Phase 2 |
|---|---|---|---|---|
| **Page Format / Tuple Layout** | v0：固定 8KB 页 + slot array + 行头预留 `xmin/xmax/LSN` | 完整多版本行格式 + 版本链指针 | 稳定 | 支持列存投影/混合页 |
| **Page Directory / Buffer Pool** | 内存 frame table，无 eviction | LRU + dirty page flush + 异步刷盘 | 稳定 | 云原生/计算存储分离适配 |
| **WAL / Recovery** | append-only，单写者，stop-the-world 全量 checkpoint | 多写者 group commit，增量/fuzzy checkpoint | 稳定 | 分布式/多副本日志（评估） |
| **Transaction / MVCC** | 单版本 + LSN，单写者 | 行级 MVCC + 可见性链 + undo + GC | SSI / TID+XID HNSW | 分布式事务（评估） |
| **Access Methods (B+Tree)** | heap scan 或最小 B-tree | 并发 B+Tree 主/二级索引 | 覆盖索引、index-only scan | 列存索引/向量化扫描 |
| **Vector Index (HNSW)** | in-memory PoC | 持久化 + RC-only per-tx delta | SI/SSI + GC 打通 | 压缩 + 十亿级优化 |
| **Catalog / System Tables** | 最小 catalog（system tables 存于引擎内） | `pg_catalog` 子集、`information_schema` | 完整 PG 系统表 | 多租户 catalog |
| **DataFusion TableProvider** | 最小 scan + filter pushdown | 完整 TableProvider + 索引回表 | 优化器定制 | AP 向量化执行器 |

> **设计原则**：这些组件共享同一 LSN 时钟和 WAL 流。任何新组件（HNSW、列存投影、图索引）接入时，必须先解决"如何写 WAL、如何参与 checkpoint、如何崩溃恢复"三个问题，否则不能进入内核契约。

---

### Phase 0：概念验证（8–12 周）

Phase 0 目标：一个能被 `psql` / Python driver 连接的 Rust 单节点数据库，支持单语句 auto-commit、基础 SQL、JSON + 向量，并能在 `kill -9` 后通过 WAL replay 恢复到一致状态。存储引擎处于 **v0 最小可用** 状态。

#### 里程碑 0.1：PG Wire Protocol & 前端（2 周）

- 实现 PostgreSQL wire protocol：StartupMessage、AuthenticationOK、Simple Query、Extended Query、ReadyForQuery、RowDescription、DataRow、CommandComplete。
- 支持 `psql`、SQLAlchemy、Prisma、LangChain 等标准客户端连接。
- 先只支持 text 结果格式；binary 格式 deferred。

#### 里程碑 0.2：SQL 解析与 DataFusion 集成（2 周）

- 使用 DataFusion SQL parser 解析 `CREATE TABLE`、`INSERT`、`SELECT`、`DELETE`。
- 设计 catalog provider 接口，让 DataFusion 的 planner 能看到我们的表和类型。
- 类型系统：INT8/INT4、TEXT/VARCHAR、JSONB、VECTOR(n)、AGENT_ID、TRACE_ID。
- 实现最小 `TableProvider`：全表 scan + filter pushdown（只支持简单等值/范围）。

#### 里程碑 0.3：v0 最小存储引擎（4–6 周）

这是 Phase 0 最重的部分，必须细化到可编码的粒度：

**Page & Tuple 格式**
- 固定大小页：8KB，页头包含 `page_id`、`page_lsn`、`free_space`、`slot_count`。
- Slot array 从页尾向页头生长；tuple 数据从页头向页尾生长。
- 行头（tuple header）包含：长度、状态标志、`xmin`（v0 填当前 LSN）、`xmax`（v0 留空）、`lsn`（最后修改 LSN）。
- 行格式预留 MVCC 字段位置，但 v0 不实现版本链。

**Page Directory / Buffer Pool**
- 内存中的 `HashMap<PageId, Frame>` 作为 page directory / frame table。
- 每个 frame 包含：page 数据、dirty 标志、pin count（v0 可简化）。
- Phase 0 **不做 LRU eviction**：全部数据驻内存，或简单 stop-the-world flush。

**WAL 设计**
- append-only 日志文件，文件名按 LSN 分段（如 `wal-000000001.log`）。
- 全局单调递增 LSN，由 `AtomicU64` 分配。
- WAL 记录类型（v0）：
  - `Insert { table_id, page_id, tuple_data }`
  - `Update { table_id, old_page_id, old_slot_id, new_tuple_data }`
  - `Delete { table_id, page_id, slot_id }`
  - `Commit { lsn }`
  - `CheckpointBegin { checkpoint_lsn }` / `CheckpointEnd { checkpoint_lsn }`
- 写路径：先 append WAL，fsync，再修改内存页；commit 即 WAL fsync（或简单 batch）。

**Checkpoint & Recovery**
- stop-the-world 全量 checkpoint：把所有 dirty frame 按页 ID 顺序写入 `.db` 数据文件，然后写 `CHECKPOINT_END` 并截断 WAL。
- 崩溃恢复：读取最后一个 `CheckpointEnd` 的 LSN，从该点 replay WAL；用 `page_lsn` 判断是否需要应用某条记录。
- 测试：`kill -9` 后重启，验证数据不丢、无半行。

**Catalog**
- `pg_class`、`pg_attribute` 等最小 system tables 用与普通表相同的页格式存储。
- catalog 修改（CREATE TABLE）走同一 WAL 流，保证崩溃一致性。

#### 里程碑 0.4：HNSW 原型（1–2 周）

- 用 crate（候选 `hnsw-rs` / `instant-distance`）启动 in-memory HNSW。
- 写一层 SQL-to-HNSW 路由：识别 `ORDER BY embedding <=> $1 LIMIT k` 并命中 HNSW。
- 命名为 `HnswPrototype`，接口返回 `PoCResult`，明确标注「重启即丢、不进内核契约、不参与崩溃恢复」。
- HNSW 更新在进程内与行存同步，但崩溃后不恢复。

#### 里程碑 0.5：集成验证（1 周）

- 用 `psql` 连上数据库，执行：
  ```sql
  CREATE TABLE t (id TEXT PRIMARY KEY, content TEXT, embedding VECTOR(1536));
  INSERT INTO t VALUES ('a', 'hello', '[0.1, ...]');
  SELECT * FROM t ORDER BY embedding <=> '[0.1, ...]' LIMIT 5;
  ```
- `kill -9` 后重启，验证 INSERT 数据通过 WAL replay 恢复。
- HNSW 重启后从行存重建（或重新插入测试数据）。

> Phase 0 不实现完整多语句事务（BEGIN/COMMIT/ROLLBACK），但单语句必须具备原子性：崩溃后要么全成功要么全失败，不会留下半行脏数据。行存更新具备崩溃原子性；HNSW 原型在进程内与行存同步更新，但不保证崩溃后可恢复。端到端的"向量索引 + 行存"崩溃原子性从 Phase 1a 起实现。

> 把 wire protocol 放在最前面，是因为 Agent 生态（LangChain、LlamaIndex、SQLAlchemy 等）几乎都假设后端是 Postgres 兼容的。协议兼容是生态入口，越早做代价越小。

#### Phase 0 交付物

1. `pg_rust` 可执行二进制，支持 `psql` 连接；
2. v0 存储引擎设计文档（page format、tuple layout、WAL format、LSN 分配、checkpoint/recovery 流程）；
3. 崩溃恢复测试报告（至少覆盖 kill -9、进程 panic、磁盘未满三种场景）；
4. HNSW 原型评估报告（crate 选型、路由层设计、PoC 边界标注）。

---

### Phase 1a：MVCC 存储引擎 + native HNSW（RC-only）（6–10 个月）

Phase 1a 目标：从事务层到存储层全面演进，支持多语句事务、行级 MVCC、并发 B+Tree、持久化 HNSW，并保持与 Phase 0 的接口契约兼容。

两个硬骨头并行推进，共享 LSN 序列和 WAL 流，代码上尽量解耦（MVCC 在行存层、HNSW 在索引层，通过统一事务协调器交互）。

#### Track A：MVCC 与完整存储引擎

**里程碑 1a.1：行级 MVCC 与版本链（8–10 周）**
- 事务 ID（TXID）分配器与快照构造。
- 行格式完整化：`xmin`、`xmax`、`cid`（command id）、`ctid`（指向新版本）。
- 版本链：UPDATE 时旧行保留，新行通过 `ctid` 链接；DELETE 时设置 `xmax`。
- 可见性规则：实现 RC（语句级快照）和 SI（事务级快照）。
- Undo / rollback：事务回滚时，将其写入集标记为不可见；v1 用 vacuum 延迟清理，或即时清理单事务写入集。

**里程碑 1a.2：多写者并发控制（6–8 周）**
- 从单写者演进为多写者。
- Page-level latch（读写锁）保护 buffer pool frame。
- Row-level lock manager：S/X 锁、意向锁、死锁检测（wait-for graph 或 timeout）。
- 乐观并发控制（OCC）作为可选项，用于低冲突场景。

**里程碑 1a.3：Buffer Pool 完整化（4–6 周）**
- LRU / CLOCK 替换策略。
- Dirty page 异步刷盘与 WAL-driven checkpoint。
- Pin/unpin 协议，避免 evict 被使用中的页。
- 可选：io_uring / tokio-uring 实验，fallback 到 `pread`/`pwrite`。

**里程碑 1a.4：WAL 与 Recovery 升级（4–6 周）**
- WAL 记录支持 before-image 与 after-image，用于 rollback 和 redo。
- Group commit：多个事务的 commit 记录合并一次 fsync。
- Fuzzy / incremental checkpoint：checkpoint 期间不阻塞写入，通过 `CheckpointBegin`/`CheckpointEnd` 界定一致窗口。
- Recovery：redo 到最新 LSN，undo 未提交事务。

**里程碑 1a.5：并发 B+Tree 索引（6–8 周）**
- 主键 B+Tree（聚簇或非聚簇）。
- 二级 B+Tree 索引，叶子存 `TID`（page_id, slot_id）。
- 索引并发：latch coupling 或 optimistic latch crabbing。
- 索引与行存统一 WAL：索引页也带 `page_lsn`，崩溃恢复时一起 replay。

**里程碑 1a.6：Catalog 与系统表（4–6 周）**
- 扩展 `pg_catalog`：表、列、索引、类型、约束。
- 支持 `information_schema` 子集。
- 系统表本身走 MVCC，DDL（CREATE INDEX、ALTER TABLE）作为事务操作。

#### Track B：Native HNSW（RC-only）

**里程碑 1a.7：HNSW 持久化与 WAL 协调（6–8 周）**
- 在 Phase 0 的 `HnswPrototype` 代码上扩展，而非重写。
- 设计 HNSW 文件格式：
  - 节点文件：`node_id → vector + row pointer/TID + 邻居列表偏移`
  - 邻居文件：变长邻居列表（slab 或动态数组）
- 所有图变更（add node / add edge / remove node / remove edge）写入共享 WAL。
- 崩溃恢复时通过 WAL replay 重建或修复图。

**里程碑 1a.8：per-tx delta 与 MVCC 可见性（4–6 周）**
- 每个事务维护 `Vec<DeltaOp>`，记录未提交的图操作。
- 查询时只读已提交的全局图；未提交插入通过 delta 在事务内可见（RC 语义）。
- 事务回滚：直接丢弃 delta，无需反向拆除图连接——这是 per-tx delta 的最大收益。
- 提交时原子合并 delta 到全局图，并写 WAL `HnswCommit`。

**里程碑 1a.9：事务协调器（3–4 周）**
- 统一协调行存、HNSW、catalog 在同一事务内的提交与回滚。
- Commit 协议：写 WAL → 更新行存 page → 合并 HNSW delta → 更新 catalog → 返回 OK。
- 失败时统一回滚。

#### Phase 1a 事务与线程模型

- 支持 Snapshot Isolation 级别的事务（默认 RC，复杂场景用 Serializable / SSI，catalog 上预留 SSI 钩子）。
- 线程模型替代进程模型（网络层 tokio async，执行/存储层同步线程池）。
- 与 Phase 0 的接口契约保持兼容（TableProvider / catalog / 类型系统无需大改，存储层内部演进）。

#### Phase 1a 验证

- 多语句事务（BEGIN / COMMIT / ROLLBACK）正确性。
- 并发读写同一表无脏读、不可重复读、幻读（SI 级别）。
- 向量索引正确参与事务回滚。
- `kill -9` 后行存与 HNSW 通过共享 WAL 恢复到一致 LSN。
- 百万级向量规模下 HNSW 保持 RC 可见性。

#### Phase 1a 交付物

1. MVCC 存储引擎设计文档（版本链、可见性规则、undo log、GC、锁管理器）；
2. WAL / Recovery 设计文档（记录格式、checkpoint 算法、恢复流程）；
3. HNSW 索引设计文档（文件格式、per-tx delta、WAL 协调）；
4. 并发测试套件（至少覆盖读写竞争、写写冲突、死锁、崩溃恢复）。

---

### Phase 1b：HNSW SI/SSI + 完整 PG 兼容 + AI 原生接口 + 可观测性（4–6 个月）

Phase 1b 目标：把 HNSW 从事务正确性升级到生产可用，补完 PG 协议兼容，落地 AI 原生接口和可观测性。

- **HNSW 从 RC-only 升级到 SI/SSI 可见性**：
  - per-tx delta → TID + XID 二级索引；
  - MVCC 引擎统一垃圾回收 HNSW 条目（行版本被 GC 时，对应 HNSW 条目一起回收）；
  - 多 Agent 并发写同一向量表的 SSI 串行化验证。
- **HNSW 完整化收尾**：
  - 独立 HNSW 文件格式与 checkpoint 协调；
  - HNSW 条目与 MVCC GC 链路的端到端打通；
  - 这是 Phase 0 原型 → Phase 1a MVCC → Phase 1b 持久化 GC 三步演进的最后一步。
- **多向量空间与 AI 原生能力**：
  - 单表多向量列、跨向量空间加权融合检索；
  - 自动向量化：`embedding vector(n) GENERATED ALWAYS AS embed(content)`；
  - 原生全文检索（BM25 + 中文分词），支持向量/全文/结构化过滤的 RRF 融合。
- **Agent 生态协议接入**：
  - MCP Server：让 Agent 通过 JSON-RPC 2.0 以标准工具方式读写数据库，支持 schema 自省、只读/读写模式；
  - 为 LangGraph / LlamaIndex 等框架提供官方 `PostgresSaver` / `VectorStore` 适配器接口预留。
- 完整 PG 协议兼容（更完整的 SQL 方言、类型系统、系统表）；
- 可观测性能力落地（JSON EXPLAIN、provenance、query trace、数据血缘链路）；
- DBeaver / DataGrip 等 BI 工具直连（完整 system catalog）。

> **Phase 1b 性能方向**：向量检索、OLTP 事件日志写入和初步 AP 查询均需在统一引擎内保持竞争力；具体量化目标随 benchmark 结果逐步确定。

---

### Phase 2：HTAP + 多模态 + 云原生（12–24 个月）

Phase 2 目标：从 TP+向量数据库扩展为统一多模态 HTAP 数据库，并具备云原生形态。

- **HTAP 统一引擎**：
  - 行存主路径服务 TP，列存投影/混合格式服务 AP；
  - AP 查询与 TP 共享同一套 WAL/MVCC/LSN，保证分析结果的事务一致性；
  - 不引入独立 OLAP 引擎，分析不走外部 Parquet/Iceberg 热数据路径。
- **多模态扩展**：
  - 图模型（属性图/Cypher 或 GQL）支持 Agent 推理链、工具依赖、知识图谱；
  - 时序 + KV 能力用于 Agent 行为轨迹、会话状态与队列语义；
  - 向量压缩（SQ/PQ）与自动策略选择，支撑亿级向量规模。
- **云原生与多租户**：
  - Serverless / 计算存储分离形态；
  - COW 快照支持的秒级分支（Branch-per-Request / Agent 沙箱）；
  - 多租户 RLS、字段级动态脱敏、配额与审计。
- **生态深度集成**：
  - LangGraph / LlamaIndex 等 Agent 框架官方适配器；
  - 实时数据变更推送（CDC / WebSocket / SSE），驱动事件型 Agent；
  - Iceberg 互操作评估（视 ROI 决定，§六 #3 / §9.4 已明确不预先承诺）。
- 分布式事务 / 分片（先评估真实需求强度）；
- 内置 LLM 查询优化（仍以外部服务为主）。

---

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

### 5.2 崩溃恢复与 WAL 设计

WAL 是存储引擎的核心契约，从 Phase 0 的 v0 引擎到 Phase 1a 的完整 MVCC 都沿用同一套基础语义，**v0 不写「临时版」，从 day 1 就按最终形态的子集实现**。

#### WAL 记录类型

| 记录类型 | 内容 | 用途 |
|---|---|---|
| `Insert` | table_id, page_id, tuple_data | 插入行 |
| `Update` | table_id, old_tid, new_page_id, new_tuple_data | 更新行 |
| `Delete` | table_id, page_id, slot_id | 删除行 |
| `Commit` | txid / lsn | 事务提交点 |
| `HnswAddNode` / `HnswAddEdge` / `HnswDelNode` / `HnswDelEdge` | node_id, vector, neighbor_list | HNSW 图变更 |
| `CheckpointBegin` | checkpoint_lsn | 标记 checkpoint 开始 |
| `CheckpointEnd` | checkpoint_lsn | 标记 checkpoint 完成，可截断 WAL |

#### Checkpoint 演进

- **Phase 0**：stop-the-world full checkpoint。所有 dirty page 刷盘后写 `CheckpointEnd`，然后截断 WAL。
- **Phase 1a**：fuzzy / incremental checkpoint。checkpoint 期间允许写入，通过 `CheckpointBegin`/`CheckpointEnd` 界定一致窗口；只刷增量 dirty page。

#### 恢复算法

1. 读取控制文件，找到最后一个完整 `CheckpointEnd` 的 LSN。
2. 从该 LSN 顺序扫描 WAL 到末尾。
3. 对每条记录，若目标页的 `page_lsn < record_lsn`，则 redo 该记录；否则跳过。
4. （Phase 1a+）redo 完成后，对未提交事务的写入进行 undo，或依赖 MVCC 可见性使其不可见。
5. 恢复完成后进入一致状态，接受新连接。

#### 跨组件一致性

- LSN 是全局单调递增的逻辑时钟；从 Phase 1a 起，行存、向量索引、元数据 catalog 共享同一 LSN 序列。
- 任何组件（HNSW、列存投影、图索引）接入前，必须先回答：写哪种 WAL 记录、如何参与 checkpoint、如何 redo/undo。

### 5.3 向量索引与事务的协调

原生 HNSW 不是"把 pgvector 的 C 代码改写成 Rust"这么简单，核心难点在图结构的事务回滚和并发插入。我们分三阶段处理，对应 §四 Phase 0 原型 + Phase 1a MVCC 扩展 + Phase 1b 持久化收尾。

**Phase 0：HNSW 原型（PoC 级，in-memory）**

- **图自身**：用 crate 起步（候选 `hnsw-rs` / `instant-distance`），写最小 SQL 路由层，~500-1000 LOC；
- **接口契约**：命名为 `HnswPrototype`、接口返回 `PoCResult` 类型，编译期 + 文档双标注「不进内核契约」；
- **持久化**：无——重启即丢，不参与 WAL；
- **可见性**：忽略事务，查询即返回当前 in-memory 图状态。

**Phase 1a：HNSW RC-only 版本（在 Phase 0 原型基础上扩展）**

- **图自身**：以 Phase 0 原型为起点，扩展为持久化 + MVCC-aware 完整版，整体 ~2-3K LOC（增量约 1.5-2K LOC）；
- **文件格式**：
  - 节点文件：`node_id → vector + row pointer/TID + 邻居列表偏移`；
  - 邻居文件：变长邻居列表，使用 slab 或追加式动态数组；
- **插入并发**：单写者（与 Phase 0 v0 引擎一致）→ Phase 1a 末 / Phase 1b 升级为乐观重试；
- **事务回滚**：未提交的图更新暂存在 per-transaction delta（`Vec<DeltaOp>`），**事务回滚时直接丢弃 delta**，无需反向拆除图连接（这是 per-tx delta 设计的最大收益，避免了 B+Tree 那种「插入 → 标记 dead tuple」之外的图结构反向拆除复杂度）；
- **可见性**：查询只看已提交数据，忽略 in-flight 的插入（RC 语义）；
- **WAL 一致性**：图更新走与行存 / catalog 共享的 WAL 流和 LSN 序列，三者在 checkpoint 和崩溃恢复时天然一致；
- **条目存储**：暂存 row pointer，Phase 1b 升级为 TID + XID。

**Phase 1b：HNSW SI/SSI 升级**

- per-tx delta → TID + XID 二级索引（条目持有指向行数据的 TID + XID）；
- MVCC 引擎统一垃圾回收 HNSW 条目（行版本被 GC 时，对应 HNSW 条目一起回收）；
- 多 Agent 并发写同一向量表的 SSI 串行化验证（SSI 钩子在 §5.1 阶段就已预留）。

### 5.4 可观测性

Agent 场景下，数据库需要被 Agent 观测、理解和调试。传统 EXPLAIN 和慢查询日志对 LLM 不够友好，应提供结构化、可消费的观测能力：

- **JSON 执行计划**：`EXPLAIN (FORMAT JSON)` 默认返回结构化执行计划，包含耗时、扫描行数、索引使用、算子详情，方便 LLM 直接解析；
- **行级 provenance**：每一行记录写入者身份（Agent ID、Session ID、Trace ID、事务 ID），支持 `SELECT * FROM table WITH provenance` 查询；
- **数据血缘**：记录向量/文档/行的上下游来源与依赖，支持 Agent 输出错误时追溯训练数据或知识库来源；
- **可解释性检索**：向量搜索返回距离、相似度分数与索引遍历路径，回答"为什么检索到这些结果"；
- **Query Trace**：记录每条 SQL 的完整生命周期（解析、规划、执行、提交），支持按 Trace ID 聚合和回放；
- **内置审计日志**：数据变更（谁、什么时候、改了什么）作为一等能力，可直接被 Agent 用于自我反思和错误定位。

### 5.5 明确不做

明确边界是架构设计的一部分：

| 不做 | 原因 |
|---|---|
| 不做独立 OLAP 数仓 | HTAP 由统一引擎自带，分析需求不走 ClickHouse/DuckDB 式独立引擎 |
| 不做分布式（Phase 0-2） | 先专注单机线程模型 + CoW 快照，分布式单独评估 |
| 不做 PG C 扩展二进制兼容 | 用 Rust trait + WASM 重建扩展生态 |
| 不做内置 LLM 模型 | LLM 先作为外部服务，降低部署和运维复杂度 |

---

## 六、关键决策建议

1. **语言栈**：统一使用 Rust 实现存储引擎、执行器和网络层。基于性能目标（高并发、低延迟、统一内存/并发模型），不再并行探索 Go 实现；Go 方向的其他探索（如 Paimon for Go）与 pg_rust 无关。

2. **Wire Protocol**：兼容 PostgreSQL 协议，从 Phase 0 开始就是生态入口。

3. **存储格式分层**：
   - **热数据 + 向量 + HTAP**：自研统一引擎同时服务 TP 行存与 AP 列存投影/扫描（Phase 2 完整化），共享 WAL/MVCC/LSN；Parquet / Iceberg 仅用于冷归档与外部互操作，不作为热数据存储；
   - **大对象**（图像 / 音频 / 视频）：对象存储 + 表中 BYTEA 引用列；
   - **数据导出**：Parquet / CSV / JSON（外部 Spark / DuckDB / Polars 可消费）；
   - **数据导入**：Parquet / CSV / JSON（DataFusion 直接读取，联邦查询）；
   - **内存交换**：Arrow；
   - **多引擎互操作**（Iceberg）：Phase 2 后评估。

4. **查询引擎**：初期用 DataFusion，后期性能不够再自研执行器。

5. **线程模型**：网络/连接层 tokio async，执行/存储层同步线程池。

6. **AI 集成**：LLM 先作为外部服务调用，从查询优化助手开始，不进入核心事务路径；自动向量化（`GENERATED ALWAYS AS embed`）是 AI 原生接口，但 embedding 计算仍走外部服务。

7. **性能方向**：向量检索、OLTP 写入和 HTAP 分析都需在统一引擎内保持低延迟与高吞吐；具体量化目标（向量规模、P99 延迟、TPS）随开发深入和基准测试逐步确定，当前以方向性优化和可复现 benchmark 为准。

8. **Agent 生态协议**：MCP Server 是 Agent 调用数据库工具的事实接口，应在 Phase 1b 落地，支持 schema 自省与字段级脱敏；LangGraph / LlamaIndex 等框架适配器在 Phase 2/3 完善。

9. **图模型**：作为内核原生数据形态在 Phase 2+ 支持，不走外部 Neo4j 或 WASM 模拟。

---

## 七、总结

这个项目的本质是在做 **"AI Agent 时代的 PostgreSQL"**。当前 Agent 基础设施确实是"把 Postgres、向量数据库、对象存储、缓存拼在一起"，架构脆弱。

七个方向都是正确的，但建议**先聚焦最小可用产品**：

> 一个支持基础 SQL + 向量检索 + ACID 的 Rust 单节点数据库，兼容 PostgreSQL 协议，能被 psql / Python driver 直接连接。

跑通这个 MVP，再逐步**演进**内核、扩展分布式能力、深化生态集成。

---

## 八、下一步输出物

规划阶段到此告一段落，后续需要产出以下具体交付物：

1. **系统架构图**：覆盖网络层、SQL 层、执行层、MVCC 存储引擎、向量索引、可观测性模块的交互关系；
2. **Phase 0 技术选型清单**：wire protocol 实现方案、DataFusion 集成方式、v0 存储引擎设计要点（WAL 格式、页大小、LSN 序列、单写者模型）、HNSW 原型 crate 选型（`hnsw-rs` vs `instant-distance` vs 自研极简版）、SQL-to-HNSW 路由层设计、PoC 边界的接口契约标注方案；
3. **Agent Native API / 查询语言 Spec**：工具注册协议、AGENT_ID / TRACE_ID 语义、JSON 执行计划格式、provenance 查询语法、MCP Server 接口契约与权限模型；
4. **存储引擎设计文档**（优先级最高）：
   - v0 存储引擎：page format、tuple layout、WAL format、LSN 分配、checkpoint/recovery 流程；
   - MVCC 引擎：版本链、可见性规则、undo log、GC、锁管理器；
   - HNSW 索引：文件格式、per-tx delta、WAL 协调、GC 链路；
   - HTAP 设计：列存投影/混合格式、AP 查询路径、与 TP 共享 WAL/MVCC 的方案。

---

## 九、产品定位与场景边界

### 9.1 一句话定位

> 一个**事务一致**地把**行数据 + 向量 + Agent 元数据**统一在同一 LSN 时钟上的 Rust 单机数据库内核，用**线程模型**承担 Agent 的高并发短连接负载。

### 9.2 核心差异化（按重要性排序）

| # | 特点 | 对应的竞品短板 |
|---|---|---|
| 1 | 行存 + 向量索引 + 元数据 catalog 共享同一 LSN 序列和 WAL 流，崩溃恢复 / checkpoint / MVCC 可见性天然一致 | LanceDB 无事务、pgvector 后装扩展、Limbo 单写者 |
| 2 | Agent Native 类型一等公民：`AGENT_ID` / `TRACE_ID` / `SESSION_ID` 是内置类型，自动参与 provenance 和 query trace | PG 需扩展、其它无 |
| 3 | 线程模型 + tokio async 网络层（每连接是 task，不是 process），万级短连接 Agent 调度成本极低 | PG 进程模型重、Neon 计算节点仍是 PG |
| 4 | native HNSW 作为 MVCC 二级索引（条目 TID + XID，由 MVCC 引擎统一 GC），不是「vector 表 JOIN 行表」 | pgvector 是黑箱、向量更新不回滚 |
| 5 | 默认 RC + 可选 Serializable / SSI（Agent 读-决策-写循环友好），多 Agent 竞争资源时可升 SSI | PG 需应用层 advisory lock |
| 6 | JSON EXPLAIN + 行级 provenance + query trace + 数据血缘作为一等能力，LLM 可直接消费 | PG EXPLAIN 对 LLM 不友好、需装扩展 |
| 7 | 统一自研引擎同时服务 TP 与 AP（HTAP），结构化数据不分层；冷归档通过 Parquet 导出 | DuckDB OLAP 倾向、PG + Iceberg 双引擎 |
| 8 | Arrow 作为查询结果交换格式（DataFusion 内部 + 跨进程 + 客户端返回全零拷贝） | PG text/binary protocol、DuckDB 部分 Arrow |

### 9.3 支持场景（按阶段）

**Phase 0（v0 引擎 + wire protocol）**
- Agent 记忆存取（embedding + metadata + provenance 单语句原子写）
- RAG 本地文档检索（小规模向量 + 标量过滤）
- JSON 文档存储 + 标量过滤
- 单语句 ACID 写入（kill -9 后 WAL replay 恢复）
- psql / SQLAlchemy / Prisma 直接连接

**Phase 1a（MVCC + native HNSW RC-only）**
- 多语句事务（BEGIN / COMMIT / ROLLBACK）
- Snapshot Isolation 跨语句一致性读
- 多 Agent 并发写同一张表（默认 RC）
- 向量索引正确参与事务回滚
- 百万级向量规模下 HNSW 保持 RC 可见性
- Agent 工作流检查点（SAVEPOINT / CHECKPOINT）

**Phase 1b（HNSW SI/SSI + 完整 PG 兼容 + AI 原生接口 + 可观测性）**
- 多 Agent 竞争资源场景的 SSI 事务
- 行级 provenance 查询与数据血缘追溯（`SELECT * FROM t WITH provenance`）
- 按 TRACE_ID 聚合的回放能力
- MCP Server 让 Agent 通过标准工具接口读写数据库
- 向量 + 全文 + 结构化过滤的混合检索
- 自动向量化（`GENERATED ALWAYS AS embed`）
- DBeaver / DataGrip 等 BI 工具直连（完整 system catalog）

**Phase 2（分布式 + 生态集成）**
- HTAP：同一引擎内完成 TP 写入与 AP 分析查询
- 图模型 / Cypher 或 GQL 支持 Agent 推理链
- 实时数据变更推送（CDC / WebSocket / SSE）
- COW 秒级分支支持 Agent 沙箱 / speculative branching
- 与 Spark / Snowflake 互读（视 Iceberg 集成 ROI 决定）

**Phase 3（云原生 + Agent 框架）**
- Serverless / 按需扩缩
- LangChain / LlamaIndex / AutoGen 深度集成
- 数据库分支（Agent 实验隔离）

### 9.4 明确不支持的场景

| 不支持 | 原因 |
|---|---|
| PB 级专用 OLAP 数仓 | 内核 HTAP 覆盖常规 AP；超大规模分析走 DataFusion + Parquet 联邦查询 |
| 跨地域强一致分布式事务 | 复杂度爆炸，且 Agent 场景不刚需 |
| PG 现有 C 扩展二进制兼容 | 用 Rust trait + WASM 重建（§5.5） |
| 内置 LLM 推理 | 永远外置（§5.5） |
| 高吞吐批量导入（COPY FROM 等价物） | Phase 0-1 不做，等向量稳定后评估 |
| 100GB+ 单库 | Phase 0-1 不针对 100GB+ 优化 |

### 9.5 跟竞品一句话差异

- vs **PG + pgvector**：我们是**向量一等公民 + 线程模型 + Agent 元数据原生**
- vs **LanceDB**：我们**有完整 MVCC + 单机 OLTP + HTAP**，不只是 AI 数据湖
- vs **DuckDB**：我们**面向 OLTP 短事务 + 原生 HTAP**，不是嵌入式 OLAP
- vs **Limbo**：我们目标是 **server-grade Agent 数据库**，不是嵌入式 SQLite 替代
- vs **Neon**：我们**自研内核 + HTAP + 原生向量**，不只是 PG + 远端存储
