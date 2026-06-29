# 2.1.2 Agent Native 的数据库 pg_rust（CB）

> 来源：《数据业务—架构以及底层系统》PDF 第 2.1.2 节
> 整理目的：留存原始论述，便于后续与 `pg_rust` 规划文档校对时翻阅

---

## 1. Postgres 出现在 Agent 技术栈的多个层级

**Memory & Knowledge**：L3 长期记忆持久化，对话历史，Agent 状态检查点。

内存架构已经演进为 4 层，PostgreSQL 是最底层的持久化角色：

| 层级 | 技术 | 延迟 | 职责 |
|---|---|---|---|
| L0 | 上下文窗口 | <1ms | 当前推理轨迹（ephemeral） |
| L1 | Redis | <1ms | 任务内工作记忆（volatile） |
| L2 | Qdrant / pgvector | 26–35ms | 跨会话语义记忆（向量检索） |
| L3 | PostgreSQL | 60–100ms | 全量 Agent 状态、审计日志、EU 合规 |

### 为什么是 Postgres 而非 SQLite 做 L3？

1. **崩溃零数据丢失**：LangGraph 的 `PostgresSaver` 将完整 Agent 状态持久化，避免进程重启导致任务重来。
2. **合规审计**：EU AI Act Article 12 要求可追溯性，Postgres 提供原生时间戳日志与事务历史。
3. **复杂查询**：支持对 Agent 历史决策进行 SQL 分析、聚合与再训练数据提取。

---

## 2. Vector Retrieval & AI 原生扩展

基于 pgvector 的语义搜索：它通过原生 `vector` 数据类型和 HNSW / IVFFlat 索引，使 Postgres 能够直接存储 LLM 生成的 embedding 并执行近似最近邻（ANN）搜索。

### 核心优势

- **ACID 事务保障**：embedding 与业务数据在同一事务中写入，无同步延迟。
- **混合检索**：原生支持向量相似度（`<=>`）与 PostgreSQL 全文搜索（`@@`）的 RRF 融合。
- **零额外基础设施**：已有 Postgres 的团队无需引入 Pinecone / Milvus 等新系统。

### 适用边界

pgvector 覆盖约 80% 的 Agent 用例，在 5000 万向量以下表现良好；超过十亿级规模时，VectorChord（EDB 的 pgvector 补充）可提供 100 倍更快的索引速度，或转向 Milvus 等专用向量数据库。

### AI 原生扩展，消除 ETL 管道

| 扩展 | 功能 | 对 Agent 的意义 |
|---|---|---|
| pgvector | 向量存储 + ANN 索引 | 原生语义记忆 |
| pgai (Timescale) | `create_vectorizer()` 自动同步 embedding | 消除 Kafka/Debezium 管道，INSERT 即向量化 |
| pgvectorscale | DiskANN 索引 | 十亿级向量的磁盘级高效检索 |
| pg_textsearch | BM25 级关键词搜索 | 替代 Elasticsearch 做混合检索 |

pgai 的自动向量化尤其关键：它在后台工作进程中监听 INSERT/UPDATE 事件，自动调用嵌入 API 并将向量写回同一行——零 ETL、零同步延迟、零 3AM 告警。

---

## 3. Tool Access：MCP 作为 Agent 可直接读取的结构化数据源

Model Context Protocol（MCP）已成为 2026 年 80% 生产部署的标准工具接口。Postgres MCP Server 让 Agent 能够直接以结构化、可审计的方式读写数据库：

- Agent 通过 JSON-RPC 2.0 调用 SQL 作为工具。
- 支持只读（知识库查询）和读写（状态更新）模式。
- 配合 DLP 控制平面，可对敏感列进行字段级脱敏。

这意味着 Postgres 不仅是 Agent 的"记忆后端"，更是 Agent 执行工具调用时的首选数据源——订单查询、库存检查、用户偏好读取都可通过 MCP 直接对接。

---

## 4. State Management

多步工作流的状态机持久化（如 LangGraph `PostgresSaver`）。

---

## 5. Sandboxing：通过数据库分支（Neon）实现 Agent 的隔离实验环境

Neon 等 Serverless Postgres 在 2026 年引入了计算-存储分离架构，使得数据库分支（branch）可在 500ms 内创建，且与父库共享数据块（零拷贝）。

这对 AI Agent 产生两个革命性影响：

1. **Branch-per-Request**：每个 Agent 任务可 fork 一个隔离分支，在沙箱中执行 SQL 实验、生成 diff，安全后再合并回主库。
2. **Agentic Speculative Branching**：哥伦比亚大学 2026 年 4 月的 BranchBench 论文提出，Agent 可同时 fork 多个分支尝试不同解决路径，只提交成功分支——单个任务可能生成数千个短生命周期分支。

---

## 6. 市场验证：十亿美元级基础设施押注

Postgres 在 AI Agent 领域的地位已被资本市场确认：

- 2025 年 5 月：Databricks 以 10 亿美元收购 Neon（Serverless Postgres 分支架构）。
- 2025 年 6 月：Snowflake 以 2.5 亿美元收购 CrunchyData（托管 Postgres）。

这些不是产品收购，而是基础设施押注——表明 Postgres 被视为 AI 时代的默认数据层。

Gartner 在 2026 年报告《Innovation Insight: Database Management Systems for Enterprise AI Agents》中，将 EDB Postgres AI 认可为能够同时承担 Agent 数据库三个关键角色的供应商：长期记忆、主要知识源、任务执行引擎。

---

## 7. Agent 使用数据库的场景：单一数据库的范式融合

如果存在一个"AI Agent 的单一数据库"（Single Database for AI Agents），它需要在**数据模型、查询语义、事务保证、性能特征、AI 原生接口**五个维度上实现范式融合（Paradigm Fusion）。这不是简单的"Postgres + 插件"，而是需要在**存储引擎层面**重新设计。

---

## 8. 完整能力矩阵

### 8.1 数据模型层：多模态统一存储

Agent 需要同时处理结构化决策、非结构化记忆、向量语义和图关系，数据库必须原生支持。

| 数据形态 | 具体场景 | 所需能力 |
|---|---|---|
| 关系型 | 用户画像、订单状态、权限矩阵 | 标准 ACID 表、外键、约束、JSON/JSONB 列 |
| 向量 | Embedding 存储、语义记忆、RAG 检索 | 原生 `vector` 类型、HNSW/IVFFlat/DiskANN 索引、多向量/多模态（文本+图像+音频各一个向量列） |
| 文档/半结构化 | Agent 输出日志、工具调用参数、非结构化知识 | JSONB / BSON 原生列，支持路径索引、数组展开、嵌套查询 |
| 图 | Agent 关系网络、工具依赖图、知识图谱 | 属性图模型（节点+边+属性），原生 Cypher/GQL 查询，而非关系表模拟 |
| 时序 | Agent 行为轨迹、决策时间线、指标监控 | 自动时间分片、降采样、连续聚合、TTL 过期 |
| 键值 | 会话状态、缓存、锁、分布式协调 | 原生 KV 接口，支持 `SKIP LOCKED` 队列语义、TTL、前缀扫描 |
| 全文 | 文档关键词检索、BM25 排序、混合搜索 | 原生倒排索引，支持中文分词、同义词扩展、与向量搜索的 RRF 融合 |

> 不是"一个数据库里放多个引擎"（如 MySQL + Elasticsearch + Redis + Milvus），而是**单一存储引擎通过多模态索引同时服务这些模型**。否则 Agent 的写入路径会分裂为多个同步事务，违背"单一数据库"的初衷。

---

### 8.2 查询与检索层：混合智能检索（Hybrid AI Retrieval）

Agent 的查询模式与经典 CRUD 完全不同，数据库需要支持：

#### 向量 + 结构化 + 全文的单查询融合

```sql
-- 理想形态：一条 SQL 完成 RAG 检索
SELECT content, metadata->>'source'
FROM agent_memory
WHERE
    team_id = 'sales-agent-01'                       -- 结构化过滤
    AND created_at > now() - interval '7 days'       -- 时序过滤
    AND content @@ plainto_tsquery('合同纠纷')        -- 全文匹配
ORDER BY
    embedding <=> $1 <-> 0.3 * ts_rank(...)          -- 向量相似度 + BM25 混合排序
LIMIT 10;
```

#### 多向量空间检索

一个文档可能有标题向量、正文向量、图像向量，数据库需要支持：

- 多列向量索引
- 跨向量空间的加权融合检索
- 条件路由（根据查询类型选择最优向量列）

#### 递归/图遍历查询

Agent 的推理链、工具调用链是图结构：

```cypher
// 查找影响当前决策的所有上游 Agent 输出
MATCH path = (current:Decision {id: $1})<-[:DEPENDS_ON*1..5]-(d:Decision)
RETURN d, length(path) AS depth
ORDER BY depth;
```

#### 窗口函数与流式聚合

Agent 的实时决策需要滑动窗口统计：

```sql
SELECT
    agent_id,
    avg(confidence) OVER w AS avg_confidence,
    count(*) FILTER (WHERE status = 'error') OVER w AS error_count
FROM agent_events
WINDOW w AS (PARTITION BY agent_id ORDER BY ts RANGE '5 minutes' PRECEDING);
```

---

### 8.3 事务与一致性：Agent 的状态安全

Agent 的"思考"是多步的，数据库必须保证跨步原子性。

| 能力 | 必要性 | 实现要求 |
|---|---|---|
| ACID 事务 | 工具调用结果与状态更新必须同时成功或回滚 | 原生 MVCC，支持长事务（Agent 推理可能持续数秒） |
| 分布式事务 | 多 Agent 协作时的跨库/跨分片一致性 | 2PC / Saga 模式原生支持，或基于 Raft 的分布式事务 |
| 乐观并发控制 | Agent 并行修改同一状态时的冲突解决 | 序列化隔离级别 + 自动重试机制 |
| 检查点（Checkpoint） | Agent 工作流中断后可恢复 | 原生 `SAVEPOINT` 语义暴露为 Agent 检查点 API |
| 事件溯源（Event Sourcing） | 决策可追溯、可回放、可审计 | 仅追加日志（WAL）作为事实来源，物化视图作为当前状态 |

> Agent 的"状态机"不是简单的行更新，而是**有向无环图（DAG）的持久化**。数据库需要支持将 Agent 的每一步推理、每次工具调用、每个观察结果作为不可变事件写入，同时提供物化视图快速获取当前状态。

---

### 8.4 性能

| 场景 | 性能要求 | 所需架构 |
|---|---|---|
| OLTP 写入 | 10万+ TPS 的 Agent 事件日志 | 行存引擎 + 异步 WAL 批量提交 |
| 向量检索 | P99 < 50ms 的 ANN 查询 | 内存优先的 HNSW + 磁盘级的 DiskANN 混合索引 |
| OLAP 分析 | 秒级 PB 级 Agent 行为分析 | 列存投影（Columnar Projection）或自动 HTAP |
| 实时流处理 | 毫秒级事件触发 Agent 反应 | 原生 `LISTEN/NOTIFY` 或 Change Data Capture（CDC）输出到消息队列 |

---

### 8.5 AI 原生能力层：零摩擦 AI 集成

这是区分"传统数据库 + AI 插件"与"AI 原生数据库"的核心。

#### 自动向量化

```sql
-- 插入即向量化，零 ETL
CREATE TABLE documents (
    id serial PRIMARY KEY,
    content text,
    embedding vector(1536) GENERATED ALWAYS AS (embed(content)) STORED
);
```

#### 原生 LLM 推理

```sql
-- 数据库内直接调用 LLM 生成
SELECT llm_generate(
    'gpt-4o',
    '总结以下客户反馈: ' || content,
    temperature => 0.7
) FROM feedback;
```

#### 向量量化与压缩

- 原生支持 Scalar Quantization（SQ）、Product Quantization（PQ）。
- 自动选择最优压缩策略（根据召回率要求）。

---

### 8.6 生态协议

数据库不能只是"被查询"，需要主动成为 Agent 生态的一员。

| 协议/接口 | 作用 |
|---|---|
| MCP Server | 通过 Model Context Protocol 暴露为 Agent 的标准工具，支持 schema 自省 |
| LangGraph / LlamaIndex 原生集成 | 提供 `PostgresSaver` / `VectorStore` 等官方适配器，支持检查点与语义缓存 |
| OpenAPI / REST | 自动生成 CRUD + 向量搜索的 REST API，Agent 无需 SQL 即可调用 |
| GraphQL | 前端/Agent 灵活查询，避免 N+1 问题 |
| WebSocket / SSE | 实时推送数据变更，驱动事件型 Agent |

---

### 8.7 运维与治理

| 能力 | Agent 场景 | 实现 |
|---|---|---|
| 细粒度审计日志 | EU AI Act 要求可追溯每个决策的数据来源 | 行级审计触发器，自动记录谁（Agent ID）、何时、修改了什么 |
| 数据血缘（Lineage） | Agent 输出错误时追溯训练数据/知识库来源 | 原生血缘追踪，记录每个向量/文档的上下游依赖 |
| 可解释性查询 | 为什么 Agent 检索到这些文档？ | 向量检索返回距离 + 相似度分数 + 索引遍历路径 |
| 数据脱敏 | Agent 处理 PII 时的隐私保护 | 列级动态脱敏（DLP），根据调用者身份自动 masking |
| 多租户 RLS | 不同客户的 Agent 数据隔离 | 行级安全策略 + 策略的谓词下推优化 |

---

## 9. 现实情况

上述能力如果全部实现，数据库的代码复杂度将呈指数级增长。

### 方案对比

| 方案 | 覆盖能力 | 缺口 |
|---|---|---|
| PostgreSQL + pgvector + pgai + ACID + MCP | 关系+向量+全文 | 图模型、原生列存、Serverless 分支、自动量化 |
| Neo4j + GDS | 图+向量 | ACID 事务弱、水平扩展难、无原生 MCP |
| Milvus / Zilliz | 向量+混合检索 | 无关系事务、无图模型、无 MCP |
| Databricks Delta Lake | 列存+AI+流 | 毫秒级 OLTP 弱、无向量索引原生优化 |
| TiDB / CockroachDB | 分布式 ACID+HTAP | 向量检索弱、无 AI 原生扩展 |

### 最可能的演进路径

1. **PostgreSQL 继续吞噬**：通过扩展生态（pgvector → pgvectorscale → pgai → 未来的 pggraph）逐步覆盖更多能力。
2. **专用数据库融合**：VectorChord（EDB）在 pgvector 基础上补强十亿级向量；Neon 通过分支架构补强沙箱能力。
3. **全新架构出现**：基于 DataFusion / Arrow 的 Rust 原生数据库，从第一天就设计为多模态统一引擎——这与你正在探索的"Go 实现 DataFusion 级 SQL 引擎"方向高度相关。

---

## 10. 能力清单

```
□ 多模态数据模型：关系 + 向量 + 文档 + 图 + 时序 + KV
□ 混合检索：向量相似度 + 全文 + 结构化过滤 + 图遍历，单查询完成
□ 多向量空间：单文档多 embedding，加权融合
□ ACID + 长事务 + 分布式事务 + 检查点
□ 事件溯源：仅追加日志 + 物化视图
□ HTAP：行存 OLTP + 列存 OLAP，自动路由
□ 分层存储：NVMe → S3 → Glacier，查询透明
□ Serverless + 计算存储分离 + 秒级分支（COW）
□ 自动向量化：INSERT 即 embed，零 ETL
□ 原生 LLM 推理：数据库内调用模型，批量执行
□ 向量压缩：SQ/PQ，自动策略选择
□ 审计血缘：行级日志 + 数据血缘 + 可解释性检索
□ MCP / LangGraph / OpenAPI 原生协议支持
□ 细粒度安全：RLS + 动态脱敏 + 多租户配额
```

---

## 11. 对技术探索的启示

> 你正在研究的"纯 Go 向量数据库"和"DataFusion 级 SQL 引擎"，如果能在架构设计阶段就将向量索引与关系执行器、列存扫描、事务管理器放在同一进程内（而非插件式拼接），就有机会比 Postgres 的扩展架构更接近这个"单一数据库"的理想形态。Postgres 的进程模型（backend per connection）和扩展 API 限制，恰恰是其向这个终极目标演进时的结构性约束。

---

## 12. 与当前 `pg_rust` 规划的关键对照（整理者注）

| PDF 原始要求 | `pg_rust` 规划中的处理方式 |
|---|---|
| 基于 DataFusion/Arrow 的 Rust 原生多模态统一引擎 | ✅ 项目语言已确定为 Rust，统一自研引擎 |
| HTAP（行存 OLTP + 列存 OLAP） | ✅ Phase 2 进统一引擎，与 TP 共享 WAL/MVCC/LSN |
| 图模型 | ✅ Phase 2+ 内核原生支持 |
| MCP Server | ✅ Phase 1b 落地 |
| 10万+ TPS / P99<50ms / 5000万向量 | ⚠️ 作为方向性参考，具体量化目标随 benchmark 逐步确定 |
| 内置 LLM 推理 | ❌ 明确不做，LLM 始终外置 |
| 独立 OLAP 数仓 | ❌ 明确不做，HTAP 由统一引擎自带 |
