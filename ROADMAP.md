# pg_rust 开发 Roadmap

> 详细设计原则与纪律见文末"设计原则与纪律"节（共 10 条）。

---

## 阶段总览

```
Phase 1：存储基座 + 行存 + 事务 + B+Tree
         ├── M1: 物理层（Page/WAL/BufferPool）
         ├── M2: 行存 + MVCC + B+Tree + 崩溃恢复
         ├── M3: 基础 Vacuum + 可观测性 + 简单连接协议
         └── 预留: Tier 2 框架接口

Phase 2：HNSW 向量索引
         ├── VECTOR 类型 + HNSW AM
         ├── Tier 1 同步验证
         ├── HNSW 的 Vacuum 扩展
         └── 对标 benchmark vs pgvector

Phase 3：全文倒排索引
         ├── 倒排 AM + BM25
         ├── Tier 2 完整落地（第一个异步索引）
         ├── Segment merge + GC
         └── 对标 benchmark vs Tantivy

Phase 4a：SQL 层 + 基础协议
         ├── DataFusion 集成 + 自定义 TableProvider
         ├── PG Wire Protocol 基础版（Simple Query 模式）
         ├── 基础 EXPLAIN + JSON 执行计划
         └── 单 AM 路径的端到端 CRUD 验证

Phase 4b：Multi-Path Fusion + 跨模态 Planner
         ├── MultiIndexScan 算子 + Fusion 策略（filter / RRF / hybrid）
         ├── 跨模态 Planner（初版，启发式 Cost Model）
         └── **核心 demo：一条 SQL 混合语义+关键词+结构化召回**

Phase 5：时序 + 列存投影原型
         ├── 时序 AM（分区 + TTL + 降采样）
         ├── 多 AM 统一 GC 协调器
         ├── 时序参与 Fusion
         └── 列存投影原型（验证 HTAP 架构可行性）
         ──────
         （记忆生命周期：遗忘/蒸馏/分层 → 独立项目 pg_rust-agent-sdk）

Phase 6：图索引 + 完整协议 + MCP
         ├── 图 AM（邻接表 + 多跳遍历）
         ├── 完整 PG Wire Protocol（Extended Query + 预处理语句 + 类型映射）
         ├── MCP Server
         └── RLS / 多 Agent 隔离 / 配额

Phase 7：生产化（拆 7a 性能 / 7b 完整特性）
         ├── 7a：列存投影生产版 + 向量压缩 + SIMD/io_uring
         └── 7b：完整 CBO + 备份恢复 + WAL Shipping + Jepsen + 监控
```

---

## Phase × Layer 映射

每个 Phase 实现的层次清晰可追溯，避免"不知不觉越界"。L1 从 M1 建立、L2 从 M2 起步、L3 严格按 HNSW → FT → TS → Graph → Columnar 递进。

| Phase | Layer 1（物理） | Layer 2（事务/可见性） | Layer 3（Access Methods） | 跨层能力 |
|---|---|---|---|---|
| Phase 1 M1 | ✓ Page/WAL/BufferPool/LSN/Checkpoint | — | — | File Manager |
| Phase 1 M2 | (扩展) Full Page Image | ✓ MVCC / Lock / Snapshot / Visibility | ✓ B+Tree（含 AccessMethod trait） | ARIES 崩溃恢复 |
| Phase 1 M3 | — | (扩展) Vacuum | — | gRPC 协议 + 可观测 |
| Phase 2 | (Tier 1 异步 IO) | (扩展) Per-tx delta | ✓ HNSW（Epoch + 节点锁） | HNSW Vacuum |
| Phase 3 | (Tier 2 异步 IO) | (扩展) Watermark | ✓ Inverted Index（BM25） | Segment merge |
| Phase 4a | — | (扩展) Cost hooks | (单路选择) | DataFusion + PG Wire 基础版 + EXPLAIN |
| Phase 4b | — | — | ✓ MultiIndexScan + Fusion | 跨模态 Planner（启发式） |
| Phase 5 | — | (扩展) Multi-AM GC 协调 | ✓ TimeSeries + Graph（轻量） | Fusion 接入时序 + 图 + 列存投影原型 |
| Phase 6 | — | (扩展) RLS predicate | (稳定) | MCP Server + 完整 PG Wire |
| Phase 7a | (性能优化) | — | ✓ Columnar Projection | SIMD / io_uring / 大页 |
| Phase 7b | — | ✓ 完整 CBO + 统计 | (压缩) SQ/PQ | 备份恢复 + 监控 |

---

## 阶段依赖关系

```
Phase 1 ──→ Phase 2 ──→ Phase 3 ──→ Phase 4a ──→ Phase 4b ──→ Phase 5 ──→ Phase 6 ──→ Phase 7
(基座+行存)  (向量)    (全文)    (SQL+协议)  (融合+Planner) (时序+列存) (图+完整协议) (生产化)
                                                                    
注意：记忆生命周期（遗忘/蒸馏/分层）已剥离到独立项目 pg_rust-agent-sdk，
      不在主 roadmap 7 阶段内。pg_rust 内核只提供原语（TTL/降采样/删除/插入），
      策略由 SDK 实现。
```

---

## Phase 1：存储基座 + 行存 + 事务 + B+Tree

**目标：最小可用数据库，Agent 可以存取结构化元数据**

### Milestone 1：物理层

| 模块 | 说明 |
|------|------|
| Page Allocator | 固定大小页分配/释放（8KB/16KB/64KB 可配置），freelist 管理，不假设页内容 |
| WAL Writer | append-only 日志，接受 (record_type, payload)，fsync 语义，CRC32 校验，支持物理+逻辑两种记录类型 |
| Buffer Pool | page_id → in-memory frame 映射，LRU/CLOCK 替换，pin/unpin 协议，WAL 先行规则（刷页前确保相关 WAL 已持久化） |
| LSN Clock | 全局单调递增，所有组件共享 |
| File Manager | 数据文件、WAL 文件、元数据文件的统一管理，支持 O_DIRECT 可选 |

**验证标准：**
- WAL 顺序写吞吐 ≥ 500MB/s
- Buffer Pool 随机读 ≥ 100K ops/s
- 单元测试覆盖率 ≥ 90%
- 模糊测试（proptest）验证 Page Allocator 不泄漏不重叠

### Milestone 2：行存 + 事务 + B+Tree + 崩溃恢复

| 模块 | 说明 |
|------|------|
| Heap Storage | Slotted page 行存，支持变长字段，TOAST 溢出页（大对象/向量/JSONB 不放主行） |
| Tuple 格式 | 胖 header（xmin, xmax, agent_id, trace_id, flags）+ 定长标量列 + 列指针 |
| Transaction Manager | begin/commit/abort，事务 ID 分配，事务状态表（CSN-based） |
| Snapshot | 快照获取（SI：事务开始时固定；RC：每条语句新快照） |
| Visibility Oracle | is_visible(xmin, xmax, snapshot) 统一判断，所有 AM 共享 |
| Lock Manager | 行级锁（S/X），意向锁（IS/IX），等待队列 + 死锁检测（wait-for graph，100ms 周期） |
| B+Tree Index | Latch coupling 读，乐观/悲观插入，叶子页分裂，实现 AccessMethod trait |
| 崩溃恢复 | 完整 ARIES 变体：Analysis → Redo → Undo，CLR 保证嵌套崩溃安全 |
| Checkpoint | Fuzzy Checkpoint：收集 ATT+DPT，后台刷脏页，更新超级块 |
| Full Page Image | 每个 checkpoint 周期内页首次修改时记录完整页副本，防止 torn page |

**验证标准：**
- ACID 正确性：并发读写 + 随机 kill 进程，重启后数据始终一致
- 能跑简单的 Agent 元数据存取（session 记录、用户画像、配置）
- 并发性能：100 并发连接下 TPS ≥ 10K（简单 CRUD）

### Milestone 3：基础 Vacuum + 可观测性 + 连接协议

| 模块 | 说明 |
|------|------|
| Vacuum | 扫描死元组（xmax 已提交且无活跃快照引用），回收空间，通知 B+Tree 清理对应条目 |
| 可观测性 | WAL dump 工具（人类可读）、活跃事务列表查询、锁等待关系查询、Buffer Pool 命中率统计 |
| 连接协议 | 简单的 gRPC/HTTP API，支持 CRUD 操作，Agent 团队可通过 Python/TypeScript 调用 |
| Tier 2 接口预留 | WAL tail reader 接口、watermark registry 接口、planner 可感知索引新鲜度的 hook |

**验证标准：**
- Vacuum 后空间可被复用，无无限膨胀
- Agent 团队能通过 Python 客户端完成基本数据操作
- 可通过命令行工具诊断事务和锁问题

### 对 Agent 团队的价值

- 替代 SQLite/PG 存储 Agent 的结构化元数据
- session_id、agent_id、timestamp 等字段原生支持
- 行级 provenance（谁写的、什么时候写的）
- Python/TS 客户端直接可用

---

## Phase 2：HNSW 向量索引

**目标：支持向量存储与近邻检索，Agent 可以做语义记忆召回**

### 交付物

| 模块 | 说明 |
|------|------|
| VECTOR(n) 类型 | 一等公民数据类型，DDL 级声明，支持 f32/f16/bf16 |
| HNSW Access Method | 实现 AccessMethod trait，包括 insert/delete/begin_scan/next/wal_record_types/redo/undo/dirty_pages/reclaim_tid |
| 图构建 | 分层随机图，M/M_max 参数，邻居选择策略（simple / robust prune） |
| 图搜索 | 贪心遍历，ef_search 参数控制精度-延迟权衡 |
| 并发控制 | Epoch-based reclamation + 节点级细粒度锁（非页级） |
| WAL 集成 | 逻辑 WAL 记录（add_node / connect_neighbors / remove_node），幂等 redo |
| 崩溃恢复 | Epoch Snapshot（checkpoint 时保存图元数据快照）+ 增量重放 + 一致性验证修复 |
| Tier 1 同步 | 事务内累积 delta，commit 时批量合并到 HNSW 图 |
| 可见性适配 | 搜索时多取 2x 候选，回表做 visibility 过滤后返回 top-K |
| 距离函数 | L2 / Cosine / Inner Product，SIMD 加速（SSE4.2 / AVX2 / NEON） |
| Vacuum 扩展 | Tombstone 节点的后台清理，局部图修复（补充邻居连通性） |

### API 示例

```
// 通过连接协议调用
INSERT INTO memories (id, content, embedding) VALUES ('m1', 'hello', [0.1, 0.2, ...])
SEARCH memories BY embedding NEAR [0.3, 0.4, ...] LIMIT 10
SEARCH memories BY embedding NEAR [0.3, 0.4, ...] WHERE agent_id = 'a1' LIMIT 10
```

### 验证标准

- 召回率：HNSW recall@10 ≥ 95%（sift-128-euclidean, gist-960-euclidean）
- 正确性：并发插入 + 删除 + 随机崩溃，图结构始终一致（双向边对称、连通性）
- 性能：1M 768d 向量，搜索延迟 < 10ms（ef_search=64）
- 对标：与 pgvector (HNSW) 和 Qdrant 对比 recall/latency/throughput

### 对 Agent 团队的价值

- Agent 对话/文档的 embedding 存储与语义检索
- 与结构化过滤组合（WHERE team='sales' 先 B+Tree 过滤，再向量排序）
- 记忆的语义去重（近似向量检测）
- 单一事务保证：插入一条记忆 + 对应向量，要么同时成功要么同时失败

---

## Phase 3：全文倒排索引

**目标：支持 BM25 全文检索，Agent 可以做关键词记忆召回**

### 交付物

| 模块 | 说明 |
|------|------|
| 全文索引声明 | WITH (fulltext_index = 'bm25') DDL 级声明 |
| Inverted Index AM | 实现 AccessMethod trait，segment-based 架构 |
| 分词器 | 中文（jieba-rs）+ 英文（stemming + stop words），可插拔 |
| Posting List | 每个 term 对应一个有序 TID 列表 + term frequency + 位置信息 |
| BM25 评分 | 标准 BM25 公式（k1=1.2, b=0.75），支持 WAND 加速 |
| Segment 架构 | 写入缓冲（内存）→ flush 为不可变 segment → 后台 merge |
| Tier 2 完整落地 | 后台 worker 消费 WAL 流，维护 applied_lsn，planner 感知 watermark |
| Watermark 机制 | planner 查询时检查倒排索引的 applied_lsn 是否覆盖查询 snapshot |
| Fallback 策略 | watermark 落后时，seqscan 补全缺失范围的结果 |
| Segment Merge | 后台合并小 segment，清理已删除的 posting 条目 |
| Vacuum 扩展 | segment merge 时物理清理已删除 TID 的 posting |
| 崩溃恢复 | 丢弃未 flush 的 write buffer，从 applied_lsn 继续追赶 |

### API 示例

```
SEARCH memories BY fulltext MATCH 'contract dispute' LIMIT 10
SEARCH memories BY fulltext MATCH 'contract' WHERE created_at > '2026-06-01' LIMIT 10
```

### 验证标准

- 检索质量：标准 IR 数据集 NDCG@10 对齐 Tantivy 水平
- Tier 2 延迟：写入后 < 1 秒可检索到（正常负载下）
- 崩溃恢复：随机 kill 后从 applied_lsn 正确追赶，不丢不重
- 对标：与 Tantivy / Elasticsearch 对比检索质量和延迟

### 对 Agent 团队的价值

- Agent 记忆的关键词检索（"上次讨论过合同纠纷"）
- 与向量检索互补（向量找语义相似，全文找精确关键词）
- 无需外接 ES，减少运维复杂度

---

## Phase 4a：SQL 层 + 基础协议

**目标：让 pg_rust 真正能"用 psql 跑 SQL"，打通用 SQL 客户端接入的通道**

### 交付物

| 模块 | 说明 |
|------|------|
| DataFusion 集成 | SQL 解析、逻辑计划生成、物理计划执行，自定义 TableProvider 对接 pg_rust 存储 |
| PG Wire Protocol 基础版 | 支持 psql 连接，Simple Query 模式，SELECT/INSERT/UPDATE/DELETE |
| 基础 EXPLAIN | JSON 执行计划 + 树形文本格式，planner / executor 耗时可见 |
| 单 AM 端到端 | B+Tree（必走） + 任一 AM（HNSW 或倒排）的 SQL 端到端验证 |
| 元数据表 | pg_class / pg_attribute / pg_index 系统表可读，psql \d 可用 |

### 不做

- ❌ MultiIndexScan / Fusion 算子（放到 4b）
- ❌ 复杂 Cost Model / 跨模态 Planner（放到 4b）
- ❌ Extended Query 模式 / 预处理语句 / 类型映射（放到 Phase 6）

### 验证标准

- psql 可连接，\d 查看表结构，CRUD 端到端 OK
- 单 AM 路径 EXPLAIN 输出与实际执行一致
- 主流 PG 驱动（psycopg2、rust-postgres）的 Simple Query 路径可用

### 对 Agent 团队的价值

- Agent 团队可以用 psql / SQLAlchemy / Prisma 等标准客户端直连
- 解决"协议入口"问题，不再被客户端生态排除在外

---

## Phase 4b：Multi-Path Fusion + 跨模态 Planner

**目标：单条 SQL 同时走多个索引并融合结果 — pg_rust 的核心差异化能力**

### 交付物

| 模块 | 说明 |
|------|------|
| MultiIndexScan 算子 | 并行启动多个 AM scan，按配置的 fusion strategy 合并 TID，统一做 visibility check，回表取完整行 |
| Fusion 策略 | 过滤式（A∩B∩C）、RRF 排序式（Reciprocal Rank Fusion）、混合式（硬过滤+软排序） |
| 跨模态 Planner（初版，启发式） | 识别查询中的多模态谓词，自动拆分为多条 AM scan 路径，选择 fusion 策略 |
| 基础 Cost Model（启发式） | 各 AM 的选择性估计（B+Tree 基于直方图，HNSW 基于距离分布，倒排基于 term frequency） |
| EXPLAIN 多路 | 显示多路扫描计划 + 各路径代价估算 + fusion 策略 + 估算 vs 实际行数 |

### 4a 与 4b 的关键边界

- **4a 只做"单 AM 路径的 SQL 端到端"**：1 条 SQL → 1 个 AM → HeapFetch，验证协议 + TableProvider + EXPLAIN 链路通畅。
- **4b 才做"多 AM 并行 + 融合"**：核心 demo 验证融合策略的正确性、性能和召回质量。
- 拆分原因：单阶段做两件事工期过重；4a 完成后 Agent 团队能先用 psql 干活，4b 不阻塞早期使用。

### 示例查询

```sql
SELECT * FROM agent_memory
WHERE team = 'sales'                    -- B+Tree 路径
  AND content @@ 'contract'             -- 倒排路径
ORDER BY embedding <=> $vec             -- HNSW 路径
LIMIT 10;
```

执行计划：
```
MultiIndexScan (fusion=hybrid, hard_filter=[btree, inverted], soft_rank=[hnsw])
├── BTreeScan (team = 'sales') → TID set A
├── InvertedScan (content @@ 'contract') → TID set B
├── HnswScan (embedding <=> $vec, ef=128) → TID set C (ranked)
├── Filter: A ∩ B
├── Rank: filtered set by HNSW score from C
├── VisibilityCheck
└── HeapFetch → Top 10 rows
```

### 验证标准

- 正确性：融合结果与"分别查询再应用层合并"结果一致（100% 符合）
- 性能：多路并行 < 各路串行之和的 60%
- 质量：RRF 融合的 NDCG 优于单路最佳
- 对标：与"PG + pgvector + pg_search 分别查询 + 应用层合并"对比端到端延迟

### 对 Agent 团队的价值

- **核心价值：** Agent 一次"回忆"调用 = 一条 SQL，同时利用语义+关键词+结构化
- 大幅简化 Agent 记忆召回的应用层代码（不再需要调用三个服务再合并）
- 事务保证：混合检索的结果是快照一致的
- **这是对外推广时最强的 demo 场景**

---

## Phase 5：时序 AM + 列存投影原型

**目标：支持时间维度的查询，验证 HTAP（行+列统一引擎）架构可行性**

> **重要边界**：记忆生命周期（遗忘/蒸馏/分层）已剥离到独立项目 `pg_rust-agent-sdk`，不在本 Phase 内。
> pg_rust 内核只提供原语（TTL/降采样/删除/插入），策略由 SDK 调用。

### 交付物

| 模块 | 说明 |
|------|------|
| TimeSeries AM | 时间分区存储（按天/小时自动分区），范围扫描，降采样聚合 |
| TTL 自动过期 | 声明式 TTL（`WITH ts_partition = 'day', ttl = '90d'`），后台自动清理过期分区 |
| 时间范围查询 | `WHERE created_at BETWEEN ... AND ...` 自动路由到时序索引 |
| 多 AM GC 协调器 | 统一的 Vacuum 协调：基于 oldest_active_snapshot 推进，回收死元组时通知所有引用该 TID 的索引 |
| 时序参与 Fusion | 时序索引加入 MultiIndexScan，支持"最近 7 天 + 语义相似 + 关键词匹配"组合 |
| 列存投影原型 | 时序/记忆分析场景的轻量列存物化视图，验证 HTAP 架构可行性 |

### 不做（外移到 pg_rust-agent-sdk）

- ❌ 记忆遗忘曲线算法（基于访问频率 + 时间衰减的重要性评分）
- ❌ 记忆蒸馏（外部 LLM 总结多条细节记忆）
- ❌ 记忆分层（短期 / 工作 / 长期 + 自动晋升/淘汰）
- ❌ Agent 元数据自动维护（agent_id/trace_id/session_id 关联图）

**理由**：pg_rust0706.pdf §3.6 自述"LLM 永远作为外部服务，不进入事务提交路径"——意味着这些都不是内核该做的事。内核提供原语，策略由 SDK 实现，避免 LLM 推理影响 ACID 延迟。

### 验证标准

- 时序查询性能：100M 时间点，范围查询延迟 < 5ms
- TTL 正确性：过期数据在后台清理后不可查询，空间可回收
- GC 协调：多 AM 环境下无 TID 泄漏，无悬挂索引条目
- 列存投影原型：在 10M 行测试集上做 1-2 个聚合查询，验证路径可走通（性能不作为验收标准）

---

## Phase 6：图索引 + 完整协议层

**目标：支持知识图谱式记忆关联，完整的对外服务能力**

### 交付物

| 模块 | 说明 |
|------|------|
| Graph AM | 邻接表存储，支持有向/无向边，边属性（JSONB） |
| 多跳遍历 | BFS/DFS，深度限制，边过滤，路径返回 |
| 图查询语法 | SQL 扩展（类 Cypher 子句或 LATERAL 递归），支持 MATCH 模式匹配 |
| 并发控制 | 有序节点锁（min(src, dst) 先锁），读遍历无锁（快照一致） |
| 图参与 Fusion | "与用户 A 相关的人 → 这些人的记忆中语义相似的" — 图+向量联合 |
| 完整 PG Wire Protocol | Extended Query 模式、Prepared Statement、参数绑定、类型映射 |
| MCP Server | 原生 MCP 工具接口，记忆读写/检索/管理直接暴露为 MCP 能力 |
| RLS（Row Level Security） | Agent 级别的行级安全策略，多 Agent 数据隔离 |
| 多租户配额 | Agent 维度的存储/查询配额限制 |

### 验证标准

- 图遍历：百万边规模，3 跳遍历 < 50ms
- PG 协议兼容性：主流 PG 驱动（psycopg2、node-postgres、rust-postgres）可正常连接
- MCP 集成：Claude Code / Dify 可通过 MCP 直接使用记忆能力
- RLS 正确性：Agent A 无法读取 Agent B 的数据

### 对 Agent 团队的价值

- "用户 A 上周投诉了物流 → 关联到订单 X → 关联到处理人 Y" — 图遍历
- 多 Agent 协作场景：共享记忆但有权限边界
- 对外推广：标准接口，第三方 Agent 框架可直接集成

---

## Phase 7：生产化（拆 7a 性能 / 7b 完整特性）

**目标：达到可对外推广的生产质量**

### Phase 7a：性能优化

| 模块 | 说明 |
|------|------|
| 列存投影（生产版） | AP 场景的列式物化视图，Tier 2 异步维护，向量化扫描，承接 Phase 5 原型 |
| 向量压缩 | Scalar Quantization (SQ) / Product Quantization (PQ)，自动策略选择 |
| SIMD 全面优化 | 距离计算 / 字符串比较 / CRC 校验全链路 SIMD 化 |
| io_uring（Linux） | 高并发 IO 路径，替代部分 tokio file IO |
| 大页内存 | 减少 buffer pool TLB miss，提升大内存场景吞吐 |
| 连接池优化 | pg_bouncer 风格的多客户端复用，避免每连接 1 OS 线程 |

### Phase 7b：完整特性 + 生产化能力

| 模块 | 说明 |
|------|------|
| 完整 CBO | 跨模态成本模型（统计信息收集 + 代价估算 + 计划选择），EXPLAIN 可解释 |
| 备份恢复 | 物理备份（快照）+ 逻辑备份（导出/导入）+ PITR（基于 WAL） |
| WAL Shipping | 主从复制基础，用于高可用 |
| Jepsen 测试 | 分布式/并发正确性的业界标准测试框架 |
| 监控体系 | Prometheus metrics 导出、慢查询日志、查询热力图 |
| 文档与 SDK | 完整 API 文档、Python/TypeScript/Go SDK、示例项目 |

### 验证标准

- Jepsen 测试通过（事务隔离 + 崩溃恢复）
- 7x24 稳定运行测试（72 小时高并发压测无 OOM/死锁/数据丢失）
- 对标综合 benchmark：Agent 记忆场景端到端性能 ≥ PG+pgvector+ES 方案的 80%

---

## 里程碑与 Agent 团队对接节点

| 完成阶段 | Agent 团队可开始使用的能力 | 替代的外部组件 |
|----------|------------------------|--------------|
| Phase 1 | 结构化元数据存取（Python/TS 客户端） | SQLite / PG（基础 CRUD） |
| Phase 2 | 语义记忆检索（向量近邻搜索） | pgvector / Qdrant |
| Phase 3 | 关键词记忆检索（BM25 全文） | Elasticsearch / Meilisearch |
| Phase 4a | 用 psql / SQLAlchemy / Prisma 等标准 PG 客户端连接 | 协议入口层 |
| Phase 4b | **一条 SQL 完成混合召回**（核心 demo） | 应用层多服务拼装 |
| Phase 5 | 时序查询 + TTL + 多 AM GC + 列存投影原型 | TimescaleDB（基础时序） |
| Phase 6 | 标准协议对外服务 + 多 Agent 隔离 | — |
| Phase 7 | 生产级部署 | 整套多引擎架构 |
| （独立项目）`pg_rust-agent-sdk` | 记忆遗忘 / 蒸馏 / 分层 | 自研遗忘/蒸馏逻辑 |

---

## 每阶段的 Benchmark 对标策略

| 阶段 | 对标对象 | 测试场景 | 达标线 |
|------|---------|---------|--------|
| Phase 1 | SQLite / PG | 纯 TP（CRUD 混合负载） | ≥ SQLite 性能的 50% |
| Phase 2 | pgvector / Qdrant | 纯向量检索（ANN benchmark） | recall@10 ≥ 95%, latency 差距 < 2x |
| Phase 3 | Tantivy / ES | 纯全文检索（标准 IR 数据集） | NDCG@10 对齐，latency 差距 < 3x |
| Phase 4a | psql + PG | 端到端 SQL 协议 + 单 AM 路径 | 协议兼容 + 延迟 < 1.5x PG |
| Phase 4b | PG+pgvector+ES 拼装 | 混合检索端到端 | 总延迟 < 拼装方案的 50% |
| Phase 5 | TimescaleDB | 时序范围查询 | latency 差距 < 2x |
| Phase 6 | Neo4j | 图遍历（3 跳） | latency 差距 < 3x |

---

## 风险登记

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Phase 1 事务/恢复正确性不足 | 全局阻塞 | 早期引入 proptest + 随机崩溃测试，不赶进度 |
| HNSW 并发 bug 难以复现 | Phase 2 延期 | Loom 并发模型检查 + 大量 stress test |
| DataFusion 不适合 TP 路径 | Phase 4 架构调整 | Phase 4 前调研 DataFusion 的 TP 适配性，必要时 TP 路径绕过 DataFusion |
| Tier 2 watermark 导致查询降级 | Agent 体验差 | 监控 watermark lag，异常时自动提速 worker |
| 单人/小团队工程量过大 | 整体延期 | 严格 MVP 思维，每个 Phase 只做最小必要集 |
| HNSW undo 的 tombstone 累积 | 搜索精度下降 | Vacuum 中加入 tombstone 比例监控，超过阈值触发局部重建 |

---

## 显式战略边界：HTAP / CoW / Serverless / 分布式

> 旧 planning.md 写过 CoW 快照作为 Phase 2+ 目标、HTAP 作为 Phase 2、Serverless 作为 Phase 2+ 长期目标。新 ROADMAP 一度不提会留下"模糊空间"。这里显式标"做/不做/推迟"。

| 能力 | 处理 | 阶段 | 备注 |
|---|---|---|---|
| **HTAP**（行+列统一引擎） | **做（原型 + 生产）** | Phase 5（列存投影原型）+ Phase 7a（生产版） | 满足"统一引擎"差异化定位；OLAP 通过列存投影覆盖 |
| **CoW 快照**（基础） | **做（极简）** | Phase 7b（备份恢复中实现） | 用作快照备份，不展开为通用特性 |
| **CoW 秒级分支**（Agent 沙箱） | **推迟** | 不在 7 阶段 roadmap 中 | 当前 Agent 场景不刚需；用户级分支可在 Agent SDK 层用 PostgreSQL 风格 fork |
| **Serverless / 计算存储分离** | **不做** | 永久不做（单机路线） | 战略选择：聚焦内核质量，不与 Neon 竞争 |
| **跨地域分布式事务** | **不做** | 永久不做（单机路线） | 同上 |
| **多租户 RLS** | **做** | Phase 6 | 多 Agent 隔离刚需 |
| **多租户配额** | **做** | Phase 6 | 同上 |
| **动态脱敏（DLP）** | **推迟** | 评估 | Phase 6 之后看需求 |
| **SSI（Serializable Snapshot Isolation）** | **可选** | Phase 7b | 默认 RC，复杂多 Agent 竞争场景再上 |
| **内置 LLM / 在线推理** | **不做** | 永久不做 | LLM 永远作为外部服务，不进入事务提交路径 |

**理由**：项目画像（"面向 AI Agent 的统一数据平台"）决定了 Serverless/分布式不在必做范围；HTAP 保留但推到生产化阶段；CoW 限缩到备份恢复场景。

---

## 十、开放问题

> 来自设计阶段的自我批评 + 已知的未完成设计。每个 Phase 启动前都过一遍"是否已解决"。

### 来自设计阶段自我批评（pg_rust0706.pdf §13）

1. **回表开销被低估**：图遍历多跳每步都回表做可见性检查，延迟指数放大；FT posting 含百万 TID 不能逐一回表。缓解：HNSW 多取 2x 候选、Phase 2 末升级 TID+XID 索引条目、posting 段级 snapshot 过滤。

2. **HNSW 并发控制缺失**：HNSW 图结构的并发修改是已知难题，undo() 对图意味着"删除节点+修复所有邻居"可能破坏连通性。缓解：单写者 HNSW（Phase 2 末）+ Epoch-based reclamation（Phase 2 中）+ 节点级细粒度锁（Phase 2）；参考 Vamana/DiskANN 无锁设计（Phase 1a 末研究任务）。

3. **列存投影 Tier 2 一致性窗口**：AP 查询若命中 Tier 2 投影，存在秒~分钟级滞后；fallback 到 seqscan 等于退化。缓解：精细增量合并（类似 Delta Lake Z-Order merge），watermark-aware planner 避免退化路径。

4. **跨模态 CBO 是"画饼"最重的部分**：无数学公式、无统计信息收集方案、HNSW 代价高度依赖数据分布。缓解：Phase 4b 用启发式策略选择，Phase 7b 再做完整 CBO；统计信息维护按 AM 单独设计。

5. **Multi-Path Fusion 正确性问题**：RRF 在候选集不重叠时退化为 union；融合策略自动选择没明确算法。缓解：Phase 4b 显式拆分"过滤式"（实）和"RRF"（虚）两个子阶段；可解释 EXPLAIN 让用户能 override。

### TODO（pg_rust0706.pdf §17-19 中标记为未完成）

6. **HNSW 并发协议**（参考 Vamana/DiskANN 无锁设计）：尚未设计完成，Phase 2 启动前需补齐方案或选择"单写者简化路径"。

7. **增量 Vacuum 协议**（6 种 AM 的 GC watermark 推进机制）：尚未设计完成，Phase 5 多 AM GC 协调器实现前需补齐。

---

## 跨文档交叉引用

> 7 份核心文档（planning / architecture / unified-kernel / multimodal / kernel-expert / rewrite-postgres / ROADMAP）现在有内容重叠和编号不一致，本节是它们的"导航图"。

| 文档 | 定位 | 与本 ROADMAP 的关系 |
|---|---|---|
| `planning.md` | 战略计划 + 决策 + 边界 | 本 ROADMAP 是其执行展开 |
| `agent-native-db-architecture.md` | 架构设计概念（子系统、4 大范式融合、典型数据流） | 本 ROADMAP 是其阶段化（§6 给出组件 → 阶段映射） |
| `unified-kernel-design.md` | L1/L2/L3 三层分离骨架（物理/事务/AM） | 本 ROADMAP「Phase × Layer 映射」是其阶段化映射表 |
| `architecture-multimodal-unified-kernel.md` | 多模态统一内核的 7 个设计决策 | **姊妹篇**——以"决策"角度切入（解耦存储/AM、单 WAL、跨模态 CBO、混合执行器等），与 unified-kernel-design 互补 |
| `rust-rewrite-postgres-agent-native-2026-06-10.md` | 早期架构草图（3 层策略：直接用现成 / 重点自研 3 个 / 渐进 PG 兼容） | **早期 draft**——其中"Layer 1/2/3"指"复用程度分层"，与 `unified-kernel-design.md` 的 L1/L2/L3（物理/事务/AM）命名冲突；以本文 L1/L2/L3 为准 |
| `database_kernel_expert_in_ai_era.md` | "老中医"在 AI 时代价值的 meta 分析 | **定位/团队 doc**——论证 DB 内核专家为什么仍稀缺，为团队组建和招聘提供依据 |
| `pg_rust0706.pdf` | 完整设计 RFC（8 阶段、9 创新、5 风险、2 TODO） | 本 ROADMAP 是其执行抽取，§十 开放问题源自此 PDF |
| `ROADMAP-changes.md` | 本 ROADMAP 的改动清单 | 已合并到当前 ROADMAP.md；保留作为可追溯记录 |

**关键 cross-ref 锚点**：
- 战略边界（HTAP/CoW/Serverless）→ `planning.md` §六 关键决策建议
- 三层分离骨架 → `unified-kernel-design.md` §二（注意 L1/L2/L3 是物理/事务/AM）
- 多模态 7 个设计决策 → `architecture-multimodal-unified-kernel.md` §二
- 4 大工作负载范式 → `agent-native-db-architecture.md` §3 范式融合
- 9 项架构创新 → `pg_rust0706.pdf` §6
- Phase 4 拆 4a/4b 理由 → `ROADMAP-changes.md` §五
- AI 编码 vs 内核专家 → `database_kernel_expert_in_ai_era.md`（论证为什么 pg_rust 仍需"老中医"）

---

## 技术选型

| 组件 | 选择 | 理由 |
|------|------|------|
| 语言 | Rust | 内存安全、零成本抽象、SIMD 友好、async 生态成熟 |
| 异步运行时 | tokio | 业界标准，DataFusion 依赖 |
| SQL 引擎 | DataFusion（Phase 4a 引入） | Arrow 生态、可插拔、AP 向量化执行 |
| 中文分词 | jieba-rs | 轻量、无外部依赖 |
| 序列化 | bincode / postcard | 零拷贝、紧凑 |
| 测试框架 | proptest + loom + 自研崩溃注入 | 覆盖正确性、并发、崩溃三个维度 |
| 连接协议 | gRPC（Phase 1 M3）→ PG Wire（Phase 4a 基础 + Phase 6 完整） | 渐进式，先快速可用再兼容 |

---

## 设计原则与纪律

> 合并自原"设计原则" + "原则与纪律"两节，去重后共 10 条。

1. **每阶段可交付** — 每阶段结束时内部 Agent 团队能用上 psql / gRPC / MCP 客户端
2. **严格分层构建** — Layer 1 → Layer 2 → Layer 3，地基不稳上层必塌
3. **AM 按优先级递进** — HNSW → FT → TS → Graph → Columnar，不追求一次性六种全做
4. **每加 AM 必带 Vacuum/GC** — 不留技术债到最后
5. **正确性 > 性能 > 功能** — 宁可少一个功能，不可有一个 bug
6. **AM 边界守得住** — 内核提供原语（TTL/降采样/删除），策略由 SDK 实现；LLM 永远外置
7. **内部先吃狗粮** — 每阶段必须实际使用，反馈驱动优先级
8. **不跳阶段** — 每阶段验证标准全部通过才进入下一阶段
9. **不过度设计** — 每阶段只实现当前需要的最小集，接口可扩展但实现不提前
10. **每阶段一篇技术博客** — 为对外推广积累素材
