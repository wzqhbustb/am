# ROADMAP.md 修改建议清单

> 基于工程合理性 review 的整理，所有改动按 Phase 分组，可直接对照修改。

---

## 一、全局性修改

### 1. 增加总工期估算

**位置**："阶段总览"或"设计原则"之后

**建议添加**：

```markdown
## 总工期估算（粗略）

| Phase | 预估工期 | 累计工期 |
|---|---|---|
| Phase 1 | 6–10 个月 | 6–10 个月 |
| Phase 2 | 3–5 个月 | 9–15 个月 |
| Phase 3 | 2–4 个月 | 11–19 个月 |
| Phase 4 | 4–6 个月 | 15–25 个月 |
| Phase 5 | 3–5 个月 | 18–30 个月 |
| Phase 6 | 3–5 个月 | 21–35 个月 |
| Phase 7 | 4–6 个月 | 25–41 个月 |

> 注：Phase 1 是最关键也是风险最高的阶段，实际工期可能偏向区间上限。
```

**理由**：当前 roadmap 没有总工期，团队无法判断资源投入和里程碑节奏。

---

## 二、Phase 1 修改

### 2.1 Milestone 2 增加内部检查点

**位置**：Phase 1 → Milestone 2 → 验证标准之前

**建议添加一段**：

```markdown
### Milestone 2 内部检查点

为避免一次性实现完整 MVCC/死锁检测/B+Tree 后才暴露架构问题，M2 拆为三个内部检查点：

| 检查点 | 目标 | 验证 |
|---|---|---|
| M2a | 单语句 auto-commit + 堆表 + 无并发 B+Tree | `INSERT` / `SELECT` / `UPDATE` / `DELETE` 单线程正确 |
| M2b | 多语句事务 + MVCC 快照 + 可见性判断 | 并发读写无脏读，RC/SI 快照正确 |
| M2c | 完整锁管理 + 死锁检测 + B+Tree 并发 | 100 并发 CRUD + 随机崩溃测试通过 |
```

**理由**：M2 是完整 OLTP 引擎，风险最高，必须分段验证。

---

### 2.2 验证标准中的 TPS 目标加条件

**位置**：Phase 1 → Milestone 2 → 验证标准

**原文**：

```
并发性能：100 并发连接下 TPS ≥ 10K（简单 CRUD）
```

**改为**：

```
并发性能：100 并发连接下 TPS ≥ 10K（简单 CRUD，单表，SSD，无网络协议开销）
```

**理由**：避免目标在引入网络协议后被误解为不达标。

---

### 2.3 Milestone 3 增加 PG Wire Protocol 简单版（可选）

**位置**：Phase 1 → Milestone 3 → 交付物表格

**建议添加一行**：

| 模块 | 说明 |
|------|------|
| PG Wire Protocol 极简版 | 仅支持 Simple Query + 文本结果格式，让 `psql` / 标准 PG 驱动能连上来跑 `CREATE/INSERT/SELECT` |

**理由**：Agent 生态（LangChain、SQLAlchemy、Prisma）都假设后端是 PG。Phase 1 末尾就有 PG 协议能力，能大幅降低后续集成成本。

---

## 三、Phase 2 修改

### 3.1 放宽 HNSW 崩溃恢复验证标准

**位置**：Phase 2 → 验证标准

**原文**：

```
正确性：并发插入 + 删除 + 随机崩溃，图结构始终一致（双向边对称、连通性）
```

**改为**：

```
正确性：并发插入 + 删除 + 随机崩溃，图结构的语义一致性不受影响
  - 双向边不对称率 < 1%
  - recall@10 不低于崩溃前 95%
  - 无悬挂节点（所有节点可从入口点可达）
```

**理由**：HNSW 是近似索引，"始终一致"过于严格。应关注 recall 和搜索可用性，而不是图结构的绝对对称。

---

### 3.2 SIMD 优化降级为可选交付

**位置**：Phase 2 → 交付物表格 → 距离函数

**原文**：

```
距离函数 | L2 / Cosine / Inner Product，SIMD 加速（SSE4.2 / AVX2 / NEON）
```

**改为**：

```
距离函数 | L2 / Cosine / Inner Product，基础实现正确；SIMD 优化作为 Phase 2 末尾或 Phase 7 的优化项
```

**理由**：SIMD 是性能优化，不应阻塞 Phase 2 的正确性验证。

---

## 四、Phase 3 修改

### 4.1 改进 Tier 2 fallback 策略

**位置**：Phase 3 → 交付物表格 → Fallback 策略

**原文**：

```
Fallback 策略 | watermark 落后时，seqscan 补全缺失范围的结果
```

**改为**：

```
Fallback 策略 | watermark 落后时：
  1. 优先只查询已 flush 的不可变 segment，忽略未 flush 的 write buffer；
  2. 若查询需要最新数据且 segment 覆盖不足，再回退到 seqscan；
  3. 大表场景下 seqscan fallback 必须配合超时/采样限制，避免查询挂死。
```

**理由**：大表 seqscan 不可接受，需要分层 fallback。

---

### 4.2 增加 BM25 统计信息维护说明

**位置**：Phase 3 → 交付物表格 → BM25 评分之后

**建议添加一行**：

| 模块 | 说明 |
|------|------|
| BM25 全局统计 | 维护 `doc_count`、`avgdl`、`term_df`，更新时原子递增；checkpoint 时持久化到元数据文件 |

**理由**：BM25 公式依赖全局统计，必须在架构层明确维护方式。

---

## 五、Phase 4 修改

### 5.1 拆分 Phase 4 为两个子阶段

**位置**：Phase 4 整节

**建议改为**：

```markdown
## Phase 4：SQL 层 + PG Wire Protocol（基础）

**目标：让 pg_rust 能被标准 SQL 和 PG 驱动访问**

### 交付物

| 模块 | 说明 |
|------|------|
| DataFusion 集成 | SQL 解析、逻辑计划、物理计划；自定义 TableProvider 对接 pg_rust 存储 |
| 单路索引扫描 | 查询优化器能为单个谓词选择 B+Tree / HNSW / 倒排索引 |
| PG Wire Protocol 基础版 | 支持 psql 连接，Simple Query + Extended Query（参数绑定），文本/二进制结果 |
| EXPLAIN | 显示单路扫描计划 + 代价估算 |

### 验证标准

- `psql` 能连接并执行 `CREATE/INSERT/SELECT/UPDATE/DELETE`
- DataFusion 能正确下推 filter 到 pg_rust 的 TableProvider
- 主流 PG 驱动（psycopg2、node-postgres、rust-postgres）可正常连接

---

## Phase 5：Multi-Path Fusion + 跨模态 Planner

**目标：单条 SQL 同时走多个索引并融合结果 — pg_rust 的核心差异化能力**

### 交付物

| 模块 | 说明 |
|------|------|
| MultiIndexScan 算子 | 并行启动多个 AM scan，按 fusion strategy 合并 TID，统一 visibility check，回表取行 |
| Fusion 策略 | 过滤式（A∩B∩C）、RRF 排序式、混合式（硬过滤+软排序） |
| 跨模态 Planner（初版） | 识别多模态谓词，自动拆分为多条 AM scan 路径，选择 fusion 策略 |
| 基础 Cost Model | 各 AM 选择性估计：B+Tree 直方图、HNSW 距离分布、倒排 term frequency |
| EXPLAIN 增强 | 显示多路扫描计划 + 各路径代价 + fusion 策略 |

### 示例查询（保持原有）

### 验证标准（保持原有，去掉"PG Wire Protocol 基础版"相关）

### 对 Agent 团队的价值（保持原有）
```

**理由**：Phase 4 原内容过多，DataFusion 集成 + PG Wire + MultiIndexScan + Planner 叠加风险太大。拆分后每阶段目标更聚焦。

---

### 5.2 新增 DataFusion TP 路径验证检查点

**位置**：Phase 4（新的 Phase 4）→ 验证标准

**建议添加**：

```markdown
### DataFusion TP 适配性检查点

在 Phase 4 开始时必须先完成以下验证：

1. DataFusion 能支持简单点查的延迟 < 5ms（单连接）
2. DataFusion 能正确传递事务上下文到 TableProvider
3. 若上述任一项不满足，必须启动"TP 路径绕过 DataFusion"的备用方案

备用方案概要：
- 简单点查/小范围查询直接走自研执行器
- 复杂分析查询走 DataFusion
- SQL 层统一路由，对上层透明
```

**理由**：风险登记里提到了 DataFusion 不适合 TP 的风险，但 roadmap 里没有具体检查点和 fallback 方案。

---

## 六、Phase 5 修改（原 Phase 5：时序 + 记忆管理）

### 6.1 明确记忆蒸馏是异步后台任务

**位置**：Phase 5 → 交付物表格 → 记忆蒸馏

**原文**：

```
记忆蒸馏 | 多条细节记忆 → 一条摘要记忆（调用外部 LLM），保留关键信息和关联关系
```

**改为**：

```
记忆蒸馏 | 多条细节记忆 → 一条摘要记忆（调用外部 LLM），作为后台异步 job 执行，不阻塞写入事务；蒸馏结果以新元组写入并重新建索引
```

**理由**：LLM 调用不能进入事务核心路径，否则会破坏 ACID 的延迟和稳定性。

---

### 6.2 放宽时序查询目标

**位置**：Phase 5 → 验证标准

**原文**：

```
时序查询性能：100M 时间点，范围查询延迟 < 5ms
```

**改为**：

```
时序查询性能：100M 时间点，热分区范围查询延迟 < 5ms（SSD，无聚合）；带降采样聚合的查询延迟 < 50ms
```

**理由**：5ms 对聚合查询过于激进，需要区分场景。

---

### 6.3 增加列存投影原型（HTAP 早期验证）

**位置**：Phase 5 → 交付物表格

**建议添加一行**：

| 模块 | 说明 |
|------|------|
| 列存投影原型 | 针对时序/记忆分析场景的轻量列存物化视图，Tier 2 异步维护，验证 HTAP 架构可行性 |

**理由**：与 planning.md 中 HTAP Phase 2 的决策保持一致，避免到 Phase 7 才发现列存与行格式冲突。

---

## 七、Phase 6 修改（原 Phase 6：图索引 + 完整协议）

### 7.1 把图索引提前或拆分

**位置**：Phase 6 整节

**建议方案 A（推荐）：把原生图 AM 拆到 Phase 5，Phase 6 聚焦协议层**

```markdown
## Phase 5：时序 + 图索引 + 记忆生命周期

（在原有 Phase 5 交付物中增加图索引）

| 模块 | 说明 |
|------|------|
| Graph AM（轻量版） | 邻接表存储，支持有向/无向边，边属性（JSONB），3 跳以内 BFS/DFS |
| 图查询语法 | SQL 扩展（LATERAL 递归或类 Cypher 子句） |
| 图参与 Fusion | 图遍历结果可与向量/全文/结构化联合检索 |

## Phase 6：完整协议层 + 多 Agent 隔离

**目标：对外提供标准服务能力**

| 模块 | 说明 |
|------|------|
| 完整 PG Wire Protocol | Extended Query、Prepared Statement、参数绑定、类型映射、错误码兼容 |
| MCP Server | 原生 MCP 工具接口，暴露记忆读写/检索/管理 |
| RLS / 多 Agent 隔离 | Agent 级别的行级安全策略 |
| 多租户配额 | Agent 维度的存储/查询配额 |
```

**理由**：图索引是 Agent 记忆关联的高价值能力，放在 Phase 6 太晚。先实现轻量邻接表版，后续再优化。

---

### 7.2 MCP Server 不宜晚于 Phase 6

**位置**：Phase 6 → 交付物

当前 MCP Server 已在 Phase 6，无需移动。但如果采用上述方案 A，MCP 仍然在 Phase 6，是合理的。

---

## 八、Phase 7 修改（生产化）

### 8.1 拆分 Phase 7 为 7a + 7b

**位置**：Phase 7 整节

**建议改为**：

```markdown
## Phase 7a：性能优化

**目标：把核心场景性能推到生产可用水平**

| 模块 | 说明 |
|------|------|
| 列存投影生产版 | 通用 AP 场景的列存物化视图，自动选择投影列，向量化扫描 |
| 向量压缩 | Scalar Quantization (SQ) / Product Quantization (PQ)，自动策略选择 |
| SIMD 全面优化 | 距离计算、解压缩、CRC 校验等关键路径 SIMD 化 |
| io_uring / 大页内存 | Linux 下的高性能 IO 和内存选项 |
| 连接池优化 | 会话管理、预处理语句缓存 |

## Phase 7b：生产运维

**目标：达到可对外部署的运维质量**

| 模块 | 说明 |
|------|------|
| 完整 CBO | 跨模态成本模型 + 统计信息收集 + 计划选择 + EXPLAIN 可解释 |
| SSI（可选 Serializable） | 完整的 SSI 冲突检测 |
| 备份恢复 | 物理备份（快照）+ 逻辑备份（导出/导入）+ PITR |
| WAL Shipping | 主从复制基础 |
| Jepsen 测试 | 并发 + 崩溃恢复 + 隔离级别正确性 |
| 监控体系 | Prometheus metrics、慢查询日志、查询热力图 |
| 文档与 SDK | API 文档、Python/TypeScript/Go SDK、示例项目 |
```

**理由**：Phase 7 原内容过多，且性能优化和生产运维是两个不同维度，拆分后更清晰。

---

## 九、风险登记补充

**位置**："风险登记"表格

**建议新增三条**：

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Phase 1 工期失控，占项目总工期 40% 以上 | 后续 Phase 被迫压缩 | 设 M1/M2a/M2b/M2c 内部检查点，允许分期交付；若 M2 延期，优先保证 M2a 单语句可用 |
| 多 AM GC 协调器复杂度导致 TID 泄漏或过早清理 | 索引膨胀或查询结果缺失 | 统一基于 oldest_active_snapshot 推进；每个 AM 实现 `reclaim_tid` 并做 fuzz 测试 |
| DataFusion 不适合 TP 路径 | Phase 4/5 架构大调整 | Phase 4 前做 TP 点查原型验证；保留"TP 路径绕过 DataFusion"的备用方案 |

---

## 十、技术选型补充

**位置**："技术选型"表格

**建议添加一行**：

| 组件 | 选择 | 理由 |
|------|------|------|
| 图索引 V1 | 邻接表 + B+Tree（Phase 5/6） | 先验证图语义，避免过早做原生图存储；性能不足时再引入专用 Graph AM |

---

## 十一、阶段依赖关系图更新

**位置**："阶段依赖关系"

如果采用上述拆分，建议改为：

```
Phase 1 ──→ Phase 2 ──→ Phase 3 ──→ Phase 4 ──→ Phase 5 ──→ Phase 6 ──→ Phase 7a ──→ Phase 7b
(基座+行存)   (向量)      (全文)      (SQL+PG)    (时序+图+记忆) (协议+隔离)  (性能优化)   (生产运维)
                               │           ▲
                               │           │
                               └─ 依赖 Phase 2/3 完成 ─┘
```

---

## 十二、修改优先级建议

| 优先级 | 修改项 | 原因 |
|---|---|---|
| **P0（必须）** | Phase 1 拆分 M2a/M2b/M2c | 最大工期风险 |
| **P0（必须）** | Phase 4 拆分 SQL/PG Wire 与 Multi-Path Fusion | 最大内容堆积风险 |
| **P1（强烈建议）** | Phase 5/6 加入列存投影原型 | 与 planning.md HTAP 决策保持一致 |
| **P1（强烈建议）** | Phase 7 拆分为 7a/7b | 内容过多，阶段目标不清晰 |
| **P2（建议）** | Phase 3 fallback 策略细化 | 避免大表 seqscan 灾难 |
| **P2（建议）** | Phase 2 HNSW 验证标准放宽 | 避免不可能通过的验收 |
| **P3（可选）** | Phase 1 增加 PG Wire 极简版 | 取决于团队是否急需 PG 生态验证 |

---

## 总结

`ROADMAP.md` 的基础框架已经很好，核心改动是：

1. **Phase 1 和 Phase 4 必须拆分**，否则单阶段过重。
2. **列存投影/HTAP 需要提前验证**，不要放到最后。
3. **图索引可以提前到 Phase 5 做轻量版**， Phase 6 专注协议层。
## 十三、Phase 5 边界重构（与第六节互补）

**位置**：第六节之前新增一节，或改写 Phase 5 整节

**问题**：第六节只把"蒸馏"改成异步，但没解决根本问题——Phase 5 同时承担"内核职责"（TimeSeries AM）和"应用层职责"（记忆遗忘/蒸馏/分层）。pg_rust0706.pdf §3.6 自己说"LLM 永远作为外部服务，不进入事务提交路径"——这意味着记忆蒸馏、遗忘曲线、记忆分层都不是内核该做的事。

**建议**：Phase 5 只保留 TimeSeries AM + 时序参与 Fusion；记忆生命周期整体剥离到独立项目 `pg_rust-agent-sdk`（或类似名字），作为 pg_rust 之上、Agent 框架之下的中间层。

### 13.1 Phase 5（修改后）

| 模块 | 说明 |
|------|------|
| TimeSeries AM | 时间分区存储（按天/小时自动分区），范围扫描，降采样聚合 |
| TTL 自动过期 | 声明式 TTL（`WITH ts_partition = 'day', ttl = '90d'`），后台自动清理过期分区 |
| 时间范围查询 | `WHERE created_at BETWEEN ... AND ...` 自动路由到时序索引 |
| 多 AM GC 协调器 | 统一的 Vacuum 协调：基于 oldest_active_snapshot 推进，回收死元组时通知所有引用该 TID 的索引 |
| 时序参与 Fusion | 时序索引加入 MultiIndexScan，支持"最近 7 天 + 语义相似 + 关键词匹配"组合 |
| 列存投影原型 | 时序/记忆分析场景的轻量列存物化视图，验证 HTAP 架构可行性 |

### 13.2 `pg_rust-agent-sdk`（独立项目，不在主 roadmap 内）

| 模块 | 说明 |
|------|------|
| 记忆遗忘曲线 | 基于访问频率 + 时间衰减的重要性评分，低于阈值的记忆通过 pg_rust 的 DELETE API 标记可淘汰 |
| 记忆蒸馏 | 调度后台 job，调用外部 LLM 总结多条细节记忆为一条摘要（写入 pg_rust 时是普通 INSERT） |
| 记忆分层 | 短期（会话内 Redis）→ 工作（pg_rust volatile 表）→ 长期（pg_rust 持久化表），自动晋升/淘汰策略 |
| Agent 元数据辅助 | 自动维护 agent_id/trace_id/session_id 关联、引用关系图、provenance 增强 |

**理由**：内核提供原语（TTL、降采样、删除/插入），策略由 SDK 实现。这避免"内核兼做应用层"的边界模糊，也避免 LLM 推理影响 ACID 延迟。

---

## 十四、Phase × Layer 矩阵（新增章节）

**位置**：在"阶段总览"后加一节

**背景**：设计原则 2 说"Layer 1 → Layer 2 → Layer 3 严格分层构建"，但 roadmap 没有显式映射每个 Phase 实现哪一层。

**建议**：

```markdown
## Phase × Layer 映射

| Phase | Layer 1（物理） | Layer 2（事务/可见性） | Layer 3（Access Methods） | 跨层能力 |
|---|---|---|---|---|
| Phase 1 M1 | ✓ Page/WAL/BufferPool/LSN/Checkpoint | — | — | File Manager |
| Phase 1 M2 | (扩展) Full Page Image | ✓ MVCC / Lock / Snapshot / Visibility | ✓ B+Tree（含 AccessMethod trait） | ARIES 崩溃恢复 |
| Phase 1 M3 | — | (扩展) Vacuum | — | gRPC 协议 + 可观测 |
| Phase 2 | (Tier 1 异步 IO) | (扩展) Per-tx delta | ✓ HNSW（Epoch + 节点锁） | HNSW Vacuum |
| Phase 3 | (Tier 2 异步 IO) | (扩展) Watermark | ✓ Inverted Index（BM25） | Segment merge |
| Phase 4 | — | (扩展) Cost hooks | (单路选择) | DataFusion + PG Wire + EXPLAIN |
| Phase 5 | — | (扩展) Multi-AM GC 协调 | ✓ TimeSeries + Graph（轻量） | Fusion 接入时序 + 图 |
| Phase 6 | — | (扩展) RLS predicate | (稳定) | MCP Server + 完整 PG Wire |
| Phase 7a | (性能优化) | — | ✓ Columnar Projection | SIMD / io_uring / 大页 |
| Phase 7b | — | ✓ 完整 CBO + 统计 | (压缩) SQ/PQ | 备份恢复 + 监控 |
```

**理由**：让每个 Phase 实现的层次清晰，避免"不知不觉越界"。L1 从 M1 建立、L2 从 M2 起步、L3 严格按 HNSW → FT → TS → Graph → Columnar 递进。

---

## 十五、显式处理 HTAP / CoW / Serverless / 分支（新增章节）

**位置**：在"风险登记"或"设计原则"附近

**问题**：旧 planning.md 写过 CoW 快照作为 Phase 2+ 目标、HTAP 作为 Phase 2、Serverless 作为 Phase 2+ 长期目标。新 ROADMAP 完全不提。需要明确"做/不做/推迟"。

**建议**：

| 能力 | 处理 | 阶段 | 备注 |
|---|---|---|---|
| HTAP（行+列统一引擎） | **做（原型 + 生产）** | Phase 5（列存投影原型）+ Phase 7a（生产版） | 满足"统一引擎"差异化定位；OLAP 通过列存投影覆盖 |
| CoW 快照（基础） | **做（极简）** | Phase 7b（备份恢复中实现） | 用作快照备份，不展开为通用特性 |
| CoW 秒级分支（Agent 沙箱） | **推迟** | 不在 7 阶段 roadmap 中 | 当前 Agent 场景不刚需；用户级分支可在 Agent SDK 层用 PostgreSQL 风格 fork |
| Serverless / 计算存储分离 | **不做** | 永久不做（单机路线） | 战略选择：聚焦内核质量，不与 Neon 竞争 |
| 跨地域分布式事务 | **不做** | 永久不做（单机路线） | 同上 |
| 多租户 RLS | **做** | Phase 6 | 多 Agent 隔离刚需 |
| 多租户配额 | **做** | Phase 6 | 同上 |
| 动态脱敏（DLP） | **推迟** | 评估 | Phase 6 之后看需求 |

**理由**：项目画像（"面向 AI Agent 的统一数据平台"）决定了 Serverless/分布式不在必做范围；HTAP 保留但推到生产化阶段；CoW 限缩到备份恢复场景。

---

## 十六、原则与纪律合并（新增章节）

**位置**：把当前"设计原则"和"原则与纪律"合并为单 section

**问题**：两个 section 有重叠（如"正确性 > 性能 > 功能"在两边都出现），管理纪律散落。

**建议合并为单 section：

```markdown
## 设计原则与纪律

1. **每阶段可交付**：每阶段结束时内部 Agent 团队能用上 psql / gRPC / MCP 客户端。
2. **严格分层构建**：Layer 1 → Layer 2 → Layer 3，地基不稳上层必塌。
3. **AM 按优先级递进**：HNSW → FT → TS → Graph → Columnar，不追求一次性六种全做。
4. **每加 AM 必带 Vacuum/GC**：不留技术债到最后。
5. **正确性 > 性能 > 功能**：宁可少一个功能，不可有一个 bug。
6. **AM 边界守得住**：内核提供原语（TTL/降采样/删除），策略由 SDK 实现；LLM 永远外置。
7. **内部先吃狗粮**：每阶段必须实际使用，反馈驱动优先级。
8. **不跳阶段**：每阶段验证标准全部通过才进入下一阶段。
9. **不过度设计**：每阶段只实现当前需要的最小集，接口可扩展但实现不提前。
10. **每阶段一篇技术博客**：为对外推广积累素材。
```

**理由**：10 条精简，去重 + 增边界原则（原 5+5 = 10 条，结构更清晰）。

---

## 十七、§十 开放问题（新增章节）

**位置**：ROADMAP 末尾，风险登记之后

**来源**：从 pg_rust0706.pdf §13 "架构合理性分析" 提取的 5 个自我批评 + §17-19 的 2 个 TODO

**建议添加**：

```markdown
## 十、开放问题

### 来自设计阶段自我批评

1. **回表开销被低估**：图遍历多跳每步都回表做可见性检查，延迟指数放大；FT posting 含百万 TID 不能逐一回表。缓解：HNSW 多取 2x 候选、Phase 2 末升级 TID+XID 索引条目、posting 段级 snapshot 过滤。

2. **HNSW 并发控制缺失**：HNSW 图结构的并发修改是已知难题，undo() 对图意味着"删除节点+修复所有邻居"可能破坏连通性。缓解：单写者 HNSW（Phase 2 末）+ Epoch-based reclamation（Phase 2 中）+ 节点级细粒度锁（Phase 2）；参考 Vamana/DiskANN 无锁设计（Phase 1a 末研究任务）。

3. **列存投影 Tier 2 一致性窗口**：AP 查询若命中 Tier 2 投影，存在秒~分钟级滞后；fallback 到 seqscan 等于退化。缓解：精细增量合并（类似 Delta Lake Z-Order merge），watermark-aware planner 避免退化路径。

4. **跨模态 CBO 是"画饼"最重的部分**：无数学公式、无统计信息收集方案、HNSW 代价高度依赖数据分布。缓解：Phase 4 用启发式策略选择，Phase 7b 再做完整 CBO；统计信息维护按 AM 单独设计。

5. **Multi-Path Fusion 正确性问题**：RRF 在候选集不重叠时退化为 union；融合策略自动选择没明确算法。缓解：Phase 4 显式拆分"过滤式"（实）和"RRF"（虚）两个子阶段；可解释 EXPLAIN 让用户能 override。

### TODO（在 pg_rust0706.pdf 中标记为未完成）

6. **HNSW 并发协议**（参考 Vamana/DiskANN 无锁设计）：尚未设计完成，Phase 2 启动前需补齐方案或选择"单写者简化路径"。

7. **增量 Vacuum 协议**（6 种 AM 的 GC watermark 推进机制）：尚未设计完成，Phase 5 多 AM GC 协调器实现前需补齐。
```

**理由**：把"已知未知"显式化，每个 Phase 启动前都过一遍是否解决。

---

## 十八、跨文档交叉引用（新增章节）

**位置**：在 §十 开放问题之后

**关系图**：

| 文档 | 关系 | 待补充的 cross-ref |
|---|---|---|
| `planning.md` | 战略计划 + 决策 + 边界；ROADMAP.md 是其执行展开 | §四"下一步输出物"加指向 ROADMAP.md |
| `agent-native-db-architecture.md` | 架构设计概念；ROADMAP.md 是其阶段化 | §6"与现有 Phase 规划的映射"整表 cross-ref 指向 ROADMAP.md；§3.7 工具调用标 Phase 归属 |
| `unified-kernel-design.md` | L1/L2/L3 三层分离骨架；ROADMAP §十四 是其映射 | §六"与现有文档的关系"整表 cross-ref 指向 ROADMAP.md；§三"四个创新" vs PDF 9 项的补全决策 |
| `pg_rust0706.pdf` | 完整设计 RFC；ROADMAP.md 是其执行抽取 | §十 开放问题源自此 PDF |
| `architecture-multimodal-unified-kernel.md` | 多模态统一内核设计 | 待 review（文件名暗示与 unified-kernel-design 主题接近） |
| `database_kernel_expert_in_ai_era.md` | 数据库内核专家视角 | 待 review |

**理由**：4 份核心文档（planning / architecture / unified-kernel / ROADMAP）现在有内容重叠和 Phase 编号不一致，需要 cross-ref 拉齐。

---

## 十九、衍生文档与代码需求（新增章节）

**位置**：在跨文档交叉引用之后

基于 ROADMAP-changes.md 的改动，会衍生以下新文档/代码：

| 文档/代码 | 触发条件 | 内容概要 |
|---|---|---|
| `agent-sdk-design.md` | §十三 Phase 5 拆出 Agent SDK 时 | 记忆遗忘曲线算法、蒸馏流程（外部 LLM）、短期/工作/长期分层策略、pg_rust 边界 |
| `phase1-m1-tech-selection.md` | 进入 Phase 1 M1 编码前 | Page Allocator 实现（自研 freelist）、WAL Writer（tokio + bincode/postcard）、Buffer Pool（parking_lot + lru crate）、LSN（AtomicU64）、Checkpoint（自研 stop-the-world v1）、物理页大小（8KB/16KB/64KB）、文件命名（wal-00000001.log 等） |
| Cargo workspace 骨架 | phase1-m1-tech-selection.md 完成后 | `crates/pg-storage/` (L1) / `pg-txn/` (L2) / `pg-am/` (L3) / `pg-sql/` / `pg-protocol/` / `pg-server/` (binary) |

---

## 二十、修改优先级建议（与第十二节互补）

| 优先级 | 修改项 | 原因 |
|---|---|---|
| **P0** | §五.1 Phase 4 拆 4a/4b | 已有，但需更明确的拆分边界 |
| **P0** | §十三 Phase 5 拆出 Agent SDK | 内核职责守界 |
| **P0** | §十五 HTAP/CoW/Serverless 显式处理 | 战略边界明确 |
| **P1** | §十四 Phase × Layer 矩阵 | 设计原则 2 的具体化 |
| **P1** | §一 总工期估算 | 资源投入判断 |
| **P1** | §十七 §十 开放问题 | 透明化已知风险 |
| **P2** | §十六 原则与纪律合并 | 文档清理 |
| **P2** | §十八 跨文档交叉引用 | 文档一致性 |
| **P3** | §十九 衍生文档 | 后续执行触发 |

**总结**：本文件（ROADMAP-changes.md）核心改动是 **Phase 4 拆分 + Phase 5 边界重构 + HTAP/CoW 显式处理 + Phase × Layer 矩阵**。这四项是必修，其他是清理和透明化。

---

## 二十一、当前 ROADMAP.md 剩余待改项（基于最新 review）

> 本节是 §一-二十 之外、基于当前 ROADMAP.md 实际状态的剩余 gap 分两批列出：
> (A) §一-二十 中已列出但当前 ROADMAP.md 尚未应用的改动；
> (B) 已删除文件造成的 §十八/§十九 stale 内容需清理。

### A. §一-二十 中尚未应用的具体改动

| 编号 | 来源节 | 待改位置（当前 ROADMAP.md） | 待改内容 |
|---|---|---|---|
| A1 | §一 | 缺：阶段总览之后 | 增加「总工期估算」表（Phase 1=6-10 月 / Phase 2=3-5 / Phase 3=2-4 / Phase 4=4-6 / Phase 5=3-5 / Phase 6=3-5 / Phase 7=4-6，累计 25-41 月） |
| A2 | §2.1 | Phase 1 → M2 → 验证标准之前 | 增加 M2 内部检查点：M2a（单语句 auto-commit + 堆表 + 无并发 B+Tree）/ M2b（多语句事务 + MVCC 快照 + 可见性）/ M2c（完整锁管理 + 死锁检测 + B+Tree 并发） |
| A3 | §2.2 | Phase 1 → M2 → 验证标准 | "100 并发连接下 TPS ≥ 10K（简单 CRUD）" → 加上条件"（单表，SSD，无网络协议开销）" |
| A4 | §2.3 | Phase 1 → M3 → 交付物 | M3 的 gRPC/HTTP API 之外可选加 "PG Wire Protocol 极简版（仅 Simple Query）"，或确认推迟到 Phase 4a（当前 ROADMAP 已把 PG Wire 推到 4a，建议保留 A4 但标注"已合并到 Phase 4a"） |
| A5 | §3.1 | Phase 2 → 验证标准 | "图结构始终一致（双向边对称、连通性）" → 放宽为"双向边不对称率 < 1% + recall@10 不低于崩溃前 95% + 无悬挂节点" |
| A6 | §3.2 | Phase 2 → 距离函数 | "SIMD 加速（SSE4.2 / AVX2 / NEON）" → 改为"基础实现正确；SIMD 优化作为 Phase 2 末尾或 Phase 7 的优化项" |
| A7 | §4.1 | Phase 3 → Fallback 策略 | 单行说明 → 拆为三条：优先只查不可变 segment / 缺最新数据再 seqscan / seqscan 必须配超时+采样限制 |
| A8 | §4.2 | Phase 3 → BM25 评分 之后 | 新增一行"BM25 全局统计"：维护 `doc_count` / `avgdl` / `term_df`，原子递增，checkpoint 时持久化 |
| A9 | §5.2 | Phase 4a → 验证标准 之前 | 新增"DataFusion TP 适配性检查点"：①点查延迟 < 5ms ②事务上下文传递 ③不通过则启动"TP 路径绕过 DataFusion"备用方案 |
| A10 | §6.2 | Phase 5 → 验证标准 | "100M 时间点，范围查询延迟 < 5ms" → 拆为"热分区 < 5ms（SSD 无聚合）"+"带降采样聚合 < 50ms" |
| A11 | §7.1 | Phase 5 / Phase 6 拆分 | 当前 Phase 6 同时含「图 AM」+「完整协议 + MCP + RLS」。建议把"图 AM（轻量版）"提前到 Phase 5，与时序并列；Phase 6 聚焦"完整协议 + MCP + RLS"。这样图能参与 Phase 5 的 MultiPath Fusion 验证 |
| A12 | §九 | 风险登记 表格 | 新增 3 条：①Phase 1 工期失控占项目总工期 40%+ → 设 M2a/M2b/M2c 内部检查点；②多 AM GC 协调器复杂度 → 统一基于 oldest_active_snapshot + per-AM `reclaim_tid` fuzz 测试；③DataFusion 不适合 TP → Phase 4a 前做 TP 点查原型验证 + 备用方案 |
| A13 | §十 | 技术选型 表格 | 新增一行"图索引 V1 / 邻接表 + B+Tree（Phase 5/6）"——先验证图语义，避免过早做原生图存储 |

### B. 清理：已删除文件造成的 stale 内容

| 编号 | 来源节 | 当前 ROADMAP.md 位置 | 清理动作 |
|---|---|---|---|
| B1 | §十八 | 跨文档交叉引用 | `planning.md` / `agent-native-db-architecture.md` / `unified-kernel-design.md` 已被删除，删除对应表格行和 cross-ref 锚点。`agent-sdk-design.md`（§十九 衍生项）也已被删除 |
| B2 | §十八 | "7 份核心文档（planning / architecture / unified-kernel / multimodal / kernel-expert / rewrite-postgres / ROADMAP）" | 改为"3 份核心文档"或去掉 planning/architecture/unified-kernel 引用 |
| B3 | §十八 | 关键 cross-ref 锚点列表 | 删除 planning.md §六 / unified-kernel-design.md §二 / agent-native-db-architecture.md §三 / ROADMAP-changes.md §五 这些仍可保留（changes 文件本身存在），但 §三 "四个创新 vs PDF 9 项"对应 unified-kernel-design.md 的部分需要明确指向 PDF |
| B4 | §十九 | 衍生文档表 | `agent-sdk-design.md` 已删除，从表中删除；`phase1-m1-tech-selection.md` 和 `Cargo workspace 骨架` 不在当前任务范围，保留作为"未来可触发" |
| B5 | ROADMAP.md §显式战略边界 | "旧 planning.md 写过 CoW..." | planning.md 已删除，改写为"早期 planning 阶段曾写过 CoW 快照作为 Phase 2+ 目标..."，避免指向已删文件 |
| B6 | ROADMAP.md §Phase 4a 不做 | "Extended Query 模式 / 预处理语句 / 类型映射（放到 Phase 6）" | 当前正确，保留 |
| B7 | §十九 | "phase1-m1-tech-selection.md" | 状态标记为"延后"，不在本轮范围 |
| B8 | §十九 | "Cargo workspace 骨架" | 状态标记为"延后"，不在本轮范围 |

### C. 优先级与执行建议

| 优先级 | 编号 | 说明 |
|---|---|---|
| **P0** | A2 / A11 / A12-① | M2 内部检查点 + 图提前到 Phase 5 + Phase 1 工期风险——直接关系到项目可执行性 |
| **P0** | B1 / B2 | §十八 跨文档引用指向已删文件——必须立即清理，否则误导读者 |
| **P1** | A1 / A5 / A6 / A10 / A12-②③ | 工期估算 + 验证标准放宽（避免不可能达成的验收）+ 风险登记补充 |
| **P1** | A7 / A8 | Tier 2 fallback 细化 + BM25 全局统计——Phase 3 实施时直接用到 |
| **P2** | A3 / A9 | TPS 条件 + DataFusion TP 检查点——文案级别的精确化 |
| **P2** | A13 / B3 / B5 | 图索引 V1 选型 + 锚点清理 + planning.md 引用改写——文档清理 |
| **P3** | A4 / B4 / B7 / B8 | 已合并/延后项，保留记录即可 |

**本节小结**：当前 ROADMAP.md 还有 13 项 §一-二十 的待改 + 8 项已删文件造成的 stale 内容未清理。P0 必修 5 项，其余按 P1/P2/P3 分批处理。

---

## 二十二、最新 review 补充项（2026-07-06）

> 基于对当前 ROADMAP.md 的完整 review，以下为 §一-二十一 未覆盖的新增改动。

### 22.1 Phase 1 M1 — WAL Writer 描述修正

**位置**：Phase 1 → Milestone 1 → 交付物表格 → WAL Writer

**原文**：

```
WAL Writer | append-only 日志，接受 (record_type, payload)，fsync 语义，CRC32 校验，支持物理+逻辑两种记录类型
```

**改为**：

```
WAL Writer | append-only 日志，接受 (record_type, payload)，fsync 语义，CRC32 校验；
            物理 WAL 完整实现（before/after image）；
            逻辑 WAL 接口预留（Phase 2 HNSW 接入时实现）
```

**理由**：逻辑 WAL 在 Phase 1 没有使用者（HNSW 在 Phase 2，全文在 Phase 3），M1 只需保证接口可扩展，不需要实现逻辑记录的 redo/undo 分发。

---

### 22.2 Phase 3 — 增加 watermark 监控指标

**位置**：Phase 3 → 交付物表格 → Watermark 机制 之后

**新增一行**：

| 模块 | 说明 |
|------|------|
| Watermark 监控 | `watermark_lag_seconds` 指标导出（Prometheus 或内置视图），planner 可据此判断是否 fallback 到 seqscan；当 lag 超过可配置阈值时自动提高后台 worker 消费速率 |

**理由**：Tier 2 索引的一致性依赖于 watermark 推进速度。没有监控指标，Tier 2 fallback 到 seqscan 会导致查询延迟不可预测。运维和开发阶段都需要这个可见性。

---

### 22.3 依赖关系图 — 标注可并行维度

**位置**："阶段依赖关系"

**当前**：

```
Phase 1 ──→ Phase 2 ──→ Phase 3 ──→ Phase 4 ──→ Phase 5 ──→ Phase 6 ──→ Phase 7
(基座+行存)   (向量)      (全文)      (融合+SQL)   (时序+记忆)   (图+协议)    (生产化)
                                         ▲
                                         │
                          Phase 2 & 3 完成后才能做
```

**改为**：

```
主干（必须线性）：
Phase 1 ──→ Phase 2 ──→ Phase 3 ──→ Phase 4a ──→ Phase 4b
(基座+行存)   (向量)      (全文)      (SQL+PG)     (Fusion)

可并行 track（不阻塞主干，有人力时可提前启动）：
Phase 1 完成后：
  ├── Phase 5a (时序 AM) ─────────── 不依赖 Phase 2/3/4，可与它们并行
  └── Phase 3.5 (列存投影原型) ───── 不依赖 Phase 2/3，可与它们并行

Phase 4a 完成后：
  ├── Phase 5b (图 AM 轻量版) ────── 需要 MultiIndexScan 接口就绪
  ├── Phase 6b (完整 PG Protocol) ── 需要 Phase 4a 的 PG Wire 基础
  └── Phase 6c (MCP Server) ─────── 需要 Phase 4a 的 SQL 层

Phase 4b + Phase 5 + Phase 6 → Phase 7
```

**理由**：当前严格线性依赖会显著拉长总工期。时序 AM、列存投影、完整 PG Protocol 和 MCP Server 实际上不依赖 Fusion，可以在主干推进的同时并行开发。

---

### 22.4 技术选型 — 补充 PG Wire Protocol 实现方案

**位置**："技术选型"表格

**新增一行**：

| 组件 | 选择 | 理由 |
|------|------|------|
| PG Wire Protocol | 自研（参考 PostgreSQL 官方协议文档 + `pgwire` crate 作为参考实现） | 协议消息类型多但每条简单；~3000 行 Rust 可覆盖 psql/SQLAlchemy/Prisma 兼容；自研可控性高于第三方 crate |

**理由**：PG Wire Protocol 是实现路线上的关键组件（Phase 4a），应该在技术选型中显式出现。

---

### 22.5 风险登记 — 补充 Tier 2 watermark 退化风险

**位置**："风险登记"表格

**新增一行**：

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Tier 2 watermark 长期落后导致查询退化到 seqscan | 全文/列存查询延迟暴增，Agent 体验降级 | 监控 `watermark_lag_seconds`；超过阈值时自动提高 worker 速率或临时升为 Tier 1；planner 在 EXPLAIN 中显示是否发生 fallback |

**理由**：Tier 2 是一致性模型的核心权衡——用延迟换写入性能。但如果 watermark 长期落后，这个权衡就失效了。需要监控和自动恢复机制。

---

### 22.6 设计原则与纪律 — 合并去重

**位置**：当前文档有两个 section——开头的"设计原则"（6 条）和末尾的"原则与纪律"（5 条）

**问题**：
- "正确性 > 性能 > 功能"在两个 section 都出现
- "每阶段结束后 Agent 团队能用上" 和 "内部先吃狗粮" 语义重叠
- "AM 按优先级递进" 和 "不过度设计" 语义重叠

**建议**：合并为单个 section（放在文档开头），精简为 8 条：

```markdown
## 设计原则与纪律

1. **严格分层构建**：Layer 1 → Layer 2 → Layer 3，地基不稳上层必塌
2. **每阶段可交付**：每阶段结束时内部 Agent 团队能用 psql/gRPC/MCP 客户端连接使用
3. **AM 按优先级递进**：HNSW → FT → TS → Graph → Columnar，不追求一次性六种全做
4. **每加 AM 必带 Vacuum/GC**：不留技术债到最后
5. **AM 边界守得住**：内核提供原语（TTL/降采样/删除），策略由上层 SDK 实现；LLM 永远外置
6. **正确性 > 性能 > 功能**：宁可少一个功能，不可有一个 bug
7. **不跳阶段，不过度设计**：每阶段验证标准全部通过才进入下一阶段；接口可扩展但实现不提前
8. **每阶段一篇技术博客**：为对外推广积累素材
```

**理由**：当前 6+5=11 条，合并去重后 8 条，更清晰。"内部先吃狗粮"和"每阶段 Agent 团队能用上"合并为第 2 条。

---

### 22.7 持续集成与正确性保障（新增建议）

**位置**：在"风险登记"之后，作为独立 section

**建议添加**：

```markdown
## 持续正确性保障

数据库的正确性不能靠手工测试保证。每个 Phase 应配备以下测试基础设施：

| 测试类型 | 引入阶段 | 工具 | 目标 |
|---|---|---|---|
| 单元测试 | Phase 1 M1 起 | `cargo test` | 覆盖率 ≥ 90%（核心模块） |
| 模糊测试 | Phase 1 M1 起 | `proptest` | Page Allocator 不泄漏、WAL 记录可 round-trip |
| 并发模型检查 | Phase 1 M2 起 | `loom` | Lock-free 数据结构正确性、Latch 无死锁 |
| 随机崩溃测试 | Phase 1 M2 起 | 自研 harness（`kill -9` + 重启 + 校验） | ACID 正确性 |
| 确定性模拟测试 | Phase 2 起 | 自研（I/O + 并发顺序注入） | 并发 bug 可复现 |
| Jepsen | Phase 7 | `jepsen` | 事务隔离 + 崩溃恢复 业界标准验证 |
```

**理由**：测试基础设施是数据库项目的安全网，应该在 roadmap 中显式规划，而不是放在 Phase 7 的 "Jepsen" 一条里。

---

### 22.8 §二十一 A 表补充

在 §二十一 A 表中追加以下行：

| 编号 | 来源节 | 待改位置 | 待改内容 |
|---|---|---|---|
| A14 | §22.1 | Phase 1 → M1 → WAL Writer | "物理+逻辑两种"→"物理完整实现 + 逻辑接口预留" |
| A15 | §22.2 | Phase 3 → 交付物 | 新增 watermark 监控指标行 |
| A16 | §22.3 | "阶段依赖关系" | 替换为"主干 + 可并行 track"图 |
| A17 | §22.4 | 技术选型 表格 | 新增 PG Wire Protocol 实现方案行 |
| A18 | §22.5 | 风险登记 表格 | 新增 Tier 2 watermark 退化风险行 |
| A19 | §22.6 | 文档开头 | 合并"设计原则"和"原则与纪律"为单个 8 条 section |
| A20 | §22.7 | 风险登记之后 | 新增"持续正确性保障" section |

**本节优先级**：

| 优先级 | 编号 | 说明 |
|---|---|---|
| **P0** | A16 | 依赖图标注并行维度——直接影响排期决策 |
| **P1** | A14 / A15 / A18 | 描述修正 + 监控 + 风险——预防性改动 |
| **P2** | A17 / A19 / A20 | 补充 + 清理——文档质量 |
| **P3** | — | 无 |
---

## 二十三、最新 review 改动清单（按 ROI 分层，2026-07-06）

> 基于工程合理性 + 开源准备双重视角的最新一轮 review，按 P0/P1/P2 优先级组织，每条给出 **位置 + 当前内容 → 新内容 + 理由**，可直接对照修改。
>
> 与前几节的差异：前几节是按 Phase 分组，本节是按 ROI 分组，更适合直接拍板执行顺序。

---

### 🔴 P0 必改（4 条）

#### 改动 1：每个 Phase 加时间估算

**位置**：每个 Phase 标题后、交付物表格前

**当前**：每个 Phase 标题后只有"目标：xxx"一行

**新模板**：

```markdown
## Phase X：[名称]

**目标：xxx**

**时间估算（1-2 高级 Rust 工程师）：**
- 乐观（一切顺利）：X 个月
- P50（典型工程节奏）：X-X 个月
- 长尾（遇到 P0 级阻塞）：X-X 个月

**风险等级**：🔴 高 / 🟡 中 / 🟢 低

---
```

**具体填值**：

| Phase | 乐观 | P50 | 长尾 | 风险 |
|---|---|---|---|---|
| Phase 1 (M1+M2+M3) | 6 | 9-12 | 15+ | 🔴 高（事务/恢复是核心） |
| Phase 2 (HNSW) | 9 | 12-15 | 24+ | 🔴 高（long pole） |
| Phase 3 (Inverted) | 4 | 6-8 | 10 | 🟡 中（segment 模式新） |
| Phase 4 (Fusion + SQL) | 4 | 6-8 | 12 | 🟡 中（DataFusion 集成） |
| Phase 5 (时序 + 记忆) | 3 | 5-7 | 10 | 🟡 中（SDK/内核边界） |
| Phase 6 (图 + 协议) | 6 | 9-12 | 18+ | 🔴 高（图 + 完整协议） |
| Phase 7 (生产化) | 持续 | 持续 | 持续 | 🟢 低（持续优化） |

**合计 P50：55-75 个月 / 5-6 年**

**理由**：roadmap 没有时间估算 = 愿景文档；加估算 = 计划文档。开源发布需要明确 milestone。

---

#### 改动 2：Phase 2 拆成 2a/2b/2c

**位置**：整个 Phase 2 章节（ROADMAP.md 中）

**当前**：Phase 2 单 Phase 包含 11 项交付物

**新结构**：拆成 3 个 sub-phase，每个有独立 demo

```markdown
## Phase 2：HNSW 向量索引

**目标：支持向量存储与近邻检索，Agent 可以做语义记忆召回**

**时间估算（1-2 高级 Rust 工程师）：** 乐观 9 个月 / P50 12-15 个月 / 长尾 24+ 个月
**风险等级**：🔴 高（long pole of the entire roadmap）

### 拆分理由

HNSW 是 6 种 AM 中工程量最大的：
- 并发控制（设计 doc 自己承认 HNSW undo 是 `// todo`）
- WAL 集成（HNSW 的逻辑 WAL + 一致性修复）
- 崩溃恢复（Epoch snapshot + 增量重放）
- 可见性过滤（2x 候选 + top-K）

任一项卡住会延期整个 Phase。拆成 3 个 sub-phase，每个有独立 demo。

### Phase 2a：In-memory HNSW (3 个月)

**目标**：能在内存里建图、查询、加载；不要求持久化

**交付物**：
| 模块 | 说明 |
|------|------|
| VECTOR(n) 类型 | 一等公民数据类型，DDL 级声明，支持 f32/f16/bf16 |
| HNSW 内存版 | 分层随机图、M/M_max、邻居选择、贪心搜索 |
| 距离函数 | L2 / Cosine / Inner Product，SIMD（SSE4.2 / AVX2 / NEON） |
| 加载 API | 从磁盘加载预构建的图（用 `hnswlib` 格式互通） |

**验证标准**：
- 1M 768d 向量 recall@10 ≥ 95%（sift-128-euclidean, gist-960-euclidean）
- 1M 向量加载时间 < 5 分钟
- 搜索延迟 P99 < 20ms（ef_search=64）
- 单元测试 ≥ 90%

### Phase 2b：HNSW WAL + 持久化 (4 个月)

**目标**：HNSW 变更进单一 WAL，崩溃后能完整恢复

**交付物**：
| 模块 | 说明 |
|------|------|
| 逻辑 WAL 记录 | `add_node(v, neighbors)`, `connect(a, b)`, `remove_node(tombstone)` |
| on-disk 邻居列表 | 节点 page 写入 Buffer Pool，遵循 WAL 先行 |
| Checkpoint 时的 HNSW 快照 | 入口点、最大层数、节点计数 |
| 崩溃恢复 | 从 checkpoint + WAL 重放，验证图一致性（双向边对称） |
| 幂等 redo | 重放同一 WAL 记录 N 次结果一致 |

**验证标准**：
- 1M 向量随机 kill -9 1000 次，图结构 100% 一致
- 重启恢复时间 < 30 秒（checkpoint 后增量重放）

### Phase 2c：HNSW 并发控制 + Tier 1 同步 (5-7 个月)

**目标**：高并发下 HNSW 仍能正确维护

**交付物**：
| 模块 | 说明 |
|------|------|
| Epoch-based reclamation | 防止 use-after-free 的安全回收 |
| 节点级细粒度锁 | 非页级，避免粒度过粗 |
| Tier 1 同步策略 | 事务内累积 delta，commit 时批量合并到 HNSW 图 |
| 可见性适配 | 搜索时多取 2x 候选，回表做 visibility 过滤后返回 top-K |
| 并发 undo | abort 时标记 tombstone + 后台修复邻居连通性 |
| Vacuum 扩展 | tombstone 比例监控 + 局部重建 |

**验证标准**：
- 100 并发 INSERT + DELETE 持续运行 24 小时，图结构 100% 一致
- 并发 abort 后 HNSW 搜索精度（recall@10）≥ 单线程基线的 95%
- 对标：与 pgvector (HNSW) 和 Qdrant 对比 recall/latency/throughput
```

---

#### 改动 3：Phase 2 API 改成 SQL 语法（去掉 SEARCH 自定义语法）

**位置**：Phase 2 API 示例段

**当前**：

```
// 通过连接协议调用
INSERT INTO memories (id, content, embedding) VALUES ('m1', 'hello', [0.1, 0.2, ...])
SEARCH memories BY embedding NEAR [0.3, 0.4, ...] LIMIT 10
SEARCH memories BY embedding NEAR [0.3, 0.4, ...] WHERE agent_id = 'a1' LIMIT 10
```

**新内容**：

```sql
-- 通过 PG Wire Protocol 调用（Phase 1.3 起的最小子集即可）
INSERT INTO memories (id, content, embedding) VALUES ('m1', 'hello', '[0.1, 0.2, ...]');

-- 纯向量检索
SELECT id, content FROM memories
ORDER BY embedding <=> $1
LIMIT 10;

-- 向量 + 结构化过滤组合（Phase 1 B+Tree + Phase 2 HNSW）
SELECT id, content FROM memories
WHERE agent_id = 'a1'
ORDER BY embedding <=> $1
LIMIT 10;
```

**理由**：
- SQL 是 Agent 团队已经在用的接口（pgvector、Qdrant 都用 SQL）
- Phase 4 DataFusion 集成时不需要重写 Agent 代码
- 提前在 Phase 1.3 实现最小 SQL parser（不依赖 DataFusion），几十行代码
- **避免 Phase 2 → Phase 4 之间重写一次客户端**

---

#### 改动 4：Phase 1.2 Lock Manager 术语修正

**位置**：Phase 1.2 → Lock Manager 行

**当前**：

```
| Lock Manager | 行级锁（S/X），意向锁（IS/IX），等待队列 + 死锁检测（wait-for graph，100ms 周期） |
```

**方案 A（保留 IS/IX）**：

```
| Lock Manager | 行级锁（基于 tuple header 的 xmax 字段实现 S/X 模式），表级锁（4 标准模式），意向锁（IS/IX/SIX，用于行锁与表锁的协调），等待队列 + 死锁检测（wait-for graph，100ms 周期），锁状态可见查询（pg_locks 等价物） |
```

**方案 B（推迟 IS/IX，推荐）**：

```
| Lock Manager | 行级锁（基于 tuple header xmax 字段，S/X 模式），表级锁（4 标准模式），等待队列 + 死锁检测（wait-for graph，100ms 周期）。IS/IX 意向锁推迟到 Phase 6 ALTER TABLE/VACUUM FULL 支持时再加。 |
```

**理由**：row-level lock via tuple header xmax 是 PG 已经验证的设计；IS/IX 在 Phase 1.2 引入会增加复杂度但收益有限。

---

### 🟡 P1 应该改（5 条）

#### 改动 5：Phase 1.2 加 Snapshot 机制选择说明

**位置**：Phase 1.2 → Transaction Manager / Snapshot 行附近

**当前**：

```
| Transaction Manager | begin/commit/abort，事务 ID 分配，事务状态表（CSN-based） |
| Snapshot | 快照获取（SI：事务开始时固定；RC：每条语句新快照） |
```

**新内容**：

```
| Transaction Manager | begin/commit/abort，事务 ID 分配（64-bit 无 wraparound） |
| Snapshot 机制 | **LSN-based snapshot**（不复用 XID，复用单一 LSN 时钟）<br>理由：<br>- 与"单一 WAL + 单一 LSN"根契约一致<br>- snapshot = {xmin_lsn: my_lsn, xmax_lsn: next_assigned_lsn, active_xacts: ATT 快照}<br>- 无 XID wraparound 问题<br>- CSN-based 需要额外的全局 commit 序号分配器，与 LSN 重复<br><br>默认隔离级别：Snapshot Isolation（SI）<br>RC（每语句新快照）：可选支持<br>SSI（Serializable）：推迟到 Phase 7 |
```

**理由**：CSN-based 与 LSN 重复，是设计冗余；提前确定避免实现返工。

---

#### 改动 6：Phase 1.3 加 SegmentedStorage 接口预留

**位置**：Phase 1.3 交付物表格末

**当前**：

```
| Tier 2 接口预留 | WAL tail reader 接口、watermark registry 接口、planner 可感知索引新鲜度的 hook |
```

**在 Tier 2 接口预留行之前插入**：

```
| SegmentedStorage 接口 | Phase 3 (Inverted) 和 Phase 5 (TimeSeries) 都是 segment-based 架构（append-only + immutable + 周期性 merge），Phase 1.3 预留接口：<br>- SegmentedStorage trait: `create_segment() / freeze() / seal() / merge()`<br>- Segment lifecycle: 不可变 segment + 后台 merge worker<br>- WAL 协议扩展: `SEGMENT_SEAL` / `SEGMENT_MERGE` 记录类型<br>- segment file 与 Buffer Pool 的关系（segment file 可选走 Buffer Pool 或直读） |
```

**理由**：避免 Phase 3 引入 segment 时回头改 Layer 1 抽象。

---

#### 改动 7：Phase 5 拆"内核交付物 vs SDK 层交付物"

**位置**：Phase 5 交付物表格

**当前**：把"记忆遗忘曲线 + 蒸馏 + 分层"和 TimeSeries AM 并列放在交付物里

**新结构**：拆成两个子节

```markdown
### 5.1 内核交付物（必做）

| 模块 | 说明 |
|------|------|
| TimeSeries AM | 时间分区存储（按天/小时自动分区），范围扫描，降采样聚合 |
| TTL 自动过期 | 声明式 TTL（`WITH ts_partition = 'day', ttl = '90d'`），后台自动清理过期分区 |
| 时间范围查询 | `WHERE created_at BETWEEN ... AND ...` 自动路由到时序索引 |
| 多 AM 统一 GC 协调器 | 统一的 Vacuum 协调：基于 oldest_active_snapshot 推进，回收死元组时通知所有引用该 TID 的索引 |
| 时序参与 Fusion | 时序索引加入 MultiIndexScan，支持"最近 7 天 + 语义相似 + 关键词匹配"组合 |

### 5.2 SDK 层交付物（必做，但不在内核）

| 模块 | 说明 |
|------|------|
| 遗忘曲线 SDK | 基于访问频率 + 时间衰减的重要性评分，标记可淘汰记忆（应用层评分 + 触发内核 GC） |
| 记忆蒸馏 SDK | 多条细节记忆 → 一条摘要记忆（调用外部 LLM），摘要写回主表 |
| 记忆分层 SDK | 短期（会话内）→ 工作（任务级）→ 长期（持久化）的视图抽象 |

**Layer 边界说明**：
- 内核 trait 只暴露 `mark_for_gc(tids) / vacuum_range()` 这类原子能力
- 评分算法、LLM 调用、分层策略都是 SDK 层，不进内核
```

**理由**：与 §十三 Layer 矩阵一致；明确边界避免内核被 SDK 概念污染。

---

#### 改动 8：Phase 7 拆成 7a/7b/7c/7d

**位置**：整个 Phase 7 章节

**当前**：Phase 7 打包 10 项交付物

**新结构**：拆成 4 个 sub-phase

```markdown
## Phase 7：生产化

**目标：达到可对外推广的生产质量**

**时间估算：** 持续 / 12+ 个月
**风险等级**：🟢 低（优化阶段）

### Phase 7a：可观测性 + 备份恢复 (3-4 个月)

| 模块 | 说明 |
|------|------|
| 监控体系 | Prometheus metrics 导出、慢查询日志、查询热力图 |
| 物理备份 | 文件级快照 |
| 逻辑备份 | 导出/导入工具 |
| PITR | 基于 WAL 的 Point-in-Time Recovery |
| WAL Shipping | 用于热备（不做主从自动切换） |

### Phase 7b：性能与压缩 (3-4 个月)

| 模块 | 说明 |
|------|------|
| 列存投影 | AP 场景的列式物化视图，Tier 2 异步维护，向量化扫描 |
| 向量压缩 | Scalar Quantization (SQ) / Product Quantization (PQ)，自动策略选择 |
| SIMD 全面优化 | 覆盖所有距离函数、B+Tree 比较、JSONB 解析 |
| io_uring | Linux 异步 I/O |
| 大页内存 | 减少 TLB miss |

### Phase 7c：完整 CBO (4-6 个月)

| 模块 | 说明 |
|------|------|
| 跨模态统计信息收集 | 各 AM 的统计形态：B+Tree 直方图 / HNSW 距离分布 / Inverted term frequency / TimeSeries bucket 分布 |
| 代价模型校准 | 基于真实 workload 的代价参数校准工具 |
| EXPLAIN 跨模态分解 | 每条访问路径的代价分解，可解释计划选择 |

### Phase 7d：高可用 + 高级特性 (持续)

| 模块 | 说明 |
|------|------|
| Jepsen 测试 | 事务隔离 + 崩溃恢复的标准化测试 |
| 主从切换 | 基于 WAL Shipping + Raft/Paxos 选主 |
| SSI | 完整 SSI 冲突检测（谓词锁 + dangerous structure） |
| 文档与 SDK | Python/TypeScript/Go SDK + 完整 API 文档 + 示例项目 |

**验证标准（Phase 7 总）：**
- Jepsen 测试通过（事务隔离 + 崩溃恢复）
- 7×24 稳定运行测试（72 小时高并发压测无 OOM/死锁/数据丢失）
- 对标综合 benchmark：Agent 记忆场景端到性能 ≥ PG+pgvector+ES 方案的 80%
```

---

#### 改动 9：风险登记补 5 条

**位置**：风险登记表格

**新增 5 行**：

```
| 类型系统演进（VECTOR/AGENT_ID 加列/删列/改类型） | Phase 1+ 全程 | online DDL 设计 + 向后兼容测试 |
| RLS 在混合检索下的正确性（向量 + 全文 + 结构的权限边界） | Phase 6 | Fusion 算子统一注入 RLS 谓词，per-tenant 隔离测试 |
| pg_rust 与 PG 协议兼容性陷阱（Prepared Statement / 类型映射 / 异常） | Phase 4 | 用真实 PG 驱动（psycopg2, rust-postgres, asyncpg）做兼容性测试矩阵 |
| Segment-based AM 与 Buffer Pool 抽象的张力 | Phase 3 | Phase 1.3 提前预留 SegmentedStorage 接口 |
| Agent 长会话的 snapshot 累积（百万级活跃事务） | Phase 5+ | snapshot 老化 + oldest_active_snapshot 推进策略 |
```

---

### 🟢 P2 可选改（4 条）

#### 改动 10：Phase 1.1 性能基线修改

**位置**：Phase 1.1 验证标准

**当前**：

```
**验证标准：**
- WAL 顺序写吞吐 ≥ 500MB/s
- Buffer Pool 随机读 ≥ 100K ops/s
- 单元测试覆盖率 ≥ 90%
- 模糊测试（proptest）验证 Page Allocator 不泄漏不重叠
```

**新内容**：

```
**验证标准：**
- **正确性优先（不设硬性性能指标）**：
  - WAL 顺序写正确性：每条记录 crash 后能完整恢复
  - Buffer Pool 正确性：pin/unpin 不泄漏、eviction 不丢页
  - 单元测试覆盖率 ≥ 90%
  - proptest 验证 Page Allocator 正确性（无泄漏、无重叠）
- **性能基线推迟到 Phase 7b**：
  - WAL ≥ 200MB/s（顺序写，本地 SSD 物理上限的 60%）
  - Buffer Pool ≥ 50K ops/s（随机读 8KB page）
- **崩溃测试**：随机 kill -9 × 1000 次无数据丢失
```

**理由**：100K ops/s × 8KB = 800MB/s 随机读吞吐，v0 不可达；硬指标会逼迫实现走捷径，反而牺牲正确性。

---

#### 改动 11：Phase 1.3 可观测性加查询统计

**位置**：Phase 1.3 可观测性行

**当前**：

```
| 可观测性 | WAL dump 工具（人类可读）、活跃事务列表查询、锁等待关系查询、Buffer Pool 命中率统计 |
```

**新内容**：

```
| 可观测性 | WAL dump 工具（人类可读）、活跃事务列表查询、锁等待关系查询、Buffer Pool 命中率统计、查询统计（pg_stat_statements 等价物：每 query 的延迟、行数、扫描路径、fusion 策略选择） |
```

---

#### 改动 12：技术选型 - 连接协议路线调整

**位置**：技术选型 → 连接协议行

**当前**：

```
| 连接协议 | gRPC（Phase 1）→ PG Wire（Phase 4+） | 渐进式，先快速可用再兼容 |
```

**新内容（推荐方案）**：

```
| 连接协议 | **PG Wire Protocol 最小子集（Phase 1.3 起）+ Extended Query 模式（Phase 4）** | SQL 是 Agent 已经熟悉的接口（pgvector/Qdrant 都用 SQL），gRPC 会增加客户端学习成本。Phase 1.3 实现最小子集仅需 ~500 行 Rust 代码。 |
```

**理由**：
- PG Wire 最小子集（Simple Query 模式 + 基本类型映射）实现成本 < gRPC
- Agent 客户端（Python/TS）有现成的 PG 驱动可用
- 避免 Phase 2 → Phase 4 切换协议栈
- 与改动 3（API 改 SQL）配套

---

#### 改动 13：加"开源准备"节点

**位置**：在 Phase 4 末尾加一节，或在里程碑表里加一行

**新内容**：

```markdown
### Phase 4 末：开源 Alpha 准备

**为什么是这个节点：**
- Phase 4 完成意味着"一条 SQL 完成混合召回"是核心 demo，最适合对外
- 早期用户反馈能在 Phase 5/6 进入开发前修正

**准备清单：**
- README + CONTRIBUTING + LICENSE（Apache 2.0 或 MIT）
- CI/CD 跑通（GitHub Actions：rust check + cargo test + benchmark）
- API 文档自动生成（cargo doc + mdbook）
- 性能 benchmark 公开可复现（标准数据集 + 脚本）
- 受众声明（Apple Silicon only + Qwen3-30B-A3B gguf 风格明确范围）

**不做的事（避免过度承诺）：**
- 不承诺跨平台
- 不承诺分布式
- 不承诺 PostgreSQL 完全兼容
```

---

### 应用顺序建议（按 ROI 从高到低）

| # | 改动 | 工作量 | 影响 |
|---|---|---|---|
| 1 | 加时间估算（改动 1） | 30 min | 把 roadmap 从"愿景"变成"计划" |
| 2 | Phase 2 拆 2a/2b/2c（改动 2） | 1 h | 解决 long pole 的延期风险 |
| 3 | Phase 2 API 改 SQL（改动 3） | 30 min | 避免 Phase 2→4 重写客户端 |
| 4 | Lock Manager 术语（改动 4） | 15 min | 修正技术错误 |
| 5 | Snapshot 机制说明（改动 5） | 20 min | 提前避免实现返工 |
| 6 | SegmentedStorage 接口（改动 6） | 20 min | 避免 Phase 3 破坏 Layer 1 抽象 |
| 7 | Phase 5 SDK/内核拆分（改动 7） | 30 min | 明确 Layer 边界 |
| 8 | Phase 7 拆 7a/7b/7c/7d（改动 8） | 1 h | 避免 Phase 7 必延期 |
| 9 | 风险登记补 5 条（改动 9） | 20 min | 文档完整性 |
| 10 | Phase 1.1 性能基线改（改动 10） | 10 min | 现实化目标 |
| 11 | Phase 1.3 查询统计（改动 11） | 5 min | 调优基础 |
| 12 | 连接协议改 PG Wire（改动 12） | 10 min | 客户端友好 |
| 13 | 开源准备节点（改动 13） | 30 min | 对外节奏 |

**合计工作量：约 5-6 小时**

---

### 本节与前几节的关系

- §一-§二十是**按 Phase 分组**的改动清单（适合先 review Phase 1 全貌）
- §二十一是**§一-§二十的执行追踪**（哪些已应用）
- §二十二是**上一轮 review 的增补**
- 本节（§二十三）是**按 ROI 分组**的最新一轮 review（适合直接拍板执行顺序）

**建议执行路径**：先用本节（§二十三）的 ROI 排序确定执行顺序，再回到对应 Phase 章节对照修改；最后在 §二十一 A 表追加本次改动的追踪记录。