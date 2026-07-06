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
