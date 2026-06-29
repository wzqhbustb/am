# 多模态统一内核架构设计

> 把 TP / AP / 向量 / 全文 / 图 / 时序 内化到一套数据库内核的 7 个架构层面设计决策

**姊妹篇**：
- `planning.md` —— 项目管理与分阶段实现路径
- `agent-native-db-architecture.md` —— 整体架构与子系统设计
- **本文** —— 专门解决「在数据库内核层面,如何把多种数据形态承载到同一套内核中」

---

## 一、核心矛盾

AI Agent 对数据库的使用场景覆盖六大数据形态,每种的最优存储结构**互相矛盾**:

| 数据形态 | 最优存储结构 | 典型引擎 |
|---|---|---|
| **TP**(OLTP 短事务) | 行存 + B+Tree + 低延迟 | Postgres、MySQL |
| **AP**(OLAP 分析) | 列存 + 投影 + 向量化 | DuckDB、ClickHouse |
| **向量**(语义检索) | 图结构(HNSW)+ 大块连续内存 | Milvus、Qdrant |
| **全文**(关键词搜索) | 倒排表 + postings list | Elasticsearch |
| **图**(关系推理) | 邻接表 + 属性存储 | Neo4j、Memgraph |
| **时序**(行为轨迹) | 时间分区 + 降采样 + TTL | TimescaleDB、InfluxDB |

**传统思路**:每个场景一个独立引擎 → Agent 一次写入要同步 N 个系统 → 事务原子性 / 崩溃一致性 / 可观测性全撕裂。

**pg_rust 的核心命题**:**能不能用一套内核把这些全承载?**
答案不在「一个数据库里塞 N 个引擎」,而在「**一套事务内核 + N 种访问方法**」——六大数据形态只是行版本上的不同投影/索引,不是六个独立存储引擎。

下面 7 个设计决策,是把这件事做成的根。

---

## 二、7 个架构层面设计决策

### 决策 1:解耦"存储引擎"和"访问方法"

**传统 DB**:表 + 索引紧耦合。PG 的 heap + B+Tree 是一体的;pgvector 是 PG 进程里的一个 C 库,但跟 planner/executor 是「插件式」集成——pgvector 不知道其他访问方法的代价,其他访问方法也不知道 pgvector 的代价。

**pg_rust**:把存储引擎(行版本 + WAL + MVCC + buffer pool)和访问方法(HNSW / 倒排 / 图 / 时序)**彻底解耦**,通过 TID + XID 串起来。

**架构根因**:
- **行版本(row version)才是数据的「事实源」**
- 访问方法的条目只是「行版本上的索引视图」——HNSW 节点、倒排 postings、图节点、时序点,全都持有 `TID(page_id, slot_id) + XID`
- 行版本 GC 时,所有访问方法的条目一起 GC

**这意味着**:
- HNSW 的「删除节点」不是「在图里拆边」,而是「删行版本 → MVCC GC 顺着 TID 把 HNSW 条目一起带走」
- HNSW 事务回滚可以做到 per-tx delta 简单丢弃(见 architecture.md §3.4)——因为数据真相不在 HNSW 图里,而在行版本里

**代价**:每加一个新访问方法必须先回答「条目如何挂到 TID+XID 上」,这是设计契约,不能跳过。

---

### 决策 2:单一 WAL + 单一 LSN + 单一事务

这是把「多模态」变成「统一内核」而不是「多引擎拼装」的**根契约**。

不管走哪种访问方法,所有变更都进**同一个 append-only WAL**,共享**同一个单调 LSN 时钟**:

```
LSN 1001: Insert into agent_memory
LSN 1002: HnswAddNode for the new embedding
LSN 1003: Update inverted index for 'contract dispute'
LSN 1004: Commit
```

**具体落地**:
- 每种访问方法有自己的 page format(HNSW 节点文件、postings file、邻接文件、时序分区)
- 但所有变更通过**统一的 WAL 记录类型**进入同一个 log
- 事务协调器对所有访问方法**统一 commit / rollback**

**这个设计的代价**:每加一个新访问方法(HNSW、倒排、图、时序),必须先回答三个问题——写哪种 WAL 记录、如何参与 checkpoint、如何 redo/undo。planning.md §四.4.1 末尾的设计原则就是这个。

**这是 pg_rust 区别于「PG + pgvector + pgai + pg_textsearch 外挂拼接」的核心机制**——后者每个扩展都有自己的 WAL(实际上是绕过 PG 的 WAL 直接写文件),崩溃恢复靠各自重建,事务原子性靠应用层补偿。

---

### 决策 3:TID + XID 作为「统一数据寻址」

这是连接存储层和访问方法层的**协议**。不是新发明的(借鉴 PG),但要在内核契约里显式声明:

```
AccessMethodEntry {
    tid: Tid,    // (page_id, slot_id) → row version
    xid: Xid,    // MVCC visibility
    payload: ... // 访问方法特有数据(向量、token、邻接边、时序点)
}
```

**关键洞察**:TID+XID 让「行版本 GC」和「访问方法条目回收」变成**同一件事**,不需要每个访问方法自己实现一套 GC。

**对应 pg_rust 的设计**:
- `TupleHeader` 预留 `xmin` / `xmax` / `ctid` / `lsn`(从 day 1 即预留,即使 v0 不实现 MVCC,见 architecture.md §3.1)
- HNSW 节点条目持有 `TID + XID`(planning.md §五.3 Phase 1a 末 / Phase 1b 升级)
- 倒排 postings、图节点、时序点同理

---

### 决策 4:跨模态 Cost-Based Optimizer

这是 pg_rust 区别于 PG **最深**的一层,也是**最难**的一层。

**PG 的 planner 不知道的事**:
- HNSW 近似 KNN 的代价(图遍历步数 + 距离计算)
- BM25 评分的代价(postings 合并 + 评分)
- 图遍历的代价(递归层数 + 边遍历)
- 向量压缩(SQ/PQ)的代价(distortion error + 解码 CPU)

**PG 只能把 HNSW 当成「黑箱」,执行时 EXPLAIN 也只能给出「HNSW scan returned N rows」这种粒度**,没法联合 cost model。

**pg_rust 必须设计**:
- 一个**跨模态的成本模型**,能同时估算 TP / AP / 向量 / 全文 / 图的代价
- 能在统一 SQL 里做**联合优化**——比如 `ORDER BY embedding <=> $1 + ts_rank(...)` 的 RRF 融合,应该用 HNSW + 倒排的 candidate set 做加权融合,而不是分两个查询再 union
- 优化器决策可解释(`EXPLAIN FORMAT JSON` 给出每条访问路径的代价分解)

对应 architecture.md §4.2 的 RAG 检索流程:planner 拆分查询(结构化 → B+Tree / 时序 → 时序索引 / 全文 → 倒排 / 向量 → HNSW),各索引返回候选 TID,按 RRF 融合,再 MVCC 可见性过滤,最后回表。

**这一层是 pg_rust 的真正杠杆点**。PG 做不到,因为它的 optimizer 是「PG 内核 + 几个独立扩展」,跨扩展的 cost model 没法联合设计——pgvector 的作者在 PG 邮件列表里抱怨过这个问题,但 PG 扩展 API 的结构性约束让它没法解决。

---

### 决策 5:多模态执行器:行迭代 + 向量化 + 图遍历 混合

DataFusion 的向量化执行器对 AP 友好,对 HNSW(图遍历)和图(递归)不友好。一个统一执行器需要**支持三种执行范式**并能在同一查询里切换:

| 范式 | 适合场景 | 例子 |
|---|---|---|
| **行迭代(Volcano)** | TP、图遍历、点查 | `SELECT * FROM orders WHERE id = 1` |
| **向量化(Arrow batch)** | AP、列存投影、聚合 | `SELECT region, sum(amount) FROM sales GROUP BY region` |
| **图遍历 + 评分** | 向量(HNSW)、全文(BM25)、图(Cypher) | `ORDER BY embedding <=> $1 LIMIT 10` |

**执行器需要在 operator 层做模式切换**——比如 RRF 融合算子能同时消费 HNSW 的图遍历结果和倒排的 postings list,并在中间用 Arrow batch 做融合。

**当前 pg_rust 的状态**:用 DataFusion 做执行器(见 planning.md §六.4),但 DataFusion 是 AP-only(向量化),TP 路径用 Volcano,向量路径用 crate(HNSW 原型)。**显式的「多范式执行器」架构没规划**,是 Phase 1b / Phase 2 需要补的。

---

### 决策 6:共享 Buffer Pool + 跨模态缓存

每种访问方法的 page format 不同,但都进**同一个 buffer pool**:

- 行存页(8KB)
- HNSW 节点页(变长)
- 倒排 postings 页
- 图邻接页
- 时序分区页

**buffer pool 统一做**:
- LRU/CLOCK 替换策略
- WAL 协调(page_lsn 与 WAL record_lsn 的对比)
- Checkpoint(同一帧可被不同访问方法复用)
- pin/unpin 协议(避免 evict 被使用中的页)

**访问方法的差异在 page format 层抹平**,统一在 frame 抽象里。Phase 1a 里程碑 1a.3(Buffer Pool 完整化,planning.md §四 Track A)正是这件事的落地。

---

### 决策 7:一等公民类型 + 多模态 Schema

`AGENT_ID` / `TRACE_ID` / `SESSION_ID` / `VECTOR(n)` / `TIMESTAMP` 作为一等公民类型,自动参与:

- **provenance**:每行写入者身份,写入 Tuple Header(见 architecture.md §3.1)
- **query trace**:SQL 生命周期记录,按 TRACE_ID 聚合
- **RLS 策略**:多 Agent 隔离(Phase 2+ 评估)
- **索引路由**:向量列自动用 HNSW、文本列自动用倒排、时间列自动用时序分区

**这是 Agent Native 的元层能力**——把「多模态」从「数据形态」提升到「类型系统」层级。

planning.md §二.2.1 的 DDL 示例已经体现了:

```sql
CREATE TABLE agent_memory (
    id          TEXT PRIMARY KEY,
    content     TEXT,
    embedding   VECTOR(1536),
    metadata    JSONB,
    tags        TEXT[],
    created_by  AGENT_ID,       -- 一等公民:Agent 身份
    session_id  TRACE_ID,       -- 一等公民:会话追踪
    created_at  TIMESTAMP DEFAULT now()
) WITH (
    vector_index = 'hnsw',
    fulltext_index = 'bm25',    -- 同样声明式
    ts_partition = 'day',       -- 同样声明式
    provenance = true
);
```

**这种 schema 声明式地把多模态索引语义化**——`WITH (vector_index = 'hnsw', fulltext_index = 'bm25', ts_partition = 'day')` 不是 PG 的扩展 API 拼接,而是**内核元数据**,optimizer 一开始就知道有哪些索引可用。

**当前 pg_rust 的状态**:
- ✅ DDL 示例已写
- ✅ `VECTOR(n)` / `AGENT_ID` / `TRACE_ID` 类型已规划
- ⚠️ **`fulltext_index` / `ts_partition` 这种声明式元数据没显式规划**
- ⚠️ **optimizer 自动路由(基于列类型 + WITH 子句)没规划**

---

## 三、串起来

> **pg_rust 不是「Postgres + pgvector + pgai + pg_textsearch」的外挂拼接,而是把「行版本 = 数据真相 / 访问方法 = 索引视图」作为架构根因,通过 TID+XID 协议 + 单一 WAL + 跨模态 optimizer,把 TP/AP/向量/全文/图/时序全部纳入一套内核契约。**

这就是为什么 architecture.md §1.1 要画成「统一事务内核 + N 种访问方法」,而不是「PG + N 个扩展」。

**7 个决策的依赖关系**:

```
决策 1(解耦) ─┐
决策 2(单一 WAL) ─┼─→ 决策 3(TID+XID 协议) ─→ 决策 4(跨模态 optimizer)
决策 7(一等公民类型) ─┘                                      │
                                                             ↓
                                          决策 5(多范式执行器)
                                                             │
                                                             ↓
                                          决策 6(共享 Buffer Pool)
```

**根是决策 1+2+3**(存储与访问方法的解耦 + 统一 WAL + TID+XID 协议)——没有这三件事,后面四个决策都是空话。

---

## 四、pg_rust 当前到位情况

| 决策 | 现状 | 缺口 |
|---|---|---|
| **1. 解耦存储引擎和访问方法** | ✅ architecture.md §1.1 + §3.3 已设计 | 无 |
| **2. 单一 WAL + LSN + 事务** | ✅ planning.md §五.2 已规划 | 跨访问方法的 WAL 记录类型在 §五.2 表格里只列了 HNSW,缺倒排 / 图 / 时序 |
| **3. TID+XID 统一寻址** | ✅ architecture.md §3.3 + §3.4 已写 | Phase 1b 才完整落地 TID+XID,Phase 1a 是 per-tx delta |
| **4. 跨模态 Cost-Based Optimizer** | ⚠️ **缺** | architecture.md §4.2 提到 RAG 流程,但 optimizer 的 cost model **没显式规划** |
| **5. 多模态执行器** | ⚠️ **缺** | 当前用 DataFusion(AP-only),多范式(行迭代 + 向量化 + 图遍历)架构未规划 |
| **6. 共享 Buffer Pool** | ✅ planning.md §四.4.1 已规划(单组件) | 跨访问方法的 page format 在同一 frame 抽象里的具体设计待 RFC |
| **7. 一等公民类型 + 多模态 Schema** | ⚠️ **部分** | DDL 示例有,`fulltext_index` / `ts_partition` 等声明式元数据未规划;optimizer 自动路由未规划 |

**最缺的 3 块**:
1. **跨模态 optimizer 的 cost model**(决策 4)——这是 PG 做不到的核心
2. **多模态执行器的范式切换**(决策 5)——DataFusion 是 AP-only
3. **一等公民类型的 optimizer 路由**(决策 7)——`AGENT_ID` / `EMBEDDING` 自动参与 cost model 的能力没明说

这三个缺位是真正让「pg_rust ≠ PG + pgvector + pgai + pg_textsearch」的根,也是后续 RFC 该重点写的。

---

## 五、后续 RFC 路线

按优先级排列:

| RFC | 何时启动 | 核心问题 |
|---|---|---|
| **RFC-1:跨模态 Cost Model** | Phase 1a 末(MVCC + HNSW 跑通后) | HNSW / 倒排 / B+Tree / 时序的代价如何统一表达?RRF 融合算子的 cost 怎么估? |
| **RFC-2:多范式执行器** | Phase 1b | DataFusion 的向量化 + Volcano + 图遍历如何在同一查询里切换?RRF 融合算子的内部实现? |
| **RFC-3:一等公民类型系统** | Phase 1a(类型定义早做) | `AGENT_ID` / `VECTOR(n)` 的 catalog 元数据 / optimizer 路由 / 索引自动绑定如何设计? |
| **RFC-4:多模态 Schema 元数据** | Phase 1b | `WITH (vector_index=..., fulltext_index=..., ts_partition=...)` 在 catalog 里如何表达? |
| **RFC-5:共享 Buffer Pool 跨访问方法** | Phase 1a(里程碑 1a.3) | 不同访问方法的 page format 如何在同一 frame 抽象里表达?替换策略是否需要分访问方法调优? |

**写作原则**:这些 RFC 应当基于真实跑通的代码瓶颈来设计,**不要在 Phase 0 阶段提前写完**——纸上谈兵的 cost model 在真实数据上经常站不住脚。

---

## 六、与现有文档的关系

| 文档 | 关注点 | 与本文的关系 |
|---|---|---|
| `planning.md` | 项目管理 / 阶段 / 里程碑 / 竞品 | 本文是它的「架构哲学姊妹篇」,补 planning.md 在「跨模态优化」层面的缺口 |
| `agent-native-db-architecture.md` | 子系统设计 / 协议层 / 存储层 / 索引层 | 本文是它的「内核根因篇」,回答 architecture.md 没显式展开的「为什么这套架构能成立」 |
| `pdf-2.1.2-agent-native-database-pg_rust.md` | §2.1.2 节核心论点梳理 | 本文是对它 P12「同进程统一执行器」论点的工程级展开 |

**本文的定位**:**架构哲学层**,不是新规划,不是新里程碑,只是把现有规划里隐含的 7 个根决策显式说出来,作为后续 RFC 启动时的「决策参考」。

---

## 七、一句话总结

> **如果 pg_rust 的世界级命题是「一套内核 = 六大数据形态」,那这 7 个决策就是让这件事从愿景变成工程的根——前 3 个是契约(必须先做对),后 4 个是优化(可逐步演进)。任何 Phase 的实现,都应当先回到这 7 个决策上自检:这一步到底在做哪个决策?这个决策的代价是否被接受?**

---

**修订记录**:
- 2026-06-29:初版,基于与 Mavis 讨论的 7 个架构决策整理