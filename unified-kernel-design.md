# 统一内核架构设计:在单一引擎内承载 TP / AP / 向量 / 全文 / 图 / 时序

> 目标:回答"如何用一套数据库内核承载 AI Agent 时代的多种数据场景"这一核心架构问题,把 planning.md 的方向和 agent-native-db-architecture.md 的设计进一步落到代码可实现的层。

---

## 一、核心矛盾

六种场景对存储、计算、并发的需求根本不同:

| 场景 | 存储偏好 | I/O 模式 | 并发特征 | 索引结构 |
|---|---|---|---|---|
| TP | 行存,定长/变长混合 | 随机点读写 | 高并发短事务 | B+Tree |
| AP | 列存,压缩 | 大范围顺序扫描 | 少量长查询 | 列块 + zone map |
| 向量 | 高维浮点数组 | 随机图遍历 | 读多写少(图写要改连接) | HNSW / IVF |
| 全文 | 倒排 posting list | 顺序读 posting + 随机回表 | 读多写少 | 倒排索引 |
| 图 | 邻接表 | 多跳随机遍历 | 读写混合 | 邻接索引 |
| 时序 | 追加式分区 | 范围扫描 + 降采样 | 高吞吐追加 | 时间分区 + 聚合 |

传统"N 个引擎拼在一起"的做法(PG + pgvector + ES + Neo4j + TimescaleDB)破坏了事务原子性和崩溃一致性。

**核心问题**:存在什么样的内核抽象,能让这些不同的需求共存于一个引擎内,且不退化成"在一个进程里跑 6 个数据库"?

---

## 二、第一性原理:三层分离

把存储内核分成三个正交层,每层独立演进:

```
┌─────────────────────────────────────────────────┐
│  Layer 3: Access Methods (索引/投影/视图)        │  ← 场景差异在这里
├─────────────────────────────────────────────────┤
│  Layer 2: Visibility & Transaction (可见性契约)  │  ← 统一的 MVCC 语义
├─────────────────────────────────────────────────┤
│  Layer 1: Physical Storage (页/WAL/I/O)          │  ← 统一的持久化基础设施
└─────────────────────────────────────────────────┘
```

### Layer 1: 物理存储层(所有场景共享)

这一层提供场景无关的原语:

- **Page Allocator**: 分配固定大小页(8KB / 16KB / 64KB 可配置),不关心页里装什么
- **WAL Writer**: append-only 日志,接受任意 `(record_type, payload)`,保证 fsync 语义
- **Buffer Pool**: `page_id → in-memory frame` 的映射 + eviction
- **LSN Clock**: 全局单调递增,所有组件共享
- **Checkpoint Coordinator**: 定期把 dirty page 持久化,截断 WAL

**关键设计**:Page Allocator 不假设"页里面是行"。一个 page 可以是:

- 行存 slotted page(TP)
- 列存 compressed block(AP)
- HNSW 邻居列表 page
- 倒排 posting list page
- 图邻接表 page
- 时序 chunk page

它们在物理层面都是"带 `page_lsn` 的 8KB 块",Buffer Pool 统一管理,WAL 统一记录。

```rust
trait PageFormat: Send + Sync {
    fn page_type(&self) -> PageType;
    fn page_lsn(&self) -> Lsn;
    fn set_page_lsn(&mut self, lsn: Lsn);
    fn serialize(&self, buf: &mut [u8; PAGE_SIZE]);
    fn deserialize(buf: &[u8; PAGE_SIZE]) -> Self;
}
```

### Layer 2: 可见性与事务层(所有场景共享)

这是真正统一六种场景的关键抽象。核心洞察:

> **所有数据形态的"源事实"都是 Tuple(行),所有索引/投影都是 Tuple 的派生视图。**

一个 Tuple 由 `TID (page_id, slot_id)` 唯一标识,由 `(xmin, xmax, snapshot)` 决定可见性。

**Visibility Oracle**——一个共享服务,任何访问方法都可以查询:

```rust
trait VisibilityOracle: Send + Sync {
    fn is_visible(&self, xmin: TxId, xmax: TxId, snapshot: &Snapshot) -> bool;
    fn current_snapshot(&self) -> Snapshot;
    fn register_index(&self, index_id: IndexId, watermark_lsn: Lsn);
    fn advance_watermark(&self, index_id: IndexId, applied_lsn: Lsn);
}
```

**关键设计决策**:索引条目不自己判断可见性,而是存 TID,回表后由 Visibility Oracle 统一判断。

| 索引 | 叶子条目 |
|---|---|
| B+Tree | `(key, TID)` |
| HNSW | `(vector, TID)` |
| 倒排 | `(term, posting: TID list + tf + position)` |
| 图邻接 | `(edge_id/TID, target_node_TID)` |
| 时序 | `(timestamp_bucket, TID list)` |

所有索引回表后,统一执行 `visibility_oracle.is_visible(tuple.xmin, tuple.xmax, snapshot)`,不可见则跳过。

**好处**:

- 六种索引不需要各自实现 MVCC 逻辑
- GC 统一:行版本被 vacuum 时,通知所有引用该 TID 的索引删除条目
- 新增一种访问方法时,不需要重新实现事务语义

**代价与缓解**:

- 回表开销(从索引拿到 TID 后要读行确认可见性)
- 对 HNSW 等"近似"结构,会出现"索引返回 K 个候选,可见性过滤后不足 K 个"的问题
- 缓解:对 HNSW 多取 2x 候选,可见性过滤后 top-K;Phase 1b 升级为 TID+XID 索引条目,避免回表
- 对 AP 列存投影:投影本身是最新快照的物化视图,不存旧版本,读取时不需要回表

### Layer 3: Access Methods(场景差异在这里)

这一层是**可插拔的**。每种访问方法实现统一的 trait:

```rust
trait AccessMethod: Send + Sync {
    type ScanState;
    type Key;

    fn am_type(&self) -> AmType; // BTree | Hnsw | Inverted | Graph | TimeSeries | Columnar

    // 索引维护(由事务协调器在 commit / rollback 时调用)
    fn insert(&self, tid: Tid, key: &Self::Key, txn: &Transaction) -> Result<()>;
    fn delete(&self, tid: Tid, txn: &Transaction) -> Result<()>;

    // 扫描
    fn begin_scan(&self, predicate: &AmPredicate, snapshot: &Snapshot) -> Self::ScanState;
    fn next(&self, state: &mut Self::ScanState) -> Option<Tid>;

    // WAL 参与
    fn wal_record_types(&self) -> &[WalRecordType];
    fn redo(&self, record: &WalRecord) -> Result<()>;
    fn undo(&self, record: &WalRecord) -> Result<()>;

    // Checkpoint 参与
    fn dirty_pages(&self) -> Vec<PageId>;
    fn checkpoint_complete(&self, checkpoint_lsn: Lsn);

    // GC 协作
    fn reclaim_tid(&self, tid: Tid) -> Result<()>; // 收到 vacuum 通知后调用
}
```

每种访问方法自由决定内部存储格式,只要:

1. 通过 Layer 1 的 Page Allocator 申请页
2. 变更时写 Layer 1 的 WAL
3. 返回的结果是 TID,由 Layer 2 做可见性过滤

这就是"一个事务内核 + N 种访问方法"在代码层面的真正含义——不是概念,是一组 trait 约束。

---

## 三、四个深层架构创新

三层分离是骨架,但真正让六种场景"舒服地共存"还需要以下四个创新。

### 创新 1:分级同步模型(Tiered Freshness)

不是所有索引都需要在事务提交时同步更新。定义三级:

| 级别 | 含义 | 适用索引 | 延迟 |
|---|---|---|---|
| Tier 0(同步) | 事务提交前必须完成 | 主键 B+Tree、行存 | 0 |
| Tier 1(事务内异步) | 事务提交时批量合并 | HNSW per-tx delta、二级 B+Tree | 0(对外)但写入路径可 batch |
| Tier 2(后台异步) | 后台任务追赶,允许有界滞后 | 列存投影、全文倒排、时序聚合 | 秒~分钟级 |

**为什么这很重要**:如果 INSERT 一行要同步更新 6 个索引,写入吞吐会崩。Tier 2 异步让"写入路径"只需要:写 WAL + 更新行存 + 更新主键 B+Tree + 合并 HNSW delta。全文、列存、时序后台追赶。

**一致性保证**:

- Tier 0 / 1:commit 后即可见(强一致)
- Tier 2:提供 `await_index_fresh(index_name, target_lsn)` 接口;或查询时自动检测索引 watermark < 查询 snapshot LSN 时,fallback 到 seq scan 补全

**实现**:后台 worker 消费 WAL 流(类似 CDC consumer),维护每个 Tier 2 索引的 `applied_lsn`。Visibility Oracle 暴露 watermark 信息,Planner 据此判断索引是否可用。

### 创新 2:统一的多路检索融合(Multi-Path Fusion)

Agent 的典型查询同时涉及结构化 + 向量 + 全文:

```sql
SELECT * FROM memory
WHERE team = 'sales' AND created_at > '2026-06-01'
  AND content @@ 'contract'
ORDER BY embedding <=> $vec
LIMIT 10;
```

传统数据库做法是:优化器选"最好的一条路径"(要么走索引 A,要么走索引 B)。但混合检索需要**多路并行 + 融合**:

```
           ┌── B+Tree(team='sales') ──→ TID set A
           │
Query ─────┼── Inverted(content @@ 'contract') ──→ TID set B  ──→ Fusion ──→ Top-K
           │
           └── HNSW(embedding <=> $vec, K=50) ──→ TID set C
```

Fusion 策略:

- **过滤式**:`A ∩ B ∩ C`(适合结构化/全文是硬约束)
- **RRF 排序式**:各路径独立排序后按 Reciprocal Rank Fusion 合并(适合软排序)
- **混合式**:结构化/全文作硬过滤,向量作排序

**架构要求**:执行引擎(DataFusion)需要一个 `MultiIndexScan` 算子,能:

1. 并行启动多个 AccessMethod scan
2. 按配置的 fusion strategy 合并 TID
3. 统一做 visibility check
4. 回表取完整行

这不是 DataFusion 现有能力,需要自研算子。但 DataFusion 的 `ExecutionPlan` trait 足够灵活,可以插入自定义算子。

### 创新 3:行格式的"胖 header + 瘦 payload"设计

六种场景对行格式的需求:

- TP:定长列紧凑排列,快速随机访问
- 向量:大块连续浮点数组(1536×f32 = 6KB),不适合和标量混存
- JSONB:变长、可能很大
- 图边:from / to / type / properties,properties 可能是 JSONB
- 时序事件:timestamp + payload

**设计决策**:主行存 slotted page 只存"胖 header + 定长标量列 + 列指针",大对象/向量/JSONB 存在 TOAST-like 的溢出页。

```
Tuple Layout (in slotted page):
┌──────────────────────────────────────────────────┐
│ TupleHeader (xmin/xmax/cid/ctid/lsn/agent_id/...) │  ← 固定 ~48 bytes
├──────────────────────────────────────────────────┤
│ Fixed-width columns (INT, FLOAT, TIMESTAMP, ...)   │  ← 紧凑,O(1) 访问
├──────────────────────────────────────────────────┤
│ Varlen pointers (TOAST pointer for JSONB/VECTOR/TEXT) │  ← 4-8 bytes each
└──────────────────────────────────────────────────┘

Overflow pages (TOAST):
┌──────────────────────────────────────────────────┐
│ VECTOR(1536) = 6144 bytes raw float32             │  ← 独立页
│ JSONB payload                                     │  ← 独立页(可跨多页)
└──────────────────────────────────────────────────┘
```

**好处**:

- 行存 slotted page 保持高密度,TP 点查快
- 向量数据在物理上是连续的,HNSW 遍历时 cache 友好
- JSONB 不会把行存页撑爆
- Buffer Pool 可以对不同 page type 做不同 eviction 策略(向量页 pin 更久)

**对 HNSW 的影响**:HNSW 索引条目只存 `TID + 向量的 TOAST page_id`,遍历时先从 HNSW 页拿到邻居的 vector(通过 TOAST pointer 直读溢出页),距离计算完成后再通过 TID 回表。

### 创新 4:WAL 的"逻辑 + 物理"双模式

不同访问方法对 WAL 的需求不同:

- **行存 / B+Tree**:需要物理 WAL(page-level redo/undo),因为要精确恢复页状态
- **HNSW**:更适合逻辑 WAL(record "add node X with vector V and neighbors [a,b,c]"),因为图结构重建比 page-level replay 更可靠
- **列存投影**:不需要自己的 WAL,从行存 WAL 派生即可(Tier 2 异步)
- **全文倒排**:逻辑 WAL(record "add posting (term, tid)")

**设计**:WAL 记录分两类:

```rust
enum WalRecord {
    // 物理记录:精确描述页变更(行存 / B+Tree 用)
    Physical {
        page_id: PageId,
        offset: u16,
        before_image: Vec<u8>,
        after_image: Vec<u8>,
    },

    // 逻辑记录:描述高层操作(HNSW / 倒排 / 图 用)
    Logical {
        am_type: AmType,
        operation: Vec<u8>,  // operation 由 AM 自行序列化
    },
}
```

**恢复策略**:

- Physical record:直接 redo/undo 到页
- Logical record:调用对应 AM 的 `redo()` / `undo()` 方法,AM 自行决定如何重建

**好处**:

- 行存 / B+Tree 用成熟的物理 WAL,性能确定
- HNSW 等新型结构用逻辑 WAL,避免图状态的"部分页恢复"导致图不一致
- 新增 AM 时,只需实现 `redo()` / `undo()`,不需要理解物理页布局

---

## 四、完整图景

把四个创新画在一起:

```
┌────────────────────────────────────────────────────────────────────┐
│                    Query Engine (DataFusion + 自研算子)              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────┐  │
│  │ B+Tree   │ │ HNSW     │ │ Inverted │ │ Graph    │ │ TS / KV │  │
│  │ Scan     │ │ ANN Scan │ │ FTS Scan │ │ Traverse │ │ Range   │  │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬────┘  │
│       └─────────────┴─────────────┴─────────────┴───────────┘       │
│                         MultiIndexScan + RRF Fusion                  │
│                                    │                                 │
│                         Visibility Oracle (MVCC)                     │
│                                    │                                 │
│                              Row Store (回表)                        │
├────────────────────────────────────────────────────────────────────┤
│                 Tiered Freshness Controller                          │
│   Tier 0 (sync): Row + PK B+Tree                                    │
│   Tier 1 (tx-batch): HNSW delta + Secondary B+Tree                  │
│   Tier 2 (async): Columnar Projection + Inverted + TS Aggregation   │
├────────────────────────────────────────────────────────────────────┤
│              WAL (Physical + Logical dual-mode)                      │
│              Buffer Pool (unified page management)                   │
│              Page Allocator (type-agnostic 8KB pages)                │
│              LSN Clock (global monotonic)                            │
└────────────────────────────────────────────────────────────────────┘
```

---

## 五、边界与风险

**能做到的**:

- 单事务内同时更新行 + 多索引,ACID 保证
- 一条 SQL 融合向量 / 全文 / 结构化 / 图遍历
- 统一 GC(行版本删除时通知所有 AM 清理条目)
- 新增 AM 不需要改事务层
- 崩溃恢复:一套算法覆盖所有 AM

**可能的问题与缓解**:

1. **回表开销**:所有索引 → TID → 回表 → 可见性检查。对 HNSW 的 ANN 搜索,如果 top-100 候选中 30% 不可见,召回率会下降。缓解:HNSW 多取 + TID+XID 提前过滤(Phase 1b)。
2. **写放大**:一行数据可能同时 trigger 3-5 个索引更新。Tiered Freshness 缓解,但 Tier 0 / 1 仍有放大。
3. **AM 间优化器交互**:DataFusion 不知道"先过滤再向量搜索"比"先向量搜索再过滤"好(取决于过滤选择性)。需要自研 cost model。
4. **图遍历的事务语义**:多跳图遍历中间步骤看到的是同一 snapshot 吗?如果是 SI,整个遍历看同一快照;如果是 RC,每一跳看最新。需要明确语义。
5. **Tier 2 索引的水位线**:查询优化器必须能判断"用 Tier 2 索引结果"和"用行存 + filter"哪个更优,水位线是关键信号,实现复杂度不低。

---

## 六、与现有文档的关系

| 文档 | 本文档补充 |
|---|---|
| `agent-native-db-architecture.md` §3.3 Access Methods | 缺少 trait 定义 + Tiered Freshness 设计,本文 §二 Layer 3 + §三 创新 1 补全 |
| `agent-native-db-architecture.md` §3.2 One WAL | 缺少 Physical+Logical 双模细化,本文 §三 创新 4 补全 |
| `planning.md` §四.1 组件矩阵 | "Access Methods (B+Tree)" 应扩展为"Access Method Framework(框架 + 各 AM 实现)" |
| `planning.md` §2.3 范式融合表 | 缺"Multi-Path Fusion"这个执行层创新,应补一行 |
| `planning.md` §六.4 查询引擎 | "初期用 DataFusion" 应补充"自研 MultiIndexScan 算子是 Phase 1b 必须做的" |

---

## 七、Phase 0 落地映射

按本文设计,Phase 0 里程碑 0.3(v0 存储引擎)可进一步细化:

- **Layer 1**:实现 Page Allocator、WAL Writer(只支持 Physical record)、Buffer Pool(in-memory frame table + 简单 flush)、LSN Clock(`AtomicU64`)、Checkpoint Coordinator(stop-the-world 全量)
- **Layer 2**:实现 Visibility Oracle(single-version + LSN 即可,v0 不需要多版本)、Snapshot 构造
- **Layer 3**:实现 B+Tree AccessMethod trait 的最小子集(insert/scan/redo/undo),HNSW 作为 in-memory PoC(走独立路径,不接入 trait,Phase 1a 再接入)

Phase 1a 演进:

- Layer 2:多版本 + 可见性链 + GC,实现 reclaim_tid 协议
- Layer 3:HNSW 实现完整 AccessMethod trait(per-tx delta、redo/undo),接入事务协调器
- Tiered Freshness Controller 框架,先把 Columnar Projection 作为 Tier 2 实验田
- Multi-Path Fusion 算子原型(只在 HNSW + B+Tree 之间先跑通)

Phase 1b:

- WAL 升级为 Physical+Logical 双模式
- Inverted Index 作为 AccessMethod 实现
- Multi-Path Fusion 算子生产可用(RRF 策略)
- HNSW 升级为 TID+XID 索引条目(避免回表)
