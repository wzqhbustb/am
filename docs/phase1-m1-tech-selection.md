# Phase 1 M1 技术选型文档

> 本文档在写第一行代码之前，确定所有"看似小但影响全局"的技术决策。
> 每个决策点给出：选项 → 选择 → 理由 → 代价。

---

## 一、页管理（Page Allocator）

### 1.1 页大小

| 选项 | 优势 | 劣势 |
|------|------|------|
| 8 KB | 与 PG 一致；TP 点查友好；内存浪费少 | HNSW 大节点需跨页或 overflow |
| 16 KB | 减少 B+Tree 层数；HNSW 友好 | TP 小行场景浪费；与 PG 不一致 |
| 64 KB | HNSW/列存最优；单页可放完整 768d 向量+邻居列表 | TP 极度浪费；Buffer Pool 内存占用大（128MB 仅 2048 frame）；与主流数据库不一致 |

**选择：8 KB（默认），编译期常量可配置为 16 KB**

**理由：**
- 与 PostgreSQL 生态一致，便于对标和参考
- Agent 元数据场景以小行为主（JSON 字段走 TOAST）
- HNSW 邻居列表可用多页链表或 overflow page 解决（Phase 2 处理）
- 8 KB 是 Linux 文件系统和 SSD 扇区对齐的自然边界

**代价：** HNSW 节点（768d float32 = 3 KB 向量 + 邻居列表）可能需要 overflow page，Phase 2 额外设计。

### 1.2 页编号方案

| 选项 | 优势 | 劣势 |
|------|------|------|
| 全局连续 page_id（u64） | 简单统一；Buffer Pool 用单一 HashMap | 单文件过大时需拆分 |
| (file_id, page_offset) | 文件拆分自然 | 需要两级映射 |

**选择：全局连续 page_id（u64）**

**理由：**
- 简化 Buffer Pool 的 page_id → frame 映射
- 单一 page_id 空间让所有 AM（B+Tree、HNSW、倒排、图、时序）统一寻址
- 文件拆分作为存储层内部实现细节，对上层透明

**page_id 分配策略：**
- page_id 0 保留（不使用），超级块单独存于 `{data_dir}/superblock` 文件
- page_id 从 1 开始按需分配

### 1.3 空闲空间管理

| 选项 | 优势 | 劣势 |
|------|------|------|
| Freelist（链表） | 实现简单；分配 O(1) | 碎片化后遍历慢 |
| Bitmap | 批量分配友好；碎片可视 | 实现稍复杂；大数据库 bitmap 本身占空间 |

**选择：Freelist（链表），运行时内存维护 + checkpoint 持久化到 meta 文件**

**理由：**
- M1 阶段追求实现简单和正确性
- 分配/释放都是 O(1)
- PG 也用类似的 FSM（Free Space Map）机制
- 未来 Phase 7b 可升级为 bitmap（性能优化阶段）

**实现细节：**
- Freelist 的运行时表示在内存中（Vec<PageId>）
- PageAlloc 操作写 WAL（`PageAlloc` 记录），崩溃恢复时通过 replay WAL 重建 next_page_id（M1 freelist 实际为空，因为无 free 操作；M2 起 PageFree replay 才真正填充 freelist）
- Checkpoint 时将 freelist 快照写入 `meta/freelist.meta` 以加速恢复（恢复时先加载快照，再重放快照之后的 WAL）
- 分配：从内存 freelist 弹出；如果为空，扩展数据文件
- M1：无 free 路径（free_page 为 stub/no-op）
- M2：释放时压入内存 freelist + 写 PageFree WAL 记录
- M1 不使用数据页存储 freelist，避免为 freelist 页设计特殊格式

### 1.4 文件增长策略

**选择：按需扩展，扩展粒度 1 MB（128 个 8KB 页）**

**理由：**
- 避免预分配造成的磁盘浪费（Agent 场景初期数据量不确定）
- 1 MB 粒度平衡了系统调用频率和空间浪费
- fallocate() 在 Linux 上可预分配而不实际写入

---

## 二、WAL 设计

### 2.1 文件布局

| 选项 | 优势 | 劣势 |
|------|------|------|
| 单文件滚动（写到头再覆盖） | 简单 | 不利于归档和 PITR |
| 分段文件（wal-000001.log） | 归档友好；checkpoint 后删旧段 | 文件管理稍复杂 |

**选择：分段文件（wal-{segment_id:08}.log）**

**理由：**
- Checkpoint 后可直接删除/归档旧段文件
- 为 Phase 7a 的 WAL Shipping 和 PITR 做准备
- 实现复杂度增加有限（只是打开新文件）

**细节：**
- 每段文件大小：16 MB（可配置）
- 命名：`wal-00000001.log`、`wal-00000002.log`、...
- 当前活跃段写满后，创建新段
- Checkpoint 完成后，删除所有 LSN < checkpoint_lsn 的段

### 2.2 WAL 记录格式

```
┌────────────────────────────────────────────────────────────────┐
│ Record Header (固定 24 字节)                                    │
├────────────────────────────────────────────────────────────────┤
│ lsn: u64          (8B)  — 本记录的 LSN                         │
│ prev_lsn: u64     (8B)  — 同一事务的上一条记录 LSN（undo 链）    │
│ txn_id: u64       (8B)  — 事务 ID（0 表示非事务性操作）          │
├────────────────────────────────────────────────────────────────┤
│ Record Meta (固定 8 字节)                                       │
├────────────────────────────────────────────────────────────────┤
│ record_type: u8   (1B)  — 记录类型（见下方枚举）                 │
│ flags: u8         (1B)  — 标志位（FPI 标记等）                   │
│ payload_len: u16  (2B)  — payload 长度（最大 64KB）              │
│ crc32: u32        (4B)  — 整条记录（header + payload）的 CRC     │
├────────────────────────────────────────────────────────────────┤
│ Payload (变长，最大 65535 字节)                                  │
│ — 物理记录：page_id + offset + before_image + after_image       │
│ — 逻辑记录：am_type + operation（Phase 2+ 才有）                │
│ — 事务记录：commit/abort 标记                                   │
│ — Checkpoint 记录：ATT + DPT 快照                               │
├────────────────────────────────────────────────────────────────┤
│ Padding (0-7 字节，填充至 8 字节对齐)                            │
└────────────────────────────────────────────────────────────────┘

注：WAL 记录总长度按 8 字节对齐（padding 到最近 8 字节倍数），便于未来 O_DIRECT / SIMD
优化，也简化 LSN 计算（LSN 始终是 8 的倍数）。
```

**Record Types（M1 阶段需要的）：**

```rust
#[repr(u8)]
pub enum WalRecordType {
    // 物理页变更（M2 实现，M1 只定义枚举值）
    HeapInsert = 1,   // M2
    HeapUpdate = 2,   // M2
    HeapDelete = 3,   // M2
    BTreeInsert = 4,  // M2
    BTreeSplit = 5,   // M2
    BTreeDelete = 6,  // M2
    
    // Full Page Image（M1 实现）
    FullPageImage = 10,
    
    // 事务控制（M2 实现，M1 只定义枚举值）
    TxnBegin = 20,    // M2
    TxnCommit = 21,   // M2
    TxnAbort = 22,    // M2
    
    // Checkpoint（M1 实现）
    CheckpointBegin = 30,
    CheckpointEnd = 31,
    
    // 页管理（M1 实现 PageAlloc，M2 实现 PageFree）
    PageAlloc = 40,
    PageFree = 41,    // M2 实现（M1 无 free 路径）
    
    // 逻辑记录（Phase 2+ 预留，M1 不实现）
    LogicalHnsw = 100,
    LogicalInverted = 101,
    LogicalGraph = 102,
    LogicalTimeSeries = 103,
    
    // Segment 管理（Phase 3+ 预留）
    SegmentSeal = 110,
    SegmentMerge = 111,
}
```

### 2.3 LSN 与 WAL 偏移的关系

| 选项 | 优势 | 劣势 |
|------|------|------|
| LSN = 全局字节偏移 | 可直接定位到文件位置 | 跨段文件时计算复杂 |
| LSN = 逻辑序号（递增整数） | 简单；不与物理布局绑定 | 定位记录需要索引或扫描 |
| LSN = (segment_id, offset) | 直接定位 | 占 128 bit，浪费 |

**选择：LSN = 全局字节偏移（u64）**

**理由：**
- 给定 LSN 可直接计算：`segment_id = lsn / segment_size`，`file_offset = lsn % segment_size`
- 无需额外索引即可快速定位任意 WAL 记录
- PG 使用相同方案，验证充分
- u64 字节偏移可寻址 16 EB，永不溢出

**推导：**
- segment_size = 16 MB = 16,777,216 字节
- LSN 42,000,000 → segment_id = 2, file_offset = 8,445,568
- 文件名：`wal-00000003.log`（segment_id + 1），偏移：8,445,568

### 2.4 刷盘策略

| 选项 | 优势 | 劣势 |
|------|------|------|
| 每次 commit 立即 fsync | 强持久性 | 吞吐低 |
| Group commit（攒批刷盘） | 高吞吐 | 延迟增加 |
| 可配置（synchronous_commit） | 灵活 | 实现稍复杂 |

**选择：Group commit（默认），可配置为 per-commit fsync**

**理由：**
- Agent 场景写入频率高（embedding + 元数据），per-commit fsync 吞吐不可接受
- Group commit：攒 N 条记录或等 T 毫秒后批量 fsync
- 默认参数：最多攒 64 条或等 2ms，以先到者为准（配置项：`wal_group_commit_timeout`）
- Agent 工具调用场景对延迟敏感（一次调用可能包含多个 SQL），2ms 比 5ms 更合适
- Phase 7b 可根据 benchmark 进一步调优

**Group commit 实现概要：**
- WAL Writer 线程持有写缓冲区
- 事务 commit 时将 WAL 记录追加到缓冲区，挂起等待
- 满足条件（64 条 / 2ms）后执行一次 fsync
- fsync 完成后唤醒所有等待的事务

### 2.5 WAL 文件回收

**选择：Checkpoint 完成后删除旧段**

- Checkpoint 写入 `CHECKPOINT_END` 记录时记录 `redo_lsn`
- 所有 LSN < `redo_lsn` 的段文件可安全删除
- 考虑 Tier 2 worker 的 applied_lsn：只删除所有 consumer 都已消费完的段
- 删除前先 rename 到 `.recycled` 后缀，下一次需要新段时优先复用（避免频繁 create/unlink）

---

## 三、Buffer Pool

### 3.1 总容量

**选择：默认 128 MB（16384 个 8KB frame），可通过配置调整**

**理由：**
- Agent 记忆场景初期数据量不大，128 MB 足够 M1 验证
- 对标：PG 默认 shared_buffers 也是 128 MB
- 配置项名：`buffer_pool_size`

### 3.2 替换策略

| 选项 | 优势 | 劣势 |
|------|------|------|
| LRU | 实现简单；行为可预测 | 全表扫描会污染缓存 |
| CLOCK | 近似 LRU；无需维护链表 | 实现简单但精度略低 |
| LRU-K | 抗扫描污染 | 实现复杂 |

**选择：CLOCK（时钟替换算法）**

**理由：**
- 实现简单（一个环形数组 + 一个指针）
- 性能接近 LRU，无需维护双向链表
- 对 M1 的验证目标足够
- Phase 7b 可升级为 CLOCK-Pro 或 LRU-K

**实现概要：**
- 每个 frame 一个 `reference_bit: AtomicBool`
- 访问时设置 reference_bit = true
- 需要 evict 时，时钟指针扫描：reference_bit=true 则清零跳过，reference_bit=false 则选中 evict
- 附加条件：pin_count > 0 的 frame 不可 evict

### 3.3 并发方案

| 选项 | 优势 | 劣势 |
|------|------|------|
| 全局大锁 | 最简单 | 并发瓶颈 |
| 每 frame 一个 RwLock | 细粒度 | 锁对象太多，内存开销 |
| 分区锁（partition by page_id） | 平衡并发和复杂度 | 热点分区可能不均 |

**选择：分区锁（默认 256 个分区）+ 每 frame 一个轻量 RwLock**

**理由：**
- page_id → frame 映射表用分区 HashMap（256 个 shard）
- 每个 frame 内容保护用 `parking_lot::RwLock<Page>`
- 256 分区在高并发下冲突率更低（64 分区在 200+ 并发时 hash 碰撞概率升高）
- parking_lot 的 RwLock 比 std 更快（无 poisoning，更小的内存占用）
- trade-off：256 分区 × 每分区一个 Mutex ≈ 额外 ~8 KB 内存，可忽略

**具体结构：**
```
BufferPool {
    page_table: [Mutex<HashMap<PageId, FrameId>>; 256],  // page_id → frame 映射
    frames: Vec<Frame>,                                  // 预分配的 frame 数组
    clock_hand: AtomicU32,                              // 时钟指针
}

Frame {
    page_id: AtomicU64,          // 当前存放的页
    pin_count: AtomicU32,        // 引用计数
    dirty: AtomicBool,           // 脏标记
    reference_bit: AtomicBool,   // CLOCK 引用位
    page_lsn: AtomicU64,        // 该页最新的 LSN
    content: RwLock<[u8; PAGE_SIZE]>,  // 页内容
}
```

**锁顺序（必须遵守，否则死锁）：**
- 必须先获取分区锁（page_table shard），再获取 frame RwLock；反向获取会导致死锁
- Eviction 时：持有分区锁，扫描 frame 元数据（pin_count、reference_bit），但不得等待 frame RwLock（若 frame 被 pin 或写锁占用则跳过，继续时钟扫描）
- 多分区操作（极少见）：按分区编号升序获取，避免交叉死锁

### 3.4 Pin/Unpin 协议

**语义：**
- `pin(page_id) → PageGuard`：获取页的只读引用，保证该页不被 evict
- `pin_mut(page_id) → PageGuardMut`：获取页的可写引用，保证不被 evict
- `PageGuard` / `PageGuardMut` drop 时自动 unpin
- pin_count > 0 的页不可被 evict
- **写操作必须通过 pin_mut 获取**；pin 返回的 PageGuard 只读，不可升级为写（避免升级死锁）
- 如果需要先读后写，必须 unpin 读 guard 后重新 pin_mut（或一开始就用 pin_mut）

**实现：**
- pin 时：如果页在 Buffer Pool → pin_count += 1，返回 guard
- pin 时：如果页不在 → 选一个 victim frame（CLOCK），从磁盘读入，pin_count = 1
- unpin 时：pin_count -= 1
- RAII guard 保证 unpin 不会被遗忘

### 3.5 脏页跟踪

**选择：每 frame 一个 dirty flag（AtomicBool）**

**理由：**
- 修改页内容时设置 dirty = true
- Checkpoint 时扫描所有 frame，收集 dirty = true 的页
- 刷盘后 dirty = false
- 简单可靠，M1 足够

---

## 四、LSN 时钟

### 4.1 实现

**选择：AtomicU64，全局唯一实例，fetch_add 分配**

```rust
pub struct LsnClock {
    current: AtomicU64,
}

impl LsnClock {
    /// 分配下一个 LSN。仅由 WAL Writer 单线程调用。
    /// record_size 必须已按 8 字节对齐。
    /// 
    /// 设计说明：虽然使用 AtomicU64 实现，但 next() 仅由 WAL Writer 线程调用（单写者）。
    /// 使用 Atomic 是为了让 current() 可以被其他线程无锁读取。
    pub(crate) fn next(&self, record_size: u64) -> Lsn {
        debug_assert!(record_size % 8 == 0, "record_size must be 8-byte aligned");
        Lsn(self.current.fetch_add(record_size, Ordering::SeqCst))
    }
    
    /// 读取当前 LSN。可由任意线程调用。
    pub fn current(&self) -> Lsn {
        Lsn(self.current.load(Ordering::Acquire))
    }
}
```

### 4.2 LSN 类型

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const INVALID: Lsn = Lsn(0);
    pub const FIRST: Lsn = Lsn(8);  // 必须是 LSN_ALIGNMENT (8) 的倍数
    
    pub fn segment_id(&self, segment_size: u64) -> u64 {
        self.0 / segment_size
    }
    
    pub fn segment_offset(&self, segment_size: u64) -> u64 {
        self.0 % segment_size
    }
}
```

### 4.3 与其他组件的交互

- **WAL Writer**：写入记录前从 LsnClock 分配 LSN，写入后 LSN 成为该记录的唯一标识
- **Buffer Pool**：每个 frame 记录 page_lsn（该页最后一次修改对应的 WAL LSN）
- **刷页检查**：刷脏页前确认 WAL 已 fsync 到 ≥ page_lsn 的位置（WAL 先行规则）
- **Checkpoint**：checkpoint_lsn 标记恢复起点
- **Tier 2 Worker（接口预留）**：每个 worker 维护 applied_lsn，表示已追赶到的位置

---

## 五、文件布局与命名

### 5.1 目录结构

```
{data_dir}/
├── pg_rust.conf           # 配置文件
├── superblock             # 超级块（checkpoint LSN、版本号等）
├── data/
│   ├── base.dat           # 主数据文件（所有页按 page_id 顺序存储）
│   └── overflow.dat       # TOAST/overflow 数据文件（Phase 1 M2 引入）
├── wal/
│   ├── wal-00000001.log   # WAL 段文件
│   ├── wal-00000002.log
│   └── ...
├── meta/
│   ├── freelist.meta      # Freelist 持久化（checkpoint 时写入；M1 无 CRC，因 freelist 恒空；M2 起加）
│   ├── tables.meta        # 表元数据（表名、列定义、索引信息）
│   └── checkpoint.meta    # 最近 checkpoint 的 ATT + DPT
└── tmp/                   # 临时文件（排序溢出等）
```

### 5.2 超级块（Superblock）

固定存储在独立文件（而非 page_id=0），双副本原子更新：

```
superblock {
    magic: u32,               // 0x50475253 ("PGRS")
    version: u32,             // 格式版本号
    page_size: u32,           // 页大小
    padding: u32,             // 保留对齐用，使后续 64-bit 字段 8 字节对齐
    checkpoint_lsn: u64,      // 最近有效 checkpoint 的 LSN
    next_page_id: u64,        // 下一个可分配的 page_id（仅 checkpoint 时持久化）
    next_txn_id: u64,         // 下一个可分配的事务 ID（仅 checkpoint 时持久化）
    created_at: u64,          // 数据库创建时间戳
    checksum: u32,            // 超级块自身 CRC
}
```

**next_page_id 更新策略：**
- 运行时新页分配只推进内存值 + 写 WAL（PageAlloc 记录）
- next_page_id 只在 checkpoint 时持久化到超级块（避免超级块成为写入热点）
- 崩溃恢复时：从超级块加载 next_page_id，再 replay WAL 中的 PageAlloc 记录更新到最新值

**双副本更新策略：**
- superblock 文件包含 A/B 两个副本（各 512 字节）
- 更新时写非活跃副本 → fsync → 更新活跃标记
- 崩溃后选择 checksum 正确且 checkpoint_lsn 更大的副本

### 5.3 数据文件

**选择：单一数据文件（base.dat），页按 page_id 顺序存储**

**理由：**
- page_id N 在文件偏移 N * PAGE_SIZE 处，O(1) 定位
- 简单直接，M1 阶段无需多文件管理
- 文件增长时用 fallocate/ftruncate 扩展

**局限（后续解决）：**
- 单文件过大时（>1TB）某些文件系统性能下降 → Phase 7 引入多文件分片
- 不同 AM 的数据混在一个文件里 → 不影响正确性，只影响顺序扫描效率

---

## 六、错误处理与 I/O 模型

### 6.1 错误类型

**选择：thiserror 定义分层错误类型**

```rust
// Layer 1 错误
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Page {0} not found")]
    PageNotFound(PageId),
    
    #[error("Buffer pool full, no evictable frame")]
    BufferPoolFull,
    
    #[error("WAL corrupted at LSN {0}: CRC mismatch")]
    WalCorrupted(Lsn),
    
    #[error("WAL write failed: {0}")]
    WalWriteFailed(String),
    
    #[error("Checkpoint failed: {0}")]
    CheckpointFailed(String),
}
```

**原则：**
- 每层有自己的 Error enum
- 上层 Error 可 `From` 下层 Error
- 不使用 anyhow（编译期可知的错误类型更利于正确性）

### 6.2 I/O 模型

| 选项 | 优势 | 劣势 |
|------|------|------|
| 同步 pread/pwrite | 简单可控；崩溃语义明确 | 阻塞线程 |
| tokio::fs（线程池代理） | 与 async 生态一致 | 额外线程池开销；崩溃语义不透明 |
| io_uring | 最高性能 | Linux-only；实现复杂；M1 不需要 |
| O_DIRECT | 绕过 page cache，减少双缓冲 | 对齐要求严格；macOS 不支持 |

**选择：同步 pread/pwrite（M1 默认）；O_DIRECT 在 M1 不启用，Phase 7b 正式优化**

**理由：**
- M1 目标是正确性而非极致性能
- pread/pwrite 的 fsync 语义最清晰，崩溃恢复推理最简单
- 数据库自带 Buffer Pool，不需要操作系统 page cache 再缓存一层
- macOS 开发用普通 I/O，Linux 生产环境 Phase 7b 正式启用 O_DIRECT
- O_DIRECT 代码路径用 `#[cfg(target_os = "linux")]` 条件编译预留接口，M1 不实际启用
- Phase 7b 再考虑 io_uring

**线程模型：**
- WAL Writer：独立线程，持有 WAL 文件 fd，顺序写入 + fsync
- Buffer Pool I/O：使用 tokio::spawn_blocking 包装同步 pread/pwrite
- 这样上层仍可用 async/await，但 I/O 路径的崩溃语义明确

### 6.3 崩溃安全的文件操作

| 操作 | 崩溃安全策略 |
|------|-------------|
| 超级块更新 | 双副本 + 先写后标记 |
| WAL 追加 | append + fsync（追加天然崩溃安全） |
| 脏页刷盘 | WAL 先行规则保证；torn page 由 FPI 修复 |
| 数据文件扩展 | fallocate 扩展 + fsync 目录 |
| 元数据文件更新 | 写临时文件 → fsync → rename（原子替换） |

---

## 七、序列化方案

### 7.1 WAL 记录序列化

**选择：手写字节布局（固定 header）+ bincode（变长 payload）**

**理由：**
- Header（32 字节）是性能关键路径，手写避免序列化框架开销
- Payload 结构因 record_type 而异，用 bincode 保持灵活性
- bincode 零拷贝反序列化友好

### 7.2 页内数据序列化

**选择：纯手写字节布局**

**理由：**
- 页内结构（slotted page header、slot 数组、tuple data）是固定格式
- 性能关键路径，每次读页都要解析
- 手写保证零拷贝和最小开销
- 参考 PG 的 `PageHeaderData` 结构

**Slotted Page 布局（Phase 1 M2 实现，M1 只定义格式）：**

```
┌──────────────────────── 8 KB ────────────────────────┐
│ Page Header (固定 24 字节)                            │
│   page_lsn: u64                                      │
│   flags: u16                                         │
│   lower: u16 (slot 数组尾部偏移)                     │
│   upper: u16 (空闲空间起始偏移)                       │
│   special: u16 (特殊区域起始，如 B+Tree 用)           │
│   page_id: u64 (自身 page_id，用于校验)               │
├──────────────────────────────────────────────────────┤
│ Slot Array (从低地址向高地址增长)                      │
│   slot[0]: (offset: u16, length: u16, flags: u16)    │
│   slot[1]: ...                                       │
│   ...                                                │
├──────────────────── 空闲空间 ─────────────────────────┤
│                                                      │
├──────────────────────────────────────────────────────┤
│ Tuple Data (从高地址向低地址增长)                      │
│   tuple N: [tuple_header | col1 | col2 | ...]        │
│   ...                                                │
│   tuple 0: [tuple_header | col1 | col2 | ...]        │
├──────────────────────────────────────────────────────┤
│ Special Area (可选，如 B+Tree 的 right_sibling 指针)  │
└──────────────────────────────────────────────────────┘
```

**数据页 CRC 说明：**
- M1 不实现数据页 CRC；torn page 通过 FPI（Full Page Image）修复
- 静默磁盘损坏（bit rot）检测推迟到 Phase 7（在 Page Header 中预留 checksum 字段位置，但 M1 不计算/不校验）

### 7.3 元数据序列化

**选择：serde + bincode**

**理由：**
- 元数据（表定义、索引定义、checkpoint 信息）读写频率低
- serde 的 derive 宏减少手写错误
- bincode 紧凑且快速

---

## 八、依赖 crate 清单

### 核心依赖（M1 使用）

| crate | 版本 | 用途 | 稳定性 |
|-------|------|------|--------|
| `tokio` | 1.x | 异步运行时 | ⭐⭐⭐⭐⭐ 业界标准 |
| `parking_lot` | 0.12 | 高性能 Mutex/RwLock | ⭐⭐⭐⭐⭐ 广泛使用 |
| `bytes` | 1.x | 零拷贝字节缓冲区 | ⭐⭐⭐⭐⭐ |
| `crc32fast` | 1.x | CRC32 计算（SIMD 加速） | ⭐⭐⭐⭐⭐ |
| `thiserror` | 2.x | 错误类型派生 | ⭐⭐⭐⭐⭐ |
| `bincode` | 2.x | 序列化（WAL payload、元数据） | ⭐⭐⭐⭐ |
| `serde` | 1.x | 序列化框架 | ⭐⭐⭐⭐⭐ |
| `tracing` | 0.1 | 结构化日志 | ⭐⭐⭐⭐⭐ |

### Phase 2 引入（M1 不使用，Cargo.toml 中不添加）

| crate | 版本 | 用途 | 引入时机 |
|-------|------|------|----------|
| `crossbeam` | 0.8 | 无锁数据结构、epoch reclamation | Phase 2（HNSW 并发访问） |

### 测试依赖

| crate | 用途 |
|-------|------|
| `proptest` | 模糊测试 / 属性测试 |
| `loom` | 并发模型检查（Phase 1 M2b 起） |
| `tempfile` | 测试用临时目录 |
| `criterion` | 性能基准测试 |

### 不引入的 crate（显式排除）

| crate | 排除理由 |
|-------|---------|
| `anyhow` | 需要编译期错误类型，不用动态错误 |
| `rocksdb` / `sled` | 自研存储引擎，不依赖第三方 KV |
| `dashmap` | 自研分区 HashMap（更可控） |
| `mio` | 用 tokio 已包含 |

---

## 九、Rust 代码风格与约定

### 9.1 命名约定

| 概念 | 命名风格 | 示例 |
|------|---------|------|
| 页 ID | `PageId` | `PageId(42)` |
| LSN | `Lsn` | `Lsn(1024)` |
| 事务 ID | `TxnId` | `TxnId(7)` |
| Tuple ID | `Tid` | `Tid { page_id: PageId(1), slot_id: 3 }` |
| Frame ID | `FrameId` | `FrameId(128)` |

### 9.2 unsafe 使用策略

- **原则：尽可能不用 unsafe**
- 允许 unsafe 的场景：
  - 页内字节操作（已知对齐和边界）
  - mmap（如果未来引入）
  - 特定的 atomic 操作
- 每处 unsafe 必须有 `// SAFETY:` 注释说明不变量

### 9.3 测试约定

- 每个 public 函数至少一个单元测试
- 每个模块一个集成测试文件
- 崩溃测试作为独立的 test binary（需要 fork + kill）
- proptest 用 `#[cfg(test)]` 标注，不影响正常编译速度

---

## 十、M1 阶段不做的事（显式排除）

| 不做 | 原因 | 何时做 |
|------|------|--------|
| 逻辑 WAL 分发（路由到具体 AM） | 没有 AM 使用者 | Phase 2 HNSW 接入时 |
| 事务语义（begin/commit/abort） | Layer 2 职责 | Phase 1 M2 |
| TOAST / overflow page | 依赖 Tuple 格式 | Phase 1 M2 |
| Slotted page 实现 | 依赖 Tuple 格式 | Phase 1 M2 |
| O_DIRECT | M1 不实现；macOS 不支持；Linux 生产环境可手动启用但属 Phase 7b 正式优化项。代码中用 `#[cfg(target_os = "linux")]` 条件编译预留 O_DIRECT 路径 | Phase 7b |
| io_uring | Linux-only 优化 | Phase 7b |
| 多文件数据存储 | 单文件 M1 够用 | Phase 7b |
| WAL 压缩 | 性能优化 | Phase 7b |
| Group commit 参数调优 | 需要 benchmark 数据 | Phase 7b |

---

## 十一、崩溃恢复流程（M1 版）

M1 的崩溃恢复是简化版 ARIES（无 Undo，因为 M1 没有事务；无传统物理 Redo，因为 M1 没有堆/BTree 变更记录；只有 FPI 修复 + next_page_id 推进 + freelist 重建）。

### 恢复步骤

```
1. 读取 Superblock
   ├── 加载 checkpoint_lsn（即 CheckpointBegin 的 LSN，又称 redo_lsn；恢复起点）
   │     设计要点：checkpoint_lsn 必须指向 CheckpointBegin 记录，而不是 CheckpointEnd。
   │     这样 checkpoint 周期内产生的 FPI 才会被 replay，才能修复 torn page。
   ├── 加载 next_page_id（checkpoint 时刻的最大页号）
   └── 加载 next_txn_id（checkpoint 时刻的最大事务号，M1 未用但预留）

2. 加载 Freelist 快照
   └── 读取 meta/freelist.meta（checkpoint 时刻的 freelist 快照）
       若文件不存在或损坏 → freelist 初始化为空（后续 WAL replay 会重建）

3. 从 checkpoint_lsn 开始顺序 Replay WAL
   ├── 遇到 PageAlloc 记录：
   │     推进内存中的 next_page_id = max(next_page_id, record.page_id + 1)
   │     （M1 无 PageFree，不需要加入 freelist）
   ├── 遇到 FullPageImage 记录：
   │     将 FPI 中的完整页内容写回对应的数据文件位置（修复 torn page）
   ├── 遇到 CheckpointEnd 记录：
   │     验证 checkpoint 一致性标记（如果 CRC 不匹配则报告警告但继续）
   └── 遇到 CRC 校验失败或记录截断：
         停止 replay（该记录及之后的数据视为未提交，丢弃）

4. 完成
   ├── 此时内存中的 next_page_id 是最新值
   ├── Freelist 已重建（M1 实际为空，因为无 free 操作）
   ├── 所有 torn page 已通过 FPI 修复（限定：FPI 仅保证页在 checkpoint 后首次修改的 torn page 可修复；同一页多次修改的中间状态不在 M1 保证范围内，M2 起 Heap/BTree WAL 记录补齐后才有完整 Redo）
   └── 系统可接受新的读写请求
```

### 设计约束

- **WAL-ahead 规则**：任何数据页修改必须先将对应 WAL 记录 fsync 到磁盘，才能将脏页刷盘
- **FPI 时机**：每页在 checkpoint 后首次被修改时，写一条 FullPageImage 记录（保证 torn page 可修复）
- **最后一条 WAL 记录**：如果 CRC 不匹配或长度截断，视为 crash 时正在写入的半成品，安全丢弃
- **幂等性**：所有物理 redo 操作都是幂等的（FPI 是全页覆盖，PageAlloc 是 max 语义）

---

## 十二、决策验证计划

每个关键决策在实现后需要验证：

| 决策 | 验证方法 |
|------|---------|
| 8KB 页大小 | 实现后用 B+Tree 和堆表 benchmark 确认无性能异常 |
| LSN = 字节偏移 | 验证跨段文件的 LSN → 文件位置计算正确 |
| CLOCK 替换 | 与 LRU 对比，确认全表扫描不污染缓存 |
| Group commit | 验证 fsync 返回后 WAL 记录持久化（kill -9 后重启读 WAL 确认记录在）；M2 起验证事务 commit 语义 |
| 分区锁 Buffer Pool | 100 并发下无死锁、无 frame 泄漏 |
| 分段 WAL 文件 | 验证段切换时无记录丢失、跨段记录正确处理 |
| WAL 吞吐 | 单线程顺序写 ≥ 200 MB/s（本地 SSD） |
| 崩溃 fuzz | 随机 kill -9 × 1000 次，重启后 WAL 可完整回放，无 CRC 错误（除最后一条可能截断） |
| FPI torn page | 模拟半写页（写入 4KB 后 kill），恢复后 FPI 正确覆盖损坏页 |
| 多线程 LSN | 10 线程并发调用 LsnClock::current()，验证单调递增且无重复 |
| Freelist 恢复 | kill 后重启，freelist 通过 WAL replay 重建，与 crash 前一致 |

---

## 十三、总结：M1 交付物与接口边界

```
M1 对外暴露的接口（供 M2 使用）：

PageAllocator:
  - alloc_page() -> PageId
  - free_page(page_id)  // M1 为 stub/no-op，M2 实现

BufferPool:
  - pin(page_id) -> PageGuard (读)
  - pin_mut(page_id) -> PageGuardMut (写)
  - new_page() -> (PageId, PageGuardMut) (分配新页并 pin)
  - flush(page_id) (强制刷盘)

WalWriter:
  - append(record: &WalRecord) -> Lsn
  - flush() (强制 fsync 到当前位置)
  - flush_to(lsn: Lsn) (fsync 到指定 LSN)
  - synced_lsn() -> Lsn (返回已 fsync 到磁盘的最新 LSN，Buffer Pool flush 前用其检查 WAL 先行规则)

LsnClock:
  - current() -> Lsn

CheckpointCoordinator:
  - trigger_checkpoint() (手动触发)
  - 后台自动 checkpoint（按配置的条件）

WalReader (恢复 + Tier 2 预留):
  - read_from(start_lsn: Lsn) -> impl Iterator<WalRecord>
  - tail_follow(start_lsn: Lsn) -> impl Stream<WalRecord> (预留，Phase 3 实现)

注：File Manager 不作为独立对外接口暴露。文件管理职责分散在各组件内部：
  - PageAllocator 管理数据文件（data/）
  - WalWriter 管理 WAL 段文件（wal/）
  - CheckpointCoordinator 管理 superblock 和 meta/ 下的元数据文件
  这是有意设计：M1 的文件操作语义与各组件的崩溃安全策略紧密耦合，抽象出独立
  File Manager 反而增加复杂度且模糊崩溃安全保证。
```
