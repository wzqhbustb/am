# Phase 1 M2 编码顺序

> 基于 `docs/phase1-m2-tech-selection.md` v2.3（八轮 review / 35 条编号修订），按依赖
> 关系排列的 M2 阶段编码执行计划。每个阶段必须先通过单元 / 集成 / 崩溃测试，再进入
> 下一阶段。字母 stage 与 tech-selection §0 / §19 的四大 phase 映射如下：
>
> ```
> Stage 0a (阻塞主线)     → A / B / C / D           1.5–2 周
> Stage 0b (与 M2a 并行)  → E / F                   1–1.5 周
> M2a  (单语句 auto-commit) → G / H / I / J / K     4.5–6 周
> M2b  (MVCC + 单线程 BTree)→ L / M / N / O         6–8 周
> M2c  (并发 + Blink + Undo)→ P / Q / R / S / T     5.5–8 周
> ```
>
> **总计 20 个 stage，全长 16–22 周**，与 §19 预算一致。

---

## v2.3 硬约束速查

M2 主线开工前请通读 tech-selection §0 / §7 / §11 / §14 / §21，以下 10 条为**编码期
每天都要对照**的硬性约束（若违反则退回该 stage 重做）：

- **v2.3-9 / v2.3-30**：`WalWriter::append(record) -> Lsn` **不再隐式 flush_to**；上层
  显式调 `flush_to(commit_lsn)` 触发 group commit。`LsnClock::next(size)` 与新加的
  `LsnClock::reserve(size)`（占位不写）并存，语义不同。
- **v2.3-10 / v2.3-12**：page[0..8] 内的 `pd_lsn` 是权威 LSN 源，frame metadata 降级为
  只读缓存；PageHeader 定型 **32 字节**（26 字节字段 + 6 字节 padding），tuple payload 起点
  8 字节对齐。
- **v2.3-11**：任何脏页写盘前 `flush_to(page.pd_lsn)` —— M1 `BufferPool::flush` 已实现，
  Stage 0 只需补 evict 路径回归 + `pd_lsn` 直读改造。
- **§3 P1-5（Commit 硬顺序）**：`wal.append → wal.flush_to → clog.set_state → remove_active`
  四步顺序不可打乱（否则脏读）；hint bit 回写是**可选**后置动作，不属于硬顺序。
- **v2.3-2**：M2a in-memory `ClogAccessor` 只允许清理 `COMMITTED`，**ABORTED 禁止清理**；
  M2a HashMap 保底只增不删。
- **v2.3-3 / v2.3-Q4**：tuple `t_cid: u32` + `Snapshot.curcid: u32`；M2a 恒 0（dead code
  path），M2b 由 executor 在语句**开始执行前** +1，同一语句内 self-scan 共用
  同一 curcid。
- **v2.3-24**：`RedoRegistry::register` 同一 `WalRecordType` 二次注册立即 panic（非静默
  覆盖），未注册 record → recovery 硬失败。
- **v2.3-17**：`CheckpointEnd` 版本号走 `WalRecord.flags` 高 4 位。M1 emit 时 `flags=0` →
  隐式 v1（3 字段）；M2 emit 写 `flags = (1<<12)` → v2（6 字段）。decode 分支按版本分派。
- **v2.3-6 / v2.3-7 / v2.3-8**：`WalRecordType` discriminant 与 `pg-storage/src/wal/record.rs`
  严格对齐（`FullPageImage=10 / CheckpointBegin=30 / CheckpointEnd=31 / PageAlloc=40 /
  100..103 / 110..111`）；Superblock v1→v2 布局 `next_oid @ 40..48`；`BTreeSplit=5` 重命名
  为 `BTreeSplitPrepare=5`，`BTreeSplitCopy=51` / `BTreeSplitCommit=52` 为 Stage 0 追加。
- **v2.3-18**：M2a 出口验证必须包含 **100 线程 × 1000 条 INSERT** 并发压测。

---

## 阶段 A：Workspace 拆分 + Oid 新类型 + AM/Vacuumable trait 骨架（3–4 天）

**归属**：Stage 0a（阻塞）
**目标**：把 M1 的单一 `pg-storage` 扩展为 M2 的 6-crate workspace 骨架，避免后续 stage
在 crate 边界上反复重构。

| 任务 | 交付物 |
|------|--------|
| 保留 `pg-storage`，新建 5 crate 骨架 | `crates/pg-txn/`、`crates/pg-catalog/`、`crates/pg-am-heap/`、`crates/pg-am-btree/`、`crates/pg-engine/`，每个仅含 `lib.rs` 空模块 + `Cargo.toml` |
| workspace 依赖梳理 | 根 `Cargo.toml` 更新 members；6 crate 依赖方向严格单向（见 §1 表） |
| 追加 `Oid` 类型 | `pg-storage::types` 新增 `pub struct Oid(pub u64);`，与 `PageId / TxnId / FrameId` 平级 |
| `TableOid` / `TypeOid` 别名 | 在 `pg-catalog` 定义 newtype，M2 起用（`pg_class` / `pg_type` 已需要） |
| AM trait 定义 | `pg-catalog` 定义 `AccessMethod` + `UpdatableAM` trait 骨架（§14）；`pg-catalog` 定义 `Vacuumable` trait（§15，M2 只留接口 + heap `scan_dead_tuples`，Stage I 实现） |
| 移动 `Tid` 到 `pg-storage` | 若 M1 未在 `pg-storage`，一次性上移；`repr(C)` 保证字段顺序；payload 10B、repr(C) 对齐后 16B、磁盘 12B（padding 声明） |
| CI 更新 | GitHub Actions 矩阵按 crate 拆开 fmt / clippy / test / doc |

**关键 v2.3 约束**：
- §1 crate 分层：`pg-txn::LockManager` 直接引用 `pg_storage::Oid`，避免 `pg-txn → pg-catalog` 反向依赖
- v2.3-15：`Oid` 明确标注为 **M2 新增**（M1 `types.rs` 只有 `PageId/Lsn/TxnId/FrameId/Tid`）

**验收标准**：
- **功能**：`cargo build --workspace` 通过；6 个 crate 单独可以 `cargo build -p <name>`
- **正确性**：`Oid` newtype 单元测试（`Ord` / `Debug` / `serde` round-trip）
- **回归**：M1 全部集成测试 + crash_recovery 1000 轮继续绿灯

**验收命令**：`cargo test --workspace && cargo clippy --workspace -- -D warnings`

---

## 阶段 B：WAL append 拆分 + LsnClock::reserve + Checkpoint FPI race 修复（3–4 天）

**归属**：Stage 0a（阻塞）
**前置**：A
**目标**：解决 M1 债务 #1 与 #3，让上层可以显式控制 group commit 与 checkpoint LSN 占位。

| 任务 | 交付物 |
|------|--------|
| `WalWriter::append` 签名改造 | `append(record) -> Lsn` 不再内部调 `flush_to`；调用方（`BufferPool` / `PageAllocator` / 未来 `TxnManager`）显式 `flush_to(lsn)` |
| 保留 group commit worker | M1 的 `writer.rs:135` group commit worker + `wal_group_commit_batch_size` / `_timeout_ms` 保持不变 |
| 新增 `LsnClock::reserve` | `pub fn reserve(&self, size: u64) -> Lsn`：占位并推进 next_lsn，**但不写入 WAL**；供 checkpoint / 未来场景先占后 emit |
| Checkpoint FPI race 修复 | `Checkpointer` 走 `reserve → BufferPool::set_checkpoint_lsn → emit CheckpointBegin (写入已占位 LSN)`，窗口消除 |
| 补 evict 路径 WAL-before-data 回归 | 新增 `test_wal_before_data_on_evict`：LRU 淘汰脏页时验证 `synced_lsn() >= frame.page_lsn`（M1 现有 API；Stage D 引入 `pd_lsn` 后改为 `page[0..8]` 直读） |
| CheckpointEnd payload 版本保持 v1 | 本 stage emit 时仍走 M1 v1（3 字段，`flags=0`），v2 切换延后到 Stage N；确保 0a→M2b 中间态 recovery 全程读 v1 |

**关键 v2.3 约束**：
- v2.3-9：`append` 拆分 + `reserve` 新增
- v2.3-11：evict 路径必须走同一 WAL-before-data 协议（M1 `flush` 已实现，evict 需回归）
- v2.3-30：`next` 与 `reserve` 是**并存两个 API**，不是"reserve 替代 next(0)"
- v2.3-17：本 stage 保持 v1 emit，v2 切换在 Stage N 与 ATT/DPT snapshot 一起落地

**验收标准**：
- **功能**：`BufferPool::flush_frame` + `Checkpointer::run` 全部调用点显式 `flush_to`
- **正确性**：`test_checkpoint_fpi_race` 注入 crash 于 `reserve` 与 `emit` 之间，恢复后
  page 无 torn-write
- **性能**：group commit 攒批吞吐较 M1 提升 ≥ 20%（测量方法：100 线程并发 commit，batch
  size ≥ 8 时 criterion 对比基线）

**验收命令**：`cargo test -p pg-storage wal_before_data_on_evict && cargo bench -p pg-storage --bench wal_group_commit`

---

## 阶段 C：Superblock v2 + WalRecordType 全枚举 + clog/ 目录（2–3 天）

**归属**：Stage 0a（阻塞）
**前置**：A
**目标**：一次性把 M2 需要的 on-disk 保留位与目录布局全部到位，避免后续 stage 反复
迁移。

| 任务 | 交付物 |
|------|--------|
| Superblock v1→v2 迁移 | 在 offset 40..48 插入 `next_oid: u64`；`created_at` 后移到 48..56、`crc32` 后移到 56..60；`reserved` 收缩至 60..512；一次性搬字段路径 |
| v1→v2 迁移测试 | `test_superblock_v1_to_v2_migration`：读 v1 → 初始化 `next_oid=16384` + 搬 `created_at` → 写回 v2 |
| `WalRecordType` 全枚举保留 | 新增 `HeapInsert=1 / HeapUpdate=2 / HeapDelete=3 / BTreeInsert=4 / BTreeSplitPrepare=5 (重命名自 M1 BTreeSplit) / BTreeDelete=6 / HeapHotUpdate=7 / HeapCleanup=8 / TxnBegin=20 / TxnCommit=21 / TxnAbort=22 / PageFree=41 / BTreeSplitCLR=50 / BTreeSplitCopy=51 / BTreeSplitCommit=52` |
| M1 已有值保持不动 | `FullPageImage=10 / CheckpointBegin=30 / CheckpointEnd=31 / PageAlloc=40 / Logical*=100..103 / Segment*=110..111` 不重编号 |
| 目录布局 | `io::ensure_data_dir` 追加 `clog/`（M1 已建 `data/wal/meta/tmp`） |
| Checkpoint 路径更新 next_oid | `Checkpointer::trigger_checkpoint` 在更新 superblock 时一并写入当前 `next_oid`（Stage C 起 superblock v2 已有此字段）。CheckpointEnd WAL 记录保持 v1（3 字段，无 next_oid）直到 Stage N 切 v2；此窗口内 next_oid 的权威源是 superblock，不是 WAL record |

**关键 v2.3 约束**：
- v2.3-6：discriminant 与 `crates/pg-storage/src/wal/record.rs:19-67` 严格对齐
- v2.3-7：`next_oid` 绝对 offset 40..48
- v2.3-8：`BTreeSplit=5 → BTreeSplitPrepare=5` 是**重命名而非新增判别子**
- v2.3-13：M1 已建 `meta/`，只缺 `clog/`

**验收标准**：
- **功能**：Superblock v2 encode/decode round-trip；v1 数据文件启动自动迁移
- **正确性**：`test_wal_record_type_discriminant_matches_m1`（对照 `record.rs` 常量）
- **回归**：M1 crash_recovery 1000 轮在 v2 superblock 下继续绿灯

**验收命令**：`cargo test -p pg-storage --test superblock_v2_migration && cargo test -p pg-storage --test wal_record_type_discriminant`

---

## 阶段 D：RedoHandler / ClogAccessor trait + pd_lsn 权威性 + PageHeader 32B（3–4 天）

**归属**：Stage 0a（阻塞）
**前置**：A / C（依赖新枚举）
**目标**：所有 M2 主线代码从第一行起就能编译 —— trait 是纯接口，M2a `TxnManager` /
Heap AM 编译期就依赖它。

| 任务 | 交付物 |
|------|--------|
| `ClogAccessor` trait 定义 | `pg-storage::clog::ClogAccessor { get_state / set_state }`；trait 放 `pg-storage` 是因 `RedoContext` 持有 `&dyn ClogAccessor`（打破循环依赖） |
| `RedoHandler` trait 定义 | `pg-storage::recovery::{RedoHandler, RedoContext, RedoRegistry}`；`RedoContext` 字段完整声明（`buffer_pool / page_allocator / clog / att / dpt`） |
| `RedoRegistry` 注册协议 | `register(WalRecordType, Box<dyn RedoHandler>)` 同一 record 二次注册 → `panic!`；未注册 → recovery `RecoveryError::UnknownRecord` 硬失败 |
| PageHeader 32B 定型 | `PageHeader` 26 字节字段 + 6 字节 padding；`pd_lower` 初值 = 32；ASCII 布局与 tech §2 一致 |
| `pd_lsn` 权威性契约 | page[0..8] 作为 authoritative LSN 源；frame metadata `page_lsn` 降级为只读缓存；提供 `page_pd_lsn(page: &[u8]) -> Lsn` / `set_page_pd_lsn(page: &mut [u8], lsn: Lsn)` helper |
| `flush_frame` 改读 pd_lsn | `BufferPool::flush_frame` 从 `frame.page_lsn` 改为 `page_pd_lsn(page)` 直读（对齐 §11.5 v2.3-11）；同步更新 Stage B 的 `test_wal_before_data_on_evict` 断言为 `synced_lsn() >= page_pd_lsn(page)` |
| `test_pd_lsn_authoritative` | 任意 mutation 后 `frame.cached_lsn == page.pd_lsn` 硬 assert |

**关键 v2.3 约束**：
- v2.3-Q1：`ClogAccessor` trait 放 `pg-storage::clog`，具体实现 `ClogBuffer` 在 `pg-txn`
- v2.3-24：`RedoRegistry` duplicate = panic（非 `Result`、非静默覆盖）
- v2.3-10：`pd_lsn` 契约方向 —— M1 无此字段，M2 新引入
- v2.3-12：PageHeader 32B（26 字节字段 + 6 padding）；28B 无法满足 tuple 8 字节对齐

> **tech-selection §0 偏差说明**：§0 原文说 "Stage 0a 只落 trait 定义 + doc comment，
> NoOpClogAccessor 在 Stage 0b 交付"。本计划将 `NoOpClogAccessor` 骨架提前到 Stage D，
> 因为 Stage D 的回归测试（M1 `crash_recovery` 用新 `RedoRegistry` 分发）需要实例化
> `RedoContext`，而 `RedoContext.clog: &dyn ClogAccessor` 要求至少有一个具体实现才能编译
> 通过。Stage F 仍负责将其正式装配到 `Engine::open`。这是对 tech-selection 的实践细化，
> 不改变 trait 定义位置和后续实现计划。

**验收标准**：
- **功能**：trait 定义完整可编译；`NoOpClogAccessor` 骨架（M1 无事务，`get_state` 返回
  `COMMITTED`，`set_state` no-op）用于本 stage 编译期占位（真正装配在 Stage F）
- **正确性**：`test_redo_registry_duplicate_panics`；`test_pd_lsn_authoritative` 通过
- **回归**：M1 `crash_recovery` 用新 `RedoRegistry` 分发 `FullPageImage` 与 `PageAlloc`
  handler，1000 轮继续绿灯

**Stage 0a 出口 tag**：`phase1-m1-debt-clean-0a`
**验收命令**：`cargo test -p pg-storage --test redo_registry && cargo test -p pg-storage --test pd_lsn_authoritative`

---

## 阶段 E：Freelist CRC + WAL 重建（3–4 天）

**归属**：Stage 0b（可与 M2a 前 60% 并行）
**前置**：D
**目标**：修复 M1 债务 #2，静默丢弃 freelist 数据的行为在 M2 会导致 page id 重用。

| 任务 | 交付物 |
|------|--------|
| `FreelistMeta` header 加 CRC32 | 头部新增 4 字节 CRC，覆盖后续 body |
| `FreelistMeta::read` 硬失败 | 校验失败返回 `StorageError::MetadataCorrupted`，不再静默返空 |
| WAL `PageFree` handler | 注册到 `RedoRegistry`；恢复期从 checkpoint_lsn 起 replay `PageFree` 重建 freelist |
| Superblock freelist 快照 | 保留 M1 快照机制，作为加速；恢复期以 WAL 为准 |
| 崩溃回归 | `test_freelist_corrupted_returns_hard_error`；`test_freelist_rebuild_from_wal`（注入损坏后从 WAL 完整重建） |

> **PageFree 场景说明**：M1 无 emit（`free_page` 是 no-op），但 M2a Stage K 的 `drop_table`
> 会开始产生 `PageFree` WAL 记录。因此本 stage 的 handler 必须在 K 之前就位，否则 K 会
> 触发 v2.3-24 未注册 record 硬失败。

**验收标准**：
- **功能**：损坏 freelist 后 `Engine::open` 返回硬错，从 WAL 完整重建 freelist
- **正确性**：`test_freelist_rebuild_from_wal` 断言 freelist 与预期完全相等（顺序无关）
- **性能**：CRC 计算 < 1μs / 4KB freelist chunk（criterion）

**验收命令**：`cargo test -p pg-storage --test freelist_meta_crc`

---

## 阶段 F：NoOpClogAccessor 装配 + RedoRegistry engine wiring + read_at/write_at（3–4 天）

**归属**：Stage 0b（可与 M2a 前 60% 并行）
**前置**：D（F 与 E 平级，无相互依赖，可并行推进）
**目标**：Stage 0b 收尾 —— 完成债务 #4b、#5，让 M2a 集成测试可以起跑。

| 任务 | 交付物 |
|------|--------|
| `NoOpClogAccessor` 实现 | `pg-storage::clog::NoOpClogAccessor`：`get_state → COMMITTED`、`set_state → no-op`；M1 空事务场景默认装配 |
| `Engine::open` 装配 `RedoRegistry` | 收集所有 crate 的 `redo_handlers()` 一次性注册；未注册 record 类型 → 硬失败 |
| 数据文件 I/O 改造 | `data_file: Arc<File>`，`read_at` / `write_at` 无锁并发；Windows fallback `seek_read/seek_write` |
| BufferPool 并发压测 | 100 线程随机 read/write，QPS ≥ M1 + 50% |

**关键 v2.3 约束**：v2.3-Q1（NoOpClogAccessor 位置）

**验收标准**：
- **功能**：`Engine::open` 装配后 `crash_recovery` 走新分发路径
- **并发**：100 线程 × 10K 随机 page read/write，无锁竞争 hot path
- **回归**：M1 集成测试 + `crash_recovery` 1000 轮全绿

**Stage 0b 出口 tag**：`phase1-m1-debt-clean`（与 Stage 0a 的 `-0a` 对称；后者是中间态 tag，前者是合并后 tag，对应 tech-selection §0）
**并行边界**：Stage 0b 交付前，M2a Stage G→H（G/H 内部顺序，见 Stage H 前置）与 Stage 0b 完全并行；Stage I 后半段（Heap redo 并发压测）与 J 集成测试必须等 F 就位
**验收命令**：`cargo test --workspace && cargo bench -p pg-storage --bench buffer_pool_concurrent`

---

## 阶段 G：Slotted Page + Tuple 编解码（4–6 天）

**归属**：M2a
**前置**：D（PageHeader 32B）
**目标**：把 tech-selection §2 / §3 / §4 的物理格式变成可读写的 Rust 结构。

| 任务 | 交付物 |
|------|--------|
| PageHeader 编解码 | `SlottedPage::init(page: &mut [u8; PAGE_SIZE])` 初始化 header 32B、`pd_lower=32`、`pd_upper=PAGE_SIZE - special_size` |
| LinePointer 布局 | 32-bit LP：`lp_off:15 / lp_flags:2 / lp_len:15`；`LpFlags::{Unused, Normal, Redirect, Dead}` |
| Tuple 编解码 | TupleHeader 64B 固定（字段顺序按 §3 表），`t_cid @ offset 60..64`；null bitmap；定长 + varlena 列 |
| TOAST pointer 20B | `TOAST pointer` 5×u32 布局；vl_len_ 高 2 位标记 external |
| TOAST chunk 走 HeapInsert/Delete | 不引入新 record；`pg_toast_<oid>` 表隐式关联；**用户表首次触发 TOAST 时由 Heap AM 隐式创建** |
| free-space / add_tuple / delete_tuple | slotted page 增删 API，维护 `pd_lower / pd_upper` |
| proptest | 随机插入/删除 100 万次，pd_lower ≤ pd_upper 恒成立、LP 数组无重叠 |

**关键 v2.3 约束**：
- v2.3-12：PageHeader 32B；`pd_lower` 初值 = 32
- v2.3-3 / v2.3-16：`t_cid` 是 M2 tuple header 新增字段 @ 60..64（非"占用保留字段"）
- v2.3-32：header 定型 64B 是"8 字节对齐 memcpy/SIMD 友好"论据，非"pd_upper 对齐"
- §16 依赖：引入 `uuid = "1"`（`t_trace_id: [u8;16]` 编解码）

**验收标准**：
- **功能**：小 tuple / 大 tuple / TOAST-out-of-line 三种场景 encode → decode round-trip
- **正确性**：`test_slotted_page_add_delete_invariant`；`test_tuple_header_offsets`（每字段
  offset 与 §3 表严格一致）
- **性能**：add_tuple / lookup_tuple ≥ 5M ops/s（criterion，纯内存）

**验收命令**：`cargo test -p pg-am-heap --test slotted_page && cargo test -p pg-am-heap --test tuple_encoding`

---

## 阶段 H：Catalog bootstrap（4–5 天）

**归属**：M2a
**前置**：G
**目标**：让 `Engine::open` 在空数据目录上能自动写入所有系统表，为 Heap AM 提供 schema。

| 任务 | 交付物 |
|------|--------|
| 5 张系统表定义 | `pg_class(1259) / pg_attribute(1249) / pg_type(1247) / pg_am(2601) / pg_index(2610)`（pg_index M2b 才写入行） |
| builtin_types 硬编码 | `int4 / int8 / text / bytea / timestamptz / uuid`；`pg-catalog::builtin_types.rs` |
| Bootstrap 顺序 | init 时先按硬编码 schema 直接写第一个 heap page（pg_class 自身定义），再用读到的 schema 校验后续系统表 |
| OID 分配器 | `AtomicU64` 从 superblock `next_oid` 加载；系统 OID [1, 9999]，用户 OID ≥ 16384 |
| 集成测试 | `test_bootstrap_from_empty_dir`：空目录 open → 5 表齐全 → schema 与 builtin_types 一致 |

**关键 v2.3 约束**：无（属 §5）
- §16 依赖：引入 `arc-swap = "1"`（Catalog 快照原子换代，DDL 生效）

> ⚠️ **Stage C 遗留前置条件（next_oid 回滚窗口，H→N）**：本 stage 起开始分配 OID，但
> CheckpointEnd v2（含 `next_oid` 字段）要到 Stage N 才切换。此窗口内 `next_oid` 仅随
> checkpoint 持久化（superblock 为权威源），崩溃会把它回滚到上一个 checkpoint 的值——
> 崩溃前已分配并写入 catalog 页的 OID 可能被重复分配。本 stage 必须含应对措施：
> bootstrap / `Engine::open` 时扫描系统表既有 OID，取 `max(oid)+1` 与 superblock
> `next_oid` 的较大者校正，并在分配后做存在性检查（PG 实践经验：OID 唯一性靠存在性
> 检查保证，不依赖计数器精确）。**不推荐**"OID 分配先写 WAL 记录再使用"的路径：
> 目前没有 OID 分配的 WAL 记录类型，新增即 on-disk 格式变更，与 Stage C 的格式冻结
> 冲突。验收时需在 `test_bootstrap_from_empty_dir` 之外补一个崩溃回滚用例（分配
> OID → 崩溃 → 重开 → 不出现 OID 冲突）。

**验收标准**：
- **功能**：空目录初始化后 `SELECT * FROM pg_class`（暂用程序化 API）返回自身
- **正确性**：`test_catalog_self_describing`（pg_class 记录 pg_class 自己）
- **回归**：`Engine::open` 二次打开不重复 bootstrap

**验收命令**：`cargo test -p pg-catalog --test bootstrap`

---

## 阶段 I：Heap AM + Redo Handlers（6–8 天）

**归属**：M2a
**前置**：D（trait）/ G（Tuple 编解码）/ H（Catalog）；**并发验收部分额外依赖 F**（read_at/write_at 无锁 I/O）
**目标**：实现 M2a 单线程 heap CRUD 路径 + 崩溃恢复能重放 heap WAL。

| 任务 | 交付物 |
|------|--------|
| Heap AM 实现 | `impl AccessMethod + UpdatableAM for HeapAM`；`InsertContext.out_tid: Option<&mut Tid>` 回填 |
| Vacuumable 接口 | `impl Vacuumable for HeapAM`：`scan_dead_tuples` 实现（xmax committed 且早于 oldest snapshot）；`reclaim` / `notify_indexes` 留 `unimplemented!()`（§15，M3 实现） |
| Heap redo handlers | `HeapInsertHandler / HeapUpdateHandler / HeapDeleteHandler` 三个；`redo_handlers()` 返回 `Vec<Box<dyn RedoHandler>>` |
| mutation 写 pd_lsn | 每次修改 heap page 后同 latch 内 `set_page_pd_lsn(page, record.lsn.max(old))`，然后 `buffer_pool.mark_dirty(page_id, record.lsn)` |
| redo 幂等 | handler apply 内部 `if page_pd_lsn(page) >= record.lsn { return Ok(()); }` |
| `RedoRegistry` 注册 | Heap AM 在 `Engine::open` 时把 3 个 handler 注册；duplicate 触发 panic 单测 |
| 集成测试 | INSERT 100 万条 + kill -9 + restart → 数据完全一致；abort 事务 tuple 在下轮 SELECT 中不可见（xmin ABORTED via CLOG） |

**关键 v2.3 约束**：
- §14 P0-2：`insert` 返回 `Result<()>` 由 `out_tid` 回填
- v2.3-10：`pd_lsn` 权威源
- §11.6：redo 幂等判定；FPI 也走同一 dispatch

**验收标准**：
- **功能**：`insert / scan / update / delete` 单线程 API 全通
- **正确性**：`test_heap_crash_recovery`；`test_heap_redo_idempotent`（同一 record 重放
  10 次页面不变）
- **并发**：Stage F 就位后 100 线程 INSERT 无 slot 冲突
- **性能**：单线程 INSERT ≥ 30K ops/s（criterion，纯 heap AM 路径，尚未叠加 TxnManager / CLOG；
  该数字是后续 Stage K/T 的性能上限，加层后会逐步下降）

**验收命令**：
```bash
# 单线程验收（G/H 完成后即可）
cargo test -p pg-am-heap --test heap_am_integration && cargo test -p pg-am-heap --test heap_redo_idempotent
# 并发验收（需 Stage F 就位）
cargo test -p pg-am-heap --test heap_concurrent_insert
```

---

## 阶段 J：最小 TxnManager + In-Memory ClogAccessor（4–6 天）

**归属**：M2a
**前置**：D / I；**集成测试路径额外依赖 F**（TxnManager 装配到 Engine::open 走 F 的 wiring）
**目标**：让 M2a 每条 SQL 走一次真实的 XID 分配 + WAL commit / abort，为 M2b 换成磁盘
CLOG 铺路。

| 任务 | 交付物 |
|------|--------|
| `TxnIdClock` M2 新增 | `AtomicU64` from `next_txn_id`（superblock），起点 1；`0 = InvalidTxnId` |
| 最小 `TxnManager` | `begin_txn / commit_txn / abort_txn`；auto-commit 每条 API 一 XID |
| `TxnCommit / TxnAbort` WAL | 严格按 Commit 硬顺序 4 步；redo handler 更新 CLOG bit |
| In-Memory `ClogAccessor` | `pg-txn::InMemoryClogAccessor`：`parking_lot::RwLock<HashMap<TxnId, TxnState>>`；实现同一 trait |
| **ABORTED 禁清** | checkpoint 时只清 `COMMITTED` 且 `xid < checkpoint_next_txn_id`；ABORTED 一律保留 |
| 保底不清 | `EngineConfig.m2a_clog_never_gc = true`（M2a 默认；M2b 换磁盘版后自动关闭） |

**关键 v2.3 约束**：
- v2.3-1：删除 v2.1 `AbortedXidSet` 概念，全走 `ClogAccessor`
- v2.3-2：ABORTED **禁清**
- §3 P1-5：Commit 硬顺序 4 步

**验收标准**：
- **功能**：`begin/commit/abort` API + WAL 记录写入 → recovery 后 CLOG bit 一致
- **正确性**：`test_commit_hard_order`（注入 WAL flush 失败 → CLOG 不更新）；
  `test_aborted_never_gc`（checkpoint 后 aborted xid 仍在 map 中）
- **并发**：100 事务并发 commit / abort，`ClogAccessor::get_state` 无 race
- **性能**：单事务 commit 延迟 ≤ 5ms（本地 SSD，含 fsync）

**验收命令**：`cargo test -p pg-txn --test commit_hard_order && cargo test -p pg-txn --test aborted_never_gc`

---

## 阶段 K：Engine API + M2a 综合验证（4–5 天）

**归属**：M2a
**前置**：F（Stage 0b 装配）/ H / I / J
**目标**：M2a 出口 —— 程序化 API 可用 + 100 线程并发压测通过 + crash-restart 数据一致。

| 任务 | 交付物 |
|------|--------|
| `Engine::open` 完整装配 | storage + txn + catalog + heap AM + redo registry + in-memory CLOG |
| 程序化 API | `create_table / drop_table / insert / scan / update / delete`（M2a 无 `begin_txn`） |
| M2a 综合正确性 | 100 万 INSERT + SELECT + kill -9 + restart → 数据完全一致；abort tuple 不可见 |
| 100 线程并发压测 | 100 线程 × 1000 INSERT（10 万总量）→ tuple 数量、xmin 单调、无 slot 冲突、CLOG 状态一致 |
| 崩溃自动化 | 1000 轮随机 kill -9 + restart 全绿 |

**关键 v2.3 约束**：
- v2.3-18：100 线程 × 1000 INSERT 并发压测
- §21 M2a API 集合

**验收标准**：
- **功能**：M2a 程序化 API 完整可用（对齐 §21 M2a API 表）
- **正确性**：`test_m2a_crash_1000_rounds`；`test_m2a_abort_invisible`
- **并发**：`bench_m2a_100_threads_insert` TPS ≥ 3K（criterion）
- **性能**：单线程 INSERT ≥ 20K TPS（比 Stage I 30K 低是因为叠加 TxnManager + XID 分配 + TxnCommit WAL + CLOG update）；SELECT 全表扫 ≥ 200K rows/s

**M2a 出口 tag**：`phase1-m2a`
**验收命令**：`cargo test -p pg-engine --test m2a_integration && cargo bench -p pg-engine --bench m2a_100_threads`

---

## 阶段 L：Snapshot + curcid + Disk ClogBuffer + VisibilityOracle（7–10 天）

**归属**：M2b
**前置**：K
**目标**：把 M2a 的 in-memory CLOG 换成磁盘 SLRU，把 Snapshot 与 curcid 完整启用，让
`is_visible` 走过完整 PG 教科书判定。

| 任务 | 交付物 |
|------|--------|
| `Snapshot` 完整字段 | `xmin / xmax / xip: SmallVec<[TxnId;32]> / current_xid / curcid: u32`；`TxnManager::snapshot(current_xid)` 原子读 |
| `curcid` 递增协议 | 每条 SQL 语句由 executor 在**开始执行前** +1；同一语句 self-scan 共用同一 curcid |
| `ClogBuffer` 磁盘 SLRU | `pg-txn::ClogBuffer` 实现 `ClogAccessor` trait；N 帧 clock-sweep，8KB / 帧；`EngineConfig.clog_buffer_frames` 默认 8，范围 [4, 1024] |
| CLOG 段文件 I/O | `{data_dir}/clog/clog-XXXXXXXX.log` 128MB / 268M XID；4-bit 状态 + 高低 4-bit XID 编码 |
| CLOG flush 时机 | 单一 authoritative 定义：Checkpointer 在 `CheckpointBegin` 后、`CheckpointEnd` 前 fsync dirty CLOG 帧；别处不主动 flush |
| `VisibilityOracle::is_visible(xmin, xmax, t_cid, &snapshot)` | 完整实现（§7.2 教科书判定）；`Visibility::{Visible, Invisible, Uncertain}` |
| hint bit 回写 | 读路径可选调用；先读 CLOG 再回写；`pd_checksum` 恒 0，无 checksum 冲突顾虑 |

**关键 v2.3 约束**：
- v2.3-3 / v2.3-Q4：`t_cid` + `curcid` 递增时机；本 stage 是 curcid 从 "M2a 恒 0 dead code" 切到 "M2b 每语句 +1" 的激活边界，`xmin==self_xid` / `xmax==self_xid` 分支从此有意义
- v2.3-25：`clog_buffer_frames` 默认 8 rationale + 生产 64 / OLAP 256 建议
- §16 依赖：引入 `smallvec = "1"`（`Snapshot.xip: SmallVec<[TxnId;32]>`）
- v2.3-21：CLOG flush 只在 CheckpointBegin/End 之间发生

**验收标准**：
- **功能**：M2b 6 个验证用例全通（§7.2 表：同事务 UPDATE + SELECT、INSERT + RETURNING、
  跨事务 DELETE 未提交、DELETE 已提交、UPDATE Halloween 保护、跨事务可见性）
- **正确性**：`test_curcid_advance_on_statement_start`；`test_clog_buffer_hit_rate ≥ 95%`
- **并发**：100 事务并发访问 `ClogBuffer`，命中率随帧数配置分档 —— 默认 8 帧 ≥ 95%，256 帧配置 ≥ 99%（与 Stage T benchmark 目标一致）
- **性能**：Visibility check 单次 < 500ns（缓存命中）；miss < 20μs（含 pread）

**验收命令**：`cargo test -p pg-txn --test visibility_oracle_m2b_cases && cargo bench -p pg-txn --bench clog_buffer_hit_rate`

---

## 阶段 M：B+Tree AM 单线程 + Split 三步 WAL + redo（10–14 天）

**归属**：M2b
**前置**：L
**目标**：M2 的第一个非 heap AM，其 3 步 split 协议是 CLR / recovery 正确性的核心。

| 任务 | 交付物 |
|------|--------|
| B+Tree 页布局 | 复用 slotted page；`pd_special` 16B（`btpo_prev / btpo_next`）；`pd_flags` bit 8..11 = level、bit 12..15 = flags |
| Latch coupling 骨架 | 单线程版：读路径拿子页 latch 后释放父页 latch |
| 内部页 / 叶子页 tuple | 内部 `(key, child_page_id)`；叶子 `(key, tid)` |
| Split 3 步 WAL | `BTreeSplitPrepare=5` / `BTreeSplitCopy=51 (copy_start_slot + left_page_pre_lsn 极简 payload)` / `BTreeSplitCommit=52` |
| Split redo handler | Copy handler 从 left_page **重算**要搬的 tuples（`left_page.pd_lsn == left_page_pre_lsn` 幂等锚点），对齐 PG `xl_btree_split` |
| BTreeInsert / BTreeDelete redo | 单节点插入 / 删除；SPLIT_INCOMPLETE 标志 |
| CREATE INDEX 阻塞式 | M2b 阻塞式，读源表全量 tuple → 排序 → bulk load 到 B+Tree |

**关键 v2.3 约束**：
- §13.3 P2-9：Copy payload 极简，redo 从 left_page 重算
- v2.3-19：`BTreeSplitCopy=51` / `BTreeSplitCommit=52` 为 Stage 0 追加（非 M1 保留）
- v2.3-8：`BTreeSplitPrepare=5` 是 rename，非新增

**验收标准**：
- **功能**：`CREATE INDEX`（阻塞式）+ 点查 / 范围扫全通
- **正确性**：`test_btree_split_crash_after_prepare` / `test_btree_split_crash_after_copy` /
  `test_btree_split_crash_after_commit`，恢复后 B+Tree 结构有效
- **性能**：单线程 100 万 INSERT + CREATE INDEX ≤ 30s（criterion）

**验收命令**：`cargo test -p pg-am-btree --test btree_split_crash && cargo bench -p pg-am-btree --bench create_index`

---

## 阶段 N：ARIES Analysis + Redo + CheckpointEnd v1/v2 迁移（5–7 天）

**归属**：M2b
**前置**：L / M
**目标**：完整实现三阶段恢复的前两阶段（Undo 简化版由 §11.3 处理，无需 heap CLR）。

| 任务 | 交付物 |
|------|--------|
| Analysis 阶段 | 从 superblock `checkpoint_lsn` 起扫 WAL；构建 ATT（活跃 XID）+ DPT（`page_id → rec_lsn`）；redo LSN = `min(DPT.values.rec_lsn)` |
| Redo 阶段 | 严格 LSN 顺序 + `RedoRegistry` 统一分发；FPI 走同一 dispatch；未注册 record 硬失败 |
| CheckpointEnd v2 payload | 6 字段：`checkpoint_lsn / next_page_id / next_txn_id / next_oid / att_file / dpt_file` |
| v1 / v2 decode 分派 | `flags >> 12`：`0 = M1 legacy v1`（3 字段，`next_oid=16384` 默认），`1 = M2 v2`（6 字段） |
| ATT / DPT snapshot 文件 | `meta/att-{lsn}.snapshot` / `meta/dpt-{lsn}.snapshot` bincode `Vec<TxnId>` / `Vec<(PageId,Lsn)>` |
| 写入顺序 | 三步硬顺序：`fsync(att/dpt snapshot files) → wal.append(CheckpointEnd) → wal.flush_to(ckpt_end_lsn)`（与 §3 P1-5 commit 硬顺序同风格） |
| 旧文件清理 | 下一次 checkpoint 收尾同步删除除最近 3 个之外的旧 snapshot |

**关键 v2.3 约束**：
- v2.3-17：v1/v2 迁移路径 + `flags >> 12` 版本判定 + 前向 crash 保护
- v2.3-24：未注册 record → 硬失败

**验收标准**：
- **功能**：M1 v1 CheckpointEnd 数据文件启动 → M2 走默认值路径不主动升级；M2 emit → v2
  格式
- **正确性**：`test_checkpoint_v1_v2_migration`；`test_analysis_redo_from_100k_wal`
- **性能**：Analysis + Redo 10 万 record ≤ 10s

**验收命令**：`cargo test -p pg-storage --test aries_analysis_redo && cargo test -p pg-storage --test checkpoint_v1_v2`

---

## 阶段 O：SQL parser + M2b 综合验证（7–10 天）

**归属**：M2b
**前置**：L / M / N
**目标**：M2b 出口 —— 硬编码 SQL 子集能跑通，SI 快照与索引点查通过验证。

| 任务 | 交付物 |
|------|--------|
| 硬编码 SQL parser | `BEGIN / COMMIT / ROLLBACK / CREATE TABLE / INSERT INTO / SELECT [WHERE eq/lt/gt] [ORDER BY 单列] [LIMIT N] / UPDATE (WHERE) / DELETE (WHERE) / CREATE INDEX` |
| `exec(Option<&TxnHandle>, sql)` | auto-commit 场景传 None；显式事务传 handle |
| `TxnHandle::{commit, abort}` | Consume self；abort 后 handle 不可用（编译期保证） |
| M2b 综合正确性 | SI 快照隔离（读者不看到并发写者未提交）；索引点查；同事务 UPDATE + SELECT 不返回双行（§7.2 用例） |
| Halloween problem | 同 UPDATE 语句不反复更新自己刚写的 tuple（`t_cid == curcid` 分支保护） |
| 崩溃自动化 | 1000 轮随机 kill -9（含 checkpoint 中途）全绿 |

**关键 v2.3 约束**：
- §21 M2b API
- §7.2 M2b 验证用例
- v2.3-3 / v2.3-Q4：M2b 才激活 `xmin==self / xmax==self` 分支（curcid 递增语义生效）

**验收标准**：
- **功能**：所有 §21 M2b API 可跑通
- **正确性**：§7.2 全部 6 条用例；SSI 反例不覆盖（Phase 7d）
- **并发**：50 事务并发 SI 隔离压测
- **性能**：单事务 INSERT + COMMIT ≤ 5ms；索引点查 ≥ 100K QPS

**M2b 出口 tag**：`phase1-m2b`
**验收命令**：`cargo test -p pg-engine --test m2b_integration && cargo test -p pg-engine --test si_isolation_50_txn`

---

## 阶段 P：LockManager 表锁 + 行锁 xmax 协议（5–7 天）

**归属**：M2c
**前置**：O
**目标**：为并发写入提供正确的锁获取与等待唤醒。

| 任务 | 交付物 |
|------|--------|
| 表级 4 模式锁 | `AccessShare / RowExclusive / Exclusive / AccessExclusive`；`HashMap<TableOid, LockEntry>` + grant/wait 队列 |
| 行锁 xmax 协议 | 完整 5 步流程（读 xmax → CAS → 走可见性 / 抢锁 / 等待）；`row_wait_registry: HashMap<TxnId, TxnId>`（self → waiting_on） |
| Wait / wake | `parking_lot::Condvar` 或 `tokio::sync::Notify`；`end_txn` 广播 |
| SELECT FOR UPDATE | 走同 xmax 协议；SELECT FOR SHARE 占位（M2c 简版 multixact） |
| 并发单测 | 100 事务对同一行 CAS 竞争，唯一胜者 |

**关键 v2.3 约束**：§9.1 P2-5（xmax 协议适用范围）
- §16 依赖：引入 `crossbeam = "0.8"`（Lock manager 无锁等待队列、CLOG 读写并发）

**验收标准**：
- **功能**：并发 UPDATE 同一行 → 逐个排队；FOR UPDATE 阻塞后续 writer
- **正确性**：`test_row_lock_wait_wake`；`test_table_lock_conflict_matrix`
- **并发**：100 并发 UPDATE 同一行 → 无 lost update
- **性能**：无冲突 UPDATE ≥ 30K TPS

**验收命令**：`cargo test -p pg-txn --test lock_manager && cargo test -p pg-txn --test row_lock_wait_wake`

---

## 阶段 Q：B+Tree 并发（latch coupling + Blink） + loom（7–10 天）

**归属**：M2c
**前置**：P
**目标**：把单线程 B+Tree 升级到 100 并发无死锁的 Blink 变体。

| 任务 | 交付物 |
|------|--------|
| 读路径 latch coupling | 拿子页 latch 后释放父；`btpo_next` 允许跨页找 key（split 期间 reader 不阻塞） |
| 写路径 optimistic | 先按读路径下降拿叶子 X latch；空间够则直接插入 |
| 写路径 pessimistic | 需要 split → restart 从根拿全路径 X latch |
| 空间预留 | split 前 `PageAllocator::alloc_page`，失败则 restart |
| loom 引入 | 引入 `loom` 依赖；`pg-am-btree` cfg-loom 单测覆盖 3-thread 并发 |
| 并发正确性 | 100 conn × concurrent INSERT + range scan 无 miss |

**关键 v2.3 约束**：§13.2

**验收标准**：
- **功能**：100 并发 INSERT / SCAN 混合负载稳定运行 1h
- **正确性**：`loom` 3 thread 模型检查通过；range scan 在 split 中不 miss key
- **并发**：100 conn × 10K txn 无死锁（表锁 + 行锁）
- **性能**：并发 INSERT ≥ 15K TPS（相对单线程 M2b 打折）

**验收命令**：`cargo test -p pg-am-btree --test btree_concurrent && LOOM_MAX_PREEMPTIONS=3 cargo test -p pg-am-btree --features loom --test btree_loom`

---

## 阶段 R：死锁检测（3–5 天）

**归属**：M2c
**前置**：P / Q
**目标**：把等待图周期性扫描起来，victim 事务能被 abort 唤醒对方。

| 任务 | 交付物 |
|------|--------|
| Wait-for graph | 节点 = XID，边 = 行锁等待 + 表锁等待；从 `row_wait_registry` + `LockManager` 快照构建 |
| 后台 tick | 100ms interval；避免锁 hot path |
| Victim 选择 | 环内最年轻事务（最大 XID）；abort 该事务并广播 |
| 集成测试 | 注入 2 / 3 / 4 事务环，检测延迟 ≤ 200ms |

**关键 v2.3 约束**：§9.3

**验收标准**：
- **功能**：注入 2 / 3 / 4 事务环，最年轻 victim 被 abort
- **正确性**：`test_deadlock_2_txn_cycle` / `..._3_txn` / `..._4_txn`
- **性能**：检测线程 CPU < 1%；tick p99 ≤ 5ms

**验收命令**：`cargo test -p pg-txn --test deadlock_detection`

---

## 阶段 S：HOT update + ARIES Undo（B+Tree CLR）（7–10 天）

**归属**：M2c
**前置**：M / P
**目标**：完成 §11.3 的简化 Undo（heap 天然屏蔽，仅 B+Tree 结构变更需要 CLR）+ HOT 基础版。

| 任务 | 交付物 |
|------|--------|
| HOT update 基础 | LP REDIRECT flag；`HEAP_HOT_UPDATED` / `HEAP_ONLY_TUPLE` infomask2 位；`t_ctid` 指向新版本 |
| `HeapHotUpdate` WAL | 记录 `page_id, old_slot → redirect, new_slot, new_tuple`；redo 幂等 |
| ARIES Undo 阶段 | 遍历 ATT XID：`clog.set_state(xid, ABORTED)`；heap 不需要逐条撤销（可见性天然屏蔽） |
| B+Tree CLR | Analysis 阶段扫到 `SPLIT_INCOMPLETE` 页 → Undo 阶段调 `BTreeAM::finish_incomplete_split(pid)` → 走 SplitCopy + SplitCommit 补齐 → emit `BTreeSplitCLR` |
| CLR 循环保护 | CLR 记 `redo_ref_lsn`；redo 遇到该 CLR 时对比 last redo LSN 防止循环 |
| Multixact 简版 | `FOR SHARE` 最小实现：用 `t_infomask` bit 标记共享锁，不引入独立 multixact 段；完整 multixact 推迟 Phase 6 |

**关键 v2.3 约束**：§10.3 / §11.3

**验收标准**：
- **功能**：HOT update 同页更新；空间够时旧 LP 转 REDIRECT
- **正确性**：`test_recovery_incomplete_split_undo`；`test_hot_update_page_local`；
  crash-restart 1000 轮包含 split-in-progress 场景全绿
- **并发**：与 Stage Q 并发压测联合运行不 regression

**验收命令**：`cargo test -p pg-am-btree --test btree_undo_clr && cargo test -p pg-am-heap --test hot_update`

---

## 阶段 T：100 并发压测 + Benchmark + M2c 综合验证（5–7 天）

**归属**：M2c
**前置**：所有 M2c stage
**目标**：M2c 出口 —— 100 conn × 100 txn/s 稳定 1h；benchmark 结果落盘。

| 任务 | 交付物 |
|------|--------|
| 100 并发压测 | **保底** 50 conn × 100 txn/s 混合读写 30min 无 crash、无泄漏；**挑战** 100 conn × 100 txn/s × 60min |
| 死锁注入压测 | 随机构造 2–4 事务环 1000 次，全部检测并 abort victim |
| Benchmark 集合 | WAL 顺序写 / BufferPool 随机读 / ClogBuffer 命中率 / B+Tree split 吞吐 / heap INSERT-UPDATE-DELETE |
| Benchmark 文档 | `docs/phase1-m2-benchmarks.md`：每项 target + 实测 + 未达标原因 |
| 崩溃自动化 | 1000 轮 kill -9（含并发写入中途）全绿 |
| 回归测试传承 | M1 crash_recovery 1000 轮 + M2a 100 线程 + M2b SI 50 事务全绿 |

**关键 v2.3 约束**：§20 P 类 target 允许下调；C 类 must-pass

**验收标准**：
- **功能**：50 conn × 100 txn/s × 30min 稳定（保底）；100 conn × 100 txn/s × 60min（挑战目标）
- **正确性**：1000 轮随机 crash 全绿；无 tuple 丢失 / 无双写 / 无可见性错乱
- **并发**：死锁检测 p99 ≤ 200ms；tick CPU < 1%
- **性能**（P 类；criterion 记录）：
  - WAL 顺序写 ≥ 200 MB/s（继承 M1）
  - BufferPool 随机读 ≥ 50K ops/s（继承 M1）
  - ClogBuffer 命中率 ≥ 95%（8 帧默认；256 帧配置 ≥ 99%）
  - heap INSERT ≥ 20K TPS（单线程；与 Stage K 一致），≥ 15K TPS（100 并发；比单线程低是因为叠加 LockManager 表锁 + 行锁 xmax 协议 + 死锁检测 tick）
  - 索引点查 ≥ 100K QPS

**M2c 出口 tag**：`phase1-m2c` / `phase1-m2-release`
**验收命令**：
```bash
cargo test --workspace --release
cargo bench -p pg-storage --bench wal_group_commit
cargo bench -p pg-storage --bench buffer_pool_concurrent
cargo bench -p pg-txn --bench clog_buffer_hit_rate
cargo bench -p pg-am-btree --bench create_index
cargo bench -p pg-engine --bench m2c_100_conn
```

---

## 总时间估算

| Stage | 归属 | 时间（1 名高级 Rust 工程师） |
|-------|------|---------------------------|
| A | 0a | 3–4 天 |
| B | 0a | 3–4 天 |
| C | 0a | 2–3 天 |
| D | 0a | 3–4 天 |
| **Stage 0a 小计** | | **11–15 天（1.5–2 周）** |
| E | 0b | 3–4 天 |
| F | 0b | 3–4 天 |
| **Stage 0b 小计** | | **6–8 天（1–1.5 周）** |
| G | M2a | 4–6 天 |
| H | M2a | 4–5 天 |
| I | M2a | 6–8 天 |
| J | M2a | 4–6 天 |
| K | M2a | 4–5 天 |
| **M2a 小计** | | **22–30 天（4.5–6 周）** |
| L | M2b | 7–10 天 |
| M | M2b | 10–14 天 |
| N | M2b | 5–7 天 |
| O | M2b | 7–10 天 |
| **M2b 小计** | | **29–41 天（6–8 周）** |
| P | M2c | 5–7 天 |
| Q | M2c | 7–10 天 |
| R | M2c | 3–5 天 |
| S | M2c | 7–10 天 |
| T | M2c | 5–7 天 |
| **M2c 小计** | | **27–39 天（5.5–8 周）** |
| **总计** | | **约 16–22 周（4–5.5 个月）** |

> Stage 0a + 0b 顺序执行约 3–3.5 周；Stage 0b 与 M2a 前 60%（Stage G+H）并行推进，
> 关键路径上 0b 被 G+H 吸收（G+H 8–11 天 ≥ 0b 6–8 天），实际节省约 1–1.5 周。
> M2 主线落地约 18–25 周；总表 16–22 周为乐观区间（假设无返工、0b 恰好不阻塞 M2a 后段）。

---

## 依赖图

```
                        ┌────────────────────┐
                        │ A (workspace + Oid) │
                        └─────────┬──────────┘
                                  │
                     ┌────────┬───┴──────────┐
                     ▼        ▼                ▼
                 ┌──────┐ ┌──────────────────────┐
                 │ B    │ │ C(SB v2 + record)    │   (B 独立分支，不阻塞 D)
                 │(WAL) │ └──────────┬───────────┘
                 └──────┘            │
                                     ▼
                        ┌─────────────────────────────────────┐
                        │ D (RedoHandler/ClogAccessor +       │
                        │   pd_lsn + PageHeader 32B)          │
                        └─────────┬───────────────────────────┘
                                  │  Stage 0a 出口
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼   (G/H 与 0b 完全并行)
         ┌────────┐          ┌────────┐          ┌────────┐
         │ E(FLM) │          │ F(装配)│          │ G      │
         └────┬───┘          └────┬───┘          └────┬───┘
              │                   │                   ▼
              │      Stage 0b 出口│              ┌────────┐
              │                   │              │ H      │
              │                   │              └────┬───┘
              │                   │                   ▼
              │                   │              ┌────────┐
              │                   │              │ I(Heap)│  (I 单线程验收前置 D/G/H；并发验收需 F)
              │                   │              └────┬───┘
              │                   │                   ▼
              │                   │              ┌────────┐
              │                   │              │ J(Txn) │  (J 集成测试需 F)
              │                   │              └────┬───┘
              └───────────────────┴──────────────────┘
                                  ▼
                    ┌─────────────────────────────────┐
                    │ K (Engine::open + M2a 100 线程) │ → M2a 出口
                    │   前置 F/H/I/J                  │
                    └───────────┬─────────────────────┘
                          ▼
                     ┌────────┐
                     │ L(MVCC)│
                     └────┬───┘
                          ▼
                     ┌─────────┐
                     │ M(BTree)│
                     └────┬────┘
                          ▼
                     ┌─────────┐
                     │ N(ARIES)│
                     └────┬────┘
                          ▼
                     ┌─────────┐
                     │ O(SQL)  │ → M2b 出口
                     └────┬────┘
                          ▼
                     ┌─────────┐
                     │ P(Lock) │
                     └────┬────┘
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
        ┌─────────┐              ┌──────────┐
        │ Q(BLink)│              │ S(HOT +  │  (S 前置 M/P，与 Q/R 并行推进)
        └────┬────┘              │  CLR)    │
             ▼                   └────┬─────┘
        ┌─────────┐                   │
        │ R(Dlock)│                   │
        └────┬────┘                   │
             └──────────┬─────────────┘
                        ▼
                   ┌──────────────┐
                   │ T(100 并发 + │ → M2c 出口 / M2 发布
                   │   benchmark) │
                   └──────────────┘
```

**关键并行边界**：
- Stage 0a 严格阻塞：A → {B, C} → D；B 与 C 是 A 的独立下游（B 修 WAL append 拆分，C 修 Superblock/WalRecordType/clog 目录），D 只依赖 A/C 的枚举扩展
- Stage 0b 内 E / F 平级并行（互不依赖，都从 D 下降）；G/H 也从 D 下降，与 E/F 三路并行
- M2a Stage G → H 顺序执行（H 前置 G）；G/H 整体与 Stage 0b 完全并行
- M2a Stage I 单线程部分在 G/H 完成即可开工；I 的**并发**验收 + J 的**集成测试** + K 全流程依赖 F 就位
- M2b 严格顺序（L → M → N → O），每 stage 依赖前一 stage 稳定 API
- M2c Stage P 之后分两条并行支线：Q → R（B+Tree 并发 + 死锁检测）与 S（HOT + CLR）；两支在 T 汇合

---

## 回归测试传承

每个 stage 出口必须继承通过前置阶段的所有回归：

| 出口 tag | 必须通过的历史回归 |
|---------|------------------|
| Stage 0a | M1 集成测试 + `crash_recovery` 1000 轮 |
| Stage 0b | 上述 + `test_freelist_rebuild_from_wal` + `test_read_at_100_threads` |
| M2a | 上述 + `test_m2a_crash_1000_rounds` + `bench_m2a_100_threads_insert` |
| M2b | 上述 + `test_visibility_oracle_m2b_cases` + `test_btree_split_crash` × 3 + `test_analysis_redo_from_100k_wal` |
| M2c | 上述 + `test_deadlock_2_txn_cycle`/3/4 + `test_recovery_incomplete_split_undo` + 100 conn × 100 txn/s × 60min |

任何 stage 引入的新特性若破坏前序回归，必须回退到该 stage 的实现方案，直到全绿。

---

## 第一周做什么

如果今天就开工，**Stage 0a 的前 5 天**优先级：

1. **Day 1**：Stage A —— workspace 6-crate 骨架 + `Oid` 类型 + CI 拆矩阵
2. **Day 2–3**：Stage B —— `WalWriter::append` 拆分 + `LsnClock::reserve` + `test_wal_before_data_on_evict`
3. **Day 4**：Stage C 前半 —— Superblock v1→v2 迁移（含单测）
4. **Day 5**：Stage C 后半 + Stage D 开局 —— `WalRecordType` 全枚举 + `clog/` 目录 + `ClogAccessor` trait 骨架

Stage 0a 出口目标 tag `phase1-m1-debt-clean-0a` 应在两周内完成，Stage 0b 与 M2a 从
第 3 周起并行推进。

