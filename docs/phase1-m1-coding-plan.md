# Phase 1 M1 编码顺序

> 基于 `docs/phase1-m1-tech-selection.md`，按依赖关系排列的编码执行计划。
> 每个阶段必须先通过单元测试 / 崩溃测试，再进入下一阶段。

---

## 阶段 A：项目骨架（1–2 天）

**目标**：把仓库、依赖、CI、测试框架搭好，避免后期返工。

| 任务 | 交付物 |
|------|--------|
| 创建 Cargo workspace | 根 `Cargo.toml` + `crates/pg-storage/`（M1 所有代码先放这里） |
| 添加核心依赖 | `tokio`, `parking_lot`, `bytes`, `crc32fast`, `thiserror`, `bincode`, `serde`, `tracing` |
| 添加测试依赖 | `proptest`, `tempfile`, `criterion` |
| 配置 CI | GitHub Actions：fmt + clippy + test + doc |
| 日志初始化 | `tracing_subscriber` 全局 logger |

> `loom` 在 M1 不引入，到 Phase 1 M2b 并发模型复杂时再添加。

**验收标准**：
- `cargo build` 通过
- `cargo test` 跑空测试通过
- CI 绿灯

---

## 阶段 B：基础类型与错误（1–2 天）

**目标**：定义所有 M1 会用到的原子类型、错误类型和配置。

| 任务 | 交付物 |
|------|--------|
| 页/LSN/事务 ID 类型 | `PageId`, `Lsn`, `TxnId`, `FrameId`, `Tid` 等 newtype |
| 常量配置 | `PAGE_SIZE: usize = 8192`（编译期常量，可通过 feature flag / cfg 切换为 16384）<br>`WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024` |
| 错误类型 | `StorageError` enum（I/O、页不存在、BufferPool 满、WAL 损坏等） |
| 配置结构 | `StorageConfig`（buffer_pool_size、wal_segment_size、group_commit_timeout 等） |

**配置来源（M1）：**
- M1 采用代码内默认值 + 环境变量（如 `PG_RUST_DATA_DIR`）
- `pg_rust.conf` 配置文件解析推迟到 Phase 1 M3

**验收标准**：
- 所有 newtype 有完整 unit test
- `StorageError` 能正确从 `std::io::Error` 转换

---

## 阶段 C：文件布局与 I/O 工具（2–3 天）

**目标**：实现数据目录、文件扩展、安全写、超级块读写。

| 任务 | 交付物 |
|------|--------|
| 目录创建 | `data_dir` 下创建 `data/`, `wal/`, `meta/`, `tmp/` |
| 安全文件写 | `write_atomic(path, bytes)`：tmp → fsync → rename |
| 文件扩展 | `preallocate_file(fd, new_size)` 使用 `fallocate` / `ftruncate` |
| 超级块 A/B | `Superblock::read` / `write` / `update`，双副本选择逻辑 |
| 元数据辅助 | `meta/freelist.meta` 读写 helper |

**验收标准**：
- 崩溃后超级块能选到最新有效副本
- 元数据文件原子更新正确

---

## 阶段 D：LSN 时钟 + WAL 段管理（2–3 天）

**目标**：实现 LSN 分配和 WAL 段文件的创建/切换/回收。

| 任务 | 交付物 |
|------|--------|
| LSN 时钟 | `LsnClock::next(record_size) -> Lsn` |
| 段文件名 | `wal_filename(segment_id) -> String` |
| 段管理器 | `WalSegmentManager`：open/close/rotate/recycle |
| LSN → 位置 | `segment_id(lsn)`, `segment_offset(lsn)` 计算 |

**验收标准**：
- LSN 单调递增且按 8 字节对齐
- WAL 段满后能自动创建新段
- 回收段能复用

---

## 阶段 E：WAL Writer（5–7 天）

**目标**：实现 WAL 记录序列化、组提交、刷盘。

| 任务 | 交付物 |
|------|--------|
| WAL 记录固定头手写序列化 | 32 字节固定头（24B header + 8B meta）+ payload + padding |
| Record type 枚举 | 完整定义 `WalRecordType`（含 M2 / Phase 2+ 预留变体，显式 `#[repr(u8)]` 判别子）；M1 仅实现 `PageAlloc` / `FullPageImage` / `CheckpointBegin` / `CheckpointEnd` 的处理逻辑 |
| Payload 编码 | 用 `bincode` 编码变长 payload |
| CRC32 | `crc32fast` 校验整条记录 |
| 写缓冲 + 组提交线程 | `WalWriter`：单线程顺序写入，组提交触发 |
| 同步接口 | `append()`, `flush()`, `flush_to(lsn)` |
| 查询已落盘 LSN | `synced_lsn() -> Lsn`：返回已经 fsync 到磁盘的最新 LSN |

**验收标准**：
- 单线程写入 100 万条记录不丢
- kill -9 后已 fsync 的记录可完整读取
- 跨段记录正确处理
- 组提交参数可配置

---

## 阶段 F：WalReader（2–3 天）

**目标**：实现 WAL 读取和 replay 基础设施。

| 任务 | 交付物 |
|------|--------|
| 记录读取 | `WalReader::read_from(start_lsn) -> impl Iterator<WalRecord>` |
| 跨段读取 | 自动打开下一个段文件 |
| CRC 校验 | 读取时验证，损坏则报错 |
| tail_follow 预留 | `WalReader::tail_follow(start_lsn: Lsn) -> impl Stream<WalRecord>`（Phase 3 完整实现） |

**验收标准**：
- 能正确读取 Writer 写入的所有记录
- 跨段边界读取正确
- CRC 损坏能检测

---

## 阶段 G：Page Allocator（3–4 天）

**目标**：实现页分配器，保证分配操作可恢复。

| 任务 | 交付物 |
|------|--------|
| 内存 freelist | `Vec<PageId>` |
| 分配页 | `alloc_page() -> PageId`（优先 freelist，否则扩展文件） |
| WAL 记录 | `PageAlloc { page_id }` |
| checkpoint 快照 | 将 freelist 写入 `meta/freelist.meta` |
| 恢复 | 加载快照 + replay WAL 中的 `PageAlloc` 推进 `next_page_id` |
| 释放 stub | `free_page(page_id)` 为 no-op（M2 实现） |

> M1 无 `PageFree`，恢复时不需要重建 freelist（freelist 在 M1 始终为空），只需把 `next_page_id` 推进到最新值。

**验收标准**：
- 分配 100 万次无泄漏、无重复
- 崩溃后 `next_page_id` 与文件大小一致
- proptest：分配/恢复不变量

---

## 阶段 H：Buffer Pool（5–7 天）

**目标**：实现页缓存、CLOCK 替换、pin/unpin、脏页跟踪、FPI 写入。

| 任务 | 交付物 |
|------|--------|
| Frame 结构 | `pin_count`, `dirty`, `reference_bit`, `page_lsn`, `content` |
| 分区 page table | 256 shards，每个 `Mutex<HashMap<PageId, FrameId>>` |
| pin / pin_mut / new_page | 返回 RAII guard |
| FPI 写入 | `pin_mut` 修改页前，若该页在当前 checkpoint 周期内首次修改，先写 `FullPageImage` WAL 记录，再修改页内容 |
| CLOCK 替换 | evict 时扫描 reference_bit，跳过 pinned frames |
| 脏页 flush | `flush(page_id)`，遵守 WAL 先行规则 |
| 并发测试 | 100 并发 pin/unpin 无死锁、无泄漏 |

**flush 流程：**
1. 获取 `frame.page_lsn`
2. 若 `WalWriter::synced_lsn() < frame.page_lsn`，调用 `WalWriter::flush_to(frame.page_lsn)`
3. 执行 `pwrite` 将页内容写入数据文件
4. 清除 frame.dirty flag

**验收标准**：
- pin_count > 0 的页不被 evict
- 脏页 flush 前 WAL 已 fsync
- 并发访问下无死锁
- 全表扫描不污染缓存（CLOCK 验证）

---

## 阶段 I：Checkpoint Coordinator + 统一恢复入口（3–4 天）

**目标**：实现 fuzzy checkpoint 和崩溃恢复主流程。

| 任务 | 交付物 |
|------|--------|
| Checkpoint 触发 | `trigger_checkpoint()`（手动 + 后台定时） |
| 收集脏页 | 扫描所有 frame，收集 dirty=true 的页 |
| Full Page Image | FPI 由页修改者写入 WAL（checkpoint 不补写）；checkpoint 只负责 begin/end + 刷脏页 |
| Checkpoint 记录 | `CheckpointBegin` / `CheckpointEnd`；M1 的 `CheckpointEnd` 字段：`checkpoint_lsn: Lsn, next_page_id: PageId, next_txn_id: TxnId`（无 ATT，无 DPT——M1 无事务，故无 ATT；脏页由 frame dirty flag 扫描识别，故无需 DPT。M2 起增加 ATT + DPT 支持完整 ARIES recovery） |
| 更新超级块 | `checkpoint_lsn`（即 `CheckpointBegin` 的 LSN，作为恢复起点 / redo_lsn）、`next_page_id`、`next_txn_id` |
| WAL 回收 | 删除/回收 LSN < `checkpoint_lsn` 的段 |
| 统一恢复入口 `recover()` | 按 tech selection §十一 流程：读取 superblock → 加载 freelist 快照 → 从 `checkpoint_lsn` 开始 replay WAL（含 FPI）→ 完成 |

> **checkpoint_lsn 语义**：`checkpoint_lsn` 记录 `CheckpointBegin` 的 LSN（即 redo_lsn）。恢复时从该 LSN 开始 replay，确保 checkpoint 周期内产生的 FPI 被重放，从而修复 torn page。
>
> M1 没有 Heap/BTree 修改，FPI 主要出现在集成测试里：alloc_page 后通过 BufferPool 写入页内容会触发 FPI。

**checkpoint 并发语义：**
- checkpoint 期间允许并发读写
- 单个页 flush 时短暂阻塞该页的 `pin_mut`（读不受影响）

**验收标准**：
- checkpoint 后 kill -9，恢复后数据一致
- 旧 WAL 段正确回收
- `recover()` 能从任意干净/崩溃状态恢复到一致状态

---

## 阶段 J：集成测试与崩溃测试（3–5 天）

**目标**：把 M1 所有组件串起来，验证整体正确性。

| 任务 | 交付物 |
|------|--------|
| 集成测试 | alloc page → write via BufferPool → WAL → checkpoint → restart → read |
| 手动崩溃测试 | 固定场景 kill -9 + 重启校验（先跑通流程） |
| 自动化崩溃测试 | fork + 随机 kill -9 + proptest（扩大覆盖） |
| property test | PageAllocator 无泄漏、WAL 可 round-trip、BufferPool 不变量 |
| 并发 stress test | 100 并发 pin/unpin 无数据竞争 |

> `loom` 模型检查推迟到 Phase 1 M2b；M1 并发面有限（LsnClock 单写者 + BufferPool 分区锁），stress test 足够覆盖。

**验收标准**：
- 手动崩溃测试：10 个固定场景全部通过
- 自动化崩溃测试：1000 次随机 kill -9 无数据丢失
- proptest 10000 次通过
- 并发 stress test 通过

---

## 阶段 K：基准测试（2–3 天）

**目标**：建立 M1 性能基线，验证是否达到 ROADMAP 性能基线。

| 任务 | 交付物 |
|------|--------|
| WAL 吞吐 benchmark | 顺序写 MB/s、IOPS |
| Buffer Pool benchmark | 随机读 ops/s、命中率 |
| Page Allocator benchmark | 分配 ops/s |

**验收标准**：
- WAL 顺序写 ≥ 200 MB/s（本地 SSD）
- Buffer Pool 随机读 ≥ 50K ops/s（8KB page）
- 有可复现的 benchmark 脚本
- 结果写入 `docs/phase1-m1-benchmarks.md`；未达标项记录原因和优化方向

---

## 总时间估算

| 阶段 | 时间（1 名高级 Rust 工程师） |
|------|---------------------------|
| A | 1–2 天 |
| B | 1–2 天 |
| C | 2–3 天 |
| D | 2–3 天 |
| E | 5–7 天 |
| F | 2–3 天 |
| G | 3–4 天 |
| H | 5–7 天 |
| I | 3–4 天 |
| J | 3–5 天 |
| K | 2–3 天 |
| **总计** | **约 7–9 周** |

> 注：乐观 6 周需要无返工。实际偏向 7–9 周。如果期间发现设计问题需要调整，可能延长。

---

## 关键依赖图

```
A (项目骨架)
  │
  ▼
B (类型/错误)
  │
  ▼
C (文件布局/I/O) ──▶ D (LSN/段管理)
  │                    │
  ▼                    ▼
G (Page Allocator) ◀── E (WAL Writer)
  │
  ▼
H (Buffer Pool)
  │
  ▼
I (Checkpoint + Recovery) ◀── F (WalReader)
  │
  ▼
J (集成/崩溃测试)
  │
  ▼
K (Benchmark)
```

---

## 第一阶段优先做什么？

如果今天就要开始写代码，**前 3 天的优先级**是：

1. **今天**：阶段 A + B（仓库、依赖、基础类型）
2. **第 2 天**：阶段 C（文件布局、超级块）
3. **第 3 天**：阶段 D + E 开头（LSN 时钟、WAL 段管理、WAL header 序列化）

先让 `PageAllocator` 和 `WalWriter` 能跑通最小测试，再回头补 Buffer Pool 和 Checkpoint。
