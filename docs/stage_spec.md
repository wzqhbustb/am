# Stage Spec — 各阶段实现规格与 PG 对照

> 记录每个 Stage 的交付内容、设计决策理由、以及与 PostgreSQL 的取舍。
> 本文档与 `docs/phase1-m2-tech-selection.md`（设计选型）、`docs/phase1-m2-coding-plan.md`（编码计划）配套：选型文档记录"打算怎么做"，本文档记录"实际怎么做的"。

---

## Stage L：Snapshot + curcid + Disk ClogBuffer + VisibilityOracle

**状态**：✅ 完成（M2b，commit `ad0395a`）
**工期**：预估 7–10 天
**验收**：§7.2 六用例 oracle 测试 7/7；ClogBuffer 集成测试 13/13；TP 负载 8 帧命中率 ≥95%（criterion）

### 交付内容

1. **Snapshot（`pg-txn/snapshot.rs`）**
   - §7.1 全字段：`xmin / xmax / xip`（SmallVec 32 内联槽）/ `current_xid` / `curcid`
   - 纯 XID 判定，无 LSN 参与（v2 修订 P2-8 移除 v1 的 `snapshot_lsn`）
   - `TxnManager::snapshot()` 在同一把 active-set 锁内同读 XID 时钟与活跃集合，`xmin <= xip[i] < xmax` 结构不变式由构造保证

2. **Disk ClogBuffer（`clog_buffer.rs` + `clog_file.rs`）**
   - 段文件 `clog/clog-{segment:08}.log`，128 MiB/段 = 2.68 亿 XID；首次 touch 稀疏预分配，未触碰区域读零即 `InProgress`，无需存在性检查
   - 4-bit/XID（高 nibble = 偶数 XID）：`0=InProgress / 1=Committed / 2=Aborted / 3=SubCommitted`（M3 保留）
   - SLRU：8 KiB 页 × N 帧 clock-sweep（默认 8 帧 = 12.8 万 XID 窗口，可配 [4, 1024]）；第一轮只逐干净帧，脏帧给第二次机会，整圈无解才带写回逐脏帧
   - 持久性（§6.4 / v2.3-21）：`set_state` 只标脏；驱逐写回**不 fsync**；唯一 fsync 点是 checkpoint Begin/End 之间的 `flush_dirty()`（`ClogFlush` trait 钩子）；`unsynced_segments` 兜底"驱逐写回但从未 fsync"的段（L 评审 P1 修复）
   - 崩溃丢失的 bit 由 `TxnCommit/TxnAbort` WAL redo 幂等重建
   - 单把 `RwLock` 护全部帧（"正确但粗"，分片是 Phase 7b）；自带 hits/misses 可观测量

3. **VisibilityOracle（`visibility.rs`）**
   - `Visibility` 三态（`Visible / Invisible / Uncertain`）；`Uncertain` 为 M2c 行锁等待预留，M2b 永不返回
   - `PgVisibilityOracle` 实现 §7.2 全判定**含 curcid 分支**——但 L 时仅在 oracle 层 + 单测；heap 实际调用的自由函数 `is_visible` 仍是 M2a 兼容版（无 `t_cid` 参数），executor 接线归 Stage O
   - hint bits：四变体枚举 + `set_hint_bit` trait 就位，默认 no-op（回写通道 Phase 7）

4. **begin 原子性修复（L 评审 P1）**
   - XID 时钟分配与活跃集插入合并到同一把锁内（对标 PG"释放 XidGenLock 前先注册 ProcArray"），消除"alloc 了但没 insert"的 SI 违例窗口，附确定性回归测试

5. **Engine 集成**
   - 装配顺序：ClogBuffer → storage recovery（注入同一 CLOG，redo 幂等写入终态）→ checkpoint ClogFlush 钩子 → M2a `clog-snapshot.bin` 一次性迁移（缺失 = no-op，损坏 = 硬错误）→ Catalog → HeapAM/TxnManager
   - M2a 的内存 `TrackingClog`（225 行）整体删除
   - commit/checkpoint barrier 保留承重：防"commit WAL 落在 begin_lsn 之前（replay 不重放）但 `set_state` 落在 checkpoint CLOG flush 之后"的两不沾窗口

### 设计理由

**1. 为什么 4-bit/XID 而非 PG 的 2-bit？**
每 XID 独占 nibble，无位掩码竞态、无跨 XID 读改写。代价是段密度减半（128MB/2.68 亿 XID vs PG 32MB/2.56 亿），磁盘换简单。不存 commit_lsn——判定纯靠 XID 关系。

**2. 为什么独立 ClogBuffer 而不复用 M1 BufferPool？**
CLOG 页无 `pd_lsn`/`pd_checksum`，混进按 `PageId` 索引、带 checksum/redo 路径的 BufferPool 会破坏其语义。

**3. 为什么 fsync 收紧到 checkpoint 单点？**
commit 路径持久性全靠 WAL 记录；CLOG bit 的 fsync 全部摊到 checkpoint。配合 commit barrier 保证"已 checkpoint 回收的 WAL 前缀对应的 bit 必然已 fsync"。崩溃丢失由 redo 重建，语义闭合。

**4. 为什么 curcid 在语句开始前 +1？**
同语句 self-scan 共享同一 curcid，`t_cid < curcid` 对本语句写入为假 → 跳过自身，天然 Halloween 保护；下一语句递增后前语句写入变为可见（§7.1 Q4 / v2.3-3）。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage L) | 取舍理由 |
|---|---|---|---|
| XID | 32-bit + wraparound/freeze | **64-bit**，无 wraparound、永不 freeze | xmin/xmax 各 8B（PG 4B）；无 vacuum freeze 概念 |
| CLOG 编码 | 2-bit/XID，位级 CAS | **4-bit/XID**，nibble 直读直写 | 密度减半换无锁简单 |
| SLRU 结构 | pg_clog SLRU，commit 路径也可能写回 | 结构同构（8KB 页/段/clock-sweep），fsync 收紧 checkpoint 单点 | 崩溃丢失由 WAL redo 重建兜底 |
| 快照一致性 | ProcArray 细粒度协议 | 单把 active-set 互斥锁 | M2b 规模够用；分片 Phase 7b |
| cmin/cmax | 双字段 + combo CID | **单 `t_cid`** + 分支对称判定 | combo CID 场景（同语句插删对外可见性）不处理，M2b 裁剪 |
| SSI | 可选可串行化 | **不做** | Phase 7d |

### 已知残留与后续归队

- engine scan/auto-commit 仍用 `Snapshot::everything()`，oracle/curcid 协议就位但未接线 → **Stage O 落地**
- 自由函数 `is_visible` 无 curcid 分支、heap 不盖 `t_cid`（Halloween 保护在生产路径未激活）→ **Stage O 落地**
- `txn_manager()` 后门绕过 commit barrier（注释声明 UB）→ M2c 下沉 barrier 进 TxnManager
- ClogBuffer 全局单锁 → Phase 7b 分片；hint bit 回写 → Phase 7
- 无 checkpoint ATT 快照 → **Stage N 加 AttProvider**

---

## Stage M：B+Tree AM 单线程 + Split 三步 WAL + 阻塞式 CREATE INDEX

**状态**：✅ 完成（M2b，414 个测试全绿，含 3 个崩溃点恢复测试）
**工期**：预估 10–14 天
**验收**：`btree_split_crash` 3/3；100 万 INSERT + CREATE INDEX ~14.9s（≤ 30s）；redo 幂等重放 10 次一致

### 交付内容

1. **页格式与 key 编码**
   - 复用 Stage G 的 slotted page（"一种页格式服务所有 AM"兑现）
   - 16B special 区：`btpo_prev`(0..8) / `btpo_next`(8..16)（Blink 兄弟链）
   - `pd_flags` bit 8..11 = `btpo_level`（0=leaf）、bit 12..15 = `btpo_flags`（LEAF / ROOT / DELETED / SPLIT_INCOMPLETE）
   - 保序 key 编码：Int4/Int8 用 sign-bit 翻转大端序；Text/Bytea 用原始字节序
   - 内部页 entry = `key ++ child_page_id(8B)`；叶子页 entry = `key ++ tid(10B)`；无 64B TupleHeader（索引条目无 xmin/xmax，§7.3）
   - LP 数组按 entry 保序（支持页内二分），重复 key 以 `(key, tid)` 全序决胜

2. **Split 三步 WAL 协议**（核心交付）
   - `BTreeSplitPrepare=5`：锚点 + `SPLIT_INCOMPLETE` 置位 + `left_old_next`（补 spec 的洞：左页 post-Prepare 镜像落盘后原 next 读不回，必须随 payload 携带）
   - `BTreeSplitCopy=51`：payload 极简（`copy_start_slot + left_page_pre_lsn`），redo 从 left_page **重算**搬运内容，幂等锚点 `left_page.pd_lsn == pre_lsn`
   - `BTreeSplitCommit=52`：父页插入 `(separator_key, right_page)` 分隔键 + 清 `SPLIT_INCOMPLETE`
   - 落盘纪律：**右页先 flush 才释放左页 latch**——保证左页 pre-copy 镜像永远可恢复，"左已截断但右缺拷贝"在结构上不可能（出现即 `MetadataCorrupted` 硬失败，不静默）
   - Copy 应用 = 左页重建压实（非裸截断 LP 数组——裸截断的死空间会让触发 split 的 insert 仍放不下），在线/redo 共用同一函数保证字节级一致

3. **读/写路径**
   - 点查：meta → root → 下降到叶（latch coupling 骨架），叶子内二分
   - 范围扫：叶链 `btpo_next` walk；下降过程内部层右跳、叶层**双向 sibling walk**（stale 分隔键兜底——兄弟链是 ground truth，分隔键只是提示）
   - 内部页 slot 0 = ∅(-inf) 标记（PG P_HIKEY 惯例，见下"设计理由"）
   - 单线程全路径独占 latch；`validate()` 做全序子树边界校验（`last < first`）

4. **阻塞式 CREATE INDEX（bulk load）**
   - 全表扫 → `(key, tid)` 全序排序 → 叶子 100% 写满自左向右 → 内部层自底向上（slot 0 = ∅）→ 每页一条 post-image FPI（~22MB WAL / 1M entries）→ **meta 记录最后写**（中途崩溃只剩孤儿页，零半成品）
   - 实测：1M entries ≈ 1.02s，比逐条 insert 快约两个数量级

5. **Engine 集成**
   - `Engine::create_index(table, column) -> Oid` / `index_lookup(table, column, key) -> Option<Tid>`
   - catalog 写入：`pg_class`(relkind='i', relam=403) + `pg_attribute`(兼任"索引哪列") + `pg_index`(page 5) + `pg_rust_relpages`(meta 页位置)
   - **DML 事务内同步维护索引**（review 后补齐）：insert/delete/update 与被维护表的索引在同一 auto-commit 事务内原子完成（NULL key 跳过）
   - redo registry 追加 5 个 btree handler（Insert/Delete/SplitPrepare/SplitCopy/SplitCommit）

6. **Review 修复清单**（三轮对抗审查后）
   - DML 同步维护索引（P1：修复前索引"建成即陈旧"）
   - `split_prepare` 的 `SPLIT_INCOMPLETE` 防护（禁止对未完成分裂的页二次分裂，否则旧右孪生永久孤儿化、毁掉 M2c CLR 收尾前提）
   - root 代际校验（防止旧句柄创建第二 root 覆写 meta 导致半树不可达）
   - Copy redo 静默跳过分支硬化（右页已持拷贝→重建左页；双侧不符→硬失败）
   - CatalogFull 预检前移（build 前按真实编码预检 4 行）
   - `create_new_root` 的 level ≥ 0x0F 显式报错（4-bit 溢出变契约）
   - `set_flag` 静默吞错改 `Result`；`apply_prepare/commit_left` 补全零页 init 守卫

### 设计理由

**1. 为什么 split 用"声明意图 → 可重算执行 → 提交"三步？**
分裂是跨页多步操作（左截断 + 右搬运 + 父插入），崩溃可落任意中间点。三步把"原子性"翻译成 WAL 语义：Prepare 给幂等锚点；Copy 利用"给定左页状态 + 起始 slot，搬运内容确定"的事实让 payload 保持 O(20B)（半页数据可能 10KB，记进 WAL 太奢侈）；Commit 显式标记不归点。配合 `SPLIT_INCOMPLETE`，崩溃后的任何前缀都能被 redo 精确续上或安全跳过。

**2. 为什么 Copy 的 redo 是"重算"而非"搬运"？**
redo 不是把 WAL 里的数据抄回页面，而是重新执行搬运操作。WAL 体积最小化 + 重放天然幂等（`pd_lsn == pre_lsn` 说明"还没动过"才执行）。代价是必须严格维护"左页 pre-image 可恢复"的落盘纪律——这个前提在 review 后从注释升级为有硬失败背书的协议。

**3. 为什么内部页 slot 0 用 ∅(-inf) 标记？**
内部页分隔键会 stale（删除推高孩子 low key、分裂改变边界）。真实 low key 作标记会 stale-high（逆序插入时把孩子藏到标记左边），∅ 标记只会 stale-low，由叶层双向 sibling walk 兜底。Blink 的设计核心是"分隔键是提示不是真理，兄弟链才是 ground truth"——`validate()` 因此检查全序链而非父键区间。

**4. 为什么 bulk load 不用 split 协议而直接铺页 + FPI？**
split 的 Copy redo 语义是"从既有左页重算搬运"，bulk load 的页是全新内容，语义不符。每页一条 post-image FPI 比逐条 insert 快两个数量级；meta 最后写让中途崩溃只剩孤儿页、零半成品。

**5. 为什么索引条目不参与可见性判定？**
索引只有 `(key, tid)`，没有 xmin/xmax。悬空引用（指向已删/abort 的行）由 heap 层可见性判定天然屏蔽（§7.3 契约）。索引不复制 MVCC 状态。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage M) | 取舍理由 |
|---|---|---|---|
| Split WAL | 单条 `xl_btree_split` 记录，右页内容随记录携带（block data） | **三条记录**，右页内容不记、redo 重算 | WAL 体积更小；借鉴 PG 全部锚点语义（`firstrightoff`↔`copy_start_slot`、incomplete-split 标志）但骨架自构 |
| 并发 | Blink latch coupling 读 + 乐观/悲观写 | **M2b 全单线程**（整路径独占），M2c 才做 Blink 并发（Stage Q，含 loom） | 先把协议骨架和恢复正确性做对，并发后上 |
| 内部页分隔键 | high key 完整键 + tid tiebreaker（PG ≥12） | key-only 分隔 + ∅ 左脊柱 + sibling walk 兜底 | 简单；已知残留：重复 key 跨内部页边界（~20 万同 key）退化，M2c 上 tid 分隔符根治 |
| CREATE INDEX | 在线建索引（读写不阻塞） | **阻塞式**（全表扫+排序+bulk load），无表锁（扫描期间新写入不进索引，已文档声明） | M2b 规模下正确性优先；在线建索引归 Phase 7 |
| 页合并 | 有（balance merge） | **不做**（只分裂不合并） | M2b 明确裁剪；删除只回收 tuple 不回收页 |
| 唯一索引 | 执行层唯一性检查 | `indisunique` 字段就位**不执行** | 归 Stage O/后续 |
| 索引维护 | DML 自动维护所有索引 | review 前"建成即快照"，**修复后** DML 事务内同步 | 对抗审查抓出后补齐，避免语义炸弹 |
| DROP INDEX | 支持 | **不支持** | 后续 stage |
| MVCC 集成 | 索引无可见性，靠 heap 回查 | 同 PG（§7.3 契约） | 一致，无取舍 |

### 已知残留与后续归队

- 重复 key 跨内部页边界（~20 万+ 相同 key 门槛）：结构可读但退化 → M2c tid 分隔符根治
- `BTreeSplitCLR` / `finish_incomplete_split`（未完成分裂收尾）→ M2c undo（Stage S）
- BTreeDelete 在线语义（现在只有 redo handler；trait delete 为 O(n) 链扫兜底）→ M2c
- 唯一索引执行、DROP INDEX、多列索引 → 后续 stage
- SQL 层入口（`CREATE INDEX` 语句、planner 索引选择）→ Stage O

---

## Stage N：ARIES Analysis + Redo + CheckpointEnd v1/v2 迁移

**状态**：✅ 完成（M2b，commit `3435ce6`；448 测试全绿）
**工期**：预估 5–7 天
**验收**：`aries_analysis_redo` 8/8（10 万 record analysis+redo 2.24s ≤ 10s）；`checkpoint_v1_v2` 迁移测试通过

### 交付内容

1. **Analysis（`pg-storage/analysis.rs`）**
   - `find_latest_checkpoint_end`：从 superblock `checkpoint_lsn` 起扫，取最后一个**已完成**的 CheckpointEnd（悬空 Begin 天然忽略）
   - `run_analysis`：ATT/DPT 快照文件 seed 基线 + 扫到 WAL 尾；带 `txn_id` 的记录入 ATT、Commit/Abort 移除；DPT `or_insert` 保留首次脏页 LSN
   - `for_each_touched_page`：bincode 前缀解码只取 PageId，不解 tuple/FPI 全载荷（11 种 page-modifying 类型逐一比对字段序）

2. **Redo 统一分发**（Stage D 机制，本 stage 接线验证）：严格 LSN 序；FPI 与其他 handler 同一 `RedoRegistry`；未注册类型硬失败（v2.3-24）

3. **CheckpointEnd v2**
   - 6 字段：`checkpoint_lsn / next_page_id / next_txn_id / next_oid / att_file / dpt_file`
   - v1/v2 分派 `flags >> 4`（M1 冻结的记录头 `flags` 是 u8，版本号占高 4 位，低 4 位留记录级 flag）；未知版本硬错误（前向 crash 保护，v2.3-17）
   - v1 默认 `next_oid=16384`；权威源仍是 superblock，record 值永不消费（write-only，防未来误信）
   - v1 记录永不改写，升级靠 M2 自己 emit v2 自然完成

4. **ATT/DPT 快照文件**
   - `meta/{att,dpt}-{lsn:016}.snapshot`，bincode + CRC32，`write_atomic`（temp + fsync + rename + 目录 fsync）
   - 三步硬序：`fsync(快照文件) → wal.append(CheckpointEnd) → flush_to(end_lsn)`，superblock 最后更新（与 §3 P1-5 commit 硬序同风格）
   - prune 保留最近 3 组 + **superblock 当前组**（防连续夭折 checkpoint 的孤儿组挤出有效基线）；文件名解析接受任意长度数字串
   - 快照缺失/CRC 损坏 → 独立降级为空基线全扫描

5. **B+Tree split redo 四态硬化**（review 修复）
   - `== anchor` 正常重放，应用后 **`pool.flush(right)`**——redo 路径补齐线上"右页先落盘才放左 latch"纪律，关闭"恢复期间部分刷盘再崩溃变砖"窗口
   - both-past 幂等跳过（修掉旧代码误截 post-copy 插入的真 bug）；左落后右已有 → 重建左页；其余 → 硬失败
   - 删除错误的 `apply_split_move_to_right_only`（其前提状态不可达，可达时反把硬失败降级为静默损坏）

6. **WAL 全零洞防护**（Stage B 遗留，review 抓出）
   - `reserve_and_append` 原子化：单次锁持内完成时钟推进 + 写段，消除 reserve→append 窗口
   - reader 遇全零 header 前探一个 header 宽度，后有非零数据 → `MetadataCorrupted` 硬失败（不再静默截断、不再让新 WAL 覆盖洞后已提交记录）

7. **Buffer pool 两处竞态修复**
   - P2-1：`pin_mut` 持 guard 期间即标脏——fuzzy checkpoint 不再漏"已写 WAL 但 guard 未 drop"的页
   - P2-2：flush 失败 `first_dirty_lsn` 改 min-merge 恢复（取最旧锚点，防 rec_lsn 高估跳 redo）；`flush_frame` 对 replayed-LSN 页跳过 `flush_to`

8. **ATT 正确性接线**
   - `AttProvider` trait + `TxnManager` 实现；recovered ATT 在 redo 重建 CLOG 后过滤（去已知终结成员，闭合 §11.4 快照竞态）
   - pg-engine `commit_barrier`：commit 硬序与 checkpoint 互斥；无 barrier 的 storage-only 路径文档明确标注 unsafe（M2c 下沉进 TxnManager）

### 设计理由

**1. 为什么 redo LSN 恒等于 `checkpoint_lsn`（而非 `min(DPT.rec_lsn)`）？**
双向钳制（coding plan Stage N 实现修订注）：不能更晚——redo point 与首个脏页记录之间的 `TxnCommit/Abort` 必须重放以重建 CLOG；不能更早——DPT 快照摄于 CheckpointBegin，条目 rec_lsn 均 < begin_lsn，而完成的 checkpoint 在 emit End 前已将这些页全部刷盘，其 WAL 段可能已被回收。DPT 仍完整返回（观测 + 未来 Undo 用）。

**2. 为什么不做显式 Heap Undo？**
无终止记录的 XID 在重建 CLOG 中读作 `InProgress`，MVCC 下与显式 `Aborted` 等效——"过滤"替代"补偿"，省掉整个 undo/CLR 子系统（B+Tree 结构变更的 CLR 收尾归 M2c Stage S）。

**3. 为什么 ATT/DPT 存独立文件而非塞进 CheckpointEnd payload？**
大 ATT（10 万级 XID）进单条 WAL 记录太奢侈；CheckpointEnd 只带文件名引用，快照文件独立 atomic write + 独立降级。

**4. CheckpointEnd 新于 superblock 怎么办（crash 在 flush_to 与 superblock 写之间）？**
不用 WAL 里更新的 End，而是合成 v1 等价空基线锚点、从 superblock redo point 全量重建——保守但确保两个 redo point 之间的记录全覆盖。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage N) | 取舍理由 |
|---|---|---|---|
| 恢复模型 | 无 ATT/DPT 概念，从 checkpoint redo 点全量重放 | 教科书 ARIES Analysis 建 ATT/DPT，但 redo 起点恒等于 `checkpoint_lsn` | 效果与 PG 等价；DPT 为 M2c Undo/CLR 预留观测面 |
| Heap Undo | 不做（abort = CLOG 标记，非教科书 ARIES） | 同 PG：不做，`InProgress ≡ Aborted` | 语义等效，省掉补偿日志 |
| Checkpoint 元数据 | `pg_control` 内嵌 checkpoint 记录 | superblock（双副本）+ CheckpointEnd v2 + ATT/DPT 独立快照文件 | ATT/DPT 独立文件支持大事务数；PG 无此需求 |
| 格式迁移 | 大版本 `pg_upgrade` 离线迁移 | v1/v2 在线 decode 分派，v1 永不改写 | 单文件格式内渐进升级 |
| CLOG 持久化 | checkpoint 时 CheckPointCLOG | 同（Begin/End 之间 `flush_dirty`） | 一致，无取舍 |
| 恢复扫描 | 单遍 redo | 三遍（find_latest / analysis / replay） | 可读性优先；合并是已知优化项 |

### 已知残留与后续归队

- ATT 空基线降级对"begin 前已无 WAL 活动的空闲事务"不完整（当前无生产消费者，**M2c undo 落地前必须处理**：文档限定 + 降序重试旧快照）
- `open_at` 起始段缺失时硬失败，文档承诺的 warn + 空基线降级未实现（灾难-only 路径，多段丢失无测试）
- `reserve_and_append` 时钟推进后 encode/IO 失败可留 >32B 洞，reader 前探（仅 32B）漏检（待修：推进前校验 payload 长度 + IO 失败毒化 writer）
- analysis/replay 的 catch-all 对 `MetadataCorrupted`/`WalReadFailed` 仍 warn+break（engine 路径由 writer open 先行拦截，pub API 直接调用有静默截断风险）
- 恢复三遍全量扫描可合并（find_latest 可短路）
- both-past 跳过的回归测试输入不含 post-copy 插入，对新旧行为不可区分（待补强）
- 测试缺口：小段 + 段回收、快照 CRC 损坏降级、孤儿快照、checkpoint 介入 split 三步、恢复中二次崩溃
- `evict_frame` 刷盘失败帧泄漏（pre-existing，非本 stage 引入）
- coding plan Stage N 表格 `flags >> 12` 系笔误（实现为 `>> 4`），待回写

---

## Stage O：SQL parser + M2b 综合验证（M2b 出口）

**状态**：✅ 完成（496 测试全绿；M2b 出口，`phase1-m2b` tag 待打）
**工期**：预估 7–10 天
**验收**：`m2b_integration` 20/20 + `si_isolation_50_txn`（50 线程 SI 硬断言）；§7.2 SQL 层 4 用例（2 个 RETURNING 用例子集 N/A，由 pg-txn 单测覆盖）；INSERT+COMMIT **4.24ms**（≤5ms）；索引点查 **~927K QPS**（≥100K，criterion `m2b_perf`）

### 交付内容

1. **硬编码 SQL parser（`pg-engine/sql.rs`）**
   - 子集：`BEGIN / COMMIT / ROLLBACK / CREATE TABLE / INSERT（多行，可选列清单）/ SELECT [WHERE eq/lt/gt] [ORDER BY 单列 ASC|DESC] [LIMIT N] / UPDATE (WHERE) / DELETE (WHERE) / CREATE INDEX`
   - tokenizer → AST → Datum 全程无字符串拼接（无注入面）；标识符统一折叠小写（同 PG 未加引号语义）；可选单末尾分号
   - 不支持清单（`--`/块注释、quoted identifier、多语句、RETURNING、JOIN、聚合、`<=/>=/<>`）在模块文档如实列出

2. **`exec(Option<&TxnHandle>, sql)` + `TxnHandle`**
   - auto-commit 传 `None`，显式事务传 handle；`commit/abort` consume self（abort 后不可用是**编译期**保证）；`RefCell<Snapshot>` 使 handle `!Sync`
   - Drop 自动 best-effort abort（持 `commit_barrier` + 失败 warn 日志）；`instance_id` 校验防跨 Engine 实例混用
   - `exec(None)` 收到 BEGIN/COMMIT/ROLLBACK 显式报错（review 修复：原静默 Ok 是"测试全绿、数据悄悄持久化"型 footgun）
   - 显式事务内 DDL 显式拒绝；语句中途失败无语句级回滚，唯一安全操作是 `abort()`（已文档化）

3. **curcid executor 接线**（Stage L 协议落地）
   - 每语句开始前 `advance_curcid()`；新 tuple `t_cid = curcid`；`stamp_deleted` 盖删除时 curcid
   - 自由函数 `is_visible` 重写为完整 §7.2（`xmin==self / xmax==self` 分支激活，v2.3-3 / Q4）
   - `begin_txn` 一次快照全事务复用（SI）；auto-commit 每语句新快照（等价 RC）

4. **Halloween 双保险**：UPDATE/DELETE 先物化全量扫描再逐行写 + `t_cid == curcid` 分支

5. **索引事务性**（review 拦路虎修复）
   - `index_lookup` 可见性掩码：`lookup_all` 走叶子链枚举重复 key 全部 TID，逐个回堆 §7.2 判定，第一个可见胜出
   - per-txn 索引 undo 日志：`Inserted/Deleted` 按 XID 记录（UPDATE = 两条），abort / Drop / auto-commit 失败三处**逆序**回放（Inserted → `(key,tid)` 精确删，Deleted → 重插）；commit 丢弃；undo 失败 best-effort + warn
   - 8 个专项测试（`m2b_index_txn.rs`）：insert/delete/update-abort 三向发散全部钉死，修复前必然失败

6. **崩溃自动化**：`m2b_crash_rounds` 子进程 kill -9；偶数轮全量精确比对，奇数轮前缀持久性验证（"至多 1 行多余"由 seed 设计保证逻辑严密）；默认 25 轮（CI），`M2B_CRASH_ROUNDS=1000` 为验收配置（~30–60 分钟，手动）

7. **Review 修复清单**（四轮对抗审查后）
   - 索引事务性（见 5）；`exec(None)` 事务语句报错；i64→i32 静默截断改 `try_from` 报错（插入错值 + WHERE 查错行双危害）
   - Drop abort 补 barrier + warn；标识符大小写规则统一；尾部分号；`instance_id`；`lookup_all` 跨 7 叶 3000 重复项测试
   - 负例测试（畸形 SQL / 未知表 / 类型不匹配 / 事务内 DDL）；§7.2 用例注释去虚报（case2/3 标 N/A）；`exec_crash_recovery_basic` 正名 `exec_clean_shutdown_reopen`

### 设计理由

**1. 为什么硬编码 parser 而非引入 sqlparser crate？**
子集极小（约 10 种语句形态），零新增依赖，tokenizer→AST 直译。它是 Phase 4a DataFusion 到来前的一次性脚手架——控制依赖面优先于表达能力。

**2. 为什么 TxnHandle consume-self？**
commit/abort 后句柄不可用由类型系统保证，消灭"use after abort"整类错误；`!Sync` 阻止跨线程共享同一事务上下文。

**3. 为什么索引 undo 用逆操作回放而非 PG 式"留着等 vacuum"？**
pg_rust 的 DELETE 是物理删索引条目（M2b 无 vacuum 回收死条目），abort 必须能恢复条目；插入侧悬挂条目与 PG 同构（可见性掩码兜底）。逆序回放是必须的：同事务 INSERT(k,t)+DELETE(k,t) 正序回放会留下悬挂项。

**4. 为什么默认 SI 而非 PG 的 RC？**
Agent 场景长事务多读，SI 避免语句间视图漂移；M2b 实现上 auto-commit 每语句新快照即等价 RC，两种语义同构复用。

### 与 PostgreSQL 的 trade-off

| 维度 | PostgreSQL | pg_rust (Stage O) | 取舍理由 |
|---|---|---|---|
| 默认隔离级别 | RC | **SI**（begin_txn 一次快照） | §8 决策；SSI 留 Phase 7d |
| cmin/cmax | 双字段 + combo CID | 单 `t_cid`（同语句插删对外判定为不可见，方向保守） | M2b 裁剪；扫描全物化使分歧不可达 |
| 语句级回滚 | 子事务 | **不做**：语句失败唯一安全操作是 abort() | M2b 无子事务，已文档化 |
| RETURNING | 支持 | **不支持**（§7.2 case2/3 由 pg-txn 单测覆盖） | 出口裁剪 |
| 索引删除 | DELETE 不动索引，vacuum 回收死条目 | DML 时物理删条目，abort 逆操作恢复 | M2b 无 vacuum；条目即删即净 |
| 索引可见性 | index scan 回堆判定 | `index_lookup` 回堆 §7.2 掩码（同构） | 一致；但 SQL SELECT 暂不走索引（无 planner），点查仅程序化 API |
| 索引 abort | 插入侧悬挂条目留待 vacuum | undo 回放立即清除；undo 失败仅 warn（掩码兜底） | M2b 无索引一致性修复工具 |
| 事务内 DDL | 事务性 DDL | **显式拒绝** | 目录改动无 undo，M2b 裁剪 |
| SQL 方言 | 完整 SQL | 硬编码子集 + 不支持清单 | Phase 4a DataFusion 前的脚手架 |

### 已知残留与后续归队

- `index_lookup` fresh-snapshot：显式事务内无 read-your-writes（文档 WARNING 已明示；上层逻辑应走 `exec`）
- undo 回放失败仅 warn，无索引一致性修复工具 → 后续 stage
- redo 不恢复 `t_cid`（良性：写入事务不可能活过崩溃；子事务/语句级回滚到来时重审）
- hint bit 回写仍未接线 → Phase 7；`set_hint_bit` 占位
- 唯一索引不执行、DROP INDEX、多列索引 → 后续 stage
- 1000 轮崩溃验收配置需手动执行（默认 25 轮过 CI）；mid-checkpoint kill 仅概率性覆盖，无确定性保证
- `exec_auto` 的 SELECT / CREATE INDEX 不走 commit barrier（只读路径，已注释说明）
- 锁管理器、行锁 xmax 协议、B+Tree 并发、HOT update、ARIES Undo/CLR → **M2c（Stage P–T）**

---
