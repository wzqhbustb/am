# Stage Spec — 各阶段实现规格与 PG 对照

> 记录每个 Stage 的交付内容、设计决策理由、以及与 PostgreSQL 的取舍。
> 本文档与 `docs/phase1-m2-tech-selection.md`（设计选型）、`docs/phase1-m2-coding-plan.md`（编码计划）配套：选型文档记录"打算怎么做"，本文档记录"实际怎么做的"。

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
