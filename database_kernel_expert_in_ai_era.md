# 数据库内核"老中医"在 AI 时代的价值分析

> 核心问题：数据库内核的技术专家被称为"老中医"，职业生涯长、经验极有价值。AI 编码能力如此强且不断进化，这个判断现在还成立吗？

---

## 一、结论概要

**"老中医"判断的核心依然成立，但其内涵正在发生结构性变化。** 数据库内核专家的经验价值不但没有被 AI 消解，在某些维度上反而变得更加稀缺。但与此同时，经验的"表现形式"和"杠杆效应"正在被 AI 重塑。

---

## 二、实证数据：AI 对资深工程师的真实影响

### 2.1 METR 研究（2025）—— 最具权威性的随机对照实验

- **样本**：16 位大型开源项目的资深贡献者，246 个真实任务
- **结果**：使用 AI 工具（Cursor Pro，基于 Claude 3.5/3.7 Sonnet）后，任务完成时间**反而增加了 19%**
- **悖论**：开发者实验前预期 AI 加速 24%，实验后仍主观认为加速 20%——实际却减速 19%
- **原因**：AI 生成"几乎正确"的代码需要大量审查和修复时间
- **关键结论**：研究指出"在质量标准极高、隐含要求（文档、测试）需要人类长期学习的环境中，模型能力相对更低"——这正是数据库内核开发的特征
- **注意**：研究者指出这不意味着 AI 对所有人无效，它可能仍对新手或探索不熟悉代码库者有帮助

> 来源：[METR: Measuring the Impact of Early-2025 AI on Experienced OS Developers](https://metr.org/blog/2025-07-10-early-2025-ai-experienced-os-dev-study/)

### 2.2 AI 生成代码的质量问题

对 470 个真实 GitHub PR 的分析显示：
- AI 生成代码的缺陷率是人工代码的 **1.7 倍**
- 逻辑缺陷增加 **75%**
- 安全漏洞增加 **2.74 倍**
- 错误处理缺失翻倍
- 可读性问题激增 **3 倍以上**
- 代码重复率从 8.3% 上升到 12.3%

另一项对 800 名开发者的调查显示，使用 AI 助手的开发者 Bug 率增加了 **41%**。

第三项研究审查了 **2.11 亿行代码**，发现被丢弃或重写的代码几乎翻倍（从 3.1% 到 5.7%），代码重复增加了 8 倍，重构工作从所有变更的四分之一降至 **不到 10%**。

> 来源：[AI-Generated Code Has 1.7x More Bugs (2025 Data)](https://www.shiplight.ai/blog/ai-generated-code-has-more-bugs)

### 2.3 AI 在含缺陷上下文中的表现

- GPT-4 在含缺陷的代码上下文中，正确补全率仅 **12.27%**（正常情况为 29.85%）
- **44.44% 的 AI 生成 Bug 与历史版本完全一致**——模型在记忆缺陷模式而非真正推理
- 在 bug-prone 位置，生成正确代码与错误代码的概率几乎相等（1.01:1.00）
- 这对数据库内核开发（崩溃恢复、并发控制等对正确性要求极高的场景）构成致命风险

> 来源：[arXiv: LLM Code Generation in Bug-Prone Contexts](https://arxiv.org/html/2503.11082v1)

### 2.4 GitHub Copilot 对复杂任务无效

实证研究显示：
- Copilot 上线后提交量**无统计学显著增长**
- 采用者每周净增仅 **16 行代码**
- 代码复杂度等质量指标未变
- 工程师明确表示"Copilot 没有让难题消失"
- 对于大型系统构建，AI"缺乏所需的上下文理解能力"
- 锁协议设计、WAL 实现、MVCC 等架构性难题不在 AI 能力范围内

> 来源：[arXiv: Copilot Productivity Study](https://arxiv.org/html/2509.20353v2)

### 2.5 LLM 在高复杂度任务中的系统性缺陷

系统性文献综述确认：
- LLM 经常忽略安全开发实践，频繁引入注入漏洞和内存错误
- 在高复杂度代码任务上准确率**低于 30%**
- 模型在跨函数缺陷和复杂真实场景中表现挣扎
- 对于数据库内核——需要极其精确的内存管理、并发安全性和零容错的崩溃恢复逻辑——这些缺陷不可接受

> 来源：[arXiv: LLM Security and Correctness Survey](https://arxiv.org/html/2412.15004v3)

---

## 三、数据库内核专业知识为何难以替代

### 3.1 正确性约束的绝对性

数据库内核的核心价值在于 **ACID 保证**，这是"零容忍"领域：

| 领域 | 要求 | AI 的困难 |
|------|------|-----------|
| Crash Recovery (WAL/ARIES) | 任何崩溃场景下数据不丢、不重 | 需要推理所有可能的故障时序组合 |
| 并发控制 (MVCC/2PL) | 序列化正确性、无死锁 | 需要理解跨线程状态的全局不变量 |
| 分布式共识 (Raft/Paxos) | 网络分区下的一致性 | NP-hard 问题，形式化验证仍需人类主导 |
| 查询优化 | 在指数级搜索空间中做出最优决策 | Learned Optimizer 至今仅在单节点有限场景可用 |
| Buffer Pool 管理 | 在有限内存中最大化命中率 | 需要对工作负载特征的长期理解 |
| 存储引擎设计 | B-Tree vs LSM-Tree 等架构选择 | 需要多年 workload 特征积累的直觉判断 |

### 3.2 Learned Query Optimizer 的现实困境

学术界十年来投入大量研究，但生产环境中：
- 现有模型**仅限单节点** DBMS，分布式环境的变量过多
- 分布式查询优化本身是 **NP-hard 问题**
- 即使阿里 MaxCompute 等大规模部署，也仍然是"辅助"而非"替代"传统优化器
- 微软的 Steered Query Optimizer 采用的是"AI 建议 + 人类决策"模式
- 专家仍在探索"一种可能但通用的架构"用于分布式上下文，表明实用的 AI 优化在分布式系统中仍处于开发阶段

> 来源：[Learned Distributed Query Optimizer: Architecture and Challenges (ZTE Communications)](https://www.zte.com.cn/content/zte-site/www-zte-com-cn/global/about/magazine/zte-communications/2024/en202402/review/en20240207.html)

### 3.3 系统编程的特殊性

AI 在系统级编程中的局限性（来自 2025 年回顾分析）：
- 会生成**"看起来合理但微妙错误"**的实现，在真实压力下崩溃
- 无法可靠地调试**协调故障**（coordination breakdowns）
- 上下文窗口在最关键时刻溢出
- Agent 会陷入**"无限礼貌性分歧循环"**
- 会**自信地执行有缺陷的策略**
- 创建可靠 AI 系统本质上是一个分布式计算挑战

> 来源：[2025 Retrospective: AI vs Systems vs Humans](https://binds.ch/blog/2025-retrospective)

### 3.4 MIT 研究：AI 在大型代码库中系统性失败

- 标准基准测试评估的是"本科编程练习"级别的任务，仅触及真实项目代码库的微小部分
- 处理大型代码库时，AI 频繁生成**看似合理但实际错误的函数**
- 违反内部规则、无法通过自动化验证系统
- MIT 研究者明确表示："我们的目标不是替代程序员，而是增强他们"
- PostgreSQL 有 **130 万行代码**，正是这类大型代码库的典型代表

> 来源：[MIT: Can AI Really Code? Study Maps Roadblocks to Autonomous Software Engineering](https://news.mit.edu/2025/can-ai-really-code-study-maps-roadblocks-to-autonomous-software-engineering-0716)

### 3.5 企业级数据库场景中 AI 的失败

- Stonebraker 在 MIT 数据仓库上测试 LLM 的 text-to-SQL 翻译，**准确率为零**
- 原因：私有企业数据的 schema、术语和业务逻辑是 LLM 从未见过的
- 后续优化版本达到 2-11%，但仍远不够实用
- TigerData 团队发现：无专门指导时，AI 生成的 SQL 会错误使用 monetary 类型、混淆 identity 策略等
- LLM 从整个互联网学习，混淆了不同数据库系统的惯例

> 来源：[Database Year in Review 2025 (Vonng)](https://vonng.com/en/db/db-year-review-2025/)

---

## 四、数据库社区领袖的观点

### 4.1 Michael Stonebraker（图灵奖得主）

- **"仅仅被教会编码"的技能将不再有市场价值**——暗示深层系统知识才是核心竞争力
- AI Agent 的持久化计算**"基本就是 ACID 中的 D"**，传统数据库事务技术将成为 AI 工作流的基础层
- AI Agent 需要管理状态并在多步骤流程失败时执行回滚，这需要将应用状态存储在数据库中
- 这意味着数据库内核专家的知识**不仅不会过时，反而成为 AI 基础设施的核心**

### 4.2 Andy Pavlo（CMU 数据库组）

- AI 可以生成完整应用代码，但开发者可能因此不再手动优化存储系统
- 缺乏人类监督使自动化调优变得必不可少
- 这 paradoxically **增加了对数据库内核专家的需求**——需要构建能自主调优的智能数据库系统
- 而构建这类系统恰恰需要深厚的查询优化和存储引擎设计知识

### 4.3 Sebastian Raschka（ML 研究者，2025 LLM 综述）

> "AI 给专业人士'超能力'使其大幅提高生产力，但专家始终能比使用 AI 的新手产出更好结果。投资成为专家对于最大化 AI 效用和交付卓越成果仍然至关重要。"

> 来源：
> - [LinkedIn: Insights from Stonebraker and Pavlo](https://www.linkedin.com/pulse/what-i-learned-from-listening-mike-stonebraker-andy-pavlo-gattani-mdfvc)
> - [DBOS: 2025 Year in Review with Stonebraker and Pavlo](https://www.dbos.dev/webcast-2025-in-review-with-mike-stonebraker-and-andy-pavlo)
> - [State of LLMs 2025 (Sebastian Raschka)](https://magazine.sebastianraschka.com/p/state-of-llms-2025)

---

## 五、Linux 内核社区的态度 —— 一个重要参照系

Linux 内核在 2025 年发布了 AI 编码准则：

- **允许使用** AI 工具辅助开发
- **要求人类承担完全责任**——包括所有 Bug 和安全漏洞
- **禁止** AI agent 签署 commit
- 必须标注 `Assisted-by: AGENT_NAME:MODEL_VERSION`
- Linus Torvalds 的态度：工具可以用，但**人必须为每一行代码负责**
- 这体现了内核社区的共识：AI 是工具，不是替代品；系统级代码的责任和判断力不可委托

> 来源：[The Linux Kernel Just Published AI Coding Guidelines](https://dev.to/adioof/the-linux-kernel-just-published-ai-coding-guidelines-the-rest-of-us-should-pay-attention-4h7d)

---

## 六、AI 能做好 vs 做不好的任务（数据库内核领域）

### 6.1 AI 能做好的（提效工具）

| 任务 | 说明 |
|------|------|
| 脚手架代码生成 | 新增系统表、catalog 结构、executor 算子骨架 |
| 单元测试编写 | 基于已有接口生成测试用例 |
| 代码阅读辅助 | 解释复杂代码路径、调用链分析 |
| 文档生成 | 注释、设计文档初稿 |
| 模式化重构 | 变量重命名、接口适配、格式化 |
| Bug 定位辅助 | 根据堆栈信息缩小搜索范围 |
| 简单 SQL 生成 | 标准查询模式的生成 |
| API 适配层代码 | 胶水代码、协议适配 |

### 6.2 AI 做不好的（仍需"老中医"）

| 任务 | 原因 |
|------|------|
| 并发 Bug 诊断 | 需要推理非确定性时序，AI 缺乏执行时状态 |
| Crash Recovery 正确性证明 | 需要覆盖所有故障场景的组合爆炸空间 |
| 性能抖动根因分析 | 涉及硬件、OS、workload 的多层交互 |
| 存储引擎架构设计 | 需要多年 workload 特征积累的直觉判断 |
| 分布式一致性协议设计 | 形式化正确性验证仍需人类主导 |
| 查询优化器代价模型校准 | 需要对真实数据分布的深度理解 |
| 生产故障应急（Oncall） | 需要在极端压力下的快速决策和优先级判断 |
| 锁协议设计 | 需要对死锁预防、性能权衡的全局理解 |
| WAL 实现 | 需要保证在任何故障时序下的正确性 |
| MVCC 版本链管理 | 需要平衡垃圾回收效率和读一致性 |
| 跨系统集成 | 需要理解不同子系统间的隐含契约 |

---

## 七、历史类比：以往技术变革的启示

| 时代 | 技术变革 | 对内核专家的影响 | 规律 |
|------|----------|-----------------|------|
| 1970s | 高级语言取代汇编 | 减少了汇编程序员需求，但系统设计能力反而更稀缺 | 操作层消失，设计层上升 |
| 1990s | ORM 出现 | 减少了写 SQL 的需求，但内核/优化器专家需求不减 | 抽象层增加，底层专家更珍贵 |
| 2000s | 开源数据库崛起 | 扩大了市场，内核专家从商业转向开源 | 市场扩大，专家迁移 |
| 2010s | 云数据库 (RDS/Aurora) | 减少了 DBA 运维需求，但内核开发者需求增加 | 运维自动化，开发需求上升 |
| 2020s | NewSQL/分布式 | 增加了系统复杂度，资深专家更加稀缺 | 复杂度上升，专家更稀缺 |
| 2025+ | AI 编码助手 | 减少了重复编码需求，判断力和架构能力更加关键 | 编码民主化，判断力稀缺化 |

**核心规律**：每次技术变革消除的都是"操作层"的工作，而"设计层"和"判断层"的需求反而上升。AI 正在重复这一模式。

---

## 八、综合判断：经验价值的重新定位

### 8.1 "老中医"判断成立的维度

1. **诊断能力不可替代**——线上问题的根因分析需要跨越代码、硬件、workload 的系统性思维
2. **架构判断不可替代**——选择 B-Tree vs LSM-Tree、选择悲观锁 vs 乐观锁，需要对工作负载特征的多年积累
3. **正确性保证不可替代**——ACID 不允许"大概率正确"，AI 的概率性本质与此根本矛盾
4. **经验积累周期未缩短**——理解一个问题需要亲历一次生产事故，这个学习路径 AI 无法压缩
5. **大型代码库理解不可替代**——PostgreSQL 130 万行代码的全局理解需要数年浸润
6. **跨层交互的直觉不可替代**——硬件 → OS → 文件系统 → 存储引擎 → 执行器 的全链路优化

### 8.2 正在变化的维度

1. **杠杆率大幅提升**——一个"老中医" + AI 工具 ≈ 过去一个小团队的产出
2. **入门门槛降低**——新人借助 AI 可以更快读懂内核代码，学习曲线变陡但起点更高
3. **重复性工作减少**——模式化编码可以卸载，专家可以聚焦在真正需要判断力的工作上
4. **知识传承方式改变**——AI 可以作为"随时可用的次级专家"辅助知识传递
5. **需求增加而非减少**——AI 基础设施本身需要 ACID 保证（Stonebraker 观点），内核专家变得更重要

### 8.3 新的风险

1. **"经验中空"现象**——如果新人过度依赖 AI 跳过底层理解，可能出现一代人的经验断层
2. **虚假信心**——METR 研究证明，人会高估 AI 的帮助（主观快 20%，实际慢 19%）
3. **安全表面化**——AI 代码的安全漏洞率是人工的 2.74 倍，在数据库内核中后果极为严重
4. **缺陷复制**——AI 倾向于复制历史 Bug 模式（44.44% 的 AI Bug 与历史版本一致）
5. **监督缺失**——当 AI 生成整个应用后"没有人在看着数据库"（Pavlo），自动调优需求反增

---

## 九、最终结论

> **数据库内核"老中医"的判断不仅成立，而且在 AI 时代可能更加成立。**

原因是：AI 降低了"写代码"这一环节的门槛，使得更多人可以"进入"内核开发领域，但这恰恰让能**判断代码是否正确、架构是否合理、系统是否可靠**的资深专家变得更加稀缺和重要。

就像自动驾驶让更多人能"开车"，但飞机机长的价值反而没有下降——因为越是关键系统，越需要人类的最终判断力。

更深层的逻辑是：**AI 本身正在成为需要数据库内核技术支撑的基础设施**。AI Agent 的状态管理、持久化、故障恢复，本质上就是数据库事务技术。老中医的知识不是被 AI 替代，而是成为了 AI 的地基。

AI 是"老中医"的新工具，不是替代者。但"老中医"必须学会使用这把新工具，否则会被**会用 AI 的年轻"老中医"**超越。

---

## 十、开放问题（值得持续关注）

1. 2026 年后随着更长上下文窗口（1M+ tokens）和更好的代码推理能力出现，AI 能否突破"大型代码库理解"的瓶颈？
2. 是否会出现专门针对数据库内核代码（WAL、MVCC、B-tree 实现）训练的领域特化模型？其表现如何？
3. AI 辅助形式化验证（如 TLA+ 规格自动生成）能否弥补 AI 在正确性保证方面的不足，从而间接削弱对人类专家的需求？
4. PostgreSQL、MySQL 等社区是否会有关于 AI 辅助内核开发的正式政策或实证经验报告？
5. 在中国数据库生态（TiDB、OceanBase、openGauss）中，AI 对内核开发流程的实际影响程度如何？
6. 随着 AI 在代码审查和 Bug 检测方面的进步，"老中医"的角色会从"编写代码"转向"审查和架构设计"吗？这种转变的时间线是什么？

---

## 参考来源

### 学术研究与实证数据
- [METR: Measuring the Impact of Early-2025 AI on Experienced OS Developers](https://metr.org/blog/2025-07-10-early-2025-ai-experienced-os-dev-study/)
- [arXiv: LLM Code Generation in Bug-Prone Contexts](https://arxiv.org/html/2503.11082v1)
- [arXiv: Copilot Productivity Study](https://arxiv.org/html/2509.20353v2)
- [arXiv: LLM Security and Correctness Survey](https://arxiv.org/html/2412.15004v3)
- [arXiv: Correctness Assessment of Code Generated by LLMs](https://arxiv.org/html/2501.12934v2)
- [MIT: Can AI Really Code?](https://news.mit.edu/2025/can-ai-really-code-study-maps-roadblocks-to-autonomous-software-engineering-0716)
- [AI-Generated Code Has 1.7x More Bugs (2025 Data)](https://www.shiplight.ai/blog/ai-generated-code-has-more-bugs)

### 数据库社区领袖观点
- [LinkedIn: Insights from Stonebraker and Pavlo](https://www.linkedin.com/pulse/what-i-learned-from-listening-mike-stonebraker-andy-pavlo-gattani-mdfvc)
- [DBOS: 2025 Year in Review with Stonebraker and Pavlo](https://www.dbos.dev/webcast-2025-in-review-with-mike-stonebraker-and-andy-pavlo)
- [Database Year in Review 2025 (Vonng)](https://vonng.com/en/db/db-year-review-2025/)
- [Andy Pavlo / CMU Database Group](https://db.cs.cmu.edu/author/pavlo/)
- [State of LLMs 2025 (Sebastian Raschka)](https://magazine.sebastianraschka.com/p/state-of-llms-2025)

### 行业实践与政策
- [The Linux Kernel AI Coding Guidelines](https://dev.to/adioof/the-linux-kernel-just-published-ai-coding-guidelines-the-rest-of-us-should-pay-attention-4h7d)
- [2025 Retrospective: AI vs Systems vs Humans](https://binds.ch/blog/2025-retrospective)
- [Learned Distributed Query Optimizer: Architecture and Challenges](https://www.zte.com.cn/content/zte-site/www-zte-com-cn/global/about/magazine/zte-communications/2024/en202402/review/en20240207.html)

### 数据库与 AI 交叉研究
- [Database Meets AI: A Survey (Tsinghua)](https://dbgroup.cs.tsinghua.edu.cn/ligl/papers/aidb.pdf)
- [Neo: A Learned Query Optimizer (VLDB)](https://www.vldb.org/pvldb/vol12/p1705-marcus.pdf)
- [TigerData: Teaching AI to Write Real Postgres Code](https://www.tigerdata.com/blog/we-taught-ai-to-write-real-postgres-code-open-sourced-it)

---

*文档生成时间：2026-06-30*
*研究方法：多源搜索 + 交叉验证 + 3 人验证小组投票确认*
