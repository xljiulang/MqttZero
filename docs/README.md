# MQTTZero 文档目录

面向决策者的介绍在仓库根 [`README.md`](../README.md)；本页是开发与运维文档的两级导航。

## 架构

- [`architecture/runtime.md`](architecture/runtime.md)：运行时结构、内存所有权、已否决方案与当前状态。
- [`architecture/sharded-actor-plan.md`](architecture/sharded-actor-plan.md)：Shard 所有权重构的完整阶段记录与验收目标。

## 协议

- [`protocol/mqtt5-properties.md`](protocol/mqtt5-properties.md)：报文与 MQTT 5 属性的端到端支持矩阵。

## 性能

- [`performance/positioning.md`](performance/positioning.md)：性能定位、发布门禁与已知差距。
- [`performance/ab-harness.md`](performance/ab-harness.md)：同机 A/B、交付完整性与内存探针的使用方法。
- [`performance/branch-convergence.md`](performance/branch-convergence.md)：性能分支的收敛与归因记录。
- [`performance/mqttnet-fork-audit.md`](performance/mqttnet-fork-audit.md)：MQTTnet fork 优化项与 MQTTZero 的逐条对照。
- [`performance/hot-path-audit.md`](performance/hot-path-audit.md)：热路径方法的分配与重复计算审计结论。
- [`api-migration-0.12.md`](api-migration-0.12.md)：0.12 公开接口变更与升级指引。
- [`../perf/baseline/README.md`](../perf/baseline/README.md)：历史基准与 A/B 原始记录索引。

## 历史计划与评审

- [`superpowers/plans/`](superpowers/plans)：按日期记录的实施计划。
- [`superpowers/reviews/`](superpowers/reviews)：按日期记录的架构与代码评审。

这两个目录是过程记录，正文中出现的 `docs/xxx.md` 路径写于当时的目录结构，未随本次重排改写。

## 维护约定

- 正式文档只放在 `architecture/`、`protocol/`、`performance/` 三处，层级不超过两级。
- 新增或移动文档后运行 `pwsh -NoProfile -File ./eng/verify-doc-links.ps1` 校验相对链接。
