# 通用 Flink Source：DRS 方案可复用的范围

2026-11-13：用户已明确保持通用 Flink CDC Source，允许下游接聚合、计数等计算；
不将交付物收敛为依赖特定目标库 Applier 的迁移作业。当前首次 initial 仍默认关闭。
后续已将 DRS 中可复用的日志保留/进度检查与 Source 的落盘重叠合并结合，
当前实现及未解决项见[组合方案](HCS_HYBRID_SNAPSHOT.md)。

## 两个独立问题

如果内核明确保证目标 JDBC/COPY 读取路径在一个边界之后看到此前所有提交，则可以依赖
数据库读取契约，不一定需要应用层额外等待。信号表只是当前实验原型获得日志中精确提交
边界的一种办法，并非所有 CDC 实现的必要条件。

但读取可见性只解决“起点之前会不会漏行”；全量和增量的重叠还要另外处理。
per-table CSN 过滤仅排除表起点之前的日志，不能排除扫描已经读到的后续提交：

```text
记录 tableInitCSN = 101
事务在 CSN = 105 插入 id=7
SELECT 开始后读到 id=7，Source 输出一条全量 INSERT
WAL 事件 105 >= 101，per-table 过滤继续保留该 INSERT
Source 再输出一次 INSERT
```

本仓库行模式通过官方反序列化器输出完整 retract changelog，直接消费时 COUNT 从 1
变成 2；在聚合之后才接 UPSERT sink，无法撤销前面已经多算的记录。
另一个反例：SELECT 已读到 value=20，随后回放 UPDATE(before=10, after=20)，
原始 SUM 会按 `20 - 10 + 20 = 30` 更新，正确值应为 20。扫描已不包含某行时再发 DELETE，
也可能产生没有对应 INSERT 的回撤。

声明主键或给目标写入增加 UPSERT，不能自动把当前全 changelog Source 变成已归一化的
upsert Source。若采用主键 upsert changelog 与有状态归一化，必须显式实现/验证主键、
RowKind、旧镜像、删除、主键变更、checkpoint 状态及多表事件语义；状态没有凭空消失。
本次没有悄悄改变输出契约。

## DRS 中可复用的设计

- 全量扫描前完成建槽，保证所需日志保留。
- 使用与目标版本匹配的 GTM 配置检查作为排除条件；仅检查一个 off 参数不够，参数本身也
  不是目标 HCS 读取契约的证明。
- 全局/逐表 CSN 可用于界定初始日志范围，前提是明确函数返回值、+1 与等值边界的语义。
- native 复制与 checkpoint 仍保存完整 `(CSN, WAL_ID, LSN)`，不把单个 startCsn
  随意补成三个分量。若从槽可用位置读取再按 CSN 筛选，也必须保证槽覆盖所需事务。

DRS 的 relay log、任务元数据、目标 Applier、全量完成通知及目标提交协调是设计的一部分。
去掉本原型的内存 buffer，并不代表所需状态、WAL 保留、恢复位置和目标冲突处理不再存在。
全量与日志采集可以并行；采集并行与目标应用并行不是同一个保证。

## 去掉信号表的候选路线

1. **一致性快照及原子 CSN 边界**：若目标支持稳定快照、跨会话导入，并有与 native 提交位点
   对应的可见性及等值规则，可以流式读快照并过滤已包含事务。需要验证快照连接生命周期、
   隔离级别、跨写节点、在途长事务及 checkpoint 失效后的重做流程。
2. **分块合并与可验证日志边界**：若目标能提供不写信号的可靠边界接口，可按主键分块读取
   和合并区间日志，将状态纳入 checkpoint/磁盘，降低单块内存。需要新增完整的分块状态协议。
3. **显式 upsert Source + 有状态归一化**：需要重新设计并验证输出主键和 changelog，
   将状态纳入 Flink checkpoint；不能直接发送重复 INSERT 后宣称通用源端正确。

目前没有足够证据直接启用其中一条。`initial` 的默认限制保持，不把 DRS 目标端修复逻辑
塞入当前 Source，也不把仅有的 per-table 过滤当成完整替代。

## 新增只读跨会话快照探针

`HcsSnapshotImportDiagnostics` 使用两个新 JDBC 连接，按顺序：

```text
Exporter: BEGIN -> REPEATABLE READ READ ONLY -> 检查实际隔离级别 -> 导出 snapshot ID + CSN
Importer: BEGIN -> REPEATABLE READ READ ONLY -> SET TRANSACTION SNAPSHOT -> 检查实际隔离级别
Importer rollback -> Exporter rollback
```

Importer 导入必须发生在其任何查询之前，不能为了检查隔离级别而先执行 SHOW/SELECT。
Exporter 直到导入检查完成才结束。两边都不提交，不读业务表、不建槽、不启动流、不 ACK。

```sh
java -cp target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar \
  com.ververica.cdc.connectors.gaussdb.examples.HcsSnapshotImportDiagnostics \
  /secure/path/exporter.properties /secure/path/importer.properties
```

可省略第二个文件，用第一份配置建立两个连接；若提供两份，租户和数据库必须相同。
第二份可使用不同 CMS 入口，但这不能保证驱动实际选择了不同写节点。
沿用已有 properties 的认证字段，不需要开启 initial 实验选项，也不需要在此探针中使用槽。
配置类仍要求已有基础字段 `slot.name` 和 `table-name`，仅用于复用配置，不访问这些表/槽。

成功只说明两个连接的导出/导入命令被接受，仍输出
`snapshot.cross_node_visibility=NOT_VERIFIED` 和 `snapshot.csn_wal_boundary=NOT_VERIFIED`。
失败只打印阶段、SQLSTATE 和错误码；导出的 ID 在拼入 SET 前严格校验。退出码 2 表示
能力检查或回滚失败。不能根据一次接口成功就自动打开 initial。

配套产品文档“快照同步函数”还明确警告：部分 GaussDB 版本不支持导入所需的隔离级别，
导出的标识不能直接用于 SET TRANSACTION SNAPSHOT。因此该路线是待查候选，不是已确认能力。
需要内核侧明确目标 HCS 产品版本和路径，再做多节点并发写入的独立真机实验。

## 参考

- [官方 CDC 的 backfill 语义说明](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/docs/connectors/flink-sources/postgres-cdc/)：跳过合并再回放可能重复，仅保证至少一次；这里借用其语义解释，不表示 CDC 2.3 有同名选项。
- [GTM 配置模式判断](https://support.huaweicloud.com/intl/zh-cn/distributed-devg-v10-gaussdb/gaussdb-12-0742.html)：普通分布式文档；目标 HCS 租户读取路径仍需确认。
- [内网补全指南核查](HCS_DRS_SUPPLEMENT_REVIEW.md)：记录 ACK、读取可见性与迁移/Source 的区别。
