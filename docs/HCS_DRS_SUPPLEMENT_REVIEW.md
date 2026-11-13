# 内网补全指南：核查与处理

日期：2026-11-13。核查对象为基于 `c793e12` 的内网 DRS 对比与补全指南。
本文件记录实现判断，未复制内网源码。基线仍是官方 Flink CDC 2.3.0，Source 采集模型。

## 后续内网反馈：尚未完成一致性证明

内网确认在已检查的 DRS 路径未找到显式等待全局可见 CSN 的操作；约 1ms 的 sleep 是
时间戳精度补偿，不能作为读屏障。该结论不等同于内核不存在读取一致性机制，也不能证明
LOW 提交后通过目标 HCS JDBC/COPY 路径扫描必然覆盖所有 LOW 之前的提交。

补充中有几处需要进一步区分：

- `confirmed_flush` / `confirmed_csn` 是复制消费者确认进度，不是某个业务读取节点的可见
  进度。用它们等待 LOW，不能建立 JDBC 读取保证，还可能错误地混用未完成 checkpoint 的 ACK。
  `next_csn` 是分配/事务序号相关信息；`oldest_csn` 等字段也需先有目标版本的明确语义，
  不能根据字段名编造等待算法。
- 参数判断不能只看 `enable_gtm_free=off`。分布式官方文档的 GTM-Free 条件为
  `enable_gtm_free=on` **或** `gtm_option=2`；GTM/GTM-Lite 又依赖另一个参数取值。
  这些是普通分布式 GTM 文档，不是目标 HCS Tenant/DStore 读取路径的能力认证。
- “透明多写全量每表都全程持有 SHARE 锁”的描述与此前实际调用链不一致：此前有主键表
  跳过锁，另有锁在 COPY 前提交释放的分支。不能据此确认所有表读取时都在锁内。
  per-table CSN 过滤也无法单独找回起点之前已提交、扫描又看不到的行；目标 Applier 的
  冲突修复无法修复从未读到且以后不再变更的行。

代码现在将**首次 initial 默认关闭**：无完成 offset 时，在连接/建槽前拒绝启动。
内部快照入口同样拒绝。只有隔离实验可显式设置
`scan.snapshot.allow-unverified-consistency=true`，此开关不表示一致性已经验证。
普通增量不受影响；已有完成 checkpoint 可继续恢复增量，这并不认证其历史全量基线。

只读工具 `HcsSnapshotBoundaryDiagnostics` 新增查询当前 SQL 连接可见的
`enable_gtm_free` 和 `gtm_option`。缺权限/缺参数时报告 unavailable 或空结果；始终输出
`gtm.read_visibility=NOT_VERIFIED`，不自动放行任务，也不把本地参数当成全集群检查。

可使用已配置的私有 properties 文件执行：

```sh
java -cp target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar \
  com.ververica.cdc.connectors.gaussdb.examples.HcsSnapshotBoundaryDiagnostics \
  /secure/path/hcs.properties
```

下一步需要的是目标产品版本、实际读取路由，以及**内核对 JDBC/COPY 语句快照可见范围的
明确契约或读屏障实现**。请重点确认：LOW 提交并返回后，后续每个扫描语句如何保证覆盖
LOW 前在其他写节点的提交；是否存在可等待且有明确语义的读取可见水位；若依赖表锁，
锁是否跨写节点生效、覆盖哪些表，并且是否持续到读取完成。以上暂未由现有反馈证明。

参考证据及适用范围：

- [分布式 GTM 模式判断](https://support.huaweicloud.com/intl/zh-cn/distributed-devg-v10-gaussdb/gaussdb-12-0742.html)：明确两项参数的组合；不作为 HCS 多写读取保证。
- [GTM-Free 一致性限制](https://support.huaweicloud.com/distributed-devg-v10-gaussdb/gaussdb-12-0740.html)：普通分布式模式存在最终一致性限制。
- [逻辑复制槽字段说明](https://support.huaweicloud.com/intl/en-us/distributed-devg-v8-gaussdb/distributed-devg-v8-gaussdb-pdf.pdf)：confirmed 字段描述客户端确认进度；不能替代读可见水位。

## 已修正的实际缺口

### ACK 同时更新 applied 与 flushed

旧实现已有 `setFlushedLSN` 和 `forceUpdateStatus`，缺少 `setAppliedLSN`。
现已在 completed checkpoint 回调路径依次执行 applied、flushed、强制发送状态。
任一步失败会使回调报错，任务自己的 acknowledged 水位仅在成功后更新。
真实 507 驱动的合成协议测试同时检查 ACK 报文的三段 applied 和 flushed 字段；
新增 applied setter 失败时不继续 flushed/status 的负向测试。

这证明客户端构造并发送了对应状态，不等同于目标 HCS 已观察到 confirmed 位点或 WAL 回收。
内网仍需核对服务端对应系统表和槽保留变化，不能只用客户端日志宣称回收已生效。

### 全量完成后按有界批次发布

扫描和 LOW/HIGH 区间日志先完成主键合并，随后才输出分批结果。首批/中间批标记
`source.snapshot=true`、完成 offset=false，最后一批标记 `last`、完成 offset=true；
HIGH 后的 trailing 事务等所有全量批结束后才发送。

测试发现普通 STRING 的 snapshot 字段不被 CDC 2.3 配套的
`SnapshotRecord.fromSource` 识别，因此改用 `io.debezium.data.Enum` 逻辑类型。
官方 CDC fetcher 在批次间维持快照 checkpoint 锁，直到最后一批完成；无需修改框架源码。
测试通过门闩暂停第二次 poll，首批输出后 checkpoint 必须保持阻塞，后续批放行后才保存
HIGH 和全量完成标记，再由 completed checkpoint 执行 ACK。

`scan.snapshot.batch.rows` 与 `scan.snapshot.batch.bytes` 用于输出批次的行数/估算字节控制。
分批发布最初仍保留总行数与内存估算上限。后续[组合方案](HCS_HYBRID_SNAPSHOT.md)
增加默认 disk 存储，整份合并不再必须驻留内存；memory 模式保留旧上限。
两者均未实现大表分块 checkpoint，落盘文件不是恢复状态。

## 指南中需要修正的结论

### 滞后扫描节点不能仅靠 (LOW, HIGH] 日志补齐

反例：事务 X 在 LOW 之前提交，写入 id=1；扫描节点仍看不到 X，SELECT 没有读到 id=1；
X 的提交位点又小于 LOW，因此不会进入区间 backfill。如果直到 HIGH 都没人再修改 id=1，
最终结果就缺失这行。观察到信号出现在复制流中，不能单独证明另一条 SQL 读取路径已追上。

因此需要确认：每个扫描 SQL 的可见性覆盖所有提交位点不大于 LOW 的事务，并且不读到
HIGH 之后的版本。正常强一致读取与“允许从滞后副本返回旧结果”必须区分。请内网提供
目标 HCS 版本的读取一致性承诺，或等待全局可见位点的 SQL/驱动接口；当前代码不把
`next_csn` 当成节点可见水位，也没有伪造一个“已验证全局可见”的标志。
在这个前提没有确认时，首次 initial 默认拒绝启动，只保留显式隔离实验入口。

### 长事务不能只靠提交 CSN 推断完整捕获

槽成功建立在 LOW 和全量扫描之前；建槽失败就不会放行 LOW/扫描。槽已经存在与
“所有所需 WAL 必然完整可解码”仍有区别，尤其是建槽前已经开始并写入 WAL、HIGH 后才
提交的事务。需要内核/驱动保证槽初始化涵盖在途事务，并通过此类长事务真机测试核实。
`restart_lsn` 是槽可能仍需要的保留位置，不能简单等同于创建时的当前插入位置。

恢复校验拒绝的是 `checkpoint < restart`；相等并不拒绝。该比较表示请求起点早于当前
可验证保留边界，不代表已证明物理 WAL 文件被删除。查询最早一条 stream 元数据也不是
对每条底层 WAL 流和外部重建槽的充分证明，仍要求专用槽、禁止外部推进或重建。

### 不能先发扫描行，再发 backfill 最终值

例如扫描已输出 id=1，再把重复 INSERT 或合并后的 id=1 作为新 INSERT 发出，下游 COUNT
可能从 1 变成 2。目标数据库使用 UPSERT 也无法修复已经经过 Flink 的聚合结果。
正确顺序是先合并到一个已验证边界，再发布最终快照；大表需设计分块 LOW/HIGH、区间日志、
分块状态、边界归属和故障恢复，不能只给现有循环加一个“每 N 行输出”。

### Source 不执行目标库 INSERT，也没有目标影响行数反馈

这个仓库输出 Debezium 记录/RowData，没有目标端 INSERT SQL 可以追加冲突子句。
DRS Applier 的 delete+insert、UPDATE 0 行补 before、DELETE 0 行忽略，依赖目标库反馈，
不能直接移入 Source。当前源端已通过主键 buffer 处理全量与区间 WAL 重叠。

全量输出阶段故障可能让非事务下游留下部分结果；需要与 Flink checkpoint 协调的 sink
提供提交/回滚，或由具体目标写入方案定义幂等语义。Source 无法用统一的 UPSERT 保证所有
下游，尤其是聚合、计数和追加事件流的 exactly-once。

## 逐项处理结果

| 指南条目 | 处理 |
| --- | --- |
| P0 applied/flushed ACK | 已补 applied 并增加真实驱动报文及失败测试 |
| P0 分批发布与内存保护 | 已安全分批输出，新增默认落盘合并及磁盘配额；分块 checkpoint 未实现 |
| P0 目标 INSERT 加 UPSERT | 属于 sink/目标迁移层，不在 Source 构造目标 SQL |
| P1 无主键锁表 | 当前在读取前拒绝无主键表；锁表也不能直接为主键合并补出行身份 |
| P1 COPY TO STDOUT | 后续性能路径，需确认 HCS 租户连接 COPY API 与类型解码，暂用 JDBC 游标 |
| P1 重叠期冲突处理 | 已有源端合并；不重复输出 Applier 风格的修复事件 |
| P2 表级/分块 checkpoint、并行和分区 | 需要完整分块状态协议，未宣称支持 |
| P2 建表、序列、目标 COPY 容错 | 属于迁移/目标写入能力，不混入当前 Source |
| P2 运行中 DDL、心跳 | 后续能力；当前不支持 DDL，边界信号只用于首次全量 |

## 仍需内网提供的验证

优先确认扫描读取的全局可见性前提，并刻意构造 LOW 前在另一写节点提交、扫描后不再更新的
行；测试不能只覆盖 LOW~HIGH 区间内的写入。长事务要覆盖建槽前写入、HIGH 后提交。
客户端 ACK 后，还需核对服务端确认位点及日志保留是否按预期推进。
目前本地测试使用受控 SQL/复制流和真实官方 CDC/507 客户端，尚无 HCS 真机验收结论。

## 输出契约已确认

用户已选择保持通用 Flink CDC Source，不改为依赖目标 Applier 的数据库迁移作业。
[Source 方案与只读快照导入探针](HCS_GENERIC_SOURCE_STRATEGY.md) 给出了重叠的 INSERT/UPDATE
聚合反例、可借鉴的 DRS 部分，以及去掉信号表所需的候选接口契约。
