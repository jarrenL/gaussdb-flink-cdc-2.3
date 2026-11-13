# HCS 透明多写：CDC 2.3 在线全量开发说明

开发分支：`feature/hcs-cdc23-initial-handoff`。基线为官方
`com.ververica:flink-connector-debezium:2.3.0`、Flink 1.16.3 和配套 507 CloudNative JDBC，
Java 8 字节码。不修改官方 CDC 引擎、fetcher 或 offset 存储。

本次提供**可测试的实验实现**，未连接真实 HCS 透明多写租户。单元/框架测试通过不表示
多写节点、实际系统表权限、全局可见性、节点切换与长事务保留已经通过真机验收。

## 使用方式

在目标租户/数据库提前准备一个永久的专用信号表，结构必须恰好包含一个 VARCHAR/TEXT
主键列 `signal_id`，不要加触发器或业务用途。管理员创建表后，采集账号需要该表的
SELECT、INSERT、DELETE 权限，以及业务表/元数据的读取权限和复制权限。

```sql
CREATE SCHEMA cdc_meta;
CREATE TABLE cdc_meta.flink_snapshot_signal (
    signal_id VARCHAR(64) PRIMARY KEY
);
```

DDL 由管理员在目标版本执行，连接器不会自动建信号表、建 schema 或修改业务表。
`scan.snapshot.signal-table` 必须是简单的、大小写精确匹配的 `schema.table`。
信号表自动从业务输出和扫描选择中排除；每次初始化插入两个带 UUID 的信号，成功后只清理
本次的两行。失败可能残留信号行，需在任务停止后由管理员维护；这些行不会发往下游。

在现有连接参数中增加：

```sql
'scan.startup.mode' = 'initial',
'scan.snapshot.signal-table' = 'cdc_meta.flink_snapshot_signal',
'scan.slot.create' = 'true',
'scan.snapshot.max.rows' = '100000',
'scan.snapshot.max.bytes' = '67108864',
'scan.snapshot.fetch.size' = '1000',
'scan.snapshot.timeout.seconds' = '300'
```

完整 SQL 示例：[examples/hcs-initial.sql](../examples/hcs-initial.sql)。
首次启动可创建缺失的 `parallel_json_decoding` 专用复制槽。若希望管理员预建，设置
`scan.slot.create=false`。不会删除、重建或接管 active 槽；也不会将普通单段位点拼成三段位点。
全量失败后槽继续保留，请自行监控 WAL 占用；不要在失败重试之间擅自推进专用槽。

`HcsSmokeJob` 同样接受这些参数，输入 properties 文件后通过已有 Flink 作业入口运行。
提交账号必须能够访问该私有配置文件；请勿把真实凭证提交到 Git。

## 启动与衔接顺序

```text
验证专用槽（首次缺槽则建槽）→ 成功启动原生多写复制流
    → 验证表定义/主键/信号表
    → 提交 LOW 信号，在复制流中观察其完整提交与三段位点
    → JDBC 游标扫描所选表，全部先放入有界内存缓冲区
    → 扫描结束后提交 HIGH 信号
    → 读取并按主键合并 (LOW, HIGH] 的所有完整事务
    → 再次核对表集合、关系 OID、列类型和主键
    → 在一个内部批事件中输出合并后的全量行，保存 HIGH 与全量完成标记
    → 保留同次读取中 HIGH 之后的完整事务，接着进入持续增量
```

信号使用同一目标数据库内的真实提交，由 native stream 给出完整 `(CSN, WAL_ID, LSN)`。
因此不依赖 `next_csn + 1` 的推测边界，不需要写入业务表，也不依赖目标端主键冲突反馈。
使用普通 JDBC 事务扫描，不设置 REPEATABLE READ、不导出或导入全局快照。
各表 SELECT 可以读到区间内不同版本，随后由完整日志覆盖到统一 HIGH 位置。

合并规则：INSERT 按主键覆盖；UPDATE 删除旧主键后写入完整新镜像；DELETE 删除已有行，
不存在则不产生输出。复合主键包含表身份，避免不同表互相覆盖。扫描中已读到的 INSERT
只会成为一条全量行；扫描读到的新值也不会先被作为全量输出再由旧 UPDATE 回退。
高位后的事务作为普通增量保留，不把其位点提前写入全量 HIGH 的 offset。

输出继续使用官方反序列化器：全量 JSON 为 `op=r`、`source.snapshot=true`，行模式为
INSERT；后续增量为 INSERT、UPDATE_BEFORE/UPDATE_AFTER、DELETE。
空表同样产生一个不含业务行的内部完成批，以便空闲库进入持续增量并保存 HIGH。

## checkpoint 与失败恢复

全量读取、合并期间不发布业务行，也不确认复制槽位置。此时的 checkpoint 不带“全量完成”
offset，失败后重新执行整个全量；首版不从半张表或分块游标继续。

合并完成后的全部全量行和 HIGH offset 在官方 CDC fetcher 的同一 checkpoint 临界区输出。
其内部快照批标记为 `last`，保证首个批结束后进入持续增量。只有 completed checkpoint
回调通过 `SourceTask.commitRecord` 调用 native stream 的 applied/flushed ACK。

恢复时同时验证租户/数据库/槽、三段位点、全量完成标记与表选择/信号表配置。将旧增量
checkpoint 切换成 initial 会被拒绝，避免误跳过全量；改变表选择也需要新作业和完整初始化。
槽丢失时拒绝自动补建。起点早于 `gs_logical_replication_slot_stream` 最早三段 restart
边界时失败；无法查询该边界时同样失败。该预检查借鉴本次 DRS 反馈，是必要的保留边界
检查，不能单独证明每条底层 WAL 流或槽被外部重建后的可恢复性。槽必须专用且禁止外部重建。

## 与内网 DRS 反馈的对应

2026-11-13 的调用链反馈用于明确以下分支，未将内网 DRS 源码直接引入仓库：

| 反馈 | 本实现 |
| --- | --- |
| 多写槽存在性查询用 `gs_logical_replication_slot.name` | 复制流和全量启动查槽使用此系统表 |
| active 状态仍来自 `pg_get_replication_slots()` | 全量启动核对 active/plugin，不复用 active 槽 |
| restart 从 stream/slot 两表联查，得到 CSN/WAL-ID/LSN | 恢复前按完整三段位点检查，不调用 `asLong()` |
| 多写跳过 REPEATABLE READ | 使用普通扫描和源端日志合并，无导入快照依赖 |
| DRS 用 CSN 过滤，再由 Applier 修复重叠 | Flink 在源端消除重叠，保护下游聚合 |
| 二进制 BEGIN 多 8 字节 WAL-ID | 首版只使用 JSON 非批量模式；未宣称支持多写二进制解码 |
| reloptions 可能为空，按 ustore/dstore 处理 | 主键/列从系统目录读取，不靠 reloptions 推断存储类型 |

## 首版范围与前提

- Source 并行度固定为 1。支持多张有主键的永久普通表；拒绝无主键、分区表、继承表和临时/
  unlogged 表。业务主键当前仅开放整数和 UUID（可组合）；
  TEXT/VARCHAR 的排序规则、BPCHAR 的补空格、NUMERIC 的 scale 等键等值规则尚未验证，
  此类业务主键会在扫描前被拒绝。信号表的文本键仅存放连接器生成的 ASCII UUID 标记。
- 区间内 INSERT/UPDATE 的 after 必须包含全部业务列，UPDATE/DELETE 必须包含原主键。
  缺失镜像、列类型变化、已观察到的位点乱序均报错。后续行模式还要求完整 before 镜像，
  JSON 增量可保持实际提供的部分 before。
- 初始全量是**整份有界内存合并**，不是大表分块快照。默认最多 100000 行和约 64 MiB
  缓冲区估算数据；后续 Kafka Struct、JSON 序列化和 Flink 输出队列会额外占用内存。
  不应把 max.bytes 当成 JVM 总内存保证。大事务还有既有 native 解码缓冲压力。
- 必须暂停表结构变更和 TRUNCATE。前后目录核对能发现部分结构变化，不能检测所有 DDL。
  信号须能在同一复制流中出现，扫描连接须具备提交后的全局可见性，复制流须提供跨写节点
  有序、完整的提交事务；槽须保留扫描期间及未完成 checkpoint 所需的日志。
- LOW/HIGH 等待有超时；底层 JDBC/native 调用的实际取消响应还受驱动和网络超时约束。
  扫描期间不持续读取 WAL，必须评估槽日志保留空间和快照总时长。
- 源端 checkpoint 语义与端到端 exactly-once 不同；下游必须使用对应事务/checkpoint sink。

## 目标 HCS 真机验收

使用隔离测试 PDB、业务测试表、专用信号表和专用槽，至少验证：

1. 无业务写入时 LOW/HIGH 能正常出现，空表也可完成初始化。
2. 两个以上写节点同时 INSERT、UPDATE、DELETE，扫描读到未来值或已读到 INSERT 的场景
   不产生重复全量行；包含复合主键、主键变更、NULL、中文及目标使用的数据类型。
3. LOW 前开启、HIGH 前/后提交的长事务，以及回滚、空事务和大事务。
4. 全量扫描中 kill/restart；全量输出中失败；checkpoint 完成前失败；完成后从 checkpoint/
   savepoint 恢复；核对最终结果和聚合计数，不能只查 upsert 表最终值。
5. 槽缺失、restart 边界推进、缺权限、信号表不可写、标记不可见、缺失行镜像及节点切换。
   应明确失败，不能静默换槽、跳过变更或确认未完成 checkpoint 的位置。

构建与本地验证：`mvn -Pflink-1.16 clean verify`、`mvn spotless:check`。
本地回归测试使用真实官方 CDC 引擎/507 驱动与受控 SQL/复制流；尚无真机验收结论。
