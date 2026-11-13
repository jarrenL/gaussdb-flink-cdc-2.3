# HCS 透明多写：CDC 2.3 在线全量开发说明

开发分支：`feature/hcs-cdc23-initial-handoff`。基线为官方
`com.ververica:flink-connector-debezium:2.3.0`、Flink 1.16.3 和配套 507 CloudNative JDBC，
Java 8 字节码。不修改官方 CDC 引擎、fetcher 或 offset 存储。

本次提供**可测试的实验实现**，未连接真实 HCS 透明多写租户。单元/框架测试通过不表示
多写节点、实际系统表权限、全局可见性、节点切换与长事务保留已经通过真机验收。

## 首次全量默认关闭

内网补充未提供目标 HCS 扫描路径的全局可见性保证，也未找到已验证的读屏障接口。
因此当前版本对没有完成 offset 的 `initial` 任务，在连接数据库、建槽、写信号和扫描之前
拒绝启动。直接调用内部 InitialSnapshot 入口也会执行同样的检查。默认 incremental 不受影响。

仅在隔离测试 PDB 刻意验证未确认算法时，可显式增加
`scan.snapshot.allow-unverified-consistency=true`。这是允许实验的开关，不是数据库能力
检测、读屏障或产品方认证；设置为 true 不会等待某个 CSN，也不证明 GTM/GTM-Lite 或
GTM-Free 下的结果正确。现阶段不提供自动放行已验证环境的路径。

已经带有全量完成标记的 checkpoint 可继续恢复增量，无需该实验开关，因为不会再次扫描。
这不证明历史全量正确，也不会修复此前可能遗漏的基线；若确认旧全量有缺口，应按独立测试/
重建方案重新初始化，不能把继续恢复当成数据修复。

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

以下为原型的连接参数示例；默认仍会因未确认一致性而拒绝首次全量：

```sql
'scan.startup.mode' = 'initial',
'scan.snapshot.signal-table' = 'cdc_meta.flink_snapshot_signal',
'scan.slot.create' = 'true',
'scan.snapshot.storage' = 'disk',
'scan.snapshot.disk.cache.mb' = '16',
'scan.snapshot.disk.write-buffer.bytes' = '1048576',
'scan.snapshot.disk.max.row.bytes' = '16777216',
'scan.snapshot.disk.max.bytes' = '10737418240',
'scan.snapshot.fetch.size' = '1000',
'scan.snapshot.batch.rows' = '1000',
'scan.snapshot.batch.bytes' = '1048576',
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
    → JDBC 游标扫描所选表，先放入临时落盘合并存储（也可选有界内存）
    → 扫描结束后提交 HIGH 信号
    → 读取并按主键合并 (LOW, HIGH] 的所有完整事务
    → 再次核对表集合、关系 OID、列类型和主键
    → 清理本次信号并关闭普通 SQL 连接，复制流继续使用原有独立连接
    → 将合并后的全量行分批输出，最后一批才保存 HIGH 与全量完成标记
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

合并完成后按 `scan.snapshot.batch.rows`（默认 1000 行）与
`scan.snapshot.batch.bytes`（默认约 1 MiB 估算数据）分批输出，并释放已输出的缓冲行。
单行不可拆分，大于字节目标的单行独占一批，仍受所选存储模式的行大小/总配额约束。
首批和中间批的内部 `source.snapshot=true`、`gaussdb_snapshot_completed=false`；
只有最后一批为 `source.snapshot=last`、完成标记为 true。snapshot 字段使用 Debezium 的
`io.debezium.data.Enum` 逻辑类型，普通 STRING 会被 CDC 2.3 的快照识别逻辑忽略。

官方 fetcher 因此在所有全量批次期间持续持有同一 checkpoint 锁，不能在批次之间保存
半份全量对应的 HIGH 完成位点。最后一批之后才进入持续增量并释放锁。仅 completed
checkpoint 回调通过 `SourceTask.commitRecord` 按顺序调用 native stream 的
`setAppliedLSN`、`setFlushedLSN`、`forceUpdateStatus`；调用失败会报错，不记为 ACK 成功。
这是输出分批，不是分块 checkpoint；输出时间长时仍要相应设置 Flink checkpoint 超时。
临时落盘文件不进入 checkpoint，不能作为恢复依据；正常结束、超时、异常或取消后清理本次
拥有的目录。进程被强杀时可能残留目录，管理员应在确认无运行任务引用后清理，不能复用。
本地落盘提交不触发复制槽 ACK；首次全量未完成时重做整个全量。
普通 SQL 会话在 prepare 完成后、首批发布前关闭，避免慢输出期间长期闲置；之后不再查询
该会话，也不把普通 SQL 搬到正在 COPY BOTH 的复制会话。关闭失败同样报错并丢弃未发布
的临时快照状态；恢复会新建 SQL 会话做启动检查。详见[SQL 会话生命周期](HCS_SQL_SESSION_LIFECYCLE.md)。

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
- 初始全量仍是**整份合并后发布**，不是大表分块快照。默认 `scan.snapshot.storage=disk`，
  用私有 MVStore 文件保存最终行镜像；不受 memory 模式的 100000 行/64 MiB 总量限制，
  但默认有 10 GiB 磁盘配额、16 MiB 单行估算/编码限制。存储读缓存目标默认 16 MiB，
  编码修改默认每 1 MiB 或 1000 次提交，缓存参数不等于总堆/RSS 保证。磁盘不足或超配额
  会失败，不跳行。输出期间文件修改同样可能失败，需按 checkpoint 恢复。
  可选 `memory` 保留原有行数/估算字节上限。两种模式均未实现分块 checkpoint；
  大事务还有 native 解码缓冲压力。详细资源和故障语义见[组合方案](HCS_HYBRID_SNAPSHOT.md)。
- 必须暂停表结构变更和 TRUNCATE。前后目录核对能发现部分结构变化，不能检测所有 DDL。
  信号须能在同一复制流中出现，扫描连接须具备提交后的全局可见性，复制流须提供跨写节点
  有序、完整的提交事务；槽须保留扫描期间及未完成 checkpoint 所需的日志。
- LOW/HIGH 等待有超时；底层 JDBC/native 调用的实际取消响应还受驱动和网络超时约束。
  扫描期间不持续读取 WAL，必须评估槽日志保留空间和快照总时长。
  扫描、目录检查及全量发布期间由独立定时任务发送复制状态反馈（默认每秒），不读取业务
  WAL、不设置 applied/flushed；与读流及 checkpoint ACK 串行访问 native stream。
  反馈失败在扫描检查点/下一次 poll 或 commitRecord 传播，关闭流直接失败，不当成空闲。
  这降低未读流时的反馈超时风险，不能保证连接不会因网络、驱动或服务端故障断开。
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

针对后续内网补全指南的逐项核查，见[补全指南核查与处理](HCS_DRS_SUPPLEMENT_REVIEW.md)。
等待 LOW 断流的修正定位、诊断字段和复测步骤见[LOW 等待断流分析](HCS_LOW_SIGNAL_SOCKET_FAILURE.md)。
