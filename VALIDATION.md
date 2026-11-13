# 验证记录

## 2026-11-13：等待 LOW 断流核查、定时状态反馈与阶段诊断

基于落盘版本 `f56d629`。内网补充将故障从“全量结束后切增量”修正为“prepare 中等待
LOW 时 native readPending 报 Socket is closed”；现有实现已使用独立 SQL/COPY 连接。
约 254 秒的时间线、缺失关闭日志和堆栈中未显示 SQLSTATE，不足以确定首次关闭者。
本次没有在 HCS 复现该故障，没有将“客户端 SSL 空闲关闭”等假设写成已证实根因。

修正已确认的缺口：默认每秒独立发送 native 状态反馈，覆盖目录检查、SQL 扫描及快照
发布暂停读流的阶段；与读流/ACK 串行，反馈不读取 WAL、不设置 applied/flushed。
关闭流由返回空列表改为立即报错；反馈失败传给任务，清理未完成快照的私有文件。
初始化增加同 Connection/QueryExecutor 拒绝检查。错误中加入 phase、SQLSTATE/vendor code
和最近一次后台反馈的客户端发送时间间隔，保留原异常链；不自动重连或跳过交接边界。

执行 `scripts/verify-matrix.sh`，Flink 1.13.6、1.14.6、1.15.4、1.16.3 每版通过
165 项测试，失败/错误/跳过均为 0，编译和打包均通过。新增真实定时线程测试核对扫描、
发布暂停期间仍反馈，HIGH 后尾部事务和实时增量不遗漏、不初始化第二条流、仅显式模拟
completed checkpoint 才 ACK，stop 后线程退出。失败测试覆盖 WAIT_LOW 的 SocketException
及 SQLSTATE、扫描/发布期反馈失败与立即清理、停止后取消调度。真实 507 驱动合成
CopyDual 报文验证后台反馈不提前推进 applied/flushed、不读取 WAL、关闭流报错及原异常
保留；共享底层 executor 在 COPY 启动前被拒绝。既有官方 CDC checkpoint/恢复测试继续通过。

`mvn spotless:check` 和 `git diff --check` 通过。最终包检查确认新增方法/阶段字段存在，
修改的生产类均为 Java 8 字节码，SQL Factory SPI 保留，未包含 Flink streaming 引擎类。
Java 8 执行最终包的 `HcsConnectionDiagnostics --driver-only`，结果
`DRIVER_ONLY_COMPLETE_NO_DATABASE_CONNECTION`；没有连接数据库或启动复制。

最终 Flink 1.16.3 包大小 35734703 字节，SHA-256：
`f66f5d84aa832a34f0fef99b21603a5e75317ca66c2b7d93a38053d69c382dad`。
本次是保活、错误处理和诊断修正，不等于已完成真实 HCS 的故障根因复现/修复验收。
[定位依据与内网复测步骤](docs/HCS_LOW_SIGNAL_SOCKET_FAILURE.md)。

## 2026-11-13：组合方案第一步，源端合并默认落盘

保持通用 Source 输出契约，沿用先建槽、完整三段位点恢复和 completed checkpoint ACK，
将整份全量/区间 WAL 的主键合并新增为默认 disk 模式；memory 模式保留原有限额。
未改成只发逐表 CSN 过滤事件并要求目标 Applier 去重。LOW/HIGH 实验路径仍在，首次
initial 默认关闭；落盘不解决跨写节点读取可见性，也不提供分块 checkpoint。

执行 `scripts/verify-matrix.sh`，Flink 1.13.6、1.14.6、1.15.4、1.16.3 每版均通过
156 项测试，失败/错误/跳过均为 0，编译和打包均成功。新增测试核对磁盘与内存合并结果、
主键变更、跨表键、NULL、超过 64 KiB 的中文/emoji/NUL、磁盘配额、行限制、HIGH 超时、
重复扫描主键及目录清理。正常取消部分全量输出后清理磁盘，不允许其不完整 offset ACK。
官方 CDC 2.3 的跨全量输出批次 checkpoint 门闩测试改用真实磁盘存储，仍不能保存半份全量。

最终 Flink 1.16.3 包在 Java 8（Zulu 1.8.0_482）使用 `-Xmx64m` 执行独立检查，
不连接数据库：50000 行，每行 1024 个中文字符，完整分批读回并核对内容/数量/临时文件清理。
此次输出为 `PACKAGED_DISK_MERGE_PASS rows=50000 file_bytes=121769984 max_heap=59768832`。
文件约 116 MiB，大于该最大 Java 堆；这验证整份合并数据无需驻留 Java 堆，不是 RSS 上限证明。

`mvn spotless:check` 通过。包内检查确认 MVStore 与 DiskSnapshotStore 为 Java 8 字节码，
MVStore 的 `org.h2` 已迁移包名，未包含 H2 JDBC Driver 或 Flink streaming 引擎类，保留
官方 CDC Source 及 SQL Factory SPI。Shade 升级至 3.6.0，以处理 MVStore 的可选多版本类。
最终包大小 35732136 字节，SHA-256：
`e9de4c5ea40bb1fe70eee9d05ffb4ef11bce9b15e6445b6085a9c92bdae5d97e`。

临时文件不是 checkpoint；强杀可能残留，未完成全量失败仍全部重做。未运行目标 HCS
多节点、读取一致性、长事务和端到端 sink 验收，没有真机无丢失结论。
[组合方案、资源限制与尚未解决项](docs/HCS_HYBRID_SNAPSHOT.md)。以下保留历史验证记录。

## 2026-11-13：保持通用 Source，新增只读跨会话导入探针

用户明确选择保持可接聚合/计数的通用 Flink CDC Source。未将 handoff 改为依赖目标
Applier 去重的迁移模型，也未仅靠 per-table CSN 过滤取消源端重叠处理。
一致性前提未确认，首次 initial 默认关闭的限制继续生效。

新增 HcsSnapshotImportDiagnostics：两个新 SQL 连接，同租户/数据库；导出侧保持
REPEATABLE READ READ ONLY 事务直到导入侧检查结束；导入必须在 importer 的任何
SHOW/SELECT 前完成；最后仅 rollback。无业务表读写、槽管理、复制流或 ACK。
成功仍报告跨写节点可见性和 CSN/WAL 对应关系 NOT_VERIFIED，不自动启用 initial。

执行 scripts/verify-matrix.sh，Flink 1.13.6、1.14.6、1.15.4、1.16.3 每版均通过
151 项测试，失败/错误/跳过均为 0。新增测试核对导入顺序、导出事务生命周期、实际
隔离级别降级、非法 snapshot ID、导入失败清理、既有事务保护、同租户/数据库检查和
错误脱敏。实际目标 HCS 导出/导入尚未运行，无跨节点或快照/日志一致性验收结论。

最终包 SHA-256：`49659d518d6f7d21d9fea8258cb4360765d4bd3f663cad8b4fd23ff86b138e0f`。
[Source 方案区别与探针命令](docs/HCS_GENERIC_SOURCE_STRATEGY.md)。


## 2026-11-13：读取一致性未确认时默认拒绝首次全量

内网补充仍未提供目标 HCS 租户的全局读取可见性契约或已验证读屏障。首次 initial
默认在连接、建槽和扫描前拒绝启动；内部 InitialSnapshot.prepare 同样检查。
只保留 `scan.snapshot.allow-unverified-consistency=true` 的隔离实验入口，此开关不证明
一致性、不根据 GTM 参数或复制 ACK 自动放行。增量模式保持原行为。

执行 `scripts/verify-matrix.sh`，Flink 1.13.6、1.14.6、1.15.4、1.16.3 每版均通过
147 项测试，失败/错误/跳过均为 0。新增测试验证缺失/false 实验选项时没有建立连接，
内部快照入口没有 SQL/WAL 交互，非法选项被拒绝；真实官方 CDC 框架恢复测试还验证
已完成全量 checkpoint 可在移除实验选项后恢复增量，不重新扫描。

只读 HcsSnapshotBoundaryDiagnostics 新增当前 SQL 连接的两项 GTM 参数查询。
测试确认 off/2 组合、无权限或空结果均不产生已验证判断；错误仅输出 SQLSTATE 和错误码。
始终输出 `gtm.read_visibility=NOT_VERIFIED`，不宣称集群参数一致或目标多写读取已验证。
没有连接 HCS 真机，也没有证明或修复旧全量可能遗漏的基线数据。

最终包 SHA-256：`350019f9c591aecb58be1851f60d32f77f51a9ecd7c5a162d2e46d8b2e5019da`。
[内网反馈核查与具体内核确认项](docs/HCS_DRS_SUPPLEMENT_REVIEW.md)。


## 2026-11-13：补全指南核查后的 ACK 与安全分批修正

在 `c793e12` 基础上补齐 applied/flushed/status ACK，并将完整区间合并后的全量结果按
有界批次输出。使用 Debezium 逻辑枚举 snapshot 字段保持跨批次快照 checkpoint 锁；
仅最后一批设置全量完成 offset。总缓冲区限制保持不变，大表分块/落盘未实现。

执行 `scripts/verify-matrix.sh`：Flink 1.13.6、1.14.6、1.15.4、1.16.3 每版均通过
142 项测试，失败/错误/跳过均为 0。新增跨 poll 门闩测试证明首批输出、后续批次暂停时
checkpoint 不能完成，最终批次结束后才记录 HIGH 并由完成回调 ACK。
真实 507 合成报文同时核对三段 applied/flushed，setter 失败不继续 flushed/status。
此外验证输出批次行数、估算字节控制，以及中间批 offset 不可作为已完成全量恢复。

最终包 SHA-256：`3b0097ccac46c32fcb49bbe9c92e611b1e72974fb4232edeeb811687a8c9b44c`。
包内含 SnapshotPublication 和 Debezium Enum，未包含 Flink streaming 引擎类。
未连接真实 HCS；扫描全局可见性与建槽前在途长事务的完整捕获仍待内网确认。
[逐项核查结果与待确认项](docs/HCS_DRS_SUPPLEMENT_REVIEW.md)。


## 2026-11-13：CDC 2.3 在线 initial 实验实现

分支 `feature/hcs-cdc23-initial-handoff`，官方 CDC 2.3.0、507 CloudNative JDBC、JDK 11，
Java 8 字节码。执行 `scripts/verify-matrix.sh`，每个 profile 均进行 clean、编译、测试与打包：

| Flink 引擎 | 编译 / 打包 | 自动测试 | 失败 / 错误 / 跳过 |
| --- | --- | --- | --- |
| 1.13.6 | 通过 | 138 | 0 / 0 / 0 |
| 1.14.6 | 通过 | 138 | 0 / 0 / 0 |
| 1.15.4 | 通过 | 138 | 0 / 0 / 0 |
| 1.16.3（最终包） | 通过 | 138 | 0 / 0 / 0 |

新增验证包含：源端重叠合并、主键变更、跨表/复合键、完整镜像和缓冲区限制、空表边界、
HIGH 超时、乱序、表重建、缺槽拒绝补建、active 槽拒绝、三段 restart 边界、JDBC 游标及
取消回滚、中文/bytea 和官方 RowData 的 snapshot READ 输出。

`InitialFrameworkCheckpointTest` 启动未经修改的官方 CDC 2.3 Debezium 引擎，底层 SQL/
复制流受控：扫描中 checkpoint 不保存完成 offset；输出第一行阻塞时 checkpoint 不能
截断整批快照；completed 回调前不 ACK，完成后 ACK HIGH；新引擎从完整三段位点恢复，
不再调用全量扫描，并继续输出新增量。原有增量协议、事务和恢复测试同时通过。

最终 all.jar 已检查含有 initial 实现和 SQL Factory SPI，未打入 Flink streaming 引擎类。
包：`target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar`。
最终包的 `HcsConnectionDiagnostics --driver-only` 执行成功，输出
`DRIVER_ONLY_COMPLETE_NO_DATABASE_CONNECTION`，确认只是本地驱动入口验证。
SHA-256：`d031aac58ddbb869f4ad40b038382f40f7f543d7ea1b990b8a059e811cd533dd`。

**没有连接真实 HCS 租户。** 上述是本地代码、协议和框架验证；不代表多节点全局可见性、
长事务保留、真实系统表/权限和节点切换已通过验收。首次全量使用有界内存合并，不是大表
分块快照。适用范围、信号表建表和真机清单见
[在线全量开发说明](docs/HCS_CDC23_INITIAL_SNAPSHOT.md)。

以下保留此前增量模式的历史验证记录；测试中的探针负向用例会打印预期 FAIL 文本，
测试是否成功以 Surefire 的失败/错误统计及 Maven 退出码为准。


2026-10-14 更新：内网使用开发指南给出的 `com.huawei.opengauss.jdbc.Driver`、
`jdbc:opengauss://`、`allowConnThreadPool=false`、`replication=database`、
`preferQueryMode=simple` 建立独立复制连接后，9049 变为
`No gs_hba.conf entry for replication connection`。这说明握手已进入复制认证，
但不证明流已启动。权限和 HBA 配置已由内网调整；目前实例重启后 PDB 不可用，
`NO_AVAILABLE_TENANTNODE` 阻止了真机复测。

连接器现已采用该独立连接方式。SQL 槽检查仍使用普通租户连接，HCS 复制流使用
`opengauss` 包中的 MultiPrimary builder、位点和流；失败时关闭独立复制连接，
且不回退 SQL 解码。自动测试覆盖复制属性、三段位点、ACK、资源关闭和非 HCS 路径。
使用 JDK 11 对 Flink 1.13.6、1.14.6、1.15.4、1.16.3 执行
`scripts/verify-matrix.sh`，四个版本均编译、打包和通过 113 项测试，
失败 / 错误 / 跳过均为 0。最终 all.jar 的 `HcsConnectionDiagnostics --driver-only`
执行成功；这个入口没有连接数据库，也不能代表复制流验收。
此前的 9049 / 连接复用描述保留为历史过程。

此前启动问题：内网反馈 `START_REPLICATION` 错误 9049，尚未成功采集。
新增的真实 507 协议继承/命令构造回归和诊断入口见
[`HCS_START_REPLICATION_9049.md`](docs/HCS_START_REPLICATION_9049.md)。
下表是此前连接复用修改的四版本回归，不能据此认定服务器复制会话已建立。

本次 9049 排查提交：默认 Flink 1.16.3 的 `mvn verify` 共 112 项测试通过（失败/错误/跳过均为 0），
最终 all.jar 的 `HcsConnectionDiagnostics --driver-only` 执行成功，确认多写协议继承 V3 的 startLogical。
诊断工具的数据库连接模式仍待内网运行；本次没有重跑其余三个引擎版本，也没有完成真机复制验证。

后续内网诊断已返回：驱动哈希、租户连接、MultiPrimary 协议和 builder 均匹配，槽唯一存在。
新增六组真实驱动启动参数构造回归，确认复制标志与 assumeMinServerVersion 的组合条件；
默认 Flink 1.16.3 的 `mvn verify` 共 113 项通过（失败/错误/跳过均为 0）。
这仍不证明复制入口可用；复制会话入口及握手问题等待目标环境确认。

日期：2026-10-14。JDK 11，Maven，507 CloudNative JDBC。

本仓库使用官方 `com.ververica:flink-connector-debezium:2.3.0`；没有修改 CDC 2.3 框架源码。

| Flink 引擎 | 编译 / 打包 | 自动测试 | 失败 / 错误 / 跳过 |
| --- | --- | --- | --- |
| 1.13.6 | 通过 | 111 | 0 / 0 / 0 |
| 1.14.6 | 通过 | 111 | 0 / 0 / 0 |
| 1.15.4 | 通过 | 111 | 0 / 0 / 0 |
| 1.16.3 | 通过 | 111 | 0 / 0 / 0 |

执行命令：`scripts/verify-matrix.sh`。每个版本均运行全部测试，最后保留 1.16.3 构建结果。

## 内网联调反馈修复

根据用户提供的透明多写真机错误及槽查询验证结果，修复两处初始化路径：

- 槽检查使用 `pg_get_replication_slots()`，避免查询被路由到 DN 上不存在的 `pg_replication_slots` 视图。
- HCS 复用已有租户连接的复制 API，避免带 `replication=database` 新连 CMS 时触发
  `Replication should connect port not in thread_pool.`。非 HCS 仍新建独立复制连接。

`HcsMultiPrimaryProtocolTest` 使用真实 507 租户 builder 和受控 JDBC 连接，验证不重新连接 CMS、
完整传入恢复位点、关闭流不关闭借用连接、初始化失败禁止 SQL 回退、缺槽明确失败，以及普通模式
独立连接的创建和释放。重复关闭不重复释放复制流或独立连接。
这些回归验证了客户端修复；修复后的真实 HCS 启动及增量采集仍需内网复测。

## 关键证据

- `FrameworkCheckpointTest`：启动官方 CDC 2.3 `DebeziumSourceFunction`、Debezium Embedded Engine、
  真实 handover/fetcher 和 offset storage；底层使用受控的 GaussDB 复制流。
  阻塞首行输出期间 checkpoint 不能截断整批事务；仅完成 checkpoint 才 ACK；中止的 checkpoint
  不确认；新引擎实例从框架状态恢复完整三段位点，继续采集之后的事务。
- `HcsMultiPrimaryProtocolTest`：使用真实、未修改的 507 `MultiPrimaryReplicationStream` 和合成
  CopyDual 报文，校验提交前不输出、三段位点及 ACK 字节布局、租户 builder 和恢复参数。
- `TaskRecoveryTest`：重放过滤、跨租户状态保护及普通位点拒绝。
- `ConfigurationAndRowTest`：多 CMS 地址解析、SQL 工厂接入官方 Source、官方 JSON 和 RowData
  反序列化、INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE、布尔与小数，以及缺列失败。
- 其余测试覆盖沿用的 WAL JSON/二进制解析和复制辅助逻辑。

## 最终打包检查

使用最终 `-all.jar` 优先加载 CDC/连接器类，执行 SQL Factory SPI 发现、官方 RowData
反序列化以及上述框架 checkpoint/恢复测试，结果 `PACKAGED_CDC23_FRAMEWORK_PASS`。
同时检查未打入 Flink 引擎类，并显式保留 CDC 2.3 配套的 `EmbeddedEngineChangeEvent` 类。

## 尚未完成的验证

没有可由当前环境连接的真实 HCS 透明多写实例。上述结果是客户端协议及框架级测试，
不等于完整 Flink 集群端到端验收，也不等于真实 HCS 节点切换验收。

内网请使用专用槽验证实际日志格式、多节点写入、事务回滚、数据类型、checkpoint 恢复、
CMS 与数据节点切换。以上旧增量基线不提供全量快照、自动建槽或 DDL schema evolution；2026-11-13 的
实验性 initial 能力见文首更新，DDL schema evolution 仍未支持。
