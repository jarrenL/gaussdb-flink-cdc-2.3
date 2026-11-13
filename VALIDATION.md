# 验证记录

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
