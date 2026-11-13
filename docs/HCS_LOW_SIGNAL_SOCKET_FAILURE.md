# 等待 LOW 时 Socket is closed：定位与修正

## 内网证据与当前结论

用户反馈运行的是含 DiskSnapshotStore 的 2026-11-13 分支源码包。确认
WalReplicationStream.initializeReplicationApi 内部通过 DriverManager 新建 native 复制连接，
传入构造器的普通连接用于 SQL 槽检查。JdbcSnapshotAccess 的 SELECT、Statement/ResultSet
关闭以及 rollback 均在普通 SQL 连接上；代码未将该连接直接用于 COPY。

修正后的堆栈为 InitialSnapshot.prepare → awaitSignal → readOrdered → native readPending。
内网将其定位为等待 LOW，尚未开始业务全量扫描；仅凭源码行号不能排除等 HIGH，补丁新增
显式阶段字段以避免这种歧义。另一次 ERROR 后出现 320000 条 `op=r`，说明后续执行产生过
快照数据，不能将其作为同一尝试成功完成全量到增量交接的证明。

内网时间线：15:25:26 建立复制流，15:29:40 客户端首次报告异常，间隔约 254 秒。
服务端此间持续输出 PhysicalChangeParser 初始化/销毁日志，提供的片段未见关闭日志。
这说明服务端有活动，不证明其收到客户端状态反馈，也不排除日志范围之外的断开事件。
254 秒是启动至报告异常的时长，尚未测得 socket 真正关闭的时刻。

**目前没有确认最初关闭 socket 的组件与原因。** 不将 SSL context 共享、JVM 自动空闲
关闭或连接池回收当作已证实的根因，也不通过全量结束后盲目重建流来声称修复。

## 配套 507 驱动核对

本地核对 `507.0.0.b071-cloudnative-jdk8` 的实际类：

- TenantConnection 的 close 调用自身 PgConnection.close，并从 TenantManager 移除自身。
  未在该方法中发现“关闭另一个连接”的直接调用；这不能替代目标环境复现。
- PGStream.hasMessagePending 在尝试 peek 前临时设短读超时，随后恢复原值；正常、超时和
  异常路径都会恢复。恢复通过 Socket.setSoTimeout，若 socket 已关闭，该操作也会失败。
  因此栈顶的 setSoTimeout 不能单独确定首次关闭者，还可能掩盖更早的读取异常。
- forceUpdateStatus 发送已有 received/flushed/applied，不自行调用 applied/flushed setter。
  驱动的定时状态检查依赖 read/readPending 路径，并非独立后台保活线程。

SQLException 的默认文本/堆栈通常不包含 SQLState 与 vendor code。输出中没看到它们，
不代表异常对象没有值，也不代表一定不是连接错误；需显式调用 getSQLState/getErrorCode。

## 已确认并修正的代码缺口

1. **未读流期间的状态反馈**：此前目录检查、SQL 扫描和整份快照发布会暂停 WAL 读取，
   复制状态反馈也随之暂停。现新增 task 生命周期内的定时反馈，默认每 1000 ms，配置为
   `replication.status.interval.ms`。它不消费 WAL、不改变 lastRead、不设置 applied/flushed。
   该间隔需小于目标实际生效的发送超时并留余量，不能只根据参数名或解析线程活动判断。
2. **访问串行化与错误传播**：反馈、读流、checkpoint ACK 使用同一个 WalReplicationStream
   monitor，避免并发操作原厂流。反馈失败保存原异常，在下一次任务检查时失败；扫描的
   running 检查同样会传播。阻塞中的驱动调用仍受其网络/SQL 超时约束。
3. **关闭流不再伪装空闲**：原 readChanges 在 native isClosed=true 时返回空列表，可能让
   任务持续空转。现抛出连接失败，不忽略断流、不回退 SQL 解码、不悄悄换槽。
4. **连接隔离检查**：初始化拒绝 SQL/native 为同一 Connection，或可观察到共用同一个
   非空 QueryExecutor。Task 字段明确命名 sqlConnection。现有专用复制连接仍由 stream 管理。
5. **无需 INFO 的阶段诊断**：错误包含 `phase`、`sqlState`、`errorCode` 和
   `statusFeedbackAgeMs`，保留原始异常链；状态反馈从未成功时 age 为 `none`。
   该间隔只统计独立后台反馈，不统计读流路径内的状态发送；客户端发送成功也不证明服务端
   已处理该状态，不能据此单独判断网络健康。
   阶段包括 VALIDATING_TABLES、WRITE_LOW、WAIT_LOW、SCAN、WRITE_HIGH、WAIT_HIGH、
   VERIFY_TABLES、CLEANUP、PUBLICATION。公开错误不拼入 SQL、认证配置或服务端消息原文。
6. **生命周期清理**：stop 先停止定时调度，再关闭复制/SQL 连接；扫描失败按既有机制清理
   私有磁盘状态。仅 completed checkpoint 可以确认日志，定时反馈与本地落盘均不替代它。

这是针对确定缺口的修正与真机排查增强，**尚未证明已解决该次 254 秒断流**。
首次全量的默认一致性限制保持；未改变 LOW/HIGH 边界或 checkpoint 输出契约。

## 内网复测

在原有隔离测试环境、专用槽及信号表上，使用新包执行完整 initial 作业。先验证空闲等待、
长扫描/慢输出期间反馈，再验证全量期间写入和全量之后的持续变更；不要只数全量行。
同一个运行尝试应保留完整 TaskManager 日志并按尝试划分，不把重试前后的输出合并解释。

参数（默认已生效）：

```properties
replication.status.interval.ms=1000
```

在实际 TaskManager 使用的 Log4j2 配置中启用连接器 INFO，有助于观察 LOW/HIGH 顺序：

```properties
logger.gaussdb.name = com.ververica.cdc.connectors.gaussdb
logger.gaussdb.level = INFO
```

ERROR 即使没有 INFO 也会包含例如：

```text
HCS initial snapshot failed (phase=WAIT_LOW, sqlState=08006, errorCode=0, statusFeedbackAgeMs=...)
```

上面仅为字段格式示例，不是该次真机已经测得的 SQLSTATE。若反馈失败先被后台发现，
外层会显示 replication status feedback failed 及对应代码。应保留首次错误及此前的驱动
WARN/ERROR，而不是只保留最后一层 SocketException。

必要的只读环境核对：目标 logical_sender_timeout、wal_sender_timeout 的值及单位，复制
是否有 sender-timeout 覆盖、实际 driver/JDK 版本、复制连接 ID/服务端会话、SSL/代理空闲
策略。产品文档中逻辑解码超时可能由 logical_sender_timeout / sender-timeout 控制，不能
用普通 wal_sender_timeout 单独断言实际解码会话超时；以目标 HCS 版本的接口/契约为准。

若反馈正常仍在 WAIT_LOW 失败，应继续核对驱动早期异常、服务端对应会话和 TLS/网络路径，
必要时由驱动/内核侧追踪首次 close 调用。禁止仅为掩盖错误而提前 ACK 或跳过 LOW。

## 本地验证与适用范围

新增实际定时线程测试：阻塞 SQL 扫描时仍反馈，暂停全量输出时仍反馈；全量 HIGH 后
缓冲尾部事务和后续实时事务继续交付，不初始化第二条流、不提前 ACK，stop 后线程退出。
失败测试覆盖 WAIT_LOW 的 socket 异常、扫描/发布期反馈失败与立即清理、停止后取消调度。
真实 507 MultiPrimaryReplicationStream 的合成 CopyDual 测试验证状态包保留 applied/flushed、
不额外读 WAL、关闭流立即失败，以及保留原 SQLSTATE/SocketException。

没有连接目标 HCS 复现 254 秒故障；这些测试不能替代服务端、SSL/代理和目标驱动环境验收。
版本矩阵和最终包校验见 [VALIDATION.md](../VALIDATION.md)。

参考：

- [pgJDBC 复制反馈说明](https://jdbc.postgresql.org/documentation/server-prepare/)：read/readPending 与强制反馈的机制参考，不将其当作 HCS 产品契约。
- [GaussDB 逻辑解码参数说明](https://support.huaweicloud.com/centralized-devg-v10-gaussdb/gaussdb-42-0101.html)：sender-timeout 与 logical_sender_timeout；此页为非多写文档，目标 HCS 须另核对。
- 配套 507.1 产品文档“PGReplicationStream”“逻辑复制”：本地文本主题 `0000002693215840`、`0000002723016159`，以及配套 JAR 的上述实际字节码。
