# HCS 第三个启动错误：9049 / START_REPLICATION

日期：2026-10-14。适用分析对象：本项目配套的
`507.0.0.b071-cloudnative-jdk8` 驱动，原始 JAR SHA-256 为
`68391de275ff60c4a747d6e0d39df1259439d8fde8bf2c4d2b087790cb6896c2`。

## 结论

**堆栈出现 `V3ReplicationProtocol.startLogical`，不能证明选错了普通复制协议。**
本地 507 JAR 中，MultiPrimaryReplicationProtocol 继承这个方法；它也发送 START_REPLICATION。
强制 new MultiPrimaryReplicationProtocol、替换对象或绕过 builder，不会因此换成另一条启动命令。

内网反馈的 9049 是真实启动失败，但“目标实例不支持 START_REPLICATION”以及“使用了错误协议类”
还不能仅凭这段堆栈定论。当前更需要确认：**目标版本是否支持外部逻辑复制流，以及通过什么入口、
连接启动属性和服务端配置建立真正的复制会话。**

复用已有 SQL TenantConnection 避免了新连接的 thread_pool 拒绝，却没有证明这条 SQL 连接能够升级为复制会话。
此前代码注释对“租户 API 自动选择正确复制入口”的表述过强，已改正。
本次提交提供驱动回归证据和诊断工具，**不声称 9049 已修复或真机采集已通过**。

## 最新内网复测与握手证据

内网已完成诊断，确认原始驱动 SHA-256 相同，运行时连接为 TenantConnection、cloudNative=true，
协议为 MultiPrimaryReplicationProtocol，builder 为 MultiPrimaryLogicalStreamBuilder。
槽唯一匹配、类型 logical、插件 parallel_binary_decoding。这可以关闭 R1 的协议类型排查。

但这不等于“客户端连接配置全部正确”，也不等于“服务端不支持外部复制”。
协议对象类型和服务器会话模式是两个不同层面。

本地进一步直接调用配套驱动 TenantConnectionFactoryImpl 所继承的启动参数构造方法
`ConnectionFactoryImpl.getParametersForStartup`，得到以下六组结果（无网络连接）：

| replication 属性 | assumeMinServerVersion 属性 | 启动包包含 replication=database |
| --- | --- | --- |
| 未设置 | 未设置 | 否 |
| database | 未设置 | 否 |
| 未设置 | 9.2.4 | 否 |
| database | 9.2.4 | 否 |
| 未设置 | 9.4 | 否 |
| database | 9.4 | 是 |

该驱动版本的判断依据是客户端 **assumeMinServerVersion 属性**，不是刚查询到的服务端版本。
诊断中的 `server.version=9.2.4` 是 JDBC 返回的版本字段，不能用来替换 Kernel 507.1.0 产品版本，
也不能据此判断产品缺少某项功能。设置 assumeMinServerVersion 只是驱动行为配置，不会升级数据库。

对应回归测试：
`HcsMultiPrimaryProtocolTest.tenantStartupOnlyIncludesReplicationWhenExplicitlyRequestedWithCompatibleAssumption`。

当前 HCS 普通连接没有设置上述两个属性，所以这次诊断没有建立复制模式连接。
更早的新建复制连接分支已通过 URL 同时发送 replication=database、assumeMinServerVersion=9.4
和 preferQueryMode=simple，却触发 thread_pool 拒绝。**仅恢复这些参数并重复连接同一入口，
不是已经验证的修复。** 后续需要确认支持复制握手的正式入口、CMS 路由或目标版本所需配置/补丁。

R4 的说法也需要收窄：`startCopy → processCopyResults → receiveErrorResponse` 只能证明客户端
发起命令后读到了错误，不能证明 COPY 已成功启动。驱动 startCopy 先发送 Query 消息，再等待服务端
接受或拒绝；当前尚无成功进入复制流的证据。

可直接转给产品工程师的补充问题：

> 已确认配套 507 驱动和运行时 MultiPrimary 协议均正确。普通租户 SQL 连接启动复制时返回 9049；
> 通过 CMS 新建连接并同时设置 replication=database、assumeMinServerVersion=9.4、
> preferQueryMode=simple 时则被 thread_pool 拒绝。请确认 Kernel 507.1.0 当前 CloudNative
> 透明多写补丁支持的外部逻辑复制接入路径：如何通过 CMS/租户服务得到可进行复制启动握手的入口，
> 需要哪些服务端配置和客户端参数？如当前形态不支持，请明确能力边界；如支持，请提供能够成功 start()
> 的最小 Java 示例，并由服务端日志确认连接类型和拒绝节点。请勿仅根据 Java 协议类判定已进入复制会话。

## 1. 已收到的真机观察（脱敏摘要）

- 服务端反馈版本：GaussDB Kernel 507.1.0，集中式 CloudNative 透明多写部署。
- CMS 入口内部连接到 SQL/thread_pool 端口；enable_thread_pool=on，wal_level=logical。
- 创建带 replication=database 的新连接被 thread_pool 拒绝。
- 改用已有 TenantConnection 后，流启动阶段报 `syntax error at or near "START_REPLICATION"`，错误码 9049。
- 堆栈包含 TenantReplicationStreamBuilder$1.start、LogicalStreamBuilder.start、
  V3ReplicationProtocol.startLogical、initializeReplication、QueryExecutorImpl.startCopy。
- 槽查询函数可用，目标槽的插件据反馈为 parallel_binary_decoding。
- 尚未成功读取复制数据，因此报文边界、事务顺序、恢复位点及 ACK 语义仍未获得真机证据。

进程名和端口情况是该环境的观察，暂不把它们当作所有版本的通用能力识别标准。

## 2. 本地驱动中已经核对的事实

以下来自配套 JAR 的 javap 字节码检查，类名前缀均为 `com.huawei.gaussdb.jdbc`。

| 类/方法 | 实际行为 | 含义 |
| --- | --- | --- |
| core.tenant.TenantConnection.initReplicationProtocol(QueryExecutor) | 创建 MultiPrimaryReplicationProtocol | 租户连接本身已有多写协议选择逻辑 |
| core.tenant.TenantConnection.getReplicationAPI() | 创建 TenantReplicationConnectionImpl(this) | 获取 API 对象，不在此方法中新建或升级后端连接 |
| TenantReplicationStreamBuilder$1.start(options) | 从原连接取 getReplicationProtocol() 后调用 startLogical(options) | 应检查运行时对象类型，不能只看堆栈的方法声明类 |
| MultiPrimaryReplicationProtocol | extends V3ReplicationProtocol，未覆盖 startLogical | 多写协议也显示父类 startLogical 堆栈 |
| V3ReplicationProtocol.startLogical | 构造 START_REPLICATION SLOT … LOGICAL … 后调用 QueryExecutor.startCopy | 多写协议使用相同的命令构造流程，位点通过 asString() 写入 |
| MultiPrimaryReplicationProtocol.createReplicationStream | 创建 MultiPrimaryReplicationStream | 差异之一在成功进入 COPY 后所创建的流实现 |

PgConnection 构造过程调用可覆盖的 initReplicationProtocol(queryExecutor)，然后保存返回的协议对象。
ConnectionFactoryImpl 中则在连接启动阶段处理 replication 属性；不能把获取 API 对象等同于重新进行启动握手。
这并未确认目标服务端正确的接入方式，仍需与该部署的服务端实现或正式示例核对。

新增自动测试：
`HcsMultiPrimaryProtocolTest.real507TenantProtocolInheritsStartLogicalAndSendsStartReplication`。
测试调用真实 TenantConnection.initReplicationProtocol 方法、真实租户 builder 和真实协议实现，
只替换网络 QueryExecutor/CopyDual，验证：

1. 返回的协议对象是 MultiPrimaryReplicationProtocol。
2. startLogical 的声明类是 V3ReplicationProtocol。
3. 实际调用 startCopy 的命令为 `START_REPLICATION SLOT test LOGICAL 0/64,0/2,0/1000`。
4. 在模拟服务端接受 COPY 后，返回 MultiPrimaryReplicationStream。

该测试用于证明驱动继承与命令构造行为，不验证服务器接受命令。

内网也可对实际使用的 JAR 执行以下检查，不需数据库连接：

```bash
javap -classpath /path/to/gaussdbjdbc.jar -p -c \
  com.huawei.gaussdb.jdbc.core.v3.replication.MultiPrimaryReplicationProtocol
javap -classpath /path/to/gaussdbjdbc.jar -p -c \
  com.huawei.gaussdb.jdbc.core.tenant.TenantConnection
javap -classpath /path/to/gaussdbjdbc.jar -p -c \
  com.huawei.gaussdb.jdbc.core.v3.replication.V3ReplicationProtocol
```

## 3. 新增诊断入口

构建后可直接使用 java 启动，不需要 Flink 集群：

```bash
mvn -Pflink-1.16 clean verify

# 仅检查实际加载的驱动与方法继承，不连接数据库。
java -cp target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar \
  com.ververica.cdc.connectors.gaussdb.examples.HcsConnectionDiagnostics --driver-only

# 复用 HcsSmokeJob 的属性文件，建立普通租户 SQL 连接并检查元信息。
java -cp target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar \
  com.ververica.cdc.connectors.gaussdb.examples.HcsConnectionDiagnostics /secure/path/hcs.properties
```

第二个命令读取驱动/服务端版本、运行时连接类、协议类、逻辑 builder 类及指定槽的 plugin/slot_type。
它不调用 start()、readPending() 或 ACK，也不创建/删除/推进槽。
属性文件仍需包含连接器要求的配置项，见 `examples/hcs.example.properties`。
诊断成功只证明这些读取完成，不代表复制链路通过。

关注以下输出：

```text
driver.codeSource.file=...
driver.codeSource.sha256=...
driver.version=...
server.version=...
connection.class=...
protocol.class=...
protocol.startLogical.declaredBy=...
logicalBuilder.class=...
slot[1].plugin=...
slot.matches=...
```

SHA-256 对应实际加载 Driver 类的代码容器。如果 Driver 来自连接器 all.jar，输出就是 all.jar 的哈希，
不能直接拿它与原始 gaussdbjdbc.jar 的哈希比较。
独立 java 进程的类路径可能与 Flink 不同；实际 Flink 初始化日志也新增了连接类和协议运行时类输出，
用于检查集群是否加载了同一套实现。不要只贴截断后的父类堆栈。

## 4. 请内网 AI / 产品工程师集中回答这五个问题

请沿用原问答的证据分类，优先给出目标版本可执行的官方最小示例。

**R1：运行时实际类型与驱动是否吻合？**

请返回上述诊断结果，并核对 Flink 启动日志中的 protocol class。
如果已经是 MultiPrimaryReplicationProtocol，就不再把 V3 父类堆栈当作错误分支。
如果确实是普通 V3 对象，请核对加载的 JAR、完整 TenantConnection 类名及版本差异。

**R2：Kernel 507.1.0 的这个具体透明多写补丁是否支持外部客户端逻辑流？**

请给出适用版本的能力说明、必要许可/开关或原厂成功订阅示例。
可以查询/创建逻辑槽以及驱动有多写类，不足以单独证明此服务端入口支持 START_REPLICATION。
现有本地文档中的 JDBC 逻辑复制示例明确标有“非透明多写”，需要目标形态的对应说明。

**R3：怎样建立透明多写的复制会话？**

在 enable_thread_pool=on 时，应该通过 CMS 的哪种服务/路由、租户节点的什么端口或其他官方入口订阅？
应传哪些启动参数？CMS 是否返回独立复制入口？服务端是否要求不同的配置或配套驱动补丁？
请提供完整连接和 start() 示例，并说明正常成功时后端会话的类型。
不要从普通实例经验直接猜“端口 +1”，也不要只建议关闭线程池而缺少目标部署依据。

**R4：9049 是哪个后端返回的，命令进入了哪个解析器？**

请由服务端日志或支持的会话诊断确认：该连接当前是普通 SQL 会话还是复制/WAL sender 会话？
START_REPLICATION 被谁处理？目标版本是否需要不同协议？如果需要，请给出对应 API/命令及驱动实现，
而不是仅根据语法错误推断语法完全不支持。

**R5：现有槽和插件是否适合后续 JSON 流？**

请核对目标槽的完整元信息，以及该形态下 parallel_binary_decoding 配合 decode-style=j 的行为。
公开的通用[逻辑解码选项说明](https://support.huaweicloud.com/intl/en-us/fg-gaussdb-dist-v8/gaussdb-18-0033.html)
说明插件名称影响默认格式，显式 decode-style 可指定格式。因此暂不能仅凭插件名判定它一定输出二进制；
也不能把通用说明直接当作本透明多写版本的保证。请提供适用配置与成功解码的一小段样例。

## 5. 收到答复后的开发分支

- 若是目标运行时加载错误驱动：修正依赖/类加载，再测试同一 API；不改写复制命令。
- 若是复制入口或握手配置缺失：按正式方式增加复制连接建立流程，保留普通 SQL 连接做槽检查，
  并回归多 CMS、鉴权、资源关闭、完整三段恢复位点。
- 若需要驱动或服务端补丁：明确兼容矩阵，使用配套版本验证。
- 若该部署当前不提供外部逻辑流：明确功能限制，评估受支持的原厂采集接口，不把反复重启当作重试可恢复问题。
- 只有 start() 真正成功后，才继续验证原问卷 Q07–Q11 的报文、事务、恢复和 ACK。
