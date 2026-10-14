# GaussDB HCS Connector for Flink CDC 2.3

独立的实验性连接器工程，面向 **HCS GaussDB 透明多写部署**。使用官方
`com.ververica:flink-connector-debezium:2.3.0` 的 `DebeziumSourceFunction`、嵌入式
Debezium 引擎、反序列化接口及 checkpoint/offset 存储，并在底层接入配套 507 JDBC
的 `MultiPrimaryReplicationStream`。不是原 JDBC 连接器仓库中的 `cdc-2.4.x` 模块。

**当前仅支持增量联调，不提供生产验收保证。** 真实透明多写实例的消息布局、解码选项、
CMS 切换和节点故障恢复需要在目标 HCS 版本实测。普通 Distributed CN/DN 实例不能替代该验收。

## 版本与构建

- 官方 Flink CDC：2.3.0；目标引擎范围 Flink 1.13–1.16。
- 默认构建：Flink 1.16.3；其他 profile：`flink-1.13`（1.13.6）、`flink-1.14`（1.14.6）、`flink-1.15`（1.15.4）。
- Java 8 字节码，当前构建和测试使用 JDK 11。
- 驱动：配套 **507 CloudNative JDK8**，本地 Maven 坐标为 `507.0.0.b071-cloudnative-jdk8`。
  该坐标不表示公开 Maven 仓库中有对应发行包。本仓库不上传华为驱动二进制。
- `2.3` 是 Flink CDC 版本，不是 Flink 引擎版本。

先从你有权使用的 GaussDB 产品介质取得配套 `gaussdbjdbc.jar`，导入本地仓库：

```bash
mvn install:install-file -Dfile=/path/to/gaussdbjdbc.jar \
  -DgroupId=com.huaweicloud.gaussdb -DartifactId=gaussdbjdbc \
  -Dversion=507.0.0.b071-cloudnative-jdk8 -Dpackaging=jar -DgeneratePom=true
mvn -Pflink-1.16 clean verify
```

当前验证所用驱动 SHA-256：
`68391de275ff60c4a747d6e0d39df1259439d8fde8bf2c4d2b087790cb6896c2`。
不同配套驱动需重新执行协议测试，不要用 506 驱动替换。

`target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar` 包含 CDC 2.3、Debezium/Kafka Connect
及 GaussDB 驱动；Flink 引擎依赖为 provided。SQL Client 部署时将该包放入 Flink `lib` 并重启相关进程。
普通无 classifier 的 JAR 不含上述依赖。不要同时加载旧版 GaussDB 连接器或重复 JDBC 驱动。

## 接入前提

1. 确认实例运行的是 HCS **透明多写**形态，并使用与其配套的 507 驱动。
2. 准备 CMS 入口、租户认证信息、租户数据库名称和数据库账号。
3. 由管理员按目标版本文档开启逻辑复制，并在目标租户/数据库预建一个专用复制槽。
   本连接器不会自动建槽、删槽、修改 HBA 或数据库参数。
4. Flink TaskManager 必须能访问配置的 CMS 入口及 CMS 返回的租户节点地址。
5. 源并行度设为 1，开启 checkpoint。初次启动从复制槽可用位置读取，不能把它当成“仅采集启动后数据”。

没有全量快照与增量衔接；`scan.startup.mode` 仅接受 `incremental`。
本实现要求服务端提供按事务排列的 BEGIN、非批量 JSON DML、COMMIT；未知消息或重叠事务使任务失败，
由 Flink 按 checkpoint 恢复，不会回退到 SQL 解码。

## Flink SQL

完整样例见 [`examples/hcs.sql`](examples/hcs.sql)。核心配置：

```sql
CREATE TABLE hcs_changes (event STRING) WITH (
  'connector' = 'gaussdb-cdc',
  'hcs.cms-endpoints' = 'cms-a.example.com:8000,cms-b.example.com:8001',
  'database' = 'your_pdb',
  'username' = 'your_database_user',
  'password' = 'replace_locally',
  'hcs.tenant-name' = 'your_tenant',
  'hcs.tenant-password' = 'replace_locally',
  'slot.name' = 'your_precreated_dedicated_slot',
  'schema-name' = 'public',
  'table-name' = 'cdc_probe',
  'scan.startup.mode' = 'incremental',
  'output.format' = 'json',
  'sslmode' = 'require'
);
```

`hcs.cms-endpoints` 可填一个或多个入口，每项显式写端口，逗号分隔；支持域名、IPv4、
`[IPv6]:port` 和不同端口。这里填写 CMS 入口，不填写普通 CN 的 HA 复制端口。
节点选择及租户节点发现由原厂驱动完成。不需要为每个 CMS 建一个 Flink Source。
连接器通过 `pg_get_replication_slots()` 在普通租户 SQL 连接上检查槽，再使用
`com.huawei.opengauss.jdbc.Driver` / `jdbc:opengauss://` 建立独立复制连接。
复制连接配置 `allowConnThreadPool=false`、`replication=database`、
`preferQueryMode=simple` 和 `assumeMinServerVersion=9.4`，由其 `getReplicationAPI()`
启动多写逻辑流。内网单独验证这一路径时，错误从 9049 变为 HBA 复制连接拒绝，
表明已到达复制认证阶段；后续 HBA 调整引发实例重启，目标 PDB 尚未恢复，
因此此版本仍未完成真实复制流启动及增量验收。
参见[驱动证据、诊断入口及内网追问](docs/HCS_START_REPLICATION_9049.md)。
CMS 端口按实际部署填写，例如内网部署使用的 `13003`；示例端口不是固定要求。

`schema-name` 和 `table-name` 按 Java 正则整串匹配。多表 JSON 输出中包含 `source.schema/table`。
`output.format=json` 使用官方 `JsonDebeziumDeserializationSchema`，输出一列 STRING；
`output.format=row`（默认）使用官方 `RowDataDebeziumDeserializeSchema`，输出 INSERT、UPDATE_BEFORE、
UPDATE_AFTER、DELETE。行模式要求 WAL 包含声明字段的完整前后镜像；缺失字段报错，不补造 NULL。
请与管理员确认 REPLICA IDENTITY FULL 及解码配置；异构表采集优先使用 JSON 模式。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `hcs.cms-endpoints` | 必填 | CMS `host:port` 列表 |
| `database` / `username` / `password` | 必填 | 目标租户中的数据库与账号 |
| `hcs.tenant-name` / `hcs.tenant-password` | 必填 | 租户认证信息，与数据库密码分开 |
| `slot.name` | 必填 | 已存在的专用复制槽 |
| `table-name` | 必填 | 表名正则 |
| `schema-name` | `public` | Schema 正则 |
| `sslmode` | `require` | 507 JDBC SSL 模式 |
| `scan.startup.mode` | `incremental` | 仅增量 |
| `output.format` | `row` | `row` 或 `json` |
| `server-time-zone` | `UTC` | 官方行反序列化器使用的时区 |
| `connect.timeout.seconds` | `15` | JDBC 单次连接超时，不是多入口总超时 |
| `poll.interval.ms` | `100` | 无变更时轮询间隔 |
| `batch.size` | `1000` | 读取目标批量；不会拆开尚未提交的事务 |

## DataStream 和内网验证入口

```java
GaussDBConfig config = new GaussDBConfig(options); // Map<String, String>
env.addSource(new GaussDBSource<>(config, new JsonDebeziumDeserializationSchema()))
    .uid("gaussdb-hcs-cdc23").setParallelism(1).print();
```

可直接提交内置测试作业：

```bash
cp examples/hcs.example.properties /secure/path/hcs.properties
# 在本地编辑连接信息，避免提交真实凭据。
chmod 600 /secure/path/hcs.properties
flink run -c com.ververica.cdc.connectors.gaussdb.examples.HcsSmokeJob \
  target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar /secure/path/hcs.properties
```

该作业并行度为 1，每 10 秒 checkpoint，输出 Debezium JSON。属性文件由提交进程读取。
作业不写数据库业务表，但成功 checkpoint 会推进复制槽；必须使用专用测试槽。
正式恢复测试请在 Flink 配置可靠的 checkpoint 存储，并保留作业 UID，使用 `flink run -s <savepoint>`
恢复。打印输出只用于观察，不能据此认定端到端 exactly-once；下游也必须具备相应语义。

## 框架接入与位点语义

```text
GaussDBSource extends CDC 2.3 DebeziumSourceFunction
  → 官方 Debezium Embedded Engine
  → GaussDBConnector / GaussDBSourceTask
  → 507 TenantConnection / MultiPrimaryReplicationStream
```

底层读取完整提交事务后，构造一个内部批事件。`TransactionDeserializer` 把批事件展开成标准
Debezium 行记录，交给官方反序列化器。官方 fetcher 在同一 checkpoint 锁内输出整批行并更新 offset，
避免“已经记录事务提交位点，但事务内还有行没有输出”。空提交及过滤掉全部业务行的批次也可推进安全位点。

checkpoint 中使用字符串 `gaussdb_position` 保存完整 `CSN,WAL-ID,LSN`，不压成单个 long。
只有官方 CDC 的 checkpoint 完成回调才通过 `SourceTask.commitRecord` 确认服务器位点。
普通位点及其他租户/数据库/槽的状态会被拒绝。旧独立连接器的 checkpoint 与本仓库状态格式不兼容；
不要直接用旧作业 savepoint 启动此版本。

## 测试与验收

```bash
mvn verify
scripts/verify-matrix.sh
```

版本测试记录见 [`VALIDATION.md`](VALIDATION.md)。测试覆盖真实 CDC 2.3 引擎的 checkpoint/恢复、
真实 507 驱动的三段位点及 ACK 报文、事务边界、多 CMS 参数传递、SQL 工厂和官方反序列化。
协议测试使用合成复制报文，框架测试使用受控复制流，没有模拟成“已完成真实 HCS 验收”。

供内网 AI 或产品工程师补充开发依据的文档：
[`透明多写 CDC 开发背景与问题`](docs/HCS_TRANSPARENT_MULTIWRITE_DEVELOPMENT_QUESTIONS.md)。
包含当前实现假设、已反馈的错误，以及协议、位点、恢复和后续扩展需要明确的问题，可独立下载后回答。

内网需继续验证：
- 两个及以上写节点产生 INSERT / UPDATE / DELETE，核对漏数、重复及事务顺序。
- 回滚事务、长事务、空提交、过滤表、较大事务。
- checkpoint 成功后故障恢复，核对源位点和下游最终结果。
- 首个 CMS 不可用、CMS 切换、实际数据节点切换。
- 目标版本的日志格式、数据类型、复制槽保留和权限要求。

## 代码来源

这是非官方 GaussDB 扩展，不属于 Apache Flink CDC 已发布的 GaussDB 官方连接器。
WAL 解析和 507 适配辅助类来自 `jarrenL/gaussdb-flink-connector-jdbc` 的本地实验性适配，
保留 Apache-2.0 许可声明。框架接入代码位于本独立仓库；官方 CDC 2.3 以 Maven 依赖引入，
没有修改其引擎或 checkpoint 实现。

参考：[官方兼容表](https://github.com/apache/flink-cdc#flink-version-compatibility)、
[官方 release-2.3 源码](https://github.com/apache/flink-cdc/tree/release-2.3)。
