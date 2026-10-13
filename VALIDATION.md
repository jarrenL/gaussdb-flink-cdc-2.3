# 验证记录

日期：2026-10-13。JDK 11，Maven，507 CloudNative JDBC。

本仓库使用官方 `com.ververica:flink-connector-debezium:2.3.0`；没有修改 CDC 2.3 框架源码。

| Flink 引擎 | 编译 / 打包 | 自动测试 | 失败 / 错误 / 跳过 |
| --- | --- | --- | --- |
| 1.13.6 | 通过 | 107 | 0 / 0 / 0 |
| 1.14.6 | 通过 | 107 | 0 / 0 / 0 |
| 1.15.4 | 通过 | 107 | 0 / 0 / 0 |
| 1.16.3 | 通过 | 107 | 0 / 0 / 0 |

执行命令：`scripts/verify-matrix.sh`。每个版本均运行全部测试，最后保留 1.16.3 构建结果。

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
CMS 与数据节点切换。当前不支持全量快照、自动创建槽或 DDL schema evolution。
