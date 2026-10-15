# HCS 首次全量与增量衔接

当前 `scan.startup.mode=incremental` 只读取复制槽保留的增量。不能简单在它前面增加
`SELECT *`：全量查询期间仍有写入，若没有与该查询一致的复制流边界，就可能漏掉写入，
或先发全量新值再回放边界前的旧变更，使下游数据回退。

## 复制槽与全量的顺序

需要在**全量查询开始前**让目标 PDB 的专用逻辑复制槽存在并保留 WAL。之后取得同一
数据库一致性快照和与它对应的复制边界，在该快照下扫描表，最后从边界继续读取变更。
新建槽的 SQL 及插件见[实例前置条件](HCS_INSTANCE_PREREQUISITES.md)。

如果先开始全量、后来才创建槽，创建槽以前已提交的变更没有可供连接器回放的日志；
不能靠 `0/0,0/0,0/0` 或当前槽位点补出这段历史。此时应重新做一次全量，且在
重新扫描**之前**创建槽。若槽在全量完成后才创建，不能直接宣称无缝衔接。

## 尚须在目标 HCS 实例确认的边界

本地产品文档有 `pg_export_snapshot_and_csn()` 的说明，称其返回快照标识和 CSN，
但未证明它在 HCS 507 透明多写 PDB 可用，也未说明其 CSN 和三段复制位点的
等值边界、跨写节点顺序以及快照事务崩溃后的恢复规则。不能用单个 CN/DN 的 WAL LSN
代替全局边界。还需确认快照事务能否跨多个扫描 SQL 保持同一个一致视图；文档的
`pg_export_snapshot()` 条目同时提示部分 GaussDB 部署不支持所需的隔离级别。

只读探针可以在内网执行，不会建槽、删槽、启动复制流、发送 ACK 或读业务表：

```bash
java -cp target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar \
  com.ververica.cdc.connectors.gaussdb.examples.HcsSnapshotBoundaryDiagnostics \
  /secure/path/hcs.properties
```

它会显示现有槽的实际列、实例是否接受 `REPEATABLE READ`、
`pg_export_snapshot_and_csn()` 的实际返回值及事务隔离级别。如果函数或隔离级别
不可用，保留完整 SQL 错误信息，并向逻辑复制工程师确认替代方案。

在线首次全量只有在边界和恢复语义经产品方及真机验证后才能开放。实现还须覆盖
全量中途故障、checkpoint/savepoint 恢复、扫描期间 INSERT/UPDATE/DELETE、
大表分批和复制槽 WAL 保留压力；现阶段不把已有增量模式伪装成全量模式。
