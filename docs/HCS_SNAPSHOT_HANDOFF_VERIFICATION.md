# 透明多写在线全量衔接验证

`HcsSnapshotHandoffVerification` 是一次性的**集成实验**，用于判断 HCS 507 PDB 是否能把
JDBC 一致性快照和原厂三段复制流可靠地衔接。它不会改变现有连接器的 `incremental` 限制，
也不能凭单次通过就宣称故障恢复和多节点切换已经得到产品级保证。

## 准备与运行

在**独立测试 PDB** 中运行。账号须能建表、建/删逻辑复制槽和启动复制流；实例须已按
[前置条件](HCS_INSTANCE_PREREQUISITES.md)开启 `wal_level=logical`、复制权限和 HBA。
测试会写入并删除唯一命名的测试表，创建并删除一个全新的 `parallel_json_decoding` 复制槽。
如果这个 PDB 只允许一个槽且已有业务槽，就必须使用没有业务槽的隔离测试 PDB。
程序拒绝使用 `slot.name` 所指的既有槽，也不会向测试槽发送 checkpoint ACK。

将 [示例配置](../examples/hcs-handoff-probe.example.properties)复制到仓库外的私有路径，
填写真实连接信息和一个**尚不存在**的 `probe.slot-name`。然后运行：

```bash
mvn -q -DskipTests package
java -cp target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar \
  com.ververica.cdc.connectors.gaussdb.examples.HcsSnapshotHandoffVerification \
  /secure/path/hcs-handoff-probe.properties
```

程序最多等待约 90 秒读取两轮复制流；输出每个 `CASE ... PASS/FAIL` 及最终 `VERDICT`。
若进程被强制终止而未执行清理，请只针对输出的唯一测试表及 `probe.slot-name` 手动清理。
不要删除或复用业务槽。配置和日志可能包含实例信息，分享时先脱敏。

## 一次运行覆盖的边界

1. 建槽发生在全量快照前；记录建槽返回的 `xlog_position`，但不把它当成三段位点。
2. 在长事务尚未提交时导出 `pg_export_snapshot_and_csn()`；检查快照排除未提交行。
3. 在另一 JDBC 会话尝试 `SET TRANSACTION SNAPSHOT`，检查两个扫描会话是否看到同一批行。
4. 快照保持开启期间，依次提交 INSERT、UPDATE、DELETE、`NULL` 更新、中文 INSERT，
   回滚一次 INSERT，再提交快照前就开始的长事务；检查旧快照稳定、新会话看到新状态。
5. 从临时槽起点读取原厂复制流，检查九条预期变更、回滚未输出、中文无乱码、每条
   记录具有三段位点，并比较事务 CSN 与快照 CSN：快照中已有的三个初始 INSERT 应在
   边界之前，其余提交应在边界之后。
6. 在内存中将边界后的 WAL 变更应用到快照结果，检查结果与数据库当前值一致。
7. 关闭流后用第一条快照后提交的三段位点重新启动，检查 `withStartPosition()`
   返回的记录严格晚于该位点。这验证**正常重连**的边界，不等于 Flink checkpoint
   或节点故障切换验证。

若快照函数、位点比较或增量事件任一核心环节失败，输出 `NOT_PROVEN`；
跨会话快照导入失败时最多输出 `PASS_SINGLE_READER_EXPERIMENT`，表示单会话实验通过，
并行扫描尚不可用。不能仅凭 SQL 快照成功就启用在线全量。如果全部通过，仍需追加 Flink checkpoint
中断恢复、跨 CMS/写节点切换、DDL 和大表分块测试，才能考虑产品化。
