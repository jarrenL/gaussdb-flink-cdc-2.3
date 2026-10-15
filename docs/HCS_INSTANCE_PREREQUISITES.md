# HCS 透明多写实例接入前置条件

以下是新建 `test01` 租户实例的操作反馈，供后续联调复用；不是连接器自动执行的步骤。
命令中的租户、用户名、槽名和 Flink 出口 IP 应按目标环境替换。管理员操作后需在
目标 PDB/数据库实测复制连接，不能仅凭普通 SQL 连接成功判定逻辑复制可用。

## 1. 将租户的 `wal_level` 设为 `logical`

新实例默认 `minimal`，不支持逻辑复制。本实例反馈应通过 `gs_tenant` 统一设置，
以同步两个节点；不要只修改单节点配置文件。修改后需要重启集群：

```bash
gs_tenant -t set_tenant_guc_config --tenant-name=test01 --config-type=GAUSSDB --configs='{"wal_level":"logical"}'
gs_tenant -t reload_tenant_guc_config --tenant-name=test01
gs_om -t stop && gs_om -t start
```

重启和目标 PDB 恢复后，在目标数据库执行 `SHOW wal_level;`，确认返回 `logical`。
重启应由实例管理员在维护窗口执行。

## 2. 赋予采集用户复制权限

由管理员创建具有 `REPLICATION` 权限的专用用户，或给已有用户授权。密码通过
安全的管理途径设置，不写入仓库或命令示例：

```sql
ALTER USER flink_cdc_test REPLICATION;
```

## 3. 增加逻辑复制 HBA 规则

默认 `host all all` 规则不能替代复制连接的 `host replication` 规则。本实例反馈
使用 `gs_tenant` 管理 HBA，执行 `reload` 即可生效，无须因此重启集群。例如仅放行
Flink TaskManager 的实际出口 IP：

```bash
gs_tenant -t set_tenant_guc_config --tenant-name=test01 --config-type=HBA --configs='["host replication all <flink_egress_ip>/32 sha256"]'
gs_tenant -t reload_tenant_guc_config --tenant-name=test01
```

本次实例操作反馈中使用过 `0.0.0.0/0`，即放行所有 IPv4 来源；正式联调应按实际
出口 IP 或网段收窄。HBA 放行后还需确认 Flink 所在网络可访问 CMS 及返回的租户节点。

## 4. 在目标数据库预建专用并行逻辑复制槽

当前连接器不会自动建槽；本实例反馈透明多写下 `mppdb_decoding` 插件不可用，应使用
`parallel_json_decoding` 或 `parallel_binary_decoding`。优先使用与当前 JSON 解码模式
直接匹配的 `parallel_json_decoding`：

```sql
SELECT * FROM pg_create_logical_replication_slot('flink_cdc_slot', 'parallel_json_decoding');
```

随后在目标数据库核对槽：

```sql
SELECT slot_name, plugin, slot_type
FROM pg_get_replication_slots()
WHERE slot_name = 'flink_cdc_slot';
```

透明多写章节及官方 JDBC 示例没有要求 `output-order`；新实例的独立 JDBC 测试
也在未设置该选项时读到了增量。当前连接器只检查槽是否存在，不查询
`confirm_csn`，也不设置 `output-order`。槽应由一个采集任务独占。
目标实例还须实测 Flink 流启动、数据输出和断线恢复。

中文数据联调时，也应在目标会话查看 `SHOW server_encoding;`、
`SHOW client_encoding;`，并对照源表 `SELECT` 与原始 `readPending()` 报文字节。
当前实测实例的两个编码均为 `SQL_ASCII`，而复制报文中的中文是 UTF-8 字节；
连接器按 UTF-8 严格解析该报文。若源表内容或原始报文本身已乱码，
连接器无法从字符串内容反推出原文。Windows GBK 控制台也可能误显示正确的 Java 字符串，
排查时应比较字节的十六进制值或 Unicode 码点。
