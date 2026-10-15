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

随后在目标数据库核对槽及输出顺序：

```sql
SELECT slot_name, plugin, slot_type, confirmed_csn
FROM pg_get_replication_slots()
WHERE slot_name = 'flink_cdc_slot';
```

`confirmed_csn = 0` 是 LSN 序槽，符合当前连接器未设置 `output-order` 时的默认值 `0`；
非零是 CSN 序槽，需要 `output-order=1`，当前 HCS 连接器尚未提供该配置，不能直接
用于该槽的验收。槽应由一个采集任务独占。目标实例还须实测流启动、数据输出和断线恢复。
