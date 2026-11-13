# DRS 与通用 Flink Source：组合方案与落盘合并

当前结论：可以组合，但组合本身不能补出数据库读取路径从未返回、且不在消费日志范围内的行。
当前交付仍为官方 CDC 2.3 上的通用 Source，首次全量默认关闭，没有目标 HCS 真机无丢失结论。
以下对 DRS 的描述依据用户提供的内部调用链，未审计完整 DRS 产品实现。

## 哪些情况可能丢失或重复

| 情况 | DRS 风格流程 | 当前 Source 实验流程 |
| --- | --- | --- |
| 起点前提交，但扫描路径仍看不到，之后不再修改 | 仅靠 per-table CSN 过滤无法补齐；需数据库读取契约或其他产品机制 | LOW/HIGH 也无法补齐；首次全量默认拒绝启动 |
| 扫描已包含区间内的 INSERT/UPDATE，日志再读一次 | 迁移 Applier 可配合全量/增量应用顺序与冲突处理修复；简单过滤不能保证通用聚合 | 发布前按主键合并，向下游只发布合并后的全量行 |
| 所需 WAL 未保留或事务不完整 | 需槽、relay 与任务恢复协议保证 | 先建槽并校验恢复位点；仍需验证建槽前开始的长事务及每条底层日志流 |
| 先 ACK 后保存可恢复状态 | 失败后可能不可重读；需产品恢复协议防护 | 仅 completed Flink checkpoint 才 ACK；落盘写入/提交不 ACK |
| 磁盘配额、超时、读取失败 | 取决于产品错误处理 | 明确失败，不跳过行；未完成全量重做，不是宣称继续成功 |

不能由这些条件反推“DRS 产品一定丢数据”，也不能从源码片段推导“任意部署都不丢”。
同样，当前 Source 在未确认的读取环境中强行打开实验开关，仍存在漏行风险。
复制槽 `confirmed_flush` / `confirmed_csn` 表示消费者确认进度，不能充当业务 SQL 的读屏障。

GTM 检查只能先排除不满足条件的模式；普通分布式产品文档中的 GTM-Free 判断包括
`enable_gtm_free=on` **或** `gtm_option=2`，不能把 off 单参数当成 HCS 全局读取能力认证。
目标 Tenant/DStore 的语句快照和实际路由契约仍待内核确认。

## 本次实现的组合

沿用此前根据 DRS 反馈完成的先建槽、专用槽检查、三段位点恢复校验、提交后确认协议，
保留通用 Source 在发布前消除重叠的要求；新增可落盘的主键合并存储。
它借鉴“读取、存储、发布分别管理进度”的思路，没有移植 DRS relay 或目标 Applier。

```text
先建立/验证专用复制槽，启动 native 流
    → LOW 的完整提交位点
    → JDBC 游标逐行写入临时磁盘 Map
    → HIGH，读取完整区间事务并在磁盘 Map 中合并
    → 核对表定义，分批输出最终快照
    → 最后一批才标记全量完成，随后持续输出增量
    → completed checkpoint 后 ACK 完整 (CSN, WAL_ID, LSN)
```

INSERT 覆盖、UPDATE 移除旧主键后保存完整新镜像、DELETE 删除；主键带有表身份。
不会把重复 INSERT 交给下游 COUNT，也不要求下游数据库反馈影响行数。
完整 before/after、键类型、表类型等既有限制见[全量说明](HCS_CDC23_INITIAL_SNAPSHOT.md)。
目前扫描期间不并行消费 WAL，靠槽保留，不能把本次落盘存储称为异步 relay log。

## 存储与资源边界

- 默认 `scan.snapshot.storage=disk`；可选 `memory` 保留原有 100000 行/约 64 MiB 上限。
  disk 模式不使用这两个整份内存上限，不把所有全量行留在 Java Map。
- 磁盘模式使用 MVStore-only 2.2.224，不引入 H2 SQL/JDBC 服务；打包时迁移 `org.h2`
  包名以减少与作业 H2 依赖冲突。基类是 Java 8 字节码，实际打包检查也在 Java 8 运行。
- 父目录 `scan.snapshot.spill-dir` 默认 `java.io.tmpdir`，须预先存在且可写。
  每次创建独立 `gaussdb-cdc-snapshot-*` 子目录，支持 POSIX 时权限为 0700。
  它保存业务明文数据，需按源数据要求配置本地磁盘与访问权限。
- 读缓存目标默认 16 MiB；每 1 MiB 编码修改或 1000 次修改提交文件。
  单行默认限制 16 MiB 估算/编码数据；输出默认每批 1000 行/约 1 MiB，单行不可拆分。
  缓存不是 JVM/RSS 硬限制，JDBC、native 大事务、反序列化和队列另有内存消耗。
- 默认磁盘配额 10 GiB，检查逻辑行编码总量及提交后的物理文件大小。元数据、键、旧页
  和文件更新开销会使物理大小高于行总量，检查前可能短暂超出；不承诺磁盘无上限。
  不足时失败，不降级成丢行或提前确认日志。
- 行编码显式保存列名、OID、NULL 与 Unicode，不使用 Java 对象反序列化；临时格式不作为
  用户可迁移/恢复格式。正常完成、异常、超时或正常取消清理本次拥有的目录，不删除父目录
  中其他文件。强杀/主机故障仍可能残留，需在任务停止后由管理员检查清理。

## checkpoint 与下一阶段

**磁盘 Map 是本次全量的临时合并状态，不是 checkpoint，也不是可恢复的 relay。**
本地文件提交不触发 ACK，不以“文件已写入”代替 Flink 的持久化 checkpoint 完成。
官方 CDC 2.3 fetcher 仍跨全量输出批次持有快照 checkpoint 锁，最后一批才保存完成状态。
未完成全量时失败重做整个全量；完成后从 checkpoint 的完整三段位点恢复增量。
非事务打印/追加 sink 可能保留失败前的部分输出，端到端语义仍需 sink 配合。

后续若做真正的分块 checkpoint，需一起设计主键区间、表身份、每块 LOW/HIGH、日志覆盖、
块完成状态、恢复顺序和 checkpoint 后 ACK；不能只保存 JDBC 游标或复用本地文件。
若内核提供一致性快照+原子 CSN 或可靠读屏障，可进一步减少信号依赖；接口存在不等于
契约已经证明，见[只读快照导入探针](HCS_GENERIC_SOURCE_STRATEGY.md)。

内核契约与多节点/长事务真机验收完成之前，保留首次 initial 默认关闭的保护。
本次解决整份全量必须驻留内存的问题，未解决跨节点可见性、分块 checkpoint、无主键、
分区表、COPY 或运行中 DDL。

## 验证方式

常规 `mvn verify` 覆盖内存/磁盘相同变更序列的结果、主键变更、跨表、NULL、长 Unicode、
配额和超时失败、取消与目录清理；真实官方 CDC 框架测试使用磁盘存储验证跨批次 checkpoint 锁。
另有不连接数据库的打包后小堆检查：

```sh
/path/to/jdk8/bin/java -Xmx64m \
  -cp target/gaussdb-flink-cdc-2.3-0.1.0-SNAPSHOT-all.jar:target/test-classes \
  com.ververica.cdc.connectors.gaussdb.DiskSnapshotStorePackagedCheck
```

它写入并分批读回 50000 行，每行含 1024 个中文字符，检查内容、数量与临时文件清理。
实际结果及四个 Flink profile 记录在 [VALIDATION.md](../VALIDATION.md)。
这证明本地实现可在该最大堆下处理大于堆的全量数据，不证明目标数据库的读取可见性。

参考：[MVStore 官方说明](https://h2database.github.io/html/mvstore.html)、
[GTM 模式文档及适用范围](https://support.huaweicloud.com/intl/zh-cn/distributed-devg-v10-gaussdb/gaussdb-12-0742.html)。
