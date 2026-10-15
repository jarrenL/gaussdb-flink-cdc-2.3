# 给内网 AI：GaussDB HCS 透明多写 CDC 开发背景与问题

日期：2026-10-14。版本：第 1 版。

> 此问卷的连接代码摘录固定在旧提交 `a55d37c`，用于保留当时的待确认问题。
> 当前 `fc59e38` 已改为 `jdbc:opengauss://` 的独立复制连接，并设置
> `allowConnThreadPool=false`、`replication=database`、`preferQueryMode=simple`、
> `assumeMinServerVersion=9.4`。后续提供的 Kernel 507.1.0 开发指南已确认：
> 显式 `decode-style=j` 覆盖插件默认格式，`sending-batch` 默认 0，
> `output-order` 默认 0 且只能用于 LSN 序槽。当前目标槽的 `confirmed_csn`
> 尚未核对；以下旧代码摘录不能作为现行实现说明。

## 任务说明（可直接作为给内网 AI 的提示词）

我们正在开发基于官方 Flink CDC 2.3 采集框架的 GaussDB HCS 透明多写连接器。
请结合你能访问的产品文档、配套 507 JDBC 源码或示例、内部知识及已有测试结果，
回答下列问题，帮助外网开发人员确定协议实现、恢复策略和测试方案。

这不是要求你确认现有实现正确。请主动指出错误假设、遗漏的必要步骤，以及更合适的实现方式。
如果有原厂 CDC 接入示例、已有适配模块或推荐设计，请优先介绍，避免我们重复推断协议。
没有依据的内容请标为“未知”或“建议验证”，可以提出有理由的设计建议，但不要写成产品保证。
若无法访问 GitHub，本文提供的实现摘要足够作为回答起点，不必等待获取整个代码库。

请保持问题编号，区分“文档规定 / 驱动实现 / 真机观察 / 推断或建议”。文档依据尽量注明
产品版本、章节及短小原文；源码依据注明驱动版本、类和方法；实测依据注明部署形态、步骤与结果。
不同版本行为不同时，请分别说明。请提供脱敏信息，不返回密码、令牌、真实业务数据或实际内网地址。
涉及建槽、业务写入、参数调整和故障注入的内容，先给测试方案及预期，不要求直接操作生产环境。

时间有限时，优先回答 **Q03、Q05、Q06、Q07、Q08、Q09、Q10、Q11、Q15**；其他问题可以分批补充。

## 一、项目背景与当前状态

- 目标文档：`GaussDB V2.0-26.861.1 产品文档（for 华为云 Stack 8.6.1）01`。
  请确认它与内网实际服务端、补丁和透明多写部署形态是否对应。
- 独立公开仓库：<https://github.com/jarrenL/gaussdb-flink-cdc-2.3>。
- 本文对应代码基线：提交 `a55d37cc875aa7291cfd608261a82b240222c37e`，分支 `codex/hcs-cdc23`。
  [固定版本源码](https://github.com/jarrenL/gaussdb-flink-cdc-2.3/tree/a55d37cc875aa7291cfd608261a82b240222c37e)。
- 官方框架依赖：`com.ververica:flink-connector-debezium:2.3.0`，使用其
  `DebeziumSourceFunction`、嵌入式 Debezium 引擎、反序列化和 checkpoint/offset 机制。
- 当前使用的驱动本地坐标：`com.huaweicloud.gaussdb:gaussdbjdbc:507.0.0.b071-cloudnative-jdk8`。
  这是项目使用的 Maven 标识，不能单凭它认定与目标服务端配套。
  JAR SHA-256：`68391de275ff60c4a747d6e0d39df1259439d8fde8bf2c4d2b087790cb6896c2`。
- 当前只做预建槽的增量采集、单 Source 并行度，不支持全量快照、自动建槽或 DDL schema evolution。
- 四组本地构建（Flink 1.13.6 / 1.14.6 / 1.15.4 / 1.16.3）每组 111 项测试通过。
  其中协议测试使用真实 507 驱动类和合成报文，不能当作透明多写真机采集通过。
- 内网已有透明多写测试环境，正在联调。普通分布式实例与透明多写实例不能只凭“507”版本号等同。
  公有云普通分布式实例的历史测试结论也不作为本目标形态的依据。

### 已收到的两项内网反馈

1. 查询 `pg_replication_slots` 视图报错：

   ```text
   Relation "pg_replication_slots" does not exist on datanode.
   ```

   用户反馈以下函数查询已在目标环境验证可用，代码已改用它：

   ```sql
   SELECT 1 FROM pg_get_replication_slots() WHERE slot_name = ?;
   ```

2. 带 `replication=database` 重新连接 CMS 入口时报错：

   ```text
   Replication should connect port not in thread_pool.
   ```

   用户反馈该环境 CMS 入口为 13003，内部转发到 thread_pool 端口。代码已改为直接使用
   已有租户连接的 `getReplicationAPI()`，由原厂驱动路由复制流量。此修复已通过客户端回归，
   修复后在目标实例上的启动、持续采集结果尚未反馈。13003 不被假定为所有部署的固定端口。

### 原始实现摘要（a55d37c 基线，非现行代码）

连接和流初始化大致如下（省略反射、校验和异常处理）：

```java
// CMS 列表来自用户配置，每个地址都有显式端口。
String url = "jdbc:gaussdb://cms-a.example.com:13003,cms-b.example.com:13003/pdb_name";
Properties props = new Properties();
props.setProperty("user", "database_user");
props.setProperty("password", "placeholder");
props.setProperty("connectMode", "auth");
props.setProperty("tenantName", "tenant_name");
props.setProperty("tenantPassword", "placeholder");
props.setProperty("sslmode", "require");
props.setProperty("connectTimeout", "15");
Connection connection = DriverManager.getConnection(url, props);
// 预期是 TenantConnection，通过 PGConnection 接口调用 getReplicationAPI()。
// 使用原连接，不在 CMS URL 上另加 replication=database。
```

- 从 `connection.getReplicationAPI().replicationStream().logical()` 获取 builder；
  HCS 模式要求实际为 `MultiPrimaryLogicalStreamBuilder`，`start()` 返回 `MultiPrimaryReplicationStream`。
- 指定已有 `slotName`，通过 `LogSequenceNumber.valueOf(position)` 与 `withStartPosition(...)` 传入恢复位点。
- 当前发送的选项：`parallel-decode-num=2`、`decode-style=j`、`include-xids=1`、`include-timestamp=1`。
  **没有发送** `sending-batch` 或 `output-order`；代码希望得到非批量、事务有序的数据，但还需确认实际默认行为。
- 通过 `readPending()` 读取 ByteBuffer，紧接着用 `getLastReceiveLSN().asString()` 获取对应三段位点。
- HCS 解析器目前把每个非空 ByteBuffer 当作一条完整消息：文本 BEGIN、单个 JSON DML 或文本 COMMIT。
  它一次只缓存一个事务，不支持事务交错；COMMIT 时把该事务全部行的位点统一设为该 COMMIT 的接收位点。
  空提交也可推进位点。未知消息、事务交错、DML 缺少表身份会使任务失败。
- 初次启动没有 checkpoint 时传入 `0/0,0/0,0/0`。恢复时传入完整的 `CSN,WAL-ID,LSN` 字符串。
  位点格式允许每段为十六进制 `高32位/低32位`；排序直接委托 507 驱动 `LogSequenceNumber.compareTo()`。
- 代码按完整事务输出，跳过提交位点小于或等于已恢复位点的事务；这一过滤依赖位点可比较且足以唯一标识进度。
- checkpoint 保存完整三段位点。仅 checkpoint 完成后设置 `setFlushedLSN(position)`，
  再调用 `forceUpdateStatus()`；当前代码没有调用 `setAppliedLSN()`。
  读取期间也调用 `forceUpdateStatus()`，但不主动提前推进 flushed。
- HCS 初始化或读取失败不回退 SQL 解码，依赖 Flink 从 checkpoint 重启。
  WAL 对象关闭复制流，采集任务关闭其拥有的租户连接。
- 当前代码如果发现 `replicationStream.isClosed()` 为 true，会返回空批；SourceTask 尚未将这一状态
  单独转换为重启异常。请结合 Q15 判断是否会造成任务运行但不再采集。

主要文件：`GaussDBConfig.java`、`internal/GaussDBSourceTask.java`、
`internal/wal/WalReplicationStream.java`、`internal/wal/ReplicationPosition.java`。

## 二、需要内网知识帮助确定的开发问题

### A. 产品模型、环境识别与连接

**Q01【P0】怎样可靠识别透明多写能力及配套版本？**

请给出服务端版本/补丁、部署形态、功能开关、驱动版本的对应关系，以及推荐的只读识别方式。
哪些 SQL、系统函数、配置或管理面信息能形成可靠判断？函数存在、连接类为 TenantConnection、
支持 PDB 各自是否只是必要条件或线索？不能用“507”这一共同版本号证明什么？

**Q02【P1】透明多写架构如何影响 CDC 的采集范围？**

请解释 CMS、租户、PDB、写节点、日志节点/日志服务（若实际存在）之间的关系，按该产品的正式术语回答。
一个槽、一条 MultiPrimaryReplicationStream 能覆盖一个 PDB 的全部写节点吗？跨 PDB、跨租户是否要分开建槽？
多节点写入后的日志由哪层汇总、排序？是否存在只能采集部分节点但仍成功连接的情况？

**Q03【P0】原厂推荐的 Java 连接与复制初始化代码是什么？**

请提供适用于该部署和驱动的最小完整示例：CMS URL、必填属性、租户连接、查槽、创建 builder、启动流、读取和关闭。
直接复用 TenantConnection.getReplicationAPI() 是否是正式推荐用法？驱动何时建立底层复制连接、选哪个端口？
是否应显式配置 replication、connectMode、targetServerType 或其他属性？请纠正我们目前的配置。
一个租户连接上开流之后能否继续执行 SQL？流与连接的线程安全、取消和关闭顺序有什么约束？

**Q04【P1】多个 CMS 地址、租户发现与认证分别怎么工作？**

多个 CMS 是仅用于初始连接重试，还是支持运行中切换？驱动对入口顺序、超时、缓存的处理是什么？
TaskManager 除 CMS 外必须可达哪些返回地址和端口？TLS 校验与证书主机名如何匹配？
租户认证、数据库账号认证、内部复制认证各由谁负责？此前普通分布式场景遇到的 trust/gss 要求，
是否与本部署有关，若有关有哪些正式配置步骤？请不要直接套用其他部署的 HBA 方案。

### B. 槽、选项、数据格式与事务边界

**Q05【P0】透明多写复制槽应如何创建、查询和恢复使用？**

请给出确切函数签名、最小权限、调用入口、推荐插件和脱敏结果示例。
`pg_get_replication_slots()` 的结果范围是当前 PDB、租户还是全实例？仅 slot_name 是否足够定位？
同名槽是否可能出现？槽是否跨节点共享、切换后保留、允许多订阅者？
如何查询 active、消费确认进度、最早可恢复位置和 WAL 保留情况？禁止消费/推进槽的诊断查询有哪些？

**Q06【P0】目标槽的输出顺序与实际解码行为是什么？**

507.1 开发指南已给出 `sending-batch=0`、`output-order=0` 的默认值，且说明
`decode-style=j` 会覆盖 `parallel_binary_decoding` 插件的默认二进制格式。
请核对目标槽的 `confirmed_csn`、LSN/CSN 序属性及其与 `output-order` 的组合，
并用实际报文验证 `parallel-decode-num=2`、`decode-style=j`、逐条发送、
`include-xids=1`、`include-timestamp=1` 的输出和跨节点事务顺序。
若该槽为 CSN 序，请说明连接器是否必须显式发送 `output-order=1`，
以及这对三段位点比较、checkpoint 恢复和 ACK 的影响。

**Q07【P0】readPending() 返回的数据到底如何分帧？**

一个 ByteBuffer 是否恰好对应一个 BEGIN、DML 或 COMMIT？是否可能包含多条消息、半条消息、长度头、
二进制前缀、换行分隔或其他包装？这是驱动保证还是特定参数下的表现？
请给出同一个事务的原始消息序列，包括每次 readPending 的字节数、十六进制前缀、UTF-8 内容和接收位点。
至少包含 INSERT、UPDATE、DELETE。如有协议定义，请给出长度、字节序、编码和边界规则。
请把真实报文与为了说明而构造的示例区分开。

**Q08【P0】事务、跨节点顺序和 CSN 提供哪些保证？**

是否可能 BEGIN A、BEGIN B、DML A、DML B 交错？单个跨写节点事务是一个完整事务还是多个片段？
并行解码是否改变输出顺序？CSN 是开始时还是提交时确定，BEGIN/COMMIT/三段位点里的 CSN 是否同义？
是否会输出回滚、保存点回滚、预备事务/两阶段提交、心跳或控制消息？
请列出消费者必须识别的全部消息类型，以及推荐的事务重组和提交条件。

### C. 三段位点、恢复与确认（直接关系到漏数风险）

**Q09【P0】CSN、WAL ID、LSN 的精确定义与比较规则是什么？**

各段代表全局还是节点局部的进度？WAL ID 是节点编号、日志文件编号、流代次还是其他标识？
能否对不同写节点、日志轮转、节点切换前后的三段位点建立全序？相同 CSN 下另外两段如何比较？
当前 507 LogSequenceNumber.compareTo() 是否就是服务器恢复/去重所需的比较规则？
请给出至少三组非平凡比较例子，覆盖相同 CSN、WAL ID 变化和故障切换；如需额外 epoch 或节点集合，请明确说明。

**Q10【P0】应该保存哪个位点，恢复边界是否包含已提交事务？**

`getLastReceiveLSN()` 是最近返回的业务消息位置、驱动已收取位置，还是包含 keepalive 的服务器最新位置？
COMMIT 返回后立即取该值，能否作为整个事务唯一、安全的 checkpoint 位点？它可能指向后一条数据吗？
`withStartPosition(P)` 恢复时包含 P、排除 P，还是回到某个更早的事务边界？
多条事务可能共享同一三段位点吗？目前跳过 `commitPosition <= restoredPosition` 会不会漏数？
请说明 `0/0,0/0,0/0` 初始值的确切含义，以及位点过期、槽已推进超过 checkpoint 时的错误和处理。

**Q11【P0】反馈 ACK、流控与 checkpoint 应如何配合？**

setFlushedLSN、setAppliedLSN、接收位点和 forceUpdateStatus 分别影响服务端什么状态？
当前只设置 flushed、未设置 applied 是否正确？如需同时设置，应该在什么时点、传什么位置？
是否允许长时间继续读取但只在 Flink checkpoint 完成后推进 flushed/applied？服务端是否因此暂停发送，
甚至在一个大事务 COMMIT 发出之前就要求确认而造成死锁？驱动是否会自动确认尚未持久化的接收位置？
请给出推荐时序、keepalive/反馈周期及超时设置；明确哪些位置可以仅表示“收到”，哪些会允许回收日志。
还请说明初始状态和恢复后第一次反馈应该是什么，避免把恢复位置之前或之后的数据误确认。

**Q12【P1】一个三段位点是否足够表达全局恢复进度？**

我们在 checkpoint 中仅保存 tenant/database/slot 身份和一个三段位置，未保存写节点列表或每节点位点。
节点扩缩容、租户迁移、槽重建后，这个状态是否仍有效？是否需要校验集群 ID、PDB ID、槽创建标识或日志代次？
推荐如何识别“名字相同但已不是原数据源”的情况，避免错误复用 checkpoint？

### D. 数据语义和持续运行

**Q13【P1】DML 前后镜像和字段值的实际格式是什么？**

目前预期 JSON 字段为 table_name、op_type、columns_name/type/val、old_keys_name/type/val。
请说明准确字段、大小写、空值与缺列的区别，以及 UPDATE/DELETE 如何获取完整旧行。
目标版本是否支持并需要 REPLICA IDENTITY FULL，还是有不同设置？无主键表、主键更新、分区表如何输出？
请提供数值、布尔、中文/引号/换行、NULL、bytea、大字段、时间/时区、小数、数组/JSON 的脱敏样例。
同名异 schema 表、带点或引号的对象名如何表示？是否提供稳定表标识及类型 OID？

**Q14【P1】DDL、TRUNCATE、大事务与异常消息的建议策略是什么？**

DDL/TRUNCATE 会出现在复制流中吗？表结构变化如何通知消费者？未知消息应忽略、暂停还是失败？
大事务是否分段输出、是否有大小上限或流式事务协议？目前整事务放内存是否可行，何时必须落盘？
服务端过滤表与客户端过滤表分别有什么限制，过滤全部行的事务应怎样推进恢复位置？

**Q15【P0】断线和切换时，驱动与连接器各应负责什么？**

请分别说明 CMS 不可达、当前写节点重启、复制服务切换、租户迁移、网络闪断的行为。
驱动能透明恢复已打开的流吗，还是只负责连接阶段路由？恢复后会重放或改变位点语义吗？
哪些情况表现为异常、readPending 返回 null、isClosed=true 或长时间无数据？
请给出可识别的 SQLState/错误码、推荐重试范围、退避、超时，以及如何避免“任务仍运行但采集已停”。
只靠 Flink 从最近完成 checkpoint 重建整个连接和流，能否保证正确恢复？哪些错误必须人工处理？

**Q16【P1】开启逻辑复制、权限和资源保留的正式配置是什么？**

请列出必要服务端参数、推荐查询方式、作用节点/范围、是否动态生效及是否需要重启。
槽数量、解码线程、WAL 保留、断开后的槽存活、空闲超时、事务量上限如何设置和监控？
哪些监控指标可以区分没有业务写入、解码卡住、ACK 停滞、网络中断？

### E. 后续扩展与最有效的开发资料

**Q17【P2】将来如果做全量＋增量，官方一致性衔接方案是什么？**

有没有能覆盖多写节点的一致快照，以及与快照精确对应的三段位点？获取顺序、事务隔离、锁和权限要求是什么？
普通 PostgreSQL 导出快照的方案是否适用？如果尚不支持无锁一致衔接，请明确产品限制。
本轮不要求实现全量，只用于判断后续路线和避免错误接口设计。

**Q18【P1】有哪些可以直接使用的原厂参考资料或组件？**

请列出最相关的官方 Java 示例、透明多写逻辑复制说明、协议格式、已知问题、补丁说明和驱动兼容表。
如果官方/内部已有 CDC 组件，请说明它所指的 CDC 产品、是否明确支持本部署、接入接口和可复用方式。
它与 Apache Flink CDC 2.3 的关系是什么？不要把“数据库支持逻辑复制”直接当作“已有 Flink 官方连接器”。
如材料不能外传，请提供可分享的结论、API 签名和最小脱敏示例，无需上传完整内部文件。

**Q19【P1】最小真机测试应怎样安排，才最能指导下一步改代码？**

请优先设计一个不依赖 Flink 的 507 JDBC 最小探针，逐次打印消息边界、内容、接收/确认位点，
然后给出接入 Flink CDC 2.3 的测试顺序。至少覆盖多节点写同一 PDB、跨节点事务、回滚、空闲、
大事务、checkpoint 前后终止恢复、CMS 与复制节点切换、槽过期。
每项请写：验证的协议假设、准备条件、操作步骤、应保存的证据、期望结果和失败对实现的影响。
若时间只够三项，选哪三项最先做，并解释原因。

**Q20【开放问题】从原厂视角看，当前方案还漏了什么？**

请指出本文没有问到、但会导致漏数、重复、长时间停采、无法恢复或性能不可接受的问题。
可以直接建议替换当前某一层实现，并解释收益、代价及所需依据。

## 三、建议的回复格式

先给最重要的开发结论，再按问题编号展开。不要为了填满所有问题而猜测。

```text
环境与依据：
- 服务端产品/版本/补丁/部署形态：
- 驱动版本或 SHA-256：
- 可访问的文档/源码/测试条件：

最先需要改变的实现（按优先级，最多 5 项）：
1. 当前假设 → 正确做法 → 原因/证据 → 影响范围

Qxx：
- 结论：
- 适用版本/形态：
- 依据类型：文档规定 / 驱动实现 / 真机观察 / 推断或建议
- 依据位置与必要原文，或脱敏测试记录：
- 对当前连接器的具体开发建议：
- 尚不确定的部分及最小验证办法：

优先提供的附件或内容：
- 最小 JDBC 订阅示例（如可提供）
- 一组完整事务的逐次原始报文与位点
- 一次从明确位点恢复时的逐次报文
- 正确的槽创建/查询方式及解码选项组合
- 推荐开发顺序和前三项真机测试
```

返回给外网开发者时，请保留 Q 编号和证据出处。可按 P0、P1、P2 分多次回复，
无需先完成全部问题才能提供首批结果。
