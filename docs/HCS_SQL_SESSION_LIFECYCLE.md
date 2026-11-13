# SQL 会话生命周期与全量后的 TLS 中断

## 反馈与证据边界

内网反馈全量后进入增量，在高并发下偶发 `sqlState=08006`、
`SSL error: sslv3 alert unexpected message`；任务恢复后，该次核对未发现数据丢失。
此前版本在持续增量阶段仍持有已不再使用的 SQL 连接，这一资源生命周期问题可以从代码
确认并修正。它是否触发该次 TLS 故障，尚未在目标 HCS 复现验证。

“SQL 空闲被清理，close_notify 经 CMS 串扰到另一条连接”仍是待验证假设。
TLS 的 `close_notify` 与 `unexpected_message` 是不同告警；后者不能单独证明前者发生，
也不能据两个连接属于同一租户就确认 SSL 会话串扰。需要驱动/服务端对应会话及 TLS 证据。

## 本次实现

Task 与 WalReplicationStream 的 SQL 引用使用如下生命周期：

```text
initial 首次：
  SQL 槽检查/准备 → native 独立连接启动
  → SQL 信号与扫描 → native 区间回填 → SQL 表定义复核/信号清理
  → stream.detachSqlConnection → SQL.close
  → 临时存储分批发布 → 持续 native 读流/反馈/完成 checkpoint 后 ACK

incremental 或已完成全量 checkpoint 恢复：
  SQL 启动检查 → native 独立连接启动 → detach + SQL.close → native 持续运行
```

initial 在首批输出前关闭 SQL，而不等整份数据发布结束，避免慢输出时仍闲置数分钟。
关闭后既不重建 native 复制流，也不更改 lastRead、LOW/HIGH、尾部事务或恢复位点。
SQL.close 失败会使任务报错，丢弃未交付的快照临时状态，不忽略异常继续宣称初始化成功；
Task 仍保留对失败连接的清理责任，stop 再尝试关闭。

WalReplicationStream 在 native 流成功初始化后才能解除 SQL 引用。解除引用后，辅助 SQL
入口明确报 SQL 会话已释放，要求另建管理连接；不会出现空指针，也不会把查询改发到 COPY。
普通非 HCS SQL 解码路径不调用 detach，保留其 SQL 连接与既有行为。

## 为什么无需将查询改到复制连接

对当前 HCS native 调用链的核查：

| SQL 调用 | 使用阶段/分支 | 持续 HCS 增量是否依赖 |
| --- | --- | --- |
| slotExists | initialize 启动前检查 | 否，初始化后不周期调用 |
| createSlot / getCurrentLsn | 普通模式启动或旧快照辅助入口 | 否；HCS 不靠本地 LSN 拼三段位点 |
| pg_logical_slot_peek_changes | 普通 SQL 解码 fallback | 否；HCS 禁止 fallback |
| advanceSlot | SQL fallback 确认进度 | 否；HCS ACK 使用 native setter/status |
| dropSlot / getCurrentLsnPosition | 管理辅助方法 | 否；Task 持续运行不调用 |
| readChanges / sendStatus / acknowledgeLsn | 已启动 native 流 | 使用原厂复制接口，不执行普通 SQL |

因此不需要在全量之后搬迁这几条 SQL。复制连接已由 START_REPLICATION 进入 COPY BOTH，
应保持复制协议状态；混入 SELECT 可能阻塞或破坏流。参考 PostgreSQL 的协议状态说明，
配套 507 驱动也通过 CopyDual 操作该流。

## 为什么未添加并发 SELECT 1

在定时反馈线程直接访问快照 SQL 连接，会与 poll 线程的游标、信号、rollback 或关闭操作
并发；若 SELECT 阻塞，还会拖延同一调度线程的复制状态反馈。给 SQL 保活再增加阶段门闩、
锁和超时也无法消除不必要的长生命周期连接。当前没有后续 SQL 需求，直接在最后一次操作
后释放更明确，且无需新增配置或每秒查询数据库。

原有每秒 native 状态反馈保留，仍只发送已有位点，不执行 SELECT、不设置 applied/flushed。
只有 completed Flink checkpoint 回调推进确认位置。

## 本地验证与内网复测

新增/增强验证：

- 扫描未结束时 SQL 未关闭；信号清理、detach、SQL.close 按顺序发生，首批发布前已关闭。
- incremental 与 completed initial checkpoint 恢复在启动后即释放 SQL，不重新扫描。
- 真实 507 MultiPrimaryReplicationStream 配合受控 CopyDual，在 SQL 释放/关闭后仍能读取
  完整事务、发送状态并 ACK 完整三段位点；复制连接没有 prepareStatement/createStatement。
- 释放后的管理 SQL 明确失败，不向 COPY 发查询，getCurrentLsn 不返回伪造 0/0。
- SQL.close 失败时不发布全量、不 ACK、清理私有磁盘状态，stop 保留重试清理机会。
- 原有全量尾部事务、持续增量、定时反馈和官方 CDC checkpoint/恢复测试继续执行。

上述使用本地受控连接/报文，不能模拟 CMS/TLS 的实际缺陷。最终矩阵和包校验见
[VALIDATION.md](../VALIDATION.md)。

内网请使用同一新版本先验证单次 initial 全流程：观察 SQL session released 日志，核对
该 SQL 会话已退出，而 native 会话持续存活；慢输出和增量空闲时间均覆盖此前 1–2 分钟
故障窗口，再做高并发与 checkpoint 恢复。核对主键状态、聚合及重复事件，不只数全量行。
若 SQL 会话已退出仍出现 TLS 告警，应继续追查实际关闭方和 TLS 状态，而非继续假定
“闲置 SQL 连接串扰”；不要改为在 COPY 连接上执行保活查询。

参考：

- [PostgreSQL COPY BOTH 协议状态](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY)：START_REPLICATION 至复制结束的状态边界，不替代目标 HCS 产品契约。
- [pgJDBC 对同一连接并发使用的说明](https://jdbc.postgresql.org/documentation/server-prepare/)：普通连接/Statement 不宜被多个线程并发操作。
- [TLS 1.3 告警定义](https://www.rfc-editor.org/rfc/rfc8446.html#section-6)：close_notify 与 unexpected_message 分别定义，不单凭错误字符串认定跨连接串扰。
