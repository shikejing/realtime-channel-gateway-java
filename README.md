# realtime-channel-gateway-java

面向客户端的实时通道网关。服务端与客户端之间维持一条持久双向连接（WebSocket，
仅本地回环），客户端在一条连接上订阅多个独立频道并接收服务端推送；上行只承载
订阅、退订、确认接收与心跳等控制消息。核心逻辑传输无关，测试使用进程内内存
通道与可控时钟，全部场景可在本地稳定复现。

## 构建与测试

```bash
mvn test        # 运行全部测试（内存通道 + 可控时钟，另含一个真实回环 WebSocket 端到端测试）
mvn spring-boot:run   # 本地启动，端点 ws://127.0.0.1:8080/ws
```

## 架构

```
产生方 → ChannelRegistry（归一化 + 追加日志，有界重放窗口）
            │  每频道一把锁：归一化 / 落日志 / 扇出同序
            ▼
       GatewayServer ── 每连接一个有界待发队列 + 独立发送循环（慢连接只影响自身）
            │                ▲
            ▼                │ 上行控制消息（Hello/Subscribe/Unsubscribe/Ack/Ping/Bye）
     ConnectionHandle（InMemory / WebSocket 两种传输）
```

- `gateway.protocol`：上下行消息模型（sealed 类型，JSON `type` 字段区分）。
- `gateway.channel`：`EventNormalizer`（按 eventId 去重、按 sourceSeq 在有限窗口内
  重排，窗口满时强制推进容忍永久空洞）与 `ChannelLog`（单调序号 + 有界重放窗口）。
- `gateway.session`：会话与保留窗口；`SessionStore` 负责创建、去重、恢复、过期。
- `gateway.transport`：传输抽象；`InMemoryTransport` 用于测试，
  `transport.websocket` 为真实回环传输。
- `gateway.spring`：Spring 装配与 `gateway.*` 配置项。

## 协议

上行（客户端 → 服务端）：`hello`（身份令牌；重连时携带 `sessionId`、`resumeToken`
与各频道 `lastAcked`）、`subscribe`、`unsubscribe`、`ack`、`ping`、`bye`。

下行（服务端 → 客户端）：`welcome`（`sessionId`、`resumeToken`、`resumed`）、
`subscribed`/`subscribeRejected`、`unsubscribed`、`event`、`resyncRequired`、
`pong`、`error`、`closed`。错误码与关闭原因均可区分（见 `ErrorCode` /
`CloseReason`）。

## 投递与重放语义

- **频道内有序**：同一频道的事件按产生顺序（归一化后）分配单调递增序号 `seq`，
  同一连接上按序投递；订阅/退订/重放与新事件扇出在同一把频道锁内串行，
  上行控制消息不会打乱任何频道的投递顺序。频道之间互不串扰、无顺序保证。
- **确认与重放**：客户端用 `ack(channel, seq)` 确认进度；断线重连时在 `hello`
  中携带各频道 `lastAcked`，服务端只重放 `(lastAcked, latest]` 区间——
  已确认的不重发，未确认的不丢失。
- **重放窗口有限**：每频道只保留 `replay-window-size` 条。`lastAcked` 早于窗口
  下界时，服务端下发 `resyncRequired(channel, earliestSeq)`，客户端需全量
  重新同步（例如重新拉取快照后用 `subscribe(channel, lastAcked)` 续传）。
- **去重与乱序**：产生方事件按 `eventId` 去重、按 `sourceSeq` 在
  `reorder-window` 内重排后才进入日志；底层重复/乱序不会传导给客户端。

## 会话与重连

- 连接建立必须完成 `hello` 鉴权（令牌 → 身份），否则连接以 `AUTH_FAILED` 关闭。
- 断线后会话进入 `DETACHED` 并在 `session-retention-ms` 保留窗口内可恢复：
  重连携带 `sessionId + resumeToken`（令牌常量时间比较，且身份主体必须一致，
  不可冒用）。窗口超时后会话过期，恢复请求得到 `SESSION_EXPIRED`，客户端应
  重新建立会话并全量同步。
- 同一身份重复登录或同一会话被另一条连接恢复时，旧连接以
  `SESSION_TAKEN_OVER` 关闭——同一客户端不会被重复计入，也不会留下多个
  同时有效的会话。

## 心跳与清理

- 服务端按 `heartbeat-interval-ms` 巡检：超过 `heartbeat-timeout-ms` 没有任何
  上行消息（`ping` 即可续命）的连接判定为半开，以 `HEARTBEAT_TIMEOUT` 回收。
- 客户端 `bye`、传输层断开、服务端停机走同一条清理路径（`onClosed` →
  脱离会话 → 释放连接资源）；停机时所有客户端收到 `closed(SERVER_SHUTDOWN)`，
  调度器与发送线程全部回收（`activeSenderCount()` 归零可观测）。

## 慢连接与背压

- 每条连接的待发缓冲上限为 `outbound-queue-capacity`，事件产生方只做非阻塞
  入队，单个慢连接不会阻塞其他连接或拖慢产生方。
- 缓冲超限按 `slow-consumer-policy` 处理：
  - `DISCONNECT`：断开该连接（`SLOW_CONSUMER`），会话进入保留窗口可重连恢复；
  - `DROP_AND_RESYNC`：清空积压并下发 `resyncRequired`，连接保持，进入可重新
    同步状态。
- 内存占用有界：重放窗口、乱序窗口、去重窗口、待发队列、会话保留窗口均为
  固定上界，不随连接数或运行时间无界增长。

## 鉴权

订阅、退订、确认都按连接身份逐频道鉴权（`Authorizer`）。越权操作被拒绝并返回
可区分原因（`FORBIDDEN`），且拒绝路径不触碰频道任何状态，无权访问频道的内容
（包括是否存在事件）不会泄露。

## 配置项（`gateway.*`）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `heartbeat-interval-ms` | 10000 | 心跳巡检周期 |
| `heartbeat-timeout-ms` | 30000 | 无上行消息的存活判定超时 |
| `session-retention-ms` | 60000 | 断线会话保留窗口 |
| `replay-window-size` | 1000 | 每频道重放窗口条数 |
| `reorder-window` | 64 | 事件乱序重排窗口 |
| `outbound-queue-capacity` | 256 | 每连接待发缓冲上限 |
| `slow-consumer-policy` | DISCONNECT | 慢连接策略：DISCONNECT / DROP_AND_RESYNC |
| `tokens.<token>` | — | 示例认证：令牌 → 身份主体 |
| `grants.<subject>` | — | 示例鉴权：身份 → 频道白名单 |

## 测试

`src/test/java/.../gateway` 下：

- `MultiChannelIsolationOrderTest`：多频道隔离与频道内顺序、退订隔离；
- `ResumeRedeliveryTest`：重连续传（不重发/不丢失）、令牌冒用、保留窗口过期、
  重放窗口超限、会话顶替；
- `SlowConsumerTest`：两种慢连接策略与“不影响其他连接”；
- `HeartbeatHalfOpenTest`：半开连接回收、活跃心跳保活、统一清理路径；
- `AuthorizationTest`：越权订阅/退订/确认、坏令牌、会话劫持防护；
- `NormalizationTest`：去重、乱序重排、永久空洞不卡死；
- `ConcurrencyStressTest`：大量连接并发订阅/退订/断连/发布；
- `ShutdownTest`：停机通知与资源回收；
- `LoopbackEndToEndTest`：真实 WebSocket 回环端到端。

测试统一使用 `InMemoryTransport`（内存通道）与 `TestTime`（可控时钟 +
手动调度器），不依赖真实时间与外部服务，可重复执行。
