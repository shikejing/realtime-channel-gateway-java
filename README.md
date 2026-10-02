# 实时通道网关（realtime-channel-gateway）

面向客户端的实时通道网关。服务端与客户端之间维持一条持久双向连接，
客户端在这条连接上订阅多个独立频道并接收服务端推送；上行只承载订阅、
退订、确认接收（ACK）和心跳等控制消息。整套链路只在本进程和本地回环内
运行，不依赖任何外部服务。

## 1. 架构概览

```
客户端 ──CONNECT/SUBSCRIBE/UNSUBSCRIBE/ACK/PING/RESYNC──┐
                                                         │  本地回环 / 内存通道
客户端 ──MESSAGE/CONNECTED/SUBSCRIBED/PONG/ERROR/CLOSE──┘
                                    │
                                    ▼
                            ┌──────────────┐
                            │   Gateway    │
                            │  (本进程内)  │
                            └──┬───────┬──┘
               频道日志(有限窗口) │       │ 会话(绑定身份)
                  ChannelRegistry │       │ SessionRegistry
                                  ▼       ▼
                            归一化 + 扇出   鉴权 / 心跳 / 保留 / 重连
```

核心模块（`com.github.shikejing.realtimechannelgatewayjava`）：

| 包 | 职责 |
|---|---|
| `core` | 网关主类、协议处理器、时钟、异常 |
| `channel` | 频道与有限窗口消息日志，事件归一化（去重 + 乱序重排） |
| `session` | 会话、订阅关系、有界待发缓冲、关闭原因 |
| `auth` | 身份校验（token）与逐频道鉴权 |
| `protocol` | 帧类型与长度前缀 JSON 编解码 |
| `transport.memory` | 内存通道（测试用，可控、可重复） |
| `transport.nio` | 本地回环 NIO 服务端（127.0.0.1） |
| `transport.loopback` | 本地回环客户端 |
| `config` | Spring 装配与配置项 |

线程模型：

- **事件产生方**调用 `publish`，仅写入频道日志并扇入各连接的有界待发缓冲，
  立即返回，不阻塞。
- **单条 drain 线程**负责把待发缓冲写入传输层；慢连接只影响自己的队列。
- **单条调度线程**负责心跳判定与会话保留窗口清理。
- 所有线程均为守护线程，停机时按既定顺序关闭，不遗留后台任务。

## 2. 投递与重放语义

### 2.1 频道内顺序

每条消息携带频道内单调递增序号 `seq`。服务端按 `seq` 顺序投递，
与事件到达顺序无关：

- **乱序到达**：先进入待缓冲，连续后再投递。例如先到 `seq=2` 再到 `seq=1`，
  客户端收到的顺序仍是 `1, 2`。
- **重复事件**：`eventId` 相同或 `seq` 已处理的事件直接丢弃，不重复投递。

### 2.2 多频道隔离

一条连接可同时订阅多个频道。各频道独立维护序号与确认位置，
频道之间互不串扰。上行控制消息（订阅、退订、ACK、心跳）不占用数据通道，
不打乱任何频道的投递顺序。

### 2.3 确认与重放

- 客户端通过 ACK 帧上行各频道已确认到的位置（`seq`）。
- 已确认的消息不会重复下发；未确认的消息在重连后重放。
- 重放只覆盖**可配置的有限窗口**（`gateway.channel-replay-window-messages`，
  默认 1000 条/频道）。窗口按消息条数保留最近的消息，超出窗口的确认点
  会收到明确的**重新同步结论**（`RESYNC_REQUIRED`，携带窗口最早/最新序号），
  客户端从窗口边界重新同步后继续投递。

### 2.4 重新同步（RESYNC）

客户端可随时上行 `RESYNC` 帧，携带各频道当前确认位置：

- 确认点在窗口内：补发未确认消息，回复 `RESYNC_OK`。
- 确认点超出窗口：回复 `RESYNC_REQUIRED` 及窗口边界，不投递缺失消息。

慢连接导致的投递暂停也通过 `RESYNC` 恢复（见第 5 节）。

## 3. 会话保留窗口

- 连接建立时绑定可校验身份（`CONNECT` 携带 token，服务端校验后返回身份）。
- 客户端断线后，会话及其订阅关系在**保留窗口**内保留
  （`gateway.session-retention-ms`，默认 30 秒）。
- 保留窗口内重连：携带各频道确认位置，服务端恢复会话、补发未确认消息，
  `CONNECTED.resumed=true`。
- 保留窗口超时：会话与订阅关系被清理。此后重连视为全新会话，
  `CONNECTED.resumed=false`，需重新订阅。
- **顶号处理**：同一身份已有活跃连接时，新连接顶掉旧连接，
  旧连接收到 `CLOSE(reason=replaced_by_new_connection)`，
  旧会话保留并由新连接恢复。同一客户端任何时刻只有一个有效会话。

## 4. 心跳与半开连接

- 客户端按 `gateway.heartbeat-interval-ms`（默认 5 秒）上行 `PING`，
  服务端回复 `PONG`。
- 服务端记录每条连接最近一次上行活动。若超过
  `heartbeat-interval-ms × heartbeat-miss-threshold`（默认 5×3=15 秒）
  未收到任何上行帧，判定连接半开，关闭并回复
  `CLOSE(reason=heartbeat_timeout)`，会话进入保留窗口。
- 客户端主动断开与服务端关闭走**同一条清理路径**：会话转入保留、
  订阅关系在窗口内保留、后台任务不遗留。

## 5. 慢连接与背压

- 每条连接的待发缓冲有上限（`gateway.connection-buffer-capacity`，
  默认 256 帧）。
- 缓冲超限时，该连接进入**可重新同步状态**：暂停向其投递消息，
  发送 `RESYNC_REQUIRED(reason=slow_consumer)`，不阻塞事件产生方，
  也不影响其他连接。
  - 在该连接恢复前，其他连接的投递、事件产生方的发布均正常进行。
  - 客户端通过 `RESYNC` 从窗口边界重新同步后恢复投递。
- 内存占用不随连接数或运行时间无界增长：频道日志受重放窗口限制，
  待发缓冲受容量限制，保留会话超时清理。

## 6. 鉴权

- 连接建立时校验 token：校验失败返回 `ERROR(code=AUTH_FAILED)` 并关闭连接。
- 订阅时按连接身份做**逐频道鉴权**：无权限的频道返回
  `ERROR(code=PERMISSION_DENIED, channelId=...)`，不建立订阅、不投递任何内容。
- 鉴权失败不泄露无权访问频道的任何消息。
- 身份与权限在重连后不可冒用：重连需重新校验 token，
  且只能恢复属于该身份的会话。

## 7. 停机清理

停机时按既定顺序结束：

1. 停止投递线程（不再产生新的推送）。
2. 关闭所有活跃连接，客户端收到 `CLOSE(reason=server_shutdown)`。
3. 停止本地回环服务端与调度线程。
4. 清理保留会话与订阅引用。

## 8. 配置项

| 配置 | 默认值 | 说明 |
|---|---|---|
| `gateway.heartbeat-interval-ms` | 5000 | 心跳周期（毫秒） |
| `gateway.heartbeat-miss-threshold` | 3 | 允许的最大心跳缺失次数 |
| `gateway.session-retention-ms` | 30000 | 断线后会话保留窗口（毫秒） |
| `gateway.channel-replay-window-messages` | 1000 | 单频道重放窗口（消息条数） |
| `gateway.connection-buffer-capacity` | 256 | 单条连接待发缓冲上限（帧数） |
| `gateway.loopback-port` | 0 | 本地回环 NIO 端口；-1 不启动网络监听（纯内存/测试），0 为临时端口 |

## 9. 协议帧

帧格式：4 字节大端长度前缀 + UTF-8 JSON。

上行（客户端 → 服务端）：

| 帧 | 字段 | 说明 |
|---|---|---|
| `CONNECT` | `token`, `positions`(可选) | 建立连接，positions 为各频道确认位置 |
| `SUBSCRIBE` | `channelId`, `lastAck`(可选) | 订阅频道，lastAck 为确认位置 |
| `UNSUBSCRIBE` | `channelId` | 退订频道 |
| `ACK` | `channelId`, `seq` | 确认某频道消息 |
| `PING` | - | 心跳 |
| `RESYNC` | `positions` | 请求重新同步 |

下行（服务端 → 客户端）：

| 帧 | 字段 | 说明 |
|---|---|---|
| `CONNECTED` | `sessionId`, `resumed`, `heartbeatIntervalMs`, `retentionMs` | 连接建立 |
| `SUBSCRIBED` | `channelId`, `oldestSeq`, `newestSeq` | 订阅成功 |
| `UNSUBSCRIBED` | `channelId` | 退订成功 |
| `MESSAGE` | `channelId`, `seq`, `eventId`, `payload` | 频道消息 |
| `PONG` | - | 心跳响应 |
| `RESYNC_OK` | - | 重新同步完成 |
| `RESYNC_REQUIRED` | `channelId`, `oldestSeq`, `newestSeq`, `reason` | 需要重新同步 |
| `ERROR` | `code`, `channelId`(可选), `message` | 错误（AUTH_FAILED / PERMISSION_DENIED / INVALID_FRAME 等） |
| `CLOSE` | `reason` | 连接关闭原因 |

关闭原因（`reason`）：`normal`、`server_shutdown`、`replaced_by_new_connection`、
`heartbeat_timeout`、`protocol_error`、`auth_failed`、`slow_consumer`。

## 10. 测试

测试使用**内存通道**（`MemoryTransport`）和**可控时钟**（`ManualClock`），
不依赖网络与外部服务，可重复运行。

| 测试类 | 覆盖场景 |
|---|---|
| `MultiChannelOrderingTest` | 多频道隔离与顺序、乱序/重复事件归一化、上行控制不打乱投递 |
| `ResumeRedeliveryTest` | 重连续传、已确认不重复、未确认不丢失、顶号处理 |
| `ReplayWindowTest` | 重放窗口、超出窗口的重新同步结论、窗口内重放 |
| `SlowConsumerTest` | 慢连接隔离、缓冲超限、不阻塞其他连接与产生方、重新同步恢复 |
| `HalfOpenConnectionTest` | 半开连接识别与回收、心跳保活、保留超时清理 |
| `RetentionExpiryTest` | 保留窗口内恢复、超时后全新会话 |
| `UnauthorizedAccessTest` | 越权订阅拒绝、内容不泄露、token 校验、身份不可冒用 |
| `ShutdownTest` | 停机关闭原因、不遗留线程 |
| `LoopbackEndToEndTest` | 本地回环真实 Socket 全链路 |

运行测试：

```bash
mvn test
```

## 11. 本地运行

应用以 Spring Boot 启动，默认在本地回环临时端口启动 NIO 服务端。
默认开发 token 为 `dev-token`（身份 `dev`，可访问所有频道）。
可通过 `gateway.loopback-port` 配置端口，或设置为 `-1` 仅使用内存通道。

```bash
mvn spring-boot:run
```
