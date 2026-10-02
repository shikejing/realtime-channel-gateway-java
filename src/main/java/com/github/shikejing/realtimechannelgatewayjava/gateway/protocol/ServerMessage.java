package com.github.shikejing.realtimechannelgatewayjava.gateway.protocol;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * 服务端下行消息。
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ServerMessage.Welcome.class, name = "welcome"),
        @JsonSubTypes.Type(value = ServerMessage.Subscribed.class, name = "subscribed"),
        @JsonSubTypes.Type(value = ServerMessage.SubscribeRejected.class, name = "subscribeRejected"),
        @JsonSubTypes.Type(value = ServerMessage.Unsubscribed.class, name = "unsubscribed"),
        @JsonSubTypes.Type(value = ServerMessage.Event.class, name = "event"),
        @JsonSubTypes.Type(value = ServerMessage.ResyncRequired.class, name = "resyncRequired"),
        @JsonSubTypes.Type(value = ServerMessage.Pong.class, name = "pong"),
        @JsonSubTypes.Type(value = ServerMessage.Error.class, name = "error"),
        @JsonSubTypes.Type(value = ServerMessage.Closed.class, name = "closed")
})
public sealed interface ServerMessage {

    /** 握手结果：会话标识、恢复令牌、是否复用了旧会话。 */
    record Welcome(String sessionId, String resumeToken, boolean resumed) implements ServerMessage {
    }

    /** 订阅成功，fromSeq 为将从该序号（不含）之后开始投递。 */
    record Subscribed(String channel, long fromSeq) implements ServerMessage {
    }

    /** 订阅被拒绝，code 可区分原因（鉴权失败、频道不存在等）。 */
    record SubscribeRejected(String channel, String code, String reason) implements ServerMessage {
    }

    record Unsubscribed(String channel) implements ServerMessage {
    }

    /** 频道事件，seq 为频道内单调递增序号。 */
    record Event(String channel, long seq, String payload) implements ServerMessage {
    }

    /** 重放窗口不足，需要客户端全量重新同步。 */
    record ResyncRequired(String channel, long earliestSeq, String reason) implements ServerMessage {
    }

    record Pong(long clientTime) implements ServerMessage {
    }

    /** 通用错误，code 见 ErrorCode。 */
    record Error(String code, String message) implements ServerMessage {
    }

    /** 连接即将关闭，reason 见 CloseReason。 */
    record Closed(String reason) implements ServerMessage {
    }
}
