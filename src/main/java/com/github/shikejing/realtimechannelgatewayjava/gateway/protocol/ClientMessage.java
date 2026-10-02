package com.github.shikejing.realtimechannelgatewayjava.gateway.protocol;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Map;

/**
 * 客户端上行消息，只承载控制语义：握手/恢复、订阅、退订、确认、心跳、主动断开。
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ClientMessage.Hello.class, name = "hello"),
        @JsonSubTypes.Type(value = ClientMessage.Subscribe.class, name = "subscribe"),
        @JsonSubTypes.Type(value = ClientMessage.Unsubscribe.class, name = "unsubscribe"),
        @JsonSubTypes.Type(value = ClientMessage.Ack.class, name = "ack"),
        @JsonSubTypes.Type(value = ClientMessage.Ping.class, name = "ping"),
        @JsonSubTypes.Type(value = ClientMessage.Bye.class, name = "bye")
})
public sealed interface ClientMessage {

    /** 建立会话：携带身份令牌；重连时携带 sessionId + resumeToken + 各频道已确认位置。 */
    record Hello(String token, String sessionId, String resumeToken,
                 Map<String, Long> lastAcked) implements ClientMessage {
    }

    /** 订阅频道；lastAckedSeq 为重放起点（已确认的最大序号），null 表示从最新开始。 */
    record Subscribe(String channel, Long lastAckedSeq) implements ClientMessage {
    }

    record Unsubscribe(String channel) implements ClientMessage {
    }

    /** 确认某频道已收到 seq（含）之前的所有事件。 */
    record Ack(String channel, long seq) implements ClientMessage {
    }

    record Ping(long clientTime) implements ClientMessage {
    }

    /** 客户端主动断开，走与服务端关闭相同的清理路径。 */
    record Bye() implements ClientMessage {
    }
}
