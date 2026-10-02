package com.github.shikejing.realtimechannelgatewayjava.protocol;

import java.util.Map;

/**
 * 链路上传输的帧。type 区分上行/下行控制消息与数据消息。
 */
public final class Frame {
    public enum Type {
        // 上行
        CONNECT, SUBSCRIBE, UNSUBSCRIBE, ACK, PING, RESYNC,
        // 下行
        CONNECTED, SUBSCRIBED, UNSUBSCRIBED, MESSAGE, PONG,
        RESYNC_REQUIRED, RESYNC_OK, ERROR, CLOSE
    }

    private Type type;
    private String channelId;
    private Long seq;
    private String eventId;
    private String payload;
    private String identity;
    private String token;
    private String sessionId;
    private Long heartbeatIntervalMs;
    private Long retentionMs;
    private Long lastAck;
    private String code;
    private String reason;
    private String message;
    private Boolean resumed;
    private Map<String, Long> positions;
    private Long oldestSeq;
    private Long newestSeq;

    public Frame() {}
    public Frame(Type type) { this.type = type; }

    public Type getType() { return type; }
    public Frame setType(Type type) { this.type = type; return this; }
    public String getChannelId() { return channelId; }
    public Frame setChannelId(String channelId) { this.channelId = channelId; return this; }
    public Long getSeq() { return seq; }
    public Frame setSeq(Long seq) { this.seq = seq; return this; }
    public String getEventId() { return eventId; }
    public Frame setEventId(String eventId) { this.eventId = eventId; return this; }
    public String getPayload() { return payload; }
    public Frame setPayload(String payload) { this.payload = payload; return this; }
    public String getIdentity() { return identity; }
    public Frame setIdentity(String identity) { this.identity = identity; return this; }
    public String getToken() { return token; }
    public Frame setToken(String token) { this.token = token; return this; }
    public String getSessionId() { return sessionId; }
    public Frame setSessionId(String sessionId) { this.sessionId = sessionId; return this; }
    public Long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public Frame setHeartbeatIntervalMs(Long v) { this.heartbeatIntervalMs = v; return this; }
    public Long getRetentionMs() { return retentionMs; }
    public Frame setRetentionMs(Long v) { this.retentionMs = v; return this; }
    public Long getLastAck() { return lastAck; }
    public Frame setLastAck(Long lastAck) { this.lastAck = lastAck; return this; }
    public String getCode() { return code; }
    public Frame setCode(String code) { this.code = code; return this; }
    public String getReason() { return reason; }
    public Frame setReason(String reason) { this.reason = reason; return this; }
    public String getMessage() { return message; }
    public Frame setMessage(String message) { this.message = message; return this; }
    public Boolean getResumed() { return resumed; }
    public Frame setResumed(Boolean resumed) { this.resumed = resumed; return this; }
    public Map<String, Long> getPositions() { return positions; }
    public Frame setPositions(Map<String, Long> positions) { this.positions = positions; return this; }
    public Long getOldestSeq() { return oldestSeq; }
    public Frame setOldestSeq(Long oldestSeq) { this.oldestSeq = oldestSeq; return this; }
    public Long getNewestSeq() { return newestSeq; }
    public Frame setNewestSeq(Long newestSeq) { this.newestSeq = newestSeq; return this; }

    public static Frame type(Type t) { return new Frame(t); }
}
