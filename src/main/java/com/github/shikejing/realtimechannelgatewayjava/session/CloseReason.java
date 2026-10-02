package com.github.shikejing.realtimechannelgatewayjava.session;

/**
 * 连接/会话关闭原因，服务端通过 CLOSE 帧把可区分的原因送达客户端。
 */
public enum CloseReason {
    NORMAL("normal"),
    SHUTDOWN("server_shutdown"),
    REPLACED("replaced_by_new_connection"),
    HEARTBEAT_TIMEOUT("heartbeat_timeout"),
    PROTOCOL_ERROR("protocol_error"),
    AUTH_FAILED("auth_failed"),
    SLOW_CONSUMER("slow_consumer");

    private final String wire;
    CloseReason(String wire) { this.wire = wire; }
    public String wire() { return wire; }
}
