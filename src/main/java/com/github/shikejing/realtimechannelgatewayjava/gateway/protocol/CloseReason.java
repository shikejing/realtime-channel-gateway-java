package com.github.shikejing.realtimechannelgatewayjava.gateway.protocol;

/** 连接关闭原因，客户端可区分。 */
public enum CloseReason {
    CLIENT_BYE,
    SERVER_SHUTDOWN,
    HEARTBEAT_TIMEOUT,
    SESSION_TAKEN_OVER,
    SLOW_CONSUMER,
    AUTH_FAILED,
    PROTOCOL_ERROR
}
