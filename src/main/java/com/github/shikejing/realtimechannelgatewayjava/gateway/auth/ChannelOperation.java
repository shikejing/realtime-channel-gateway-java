package com.github.shikejing.realtimechannelgatewayjava.gateway.auth;

/** 需要逐频道鉴权的上行操作。 */
public enum ChannelOperation {
    SUBSCRIBE,
    UNSUBSCRIBE,
    ACK
}
