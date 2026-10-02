package com.github.shikejing.realtimechannelgatewayjava.gateway.transport;

import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.CloseReason;

/** 连接事件回调，由传输层触发、网关核心处理。 */
public interface ConnectionListener {

    void onMessage(ConnectionHandle connection, ClientMessage message);

    /** 无论哪一侧发起关闭，最终都回调到这里，走同一条清理路径。 */
    void onClosed(ConnectionHandle connection, CloseReason reason);
}
