package com.github.shikejing.realtimechannelgatewayjava.gateway.transport;

import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;

/**
 * 服务端侧单条连接句柄。send 必须非阻塞；关闭幂等。
 */
public interface ConnectionHandle {

    String id();

    void send(ServerMessage message);

    void close(CloseReason reason);

    boolean isOpen();
}
