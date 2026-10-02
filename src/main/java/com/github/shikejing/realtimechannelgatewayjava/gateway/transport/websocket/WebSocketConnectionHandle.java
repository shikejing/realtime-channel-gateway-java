package com.github.shikejing.realtimechannelgatewayjava.gateway.transport.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.transport.ConnectionHandle;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WebSocket 连接句柄适配器。关闭幂等；发送与关闭串行化以符合 WebSocketSession 约束。
 */
public class WebSocketConnectionHandle implements ConnectionHandle {

    private final WebSocketSession session;
    private final ObjectMapper mapper;
    private final Runnable onTransportClosed;
    private final AtomicBoolean open = new AtomicBoolean(true);

    public WebSocketConnectionHandle(WebSocketSession session, ObjectMapper mapper,
                                     Runnable onTransportClosed) {
        this.session = session;
        this.mapper = mapper;
        this.onTransportClosed = onTransportClosed;
    }

    @Override
    public String id() {
        return session.getId();
    }

    @Override
    public void send(ServerMessage message) {
        if (!open.get()) {
            return;
        }
        try {
            synchronized (session) {
                if (session.isOpen()) {
                    session.sendMessage(new TextMessage(mapper.writeValueAsString(message)));
                }
            }
        } catch (IOException | IllegalStateException e) {
            // 写出失败视为传输层死亡，走统一清理路径。
            close(CloseReason.PROTOCOL_ERROR);
        }
    }

    @Override
    public void close(CloseReason reason) {
        if (!open.compareAndSet(true, false)) {
            return;
        }
        try {
            synchronized (session) {
                if (session.isOpen()) {
                    session.close();
                }
            }
        } catch (IOException ignored) {
            // 已断开。
        } finally {
            onTransportClosed.run();
        }
    }

    /** 传输层回调（对端关闭/异常）时调用。 */
    public void transportClosed() {
        if (open.compareAndSet(true, false)) {
            onTransportClosed.run();
        }
    }

    @Override
    public boolean isOpen() {
        return open.get() && session.isOpen();
    }
}
