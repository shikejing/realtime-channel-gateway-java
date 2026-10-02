package com.github.shikejing.realtimechannelgatewayjava.gateway.transport.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shikejing.realtimechannelgatewayjava.gateway.GatewayServer;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.CloseReason;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * WebSocket 端点：把传输事件桥接到传输无关的网关核心。
 */
public class GatewayWebSocketHandler extends TextWebSocketHandler {

    private final GatewayServer gateway;
    private final ObjectMapper mapper;

    public GatewayWebSocketHandler(GatewayServer gateway, ObjectMapper mapper) {
        this.gateway = gateway;
        this.mapper = mapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        WebSocketConnectionHandle[] ref = new WebSocketConnectionHandle[1];
        ref[0] = new WebSocketConnectionHandle(session, mapper,
                () -> gateway.onClosed(ref[0], CloseReason.CLIENT_BYE));
        session.getAttributes().put("handle", ref[0]);
        gateway.acceptConnection(ref[0]);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        WebSocketConnectionHandle handle = (WebSocketConnectionHandle) session.getAttributes().get("handle");
        if (handle == null) {
            return;
        }
        ClientMessage clientMessage = mapper.readValue(message.getPayload(), ClientMessage.class);
        gateway.onMessage(handle, clientMessage);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        WebSocketConnectionHandle handle = (WebSocketConnectionHandle) session.getAttributes().get("handle");
        if (handle != null) {
            handle.transportClosed();
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        WebSocketConnectionHandle handle = (WebSocketConnectionHandle) session.getAttributes().get("handle");
        if (handle != null) {
            handle.transportClosed();
        }
    }

}
