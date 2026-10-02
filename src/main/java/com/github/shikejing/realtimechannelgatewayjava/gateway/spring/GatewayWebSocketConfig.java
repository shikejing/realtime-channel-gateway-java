package com.github.shikejing.realtimechannelgatewayjava.gateway.spring;

import com.github.shikejing.realtimechannelgatewayjava.gateway.transport.websocket.GatewayWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/** 注册网关 WebSocket 端点：ws://host/ws 。 */
@Configuration
@EnableWebSocket
public class GatewayWebSocketConfig implements WebSocketConfigurer {

    private final GatewayWebSocketHandler handler;

    public GatewayWebSocketConfig(GatewayWebSocketHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/ws");
    }
}
