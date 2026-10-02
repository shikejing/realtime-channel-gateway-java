package com.github.shikejing.realtimechannelgatewayjava.gateway.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shikejing.realtimechannelgatewayjava.gateway.GatewayServer;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.AuthException;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Authenticator;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Authorizer;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Identity;
import com.github.shikejing.realtimechannelgatewayjava.gateway.clock.SystemClock;
import com.github.shikejing.realtimechannelgatewayjava.gateway.clock.SystemScheduler;
import com.github.shikejing.realtimechannelgatewayjava.gateway.transport.websocket.GatewayWebSocketHandler;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 网关 Spring 装配：核心 Bean、示例鉴权实现与 WebSocket 端点。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewaySpringConfig {

    @Bean(destroyMethod = "close")
    public GatewayServer gatewayServer(GatewayProperties properties) {
        return new GatewayServer(properties.toGatewayConfig(),
                tokenAuthenticator(properties), allowlistAuthorizer(properties),
                new SystemClock(), new SystemScheduler("gateway-heartbeat"));
    }

    @Bean
    public GatewayWebSocketHandler gatewayWebSocketHandler(GatewayServer gatewayServer) {
        return new GatewayWebSocketHandler(gatewayServer, new ObjectMapper());
    }

    /** 令牌 -> 身份 的示例认证器。 */
    static Authenticator tokenAuthenticator(GatewayProperties properties) {
        Map<String, String> tokens = properties.getTokens();
        return token -> {
            String subject = tokens.get(token);
            if (subject == null) {
                throw new AuthException("invalid token");
            }
            return Identity.of(subject);
        };
    }

    /** 身份 -> 频道白名单 的示例鉴权器。 */
    static Authorizer allowlistAuthorizer(GatewayProperties properties) {
        Map<String, List<String>> grants = properties.getGrants();
        return (identity, channel, operation) -> {
            Set<String> allowed = new HashSet<>(grants.getOrDefault(identity.subject(), List.of()));
            return allowed.contains(channel)
                    ? Authorizer.Decision.allow()
                    : Authorizer.Decision.deny("no permission for channel");
        };
    }
}
