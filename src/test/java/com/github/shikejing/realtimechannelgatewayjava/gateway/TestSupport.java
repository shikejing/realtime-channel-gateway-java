package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.AuthException;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Authenticator;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Authorizer;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.ChannelOperation;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Identity;
import com.github.shikejing.realtimechannelgatewayjava.gateway.clock.TestTime;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.transport.InMemoryTransport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.fail;

/** 测试支撑：内存身份/鉴权、网关装配、异步等待。 */
public final class TestSupport {

    private TestSupport() {
    }

    /** 令牌即身份：token 为 "token-<subject>"，未知令牌拒绝。 */
    public static class TestAuthenticator implements Authenticator {
        private final Map<String, Identity> byToken = new HashMap<>();

        public TestAuthenticator register(String token, String subject) {
            byToken.put(token, Identity.of(subject));
            return this;
        }

        @Override
        public Identity authenticate(String token) throws AuthException {
            Identity id = byToken.get(token);
            if (id == null) {
                throw new AuthException("invalid token");
            }
            return id;
        }
    }

    /** 按身份白名单逐频道鉴权。 */
    public static class TestAuthorizer implements Authorizer {
        private final Map<String, Set<String>> allowed = new HashMap<>();

        public TestAuthorizer grant(String subject, String... channels) {
            allowed.computeIfAbsent(subject, s -> new java.util.HashSet<>())
                    .addAll(List.of(channels));
            return this;
        }

        @Override
        public Decision authorize(Identity identity, String channel, ChannelOperation operation) {
            Set<String> set = allowed.getOrDefault(identity.subject(), Set.of());
            return set.contains(channel)
                    ? Decision.allow()
                    : Decision.deny("no permission for channel");
        }
    }

    /** 装配好的测试网关。 */
    public record Rig(GatewayServer gateway, TestTime time, TestAuthenticator authenticator,
                      TestAuthorizer authorizer) implements AutoCloseable {
        @Override
        public void close() {
            gateway.close();
        }

        public InMemoryTransport.ClientEndpoint connect() {
            var conn = InMemoryTransport.connect(gateway);
            gateway.acceptConnection(conn.serverHandle());
            return conn.client();
        }
    }

    public static Rig newRig(GatewayConfig config) {
        TestTime time = new TestTime(1_000_000L);
        TestAuthenticator authenticator = new TestAuthenticator();
        TestAuthorizer authorizer = new TestAuthorizer();
        GatewayServer gateway = new GatewayServer(config, authenticator, authorizer, time, time);
        return new Rig(gateway, time, authenticator, authorizer);
    }

    /** 建立连接并完成 Hello 握手，返回 Welcome。 */
    public static ServerMessage.Welcome hello(InMemoryTransport.ClientEndpoint endpoint, String token) {
        endpoint.send(new ClientMessage.Hello(token, null, null, null));
        return (ServerMessage.Welcome) awaitMessage(endpoint, m -> m instanceof ServerMessage.Welcome);
    }

    /** 等待匹配的消息出现；只窥视不取走，避免误吞其他消息。 */
    public static ServerMessage awaitMessage(InMemoryTransport.ClientEndpoint endpoint,
                                             java.util.function.Predicate<ServerMessage> pred) {
        try {
            awaitUntil(() -> endpoint.received().stream().anyMatch(pred), 5_000);
        } catch (AssertionError e) {
            fail("message not received, got: " + endpoint.received());
        }
        return endpoint.received().stream().filter(pred).findFirst().orElseThrow();
    }

    public static void awaitUntil(BooleanSupplier cond, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("condition not met within " + timeoutMs + "ms");
    }

    /** 收集某频道的 Event 消息（从已收到的消息流中）。 */
    public static List<ServerMessage.Event> eventsOf(List<ServerMessage> messages, String channel) {
        List<ServerMessage.Event> out = new ArrayList<>();
        for (ServerMessage m : messages) {
            if (m instanceof ServerMessage.Event e && e.channel().equals(channel)) {
                out.add(e);
            }
        }
        return out;
    }
}
