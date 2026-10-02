package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelEvent;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地回环端到端：真实 WebSocket 连接（127.0.0.1、随机端口），
 * 验证订阅、投递、断线重连续传在真实传输上的表现。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "gateway.tokens.token-alice=alice",
        "gateway.grants.alice=ch-a,ch-b",
        "gateway.heartbeat-interval-ms=200",
        "gateway.heartbeat-timeout-ms=60000",
        "gateway.session-retention-ms=60000"
})
class LoopbackEndToEndTest {

    @LocalServerPort
    int port;

    @Autowired
    GatewayServer gateway;

    private final ObjectMapper mapper = new ObjectMapper();

    private static final class TestClient extends TextWebSocketHandler {
        final List<ServerMessage> received = new CopyOnWriteArrayList<>();
        WebSocketSession session;
        private final ObjectMapper mapper = new ObjectMapper();

        void connect(int port) throws Exception {
            session = new StandardWebSocketClient()
                    .execute(this, "ws://127.0.0.1:" + port + "/ws")
                    .get();
        }

        void send(ClientMessage msg) throws Exception {
            session.sendMessage(new TextMessage(mapper.writeValueAsString(msg)));
        }

        @Override
        protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
            received.add(mapper.readValue(message.getPayload(), ServerMessage.class));
        }

        void await(java.util.function.Predicate<ServerMessage> pred) {
            TestSupport.awaitUntil(() -> received.stream().anyMatch(pred), 10_000);
        }

        List<ServerMessage.Event> events(String channel) {
            return received.stream()
                    .filter(m -> m instanceof ServerMessage.Event e && e.channel().equals(channel))
                    .map(m -> (ServerMessage.Event) m)
                    .toList();
        }
    }

    @Test
    void subscribeDeliverAndResumeOverLoopback() throws Exception {
        TestClient client = new TestClient();
        client.connect(port);
        client.send(new ClientMessage.Hello("token-alice", null, null, null));
        client.await(m -> m instanceof ServerMessage.Welcome);
        ServerMessage.Welcome welcome = (ServerMessage.Welcome) client.received.stream()
                .filter(m -> m instanceof ServerMessage.Welcome).findFirst().orElseThrow();

        client.send(new ClientMessage.Subscribe("ch-a", null));
        client.await(m -> m instanceof ServerMessage.Subscribed);

        gateway.publish("ch-a", new ChannelEvent("e1", 1, "hello-1"));
        gateway.publish("ch-a", new ChannelEvent("e2", 2, "hello-2"));
        client.await(m -> m instanceof ServerMessage.Event e && e.seq() == 2);
        assertEquals(List.of("hello-1", "hello-2"),
                client.events("ch-a").stream().map(ServerMessage.Event::payload).toList());

        // 确认到 1，断开；断线期间再产生一条。
        client.send(new ClientMessage.Ack("ch-a", 1));
        Thread.sleep(200); // 等 Ack 到达服务端
        client.session.close();
        TestSupport.awaitUntil(() -> gateway.connectionCount() == 0, 10_000);
        gateway.publish("ch-a", new ChannelEvent("e3", 3, "hello-3"));

        // 重连恢复：只补发未确认的 2,3。
        TestClient client2 = new TestClient();
        client2.connect(port);
        client2.send(new ClientMessage.Hello("token-alice", welcome.sessionId(), welcome.resumeToken(),
                Map.of("ch-a", 1L)));
        client2.await(m -> m instanceof ServerMessage.Welcome w && w.resumed());
        client2.await(m -> m instanceof ServerMessage.Event e && e.seq() == 3);
        assertEquals(List.of(2L, 3L),
                client2.events("ch-a").stream().map(ServerMessage.Event::seq).toList());
        assertTrue(gateway.sessions().activeCount() == 1);
        client2.session.close();
    }
}
