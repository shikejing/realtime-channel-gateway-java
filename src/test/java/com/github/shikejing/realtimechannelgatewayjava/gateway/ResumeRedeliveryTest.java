package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelEvent;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ErrorCode;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：断线重连续传——已确认不重发、未确认不丢失、
 * 保留窗口超时按策略过期、重放窗口超限给出重新同步结论、重连不重复计入。
 */
class ResumeRedeliveryTest {

    private TestSupport.Rig newRig(GatewayConfig config) {
        TestSupport.Rig rig = TestSupport.newRig(config);
        rig.authenticator().register("token-alice", "alice");
        rig.authorizer().grant("alice", "ch-a");
        return rig;
    }

    @Test
    void resumeRedeliversOnlyUnacked() {
        GatewayConfig config = GatewayConfig.defaults();
        try (TestSupport.Rig rig = newRig(config)) {
            var client = rig.connect();
            var welcome = TestSupport.hello(client, "token-alice");
            client.send(new ClientMessage.Subscribe("ch-a", null));
            TestSupport.awaitMessage(client, m -> m instanceof ServerMessage.Subscribed);

            for (int i = 1; i <= 3; i++) {
                rig.gateway().publish("ch-a", new ChannelEvent("a-" + i, i, "A" + i));
            }
            TestSupport.awaitUntil(() -> TestSupport.eventsOf(client.received(), "ch-a").size() == 3, 5_000);
            // 确认到 2，然后断线。
            client.send(new ClientMessage.Ack("ch-a", 2));
            client.disconnect();
            TestSupport.awaitUntil(() -> rig.gateway().connectionCount() == 0, 5_000);

            // 断线期间又产生 2 条。
            rig.gateway().publish("ch-a", new ChannelEvent("a-4", 4, "A4"));
            rig.gateway().publish("ch-a", new ChannelEvent("a-5", 5, "A5"));

            // 保留窗口内重连：恢复同一会话与订阅。
            var client2 = rig.connect();
            client2.send(new ClientMessage.Hello(
                    "token-alice", welcome.sessionId(), welcome.resumeToken(), Map.of("ch-a", 2L)));
            var welcome2 = (ServerMessage.Welcome) TestSupport.awaitMessage(
                    client2, m -> m instanceof ServerMessage.Welcome);
            assertTrue(welcome2.resumed());
            assertEquals(welcome.sessionId(), welcome2.sessionId());

            // 只重放未确认的 3,4,5；已确认的 1,2 不重发。
            TestSupport.awaitUntil(() -> TestSupport.eventsOf(client2.received(), "ch-a").size() == 3, 5_000);
            List<ServerMessage.Event> events = TestSupport.eventsOf(client2.received(), "ch-a");
            assertEquals(List.of(3L, 4L, 5L), events.stream().map(ServerMessage.Event::seq).toList());
            assertEquals(List.of("A3", "A4", "A5"), events.stream().map(ServerMessage.Event::payload).toList());
            // 同一客户端只计入一个有效会话。
            assertEquals(1, rig.gateway().sessions().activeCount());
        }
    }

    @Test
    void resumeWithWrongTokenRejected() {
        GatewayConfig config = GatewayConfig.defaults();
        try (TestSupport.Rig rig = newRig(config)) {
            var client = rig.connect();
            var welcome = TestSupport.hello(client, "token-alice");
            client.disconnect();
            TestSupport.awaitUntil(() -> rig.gateway().connectionCount() == 0, 5_000);

            var client2 = rig.connect();
            client2.send(new ClientMessage.Hello("token-alice", welcome.sessionId(), "forged-token", Map.of()));
            var error = (ServerMessage.Error) TestSupport.awaitMessage(client2, m -> m instanceof ServerMessage.Error);
            assertEquals(ErrorCode.RESUME_TOKEN_MISMATCH.name(), error.code());
        }
    }

    @Test
    void resumeAfterRetentionWindowExpires() {
        GatewayConfig config = new GatewayConfig(1_000, 3_000, 5_000, 100, 8, 64, SlowConsumerPolicy.DISCONNECT);
        try (TestSupport.Rig rig = newRig(config)) {
            var client = rig.connect();
            var welcome = TestSupport.hello(client, "token-alice");
            client.disconnect();
            TestSupport.awaitUntil(() -> rig.gateway().connectionCount() == 0, 5_000);

            // 推进时间超过保留窗口，心跳节拍触发过期清理。
            rig.time().advanceBy(10_000);

            var client2 = rig.connect();
            client2.send(new ClientMessage.Hello("token-alice", welcome.sessionId(), welcome.resumeToken(), Map.of()));
            var error = (ServerMessage.Error) TestSupport.awaitMessage(client2, m -> m instanceof ServerMessage.Error);
            assertEquals(ErrorCode.SESSION_EXPIRED.name(), error.code());
            assertEquals(0, rig.gateway().sessions().activeCount());
        }
    }

    @Test
    void replayWindowExceededYieldsResyncRequired() {
        // 重放窗口只有 3 条。
        GatewayConfig config = new GatewayConfig(10_000, 30_000, 60_000, 3, 8, 64, SlowConsumerPolicy.DISCONNECT);
        try (TestSupport.Rig rig = newRig(config)) {
            var client = rig.connect();
            var welcome = TestSupport.hello(client, "token-alice");
            client.send(new ClientMessage.Subscribe("ch-a", null));
            TestSupport.awaitMessage(client, m -> m instanceof ServerMessage.Subscribed);
            client.disconnect();
            TestSupport.awaitUntil(() -> rig.gateway().connectionCount() == 0, 5_000);

            // 断线期间产生 10 条，远超重放窗口。
            for (int i = 1; i <= 10; i++) {
                rig.gateway().publish("ch-a", new ChannelEvent("a-" + i, i, "A" + i));
            }

            var client2 = rig.connect();
            client2.send(new ClientMessage.Hello("token-alice", welcome.sessionId(), welcome.resumeToken(), Map.of("ch-a", 0L)));
            var resync = (ServerMessage.ResyncRequired) TestSupport.awaitMessage(
                    client2, m -> m instanceof ServerMessage.ResyncRequired);
            assertEquals("ch-a", resync.channel());
            assertTrue(resync.earliestSeq() > 1);
            // 窗口内的部分仍然可用：重放结论之后不应假装完整。
            assertFalse(TestSupport.eventsOf(client2.received(), "ch-a").size() > 3);
        }
    }

    @Test
    void concurrentConnectionWithSameSessionIsTakenOver() {
        GatewayConfig config = GatewayConfig.defaults();
        try (TestSupport.Rig rig = newRig(config)) {
            var client1 = rig.connect();
            var welcome = TestSupport.hello(client1, "token-alice");

            // 旧连接未断开，新连接直接恢复同一会话：旧连接必须被踢掉。
            var client2 = rig.connect();
            client2.send(new ClientMessage.Hello("token-alice", welcome.sessionId(), welcome.resumeToken(), Map.of()));
            TestSupport.awaitMessage(client2, m -> m instanceof ServerMessage.Welcome w && w.resumed());
            TestSupport.awaitMessage(client1, m -> m instanceof ServerMessage.Closed c
                    && c.reason().equals("SESSION_TAKEN_OVER"));
            TestSupport.awaitUntil(() -> !client1.isOpen(), 5_000);
            assertEquals(1, rig.gateway().connectionCount());
            assertEquals(1, rig.gateway().sessions().activeCount());
        }
    }
}
