package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelEvent;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：单连接订阅多个频道，验证频道间隔离与频道内顺序；
 * 上行控制消息（Ack/Ping）不得打乱任何频道的投递顺序。
 */
class MultiChannelIsolationOrderTest {

    @Test
    void channelsAreIsolatedAndOrderedWithinChannel() {
        GatewayConfig config = GatewayConfig.defaults();
        try (TestSupport.Rig rig = TestSupport.newRig(config)) {
            rig.authenticator().register("token-alice", "alice");
            rig.authorizer().grant("alice", "ch-a", "ch-b");

            var client = rig.connect();
            TestSupport.hello(client, "token-alice");

            client.send(new ClientMessage.Subscribe("ch-a", null));
            client.send(new ClientMessage.Subscribe("ch-b", null));
            TestSupport.awaitMessage(client, m -> m instanceof ServerMessage.Subscribed s && s.channel().equals("ch-a"));
            TestSupport.awaitMessage(client, m -> m instanceof ServerMessage.Subscribed s && s.channel().equals("ch-b"));

            // 交错产生两个频道的事件；期间夹杂上行控制消息。
            for (int i = 1; i <= 5; i++) {
                rig.gateway().publish("ch-a", new ChannelEvent("a-" + i, i, "A" + i));
                client.send(new ClientMessage.Ack("ch-b", Math.max(0, i - 1)));
                rig.gateway().publish("ch-b", new ChannelEvent("b-" + i, i, "B" + i));
                client.send(new ClientMessage.Ping(i));
            }

            TestSupport.awaitUntil(() -> TestSupport.eventsOf(client.received(), "ch-a").size() == 5
                    && TestSupport.eventsOf(client.received(), "ch-b").size() == 5, 5_000);
            List<ServerMessage> all = client.received();

            // 频道内顺序与产生顺序一致，序号各自单调。
            List<ServerMessage.Event> a = TestSupport.eventsOf(all, "ch-a");
            List<ServerMessage.Event> b = TestSupport.eventsOf(all, "ch-b");
            for (int i = 0; i < 5; i++) {
                assertEquals(i + 1, a.get(i).seq());
                assertEquals("A" + (i + 1), a.get(i).payload());
                assertEquals(i + 1, b.get(i).seq());
                assertEquals("B" + (i + 1), b.get(i).payload());
            }
            // 互不串扰：ch-a 的事件不会出现在 ch-b 流里（由频道字段保证）。
            assertTrue(a.stream().allMatch(e -> e.channel().equals("ch-a")));
            assertTrue(b.stream().allMatch(e -> e.channel().equals("ch-b")));
        }
    }

    @Test
    void unsubscribeStopsOnlyThatChannel() {
        GatewayConfig config = GatewayConfig.defaults();
        try (TestSupport.Rig rig = TestSupport.newRig(config)) {
            rig.authenticator().register("token-alice", "alice");
            rig.authorizer().grant("alice", "ch-a", "ch-b");

            var client = rig.connect();
            TestSupport.hello(client, "token-alice");
            client.send(new ClientMessage.Subscribe("ch-a", null));
            client.send(new ClientMessage.Subscribe("ch-b", null));
            TestSupport.awaitMessage(client, m -> m instanceof ServerMessage.Subscribed s && s.channel().equals("ch-b"));

            client.send(new ClientMessage.Unsubscribe("ch-a"));
            TestSupport.awaitMessage(client, m -> m instanceof ServerMessage.Unsubscribed u && u.channel().equals("ch-a"));
            client.drainReceived();

            rig.gateway().publish("ch-a", new ChannelEvent("a-x", 1, "AX"));
            rig.gateway().publish("ch-b", new ChannelEvent("b-x", 1, "BX"));

            TestSupport.awaitUntil(() -> !TestSupport.eventsOf(client.received(), "ch-b").isEmpty(), 5_000);
            // 退订后 ch-a 不再投递，ch-b 不受影响。
            assertTrue(TestSupport.eventsOf(client.received(), "ch-a").isEmpty());
            assertEquals(1, TestSupport.eventsOf(client.received(), "ch-b").size());
        }
    }
}
