package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelEvent;
import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.EventNormalizer;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：底层重复与乱序事件先归一化再投递。
 */
class NormalizationTest {

    @Test
    void duplicatesDroppedAndOutOfOrderReordered() {
        EventNormalizer n = new EventNormalizer(8);
        // 乱序：2 先于 1 到达，1 到达前不输出。
        assertTrue(n.accept(new ChannelEvent("e2", 2, "B")).isEmpty());
        // 重复：同一事件再次到达，直接丢弃。
        assertTrue(n.accept(new ChannelEvent("e2", 2, "B")).isEmpty());
        // 补齐 1 后按序输出 1,2。
        List<ChannelEvent> out = n.accept(new ChannelEvent("e1", 1, "A"));
        assertEquals(2, out.size());
        assertEquals("e1", out.get(0).eventId());
        assertEquals("e2", out.get(1).eventId());
        // 已输出位置的重复投递被丢弃。
        assertTrue(n.accept(new ChannelEvent("e1-dup", 1, "A")).isEmpty());
        assertTrue(n.accept(new ChannelEvent("e1", 1, "A")).isEmpty());
    }

    @Test
    void permanentGapDoesNotStallStream() {
        EventNormalizer n = new EventNormalizer(2);
        assertEquals(1, n.accept(new ChannelEvent("e1", 1, "A")).size());
        // 2 永久丢失；窗口填满后强制推进，流不卡死。
        n.accept(new ChannelEvent("e3", 3, "C"));
        n.accept(new ChannelEvent("e4", 4, "D"));
        List<ChannelEvent> out = n.accept(new ChannelEvent("e5", 5, "E"));
        assertTrue(out.stream().anyMatch(e -> e.eventId().equals("e3")));
    }

    @Test
    void gatewayDeliversNormalizedOrder() {
        try (TestSupport.Rig rig = TestSupport.newRig(GatewayConfig.defaults())) {
            rig.authenticator().register("token-alice", "alice");
            rig.authorizer().grant("alice", "ch-a");
            var client = rig.connect();
            TestSupport.hello(client, "token-alice");
            client.send(new ClientMessage.Subscribe("ch-a", null));
            TestSupport.awaitMessage(client, m -> m instanceof ServerMessage.Subscribed);

            // 产生方乱序 + 重复提交。
            rig.gateway().publish("ch-a", new ChannelEvent("e2", 2, "B"));
            rig.gateway().publish("ch-a", new ChannelEvent("e2", 2, "B"));
            rig.gateway().publish("ch-a", new ChannelEvent("e1", 1, "A"));
            rig.gateway().publish("ch-a", new ChannelEvent("e3", 3, "C"));
            rig.gateway().publish("ch-a", new ChannelEvent("e3", 3, "C"));

            TestSupport.awaitUntil(() -> TestSupport.eventsOf(client.received(), "ch-a").size() == 3, 5_000);
            List<ServerMessage.Event> events = TestSupport.eventsOf(client.received(), "ch-a");
            assertEquals(List.of("A", "B", "C"), events.stream().map(ServerMessage.Event::payload).toList());
            assertEquals(List.of(1L, 2L, 3L), events.stream().map(ServerMessage.Event::seq).toList());
        }
    }
}
