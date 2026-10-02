package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 多频道隔离与顺序：各频道互不串扰，频道内顺序与产生顺序一致，
 * 上行控制消息不打乱投递顺序。
 */
class MultiChannelOrderingTest extends GatewayTestBase {

    @BeforeEach
    void grants() {
        authorizer.grant(ALICE, "ch1").grant(ALICE, "ch2").grant(ALICE, "ch3");
    }

    @Test
    void channelsAreIsolatedAndOrdered() throws Exception {
        var c = connect(ALICE_TOKEN);
        subscribe(c, "ch1");
        subscribe(c, "ch2");
        subscribe(c, "ch3");

        gateway.publish("ch2", "x");
        gateway.publish("ch1", "a");
        gateway.publish("ch3", "z");
        gateway.publish("ch1", "b");
        gateway.publish("ch2", "y");
        gateway.publish("ch1", "c");

        Map<String, List<String>> got = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            Frame f = receive(c);
            assertThat(f.getType()).isEqualTo(Frame.Type.MESSAGE);
            got.computeIfAbsent(f.getChannelId(), k -> new ArrayList<>()).add(f.getPayload());
        }
        assertThat(got.get("ch1")).containsExactly("a", "b", "c");
        assertThat(got.get("ch2")).containsExactly("x", "y");
        assertThat(got.get("ch3")).containsExactly("z");
        assertNoMoreFrames(c);
    }

    @Test
    void outOfOrderAndDuplicateEventsAreNormalized() throws Exception {
        var c = connect(ALICE_TOKEN);
        subscribe(c, "ch1");

        gateway.publish("ch1", "e2", 2, "two");
        gateway.publish("ch1", "e1", 1, "one");
        gateway.publish("ch1", "e1", 1, "one-duplicate");
        gateway.publish("ch1", "e3", 3, "three");

        List<String> payloads = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Frame f = receive(c);
            assertThat(f.getType()).isEqualTo(Frame.Type.MESSAGE);
            assertThat(f.getChannelId()).isEqualTo("ch1");
            payloads.add(f.getPayload());
        }
        assertThat(payloads).containsExactly("one", "two", "three");
        assertNoMoreFrames(c);
    }

    @Test
    void uplinkControlFramesDoNotInterruptDelivery() throws Exception {
        var c = connect(ALICE_TOKEN);
        subscribe(c, "ch1");

        gateway.publish("ch1", "m1");
        assertThat(receive(c).getPayload()).isEqualTo("m1");

        c.send(Frame.type(Frame.Type.PING));
        c.send(Frame.type(Frame.Type.ACK).setChannelId("ch1").setSeq(1L));
        c.send(Frame.type(Frame.Type.UNSUBSCRIBE).setChannelId("ch2"));
        c.send(Frame.type(Frame.Type.SUBSCRIBE).setChannelId("ch1"));

        assertThat(receive(c).getType()).isEqualTo(Frame.Type.PONG);
        assertThat(receive(c).getType()).isEqualTo(Frame.Type.UNSUBSCRIBED);
        assertThat(receive(c).getType()).isEqualTo(Frame.Type.SUBSCRIBED);

        gateway.publish("ch1", "m2");
        Frame f2 = receive(c);
        assertThat(f2.getType()).isEqualTo(Frame.Type.MESSAGE);
        assertThat(f2.getPayload()).isEqualTo("m2");
    }
}
