package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 重放窗口：超出有限窗口时给出明确的重新同步结论，
 * 从窗口边界重新同步后消息继续按序投递。
 */
class ReplayWindowTest extends GatewayTestBase {

    @BeforeEach
    void grants() {
        authorizer.grant(ALICE, "ch1");
    }

    @Test
    void reconnectBeyondWindowRequiresResync() throws Exception {
        var c = connect(ALICE_TOKEN);
        subscribe(c, "ch1");
        for (int i = 1; i <= 15; i++) {
            gateway.publish("ch1", "m" + i);
        }
        for (int i = 0; i < 15; i++) {
            receive(c);
        }
        c.close(CloseReason.NORMAL);

        // 携带已超出窗口的确认点重连
        var pair = connectWithPositions(ALICE_TOKEN, Map.of("ch1", 3L));
        assertThat(pair.getKey().getResumed()).isTrue();

        Frame required = receive(pair.getValue());
        assertThat(required.getType()).isEqualTo(Frame.Type.RESYNC_REQUIRED);
        assertThat(required.getChannelId()).isEqualTo("ch1");
        assertThat(required.getOldestSeq()).isEqualTo(6L);
        assertThat(required.getNewestSeq()).isEqualTo(15L);
        assertThat(required.getReason()).isEqualTo("replay_window_exceeded");

        // 从窗口最早位置重新同步
        pair.getValue().send(Frame.type(Frame.Type.RESYNC).setPositions(Map.of("ch1", 6L)));
        Frame ok = receive(pair.getValue());
        assertThat(ok.getType()).isEqualTo(Frame.Type.RESYNC_OK);

        List<String> payloads = new ArrayList<>();
        for (int i = 7; i <= 15; i++) {
            Frame m = receive(pair.getValue());
            assertThat(m.getType()).isEqualTo(Frame.Type.MESSAGE);
            payloads.add(m.getPayload());
        }
        assertThat(payloads).hasSize(9);
        assertThat(payloads.get(0)).isEqualTo("m7");
        assertThat(payloads.get(payloads.size() - 1)).isEqualTo("m15");
        assertNoMoreFrames(pair.getValue());
    }

    @Test
    void resyncWithinWindowReplaysMissed() throws Exception {
        var c = connect(ALICE_TOKEN);
        subscribe(c, "ch1");
        for (int i = 1; i <= 8; i++) {
            gateway.publish("ch1", "m" + i);
        }
        for (int i = 0; i < 8; i++) {
            receive(c);
        }
        c.send(Frame.type(Frame.Type.ACK).setChannelId("ch1").setSeq(5L));

        // 客户端主动要求重新同步，确认点为 5
        c.send(Frame.type(Frame.Type.RESYNC).setPositions(Map.of("ch1", 5L)));
        Frame ok = receive(c);
        assertThat(ok.getType()).isEqualTo(Frame.Type.RESYNC_OK);

        List<String> payloads = new ArrayList<>();
        Frame m;
        while ((m = c.pollDownlink(200, java.util.concurrent.TimeUnit.MILLISECONDS)) != null) {
            assertThat(m.getType()).isEqualTo(Frame.Type.MESSAGE);
            payloads.add(m.getPayload());
        }
        assertThat(payloads).containsExactly("m6", "m7", "m8");
    }
}
