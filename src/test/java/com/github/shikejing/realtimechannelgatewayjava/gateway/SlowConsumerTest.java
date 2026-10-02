package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 慢连接隔离：单条连接待发缓冲超限即进入可重新同步状态，
 * 不阻塞其他连接，也不拖慢事件产生方。
 */
class SlowConsumerTest extends GatewayTestBase {

    @BeforeEach
    void grants() {
        authorizer.grant(ALICE, "ch1").grant(BOB, "ch1");
        props.setConnectionBufferCapacity(256);
        props.setChannelReplayWindowMessages(3);
    }

    @Test
    void overflowingConnectionIsPausedAndResyncable() throws Exception {
        var alice = connect(ALICE_TOKEN);
        subscribe(alice, "ch1");

        // 缓冲容量为 256：暂停投递后前 256 条入队，第 257 条触发慢连接状态
        gateway.pauseDrain();
        for (int i = 1; i <= 257; i++) {
            gateway.publish("ch1", "m" + i);
        }
        gateway.resumeDrain();

        List<Frame> frames = new ArrayList<>();
        for (int i = 0; i < 257; i++) {
            frames.add(receive(alice));
        }
        List<Frame> messages = frames.stream()
                .filter(f -> f.getType() == Frame.Type.MESSAGE)
                .toList();
        assertThat(messages).hasSize(256);
        assertThat(messages.get(0).getPayload()).isEqualTo("m1");
        assertThat(messages.get(255).getPayload()).isEqualTo("m256");
        List<Frame> requiredFrames = frames.stream()
                .filter(f -> f.getType() == Frame.Type.RESYNC_REQUIRED)
                .toList();
        assertThat(requiredFrames).hasSize(1);
        assertThat(requiredFrames.get(0).getReason()).isEqualTo("slow_consumer");
        assertThat(requiredFrames.get(0).getChannelId()).isEqualTo("ch1");
        assertNoMoreFrames(alice);

        // 其他连接不受影响
        var bob = connect(BOB_TOKEN);
        subscribe(bob, "ch1");
        gateway.publish("ch1", "m258");
        assertThat(receive(bob).getPayload()).isEqualTo("m258");

        // 事件产生方不被阻塞（快速发布大量消息）
        for (int i = 259; i <= 300; i++) {
            gateway.publish("ch1", "m" + i);
        }
        assertThat(gateway.sessions().activeCount()).isEqualTo(2);

        // alice 从确认点 256 重新同步：窗口只保留最近 3 条，已超出窗口
        alice.send(Frame.type(Frame.Type.RESYNC).setPositions(Map.of("ch1", 256L)));
        Frame beyond = receive(alice);
        assertThat(beyond.getType()).isEqualTo(Frame.Type.RESYNC_REQUIRED);
        assertThat(beyond.getOldestSeq()).isEqualTo(298L);
        assertThat(beyond.getNewestSeq()).isEqualTo(300L);

        // 从窗口边界 297 重新同步：窗口内 3 条消息续传，不超过缓冲容量
        alice.send(Frame.type(Frame.Type.RESYNC).setPositions(Map.of("ch1", 297L)));
        Frame ok = receive(alice);
        assertThat(ok.getType()).isEqualTo(Frame.Type.RESYNC_OK);

        List<String> resumed = new ArrayList<>();
        Frame m;
        while ((m = alice.pollDownlink(200, java.util.concurrent.TimeUnit.MILLISECONDS)) != null) {
            assertThat(m.getType()).isEqualTo(Frame.Type.MESSAGE);
            resumed.add(m.getPayload());
        }
        assertThat(resumed).hasSize(3);
        assertThat(resumed.get(0)).isEqualTo("m298");
        assertThat(resumed.get(1)).isEqualTo("m299");
        assertThat(resumed.get(2)).isEqualTo("m300");

        // bob 持续收到全部消息
        List<String> bobMessages = new ArrayList<>();
        while ((m = bob.pollDownlink(200, java.util.concurrent.TimeUnit.MILLISECONDS)) != null) {
            assertThat(m.getType()).isEqualTo(Frame.Type.MESSAGE);
            bobMessages.add(m.getPayload());
        }
        assertThat(bobMessages).hasSize(42); // m259..m300
        assertThat(bobMessages.get(0)).isEqualTo("m259");
    }
}
