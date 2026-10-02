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
 * 重连续传：已确认消息不重复下发，未确认消息不丢失，
 * 断线期间消息在重连后按序补发。
 */
class ResumeRedeliveryTest extends GatewayTestBase {

    @BeforeEach
    void grants() {
        authorizer.grant(ALICE, "ch1");
    }

    @Test
    void unackedMessagesAreRedeliveredAfterReconnect() throws Exception {
        var c = connect(ALICE_TOKEN);
        subscribe(c, "ch1");

        gateway.publish("ch1", "m1");
        gateway.publish("ch1", "m2");
        gateway.publish("ch1", "m3");
        assertThat(receive(c).getPayload()).isEqualTo("m1");
        assertThat(receive(c).getPayload()).isEqualTo("m2");
        assertThat(receive(c).getPayload()).isEqualTo("m3");

        // 只确认 m1，m2/m3 未确认
        c.send(Frame.type(Frame.Type.ACK).setChannelId("ch1").setSeq(1L));
        c.close(CloseReason.NORMAL);

        // 断线期间产生的消息
        gateway.publish("ch1", "m4");
        gateway.publish("ch1", "m5");

        // 保留窗口内重连，携带各频道确认位置 ch1=1
        var pair = connectWithPositions(ALICE_TOKEN, Map.of("ch1", 1L));
        assertThat(pair.getKey().getResumed()).isTrue();

        List<String> payloads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Frame f = receive(pair.getValue());
            assertThat(f.getType()).isEqualTo(Frame.Type.MESSAGE);
            payloads.add(f.getPayload());
        }
        assertThat(payloads).containsExactly("m2", "m3", "m4", "m5");
        assertNoMoreFrames(pair.getValue());

        // 同身份只有一个有效会话
        assertThat(gateway.sessions().totalCount()).isEqualTo(1);
        assertThat(gateway.sessions().activeCount()).isEqualTo(1);
    }

    @Test
    void ackedMessagesAreNotReplayed() throws Exception {
        var c = connect(ALICE_TOKEN);
        subscribe(c, "ch1");

        gateway.publish("ch1", "m1");
        gateway.publish("ch1", "m2");
        receive(c);
        receive(c);
        c.send(Frame.type(Frame.Type.ACK).setChannelId("ch1").setSeq(2L));
        c.close(CloseReason.NORMAL);

        var pair = connectWithPositions(ALICE_TOKEN, Map.of("ch1", 2L));
        assertThat(pair.getKey().getResumed()).isTrue();
        assertNoMoreFrames(pair.getValue());
    }

    @Test
    void duplicateLoginReplacesOldConnection() throws Exception {
        var c1 = connect(ALICE_TOKEN);
        subscribe(c1, "ch1");

        // 同身份再次连接：旧连接应收到 REPLACED 关闭帧
        var pair = connectWithPositions(ALICE_TOKEN, Map.of("ch1", 0L));
        Frame close = c1.pollDownlink(2, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(close.getType()).isEqualTo(Frame.Type.CLOSE);
        assertThat(close.getReason()).isEqualTo(CloseReason.REPLACED.wire());

        // 新连接恢复会话，旧连接已失效
        assertThat(pair.getKey().getResumed()).isTrue();
        assertThat(gateway.sessions().activeCount()).isEqualTo(1);
        assertThat(gateway.sessions().totalCount()).isEqualTo(1);
    }
}
