package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 半开连接识别：超过心跳周期未收到任何上行帧即判定死亡并回收，
 * 存活心跳则保持连接。
 */
class HalfOpenConnectionTest extends GatewayTestBase {

    @BeforeEach
    void grants() {
        authorizer.grant(ALICE, "ch1");
    }

    @Test
    void silentConnectionIsReapedAndRetained() throws Exception {
        var c = connect(ALICE_TOKEN);
        subscribe(c, "ch1");

        // 不发任何上行帧，推进超过 3 个心跳周期
        clock.advance(3 * 1_000 + 1);
        gateway.runScheduledTasks();

        Frame close = receive(c);
        assertThat(close.getType()).isEqualTo(Frame.Type.CLOSE);
        assertThat(close.getReason()).isEqualTo(CloseReason.HEARTBEAT_TIMEOUT.wire());

        // 会话进入保留窗口
        assertThat(gateway.sessions().retainedCount()).isEqualTo(1);
        assertThat(gateway.sessions().activeCount()).isZero();

        // 保留超时后会话被清理
        clock.advance(5_001);
        gateway.runScheduledTasks();
        assertThat(gateway.sessions().totalCount()).isZero();
    }

    @Test
    void heartbeatsKeepConnectionAlive() throws Exception {
        var c = connect(ALICE_TOKEN);
        for (int i = 0; i < 10; i++) {
            clock.advance(1_000);
            c.send(Frame.type(Frame.Type.PING));
            gateway.runScheduledTasks();
            Frame pong = receive(c);
            assertThat(pong.getType()).isEqualTo(Frame.Type.PONG);
        }
        assertThat(c.isOpen()).isTrue();
        assertThat(gateway.sessions().activeCount()).isEqualTo(1);
        assertThat(gateway.sessions().retainedCount()).isZero();
    }

    @Test
    void uplinkActivityResetsHeartbeatTimer() throws Exception {
        var c = connect(ALICE_TOKEN);
        // 每 2.5 秒发一次上行（小于 3 秒超时），连接应保持存活
        for (int i = 0; i < 6; i++) {
            clock.advance(2_500);
            c.send(Frame.type(Frame.Type.ACK).setChannelId("ch1").setSeq((long) i));
            gateway.runScheduledTasks();
        }
        assertThat(c.isOpen()).isTrue();
        assertThat(gateway.sessions().activeCount()).isEqualTo(1);
    }
}
