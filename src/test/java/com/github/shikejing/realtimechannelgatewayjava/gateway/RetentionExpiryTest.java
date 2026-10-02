package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 会话保留窗口：窗口内重连恢复会话，窗口超时后按新会话处理。
 */
class RetentionExpiryTest extends GatewayTestBase {

    @BeforeEach
    void grants() {
        authorizer.grant(ALICE, "ch1");
    }

    @Test
    void reconnectWithinWindowResumesSession() throws Exception {
        var c = connect(ALICE_TOKEN);
        subscribe(c, "ch1");
        c.close(CloseReason.NORMAL);

        clock.advance(4_999);
        gateway.runScheduledTasks();
        assertThat(gateway.sessions().retainedCount()).isEqualTo(1);

        var pair = connectWithPositions(ALICE_TOKEN, Map.of("ch1", 0L));
        assertThat(pair.getKey().getResumed()).isTrue();
        assertThat(gateway.sessions().totalCount()).isEqualTo(1);
    }

    @Test
    void reconnectAfterWindowStartsFreshSession() throws Exception {
        var pair1 = connectWithPositions(ALICE_TOKEN, null);
        subscribe(pair1.getValue(), "ch1");
        String oldSessionId = pair1.getKey().getSessionId();
        pair1.getValue().close(CloseReason.NORMAL);

        clock.advance(5_001);
        gateway.runScheduledTasks();
        assertThat(gateway.sessions().totalCount()).isZero();

        // 超时后携带旧位置重连：视为全新会话，需要重新订阅
        var pair2 = connectWithPositions(ALICE_TOKEN, Map.of("ch1", 1L));
        assertThat(pair2.getKey().getResumed()).isFalse();
        assertThat(pair2.getKey().getSessionId()).isNotEqualTo(oldSessionId);
        assertThat(gateway.sessions().totalCount()).isEqualTo(1);

        // 旧订阅关系已不存在，需重新订阅后才能收到消息
        subscribe(pair2.getValue(), "ch1");
        gateway.publish("ch1", "fresh");
        assertThat(receive(pair2.getValue()).getPayload()).isEqualTo("fresh");
    }
}
