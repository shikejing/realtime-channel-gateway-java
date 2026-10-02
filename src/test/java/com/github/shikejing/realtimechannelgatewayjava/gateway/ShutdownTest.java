package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 停机清理：按既定顺序结束推送，客户端收到可区分的关闭原因，
 * 不遗留后台线程与会话。
 */
class ShutdownTest extends GatewayTestBase {

    @BeforeEach
    void grants() {
        authorizer.grant(ALICE, "ch1").grant(BOB, "ch1");
    }

    @Test
    void shutdownClosesAllConnectionsWithDistinctReason() throws Exception {
        var a = connect(ALICE_TOKEN);
        var b = connect(BOB_TOKEN);
        subscribe(a, "ch1");

        gateway.shutdown();

        Frame ca = a.pollDownlink(2, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(ca.getType()).isEqualTo(Frame.Type.CLOSE);
        assertThat(ca.getReason()).isEqualTo(CloseReason.SHUTDOWN.wire());
        Frame cb = b.pollDownlink(2, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(cb.getType()).isEqualTo(Frame.Type.CLOSE);
        assertThat(cb.getReason()).isEqualTo(CloseReason.SHUTDOWN.wire());

        assertThat(a.isOpen()).isFalse();
        assertThat(b.isOpen()).isFalse();
        assertThat(gateway.sessions().totalCount()).isZero();
    }

    @Test
    void noWorkerThreadsLeftAfterShutdown() throws Exception {
        connect(ALICE_TOKEN);
        gateway.shutdown();

        // 等待线程退出后断言
        Thread.sleep(200);
        Set<String> names = Thread.getAllStackTraces().keySet().stream()
                .map(Thread::getName)
                .collect(Collectors.toSet());
        assertThat(names).noneMatch(n -> n.startsWith("gateway-") || n.startsWith("loopback-"));
    }
}
