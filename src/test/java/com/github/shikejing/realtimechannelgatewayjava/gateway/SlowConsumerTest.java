package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelEvent;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.transport.ConnectionHandle;
import com.github.shikejing.realtimechannelgatewayjava.gateway.transport.InMemoryTransport;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：慢连接——待发缓冲超限按策略处理，且不阻塞其他连接与事件产生方。
 */
class SlowConsumerTest {

    /** 可暂停的发送句柄：模拟对端读取极慢的连接。初始为通畅状态。 */
    private static final class StallableHandle implements ConnectionHandle {
        private final ConnectionHandle delegate;
        private volatile CountDownLatch stall = new CountDownLatch(0);

        StallableHandle(ConnectionHandle delegate) {
            this.delegate = delegate;
        }

        /** 从此刻起阻塞所有发送。 */
        void stall() {
            stall = new CountDownLatch(1);
        }

        void release() {
            stall.countDown();
        }

        @Override
        public String id() {
            return delegate.id();
        }

        @Override
        public void send(ServerMessage message) {
            try {
                stall.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            delegate.send(message);
        }

        @Override
        public void close(CloseReason reason) {
            delegate.close(reason);
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }
    }

    private static final class Rig2 implements AutoCloseable {
        final TestSupport.Rig rig;
        final InMemoryTransport.ClientEndpoint slowClient;
        final StallableHandle slowHandle;

        Rig2(GatewayConfig config) {
            rig = TestSupport.newRig(config);
            rig.authenticator().register("token-alice", "alice");
            rig.authenticator().register("token-bob", "bob");
            rig.authorizer().grant("alice", "ch-a");
            rig.authorizer().grant("bob", "ch-a");
            var conn = InMemoryTransport.connect(rig.gateway());
            slowHandle = new StallableHandle(conn.serverHandle());
            slowClient = conn.client();
            rig.gateway().acceptConnection(slowHandle);
        }

        @Override
        public void close() {
            slowHandle.release();
            rig.close();
        }
    }

    @Test
    void slowConsumerDisconnectedWithoutBlockingOthers() {
        GatewayConfig config = new GatewayConfig(10_000, 30_000, 60_000, 100, 8, 16, SlowConsumerPolicy.DISCONNECT);
        try (Rig2 r = new Rig2(config)) {
            // 正常客户端作为对照。
            var fastClient = r.rig.connect();
            TestSupport.hello(fastClient, "token-bob");
            fastClient.send(new ClientMessage.Subscribe("ch-a", null));
            TestSupport.awaitMessage(fastClient, m -> m instanceof ServerMessage.Subscribed);

            TestSupport.hello(r.slowClient, "token-alice");
            r.slowClient.send(new ClientMessage.Subscribe("ch-a", null));
            TestSupport.awaitMessage(r.slowClient, m -> m instanceof ServerMessage.Subscribed);
            // 订阅完成后连接开始变慢（对端停止读取）。
            r.slowHandle.stall();

            // 分批发布并等正常客户端跟上：慢连接队列（容量 16）必然超限；
            // 发布调用本身不阻塞，正常客户端收全事件。
            for (int batch = 0; batch < 10; batch++) {
                for (int i = batch * 10 + 1; i <= batch * 10 + 10; i++) {
                    r.rig.gateway().publish("ch-a", new ChannelEvent("a-" + i, i, "A" + i));
                }
                int expected = (batch + 1) * 10;
                TestSupport.awaitUntil(
                        () -> TestSupport.eventsOf(fastClient.received(), "ch-a").size() == expected, 5_000);
            }

            // 慢连接被按策略断开，释放后客户端能收到可区分的关闭原因。
            r.slowHandle.release();
            TestSupport.awaitMessage(r.slowClient, m -> m instanceof ServerMessage.Closed c
                    && c.reason().equals(CloseReason.SLOW_CONSUMER.name()));
            TestSupport.awaitUntil(() -> r.rig.gateway().connectionCount() == 1, 5_000);
        }
    }

    @Test
    void slowConsumerDropAndResyncKeepsConnection() {
        GatewayConfig config = new GatewayConfig(10_000, 30_000, 60_000, 100, 8, 8, SlowConsumerPolicy.DROP_AND_RESYNC);
        try (Rig2 r = new Rig2(config)) {
            TestSupport.hello(r.slowClient, "token-alice");
            r.slowClient.send(new ClientMessage.Subscribe("ch-a", null));
            TestSupport.awaitMessage(r.slowClient, m -> m instanceof ServerMessage.Subscribed);
            r.slowHandle.stall();

            for (int i = 1; i <= 100; i++) {
                r.rig.gateway().publish("ch-a", new ChannelEvent("a-" + i, i, "A" + i));
            }
            r.slowHandle.release();

            // 连接保持，但收到明确的重新同步结论。
            TestSupport.awaitMessage(r.slowClient, m -> m instanceof ServerMessage.ResyncRequired rs
                    && rs.channel().equals("ch-a"));
            assertTrue(r.slowClient.isOpen());

            // 后续新事件继续投递，连接进入可重新同步的可用状态。
            r.rig.gateway().publish("ch-a", new ChannelEvent("a-101", 101, "A101"));
            TestSupport.awaitUntil(() -> TestSupport.eventsOf(r.slowClient.received(), "ch-a").stream()
                    .anyMatch(e -> e.seq() == 101), 5_000);
            assertEquals(1, r.rig.gateway().connectionCount());
        }
    }
}
