package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：停机——按序结束推送，客户端收到可区分的关闭原因，
 * 后台任务与线程全部回收，不遗留悬挂订阅。
 */
class ShutdownTest {

    @Test
    void shutdownNotifiesClientsAndReclaimsResources() {
        TestSupport.Rig rig = TestSupport.newRig(GatewayConfig.defaults());
        rig.authenticator().register("token-alice", "alice");
        rig.authenticator().register("token-bob", "bob");
        rig.authorizer().grant("alice", "ch-a");
        rig.authorizer().grant("bob", "ch-a");

        var alice = rig.connect();
        TestSupport.hello(alice, "token-alice");
        alice.send(new ClientMessage.Subscribe("ch-a", null));
        TestSupport.awaitMessage(alice, m -> m instanceof ServerMessage.Subscribed);
        var bob = rig.connect();
        TestSupport.hello(bob, "token-bob");
        bob.send(new ClientMessage.Subscribe("ch-a", null));
        TestSupport.awaitMessage(bob, m -> m instanceof ServerMessage.Subscribed);
        assertEquals(2, rig.gateway().connectionCount());

        assertTrue(rig.gateway().activeSenderCount() >= 2);

        // 停机：两个客户端都收到可区分的关闭原因。
        rig.close();
        TestSupport.awaitMessage(alice, m -> m instanceof ServerMessage.Closed c
                && c.reason().equals(CloseReason.SERVER_SHUTDOWN.name()));
        TestSupport.awaitMessage(bob, m -> m instanceof ServerMessage.Closed c
                && c.reason().equals(CloseReason.SERVER_SHUTDOWN.name()));

        // 后台任务与线程回收：调度器已停，本网关的发送循环全部退出。
        assertTrue(rig.time().isShutdown());
        TestSupport.awaitUntil(() -> rig.gateway().activeSenderCount() == 0, 5_000);
        assertEquals(0, rig.gateway().connectionCount());
    }

    @Test
    void publishAfterShutdownIsIgnored() {
        TestSupport.Rig rig = TestSupport.newRig(GatewayConfig.defaults());
        rig.authenticator().register("token-alice", "alice");
        rig.authorizer().grant("alice", "ch-a");
        var alice = rig.connect();
        TestSupport.hello(alice, "token-alice");
        alice.send(new ClientMessage.Subscribe("ch-a", null));
        TestSupport.awaitMessage(alice, m -> m instanceof ServerMessage.Subscribed);
        rig.close();

        alice.drainReceived();
        rig.gateway().publish("ch-a", new com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelEvent("x", 1, "X"));
        // 停机后不再产生任何投递。
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertTrue(TestSupport.eventsOf(alice.received(), "ch-a").isEmpty());
    }
}
