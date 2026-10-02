package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelEvent;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：大量连接并发订阅、退订、断连与发布，共享状态安全，
 * 无数据竞争与死锁，最终状态一致。
 */
class ConcurrencyStressTest {

    @Test
    void concurrentSubscribeUnsubscribeDisconnect() throws Exception {
        GatewayConfig config = new GatewayConfig(10_000, 30_000, 60_000, 1_000, 8, 1_024,
                SlowConsumerPolicy.DISCONNECT);
        TestSupport.Rig rig = TestSupport.newRig(config);
        int clients = 40;
        for (int i = 0; i < clients; i++) {
            rig.authenticator().register("token-u" + i, "u" + i);
            rig.authorizer().grant("u" + i, "ch-a", "ch-b");
        }

        List<com.github.shikejing.realtimechannelgatewayjava.gateway.transport.InMemoryTransport.ClientEndpoint>
                endpoints = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            var ep = rig.connect();
            TestSupport.hello(ep, "token-u" + i);
            endpoints.add(ep);
        }

        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch done = new CountDownLatch(clients + 1);
        // 一个线程持续发布两个频道的事件。
        pool.submit(() -> {
            try {
                for (int i = 1; i <= 200; i++) {
                    rig.gateway().publish("ch-a", new ChannelEvent("a-" + i, i, "A" + i));
                    rig.gateway().publish("ch-b", new ChannelEvent("b-" + i, i, "B" + i));
                }
            } finally {
                done.countDown();
            }
        });
        // 各连接并发订阅、退订、确认、断连。
        for (int i = 0; i < clients; i++) {
            var ep = endpoints.get(i);
            int idx = i;
            pool.submit(() -> {
                try {
                    for (int round = 0; round < 20; round++) {
                        ep.send(new ClientMessage.Subscribe("ch-a", null));
                        ep.send(new ClientMessage.Subscribe("ch-b", null));
                        ep.send(new ClientMessage.Ack("ch-a", round));
                        if ((round + idx) % 3 == 0) {
                            ep.send(new ClientMessage.Unsubscribe("ch-b"));
                        }
                    }
                    ep.disconnect();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(60, TimeUnit.SECONDS), "no deadlock: all workers finished");
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        // 最终一致：所有连接都走了统一清理路径。
        TestSupport.awaitUntil(() -> rig.gateway().connectionCount() == 0, 10_000);
        rig.close();
        // 会话全部进入保留窗口或被清理，没有 ACTIVE 泄漏。
        TestSupport.awaitUntil(() -> rig.gateway().sessions().all().stream()
                .noneMatch(s -> s.state().name().equals("ACTIVE")), 10_000);
    }
}
