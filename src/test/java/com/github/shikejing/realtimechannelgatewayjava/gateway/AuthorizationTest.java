package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelEvent;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ErrorCode;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：逐频道鉴权——越权订阅/退订/确认被拒绝且原因可区分；
 * 鉴权失败不泄露频道内容；身份与权限在重连后不可冒用。
 */
class AuthorizationTest {

    private TestSupport.Rig newRig() {
        TestSupport.Rig rig = TestSupport.newRig(GatewayConfig.defaults());
        rig.authenticator().register("token-alice", "alice");
        rig.authenticator().register("token-bob", "bob");
        rig.authorizer().grant("alice", "public-ch", "alice-ch");
        rig.authorizer().grant("bob", "public-ch");
        return rig;
    }

    @Test
    void unauthorizedSubscribeRejectedWithoutLeakingContent() {
        try (TestSupport.Rig rig = newRig()) {
            // 越权目标频道上先放置内容，验证拒绝时不会夹带。
            rig.gateway().publish("alice-ch", new ChannelEvent("s-1", 1, "secret"));

            var bob = rig.connect();
            TestSupport.hello(bob, "token-bob");
            bob.send(new ClientMessage.Subscribe("alice-ch", 0L));

            var rejected = (ServerMessage.SubscribeRejected) TestSupport.awaitMessage(
                    bob, m -> m instanceof ServerMessage.SubscribeRejected);
            assertEquals("alice-ch", rejected.channel());
            assertEquals(ErrorCode.FORBIDDEN.name(), rejected.code());

            // 频道上继续产生事件，越权者一条也收不到。
            rig.gateway().publish("alice-ch", new ChannelEvent("s-2", 2, "secret2"));
            TestSupport.awaitUntil(() -> !bob.received().isEmpty(), 1_000);
            assertTrue(TestSupport.eventsOf(bob.received(), "alice-ch").isEmpty());
        }
    }

    @Test
    void unauthorizedAckAndUnsubscribeRejected() {
        try (TestSupport.Rig rig = newRig()) {
            var bob = rig.connect();
            TestSupport.hello(bob, "token-bob");

            bob.send(new ClientMessage.Ack("alice-ch", 5));
            TestSupport.awaitMessage(bob, m -> m instanceof ServerMessage.Error e
                    && e.code().equals(ErrorCode.FORBIDDEN.name()));

            bob.send(new ClientMessage.Unsubscribe("alice-ch"));
            TestSupport.awaitUntil(() -> bob.received().stream()
                    .filter(m -> m instanceof ServerMessage.Error e
                            && e.code().equals(ErrorCode.FORBIDDEN.name()))
                    .count() == 2, 5_000);
        }
    }

    @Test
    void badTokenRejectedAndConnectionClosed() {
        try (TestSupport.Rig rig = newRig()) {
            var client = rig.connect();
            client.send(new ClientMessage.Hello("forged", null, null, null));
            TestSupport.awaitMessage(client, m -> m instanceof ServerMessage.Error e
                    && e.code().equals(ErrorCode.AUTH_FAILED.name()));
            TestSupport.awaitMessage(client, m -> m instanceof ServerMessage.Closed c
                    && c.reason().equals(CloseReason.AUTH_FAILED.name()));
            TestSupport.awaitUntil(() -> rig.gateway().connectionCount() == 0, 5_000);
        }
    }

    @Test
    void sessionCannotBeHijackedByAnotherIdentity() {
        try (TestSupport.Rig rig = newRig()) {
            var alice = rig.connect();
            var welcome = TestSupport.hello(alice, "token-alice");
            alice.disconnect();
            TestSupport.awaitUntil(() -> rig.gateway().connectionCount() == 0, 5_000);

            // bob 拿到 alice 的 sessionId 与 resumeToken，但身份令牌是自己的：必须拒绝。
            var bob = rig.connect();
            bob.send(new ClientMessage.Hello("token-bob", welcome.sessionId(), welcome.resumeToken(), Map.of()));
            TestSupport.awaitMessage(bob, m -> m instanceof ServerMessage.Error e
                    && e.code().equals(ErrorCode.AUTH_FAILED.name()));

            // alice 本人仍可正常恢复。
            var alice2 = rig.connect();
            alice2.send(new ClientMessage.Hello("token-alice", welcome.sessionId(), welcome.resumeToken(), Map.of()));
            TestSupport.awaitMessage(alice2, m -> m instanceof ServerMessage.Welcome w && w.resumed());
        }
    }
}
