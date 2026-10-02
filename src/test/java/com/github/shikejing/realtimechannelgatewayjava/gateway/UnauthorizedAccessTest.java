package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 逐频道鉴权：越权操作被拒绝并给出可区分原因，
 * 无权访问频道的内容不泄露，身份在重连后不可冒用。
 */
class UnauthorizedAccessTest extends GatewayTestBase {

    @BeforeEach
    void grants() {
        authorizer.grant(ALICE, "ch1");
    }

    @Test
    void unauthorizedSubscribeIsRejected() throws Exception {
        var c = connect(ALICE_TOKEN);
        c.send(Frame.type(Frame.Type.SUBSCRIBE).setChannelId("ch2"));

        Frame err = receive(c);
        assertThat(err.getType()).isEqualTo(Frame.Type.ERROR);
        assertThat(err.getCode()).isEqualTo("PERMISSION_DENIED");
        assertThat(err.getChannelId()).isEqualTo("ch2");

        // 无权频道的消息不投递、不泄露
        gateway.publish("ch2", "secret");
        assertNoMoreFrames(c);

        // 已有权频道的订阅不受影响
        subscribe(c, "ch1");
        gateway.publish("ch1", "ok");
        assertThat(receive(c).getPayload()).isEqualTo("ok");
    }

    @Test
    void wrongTokenIsRejected() throws Exception {
        var pair = connectRaw("bad-token");
        Frame err = pair.getKey();
        assertThat(err.getType()).isEqualTo(Frame.Type.ERROR);
        assertThat(err.getCode()).isEqualTo("AUTH_FAILED");
        Frame close = pair.getValue().pollDownlink(2, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(close.getType()).isEqualTo(Frame.Type.CLOSE);
        assertThat(close.getReason()).isEqualTo(CloseReason.AUTH_FAILED.wire());
        assertThat(gateway.sessions().totalCount()).isZero();
    }

    @Test
    void identityCannotBeImpersonatedOnReconnect() throws Exception {
        authorizer.grant(BOB, "ch1");

        var alice = connect(ALICE_TOKEN);
        subscribe(alice, "ch1");
        alice.close(CloseReason.NORMAL);

        // bob 携带自己的 token 重连，不能恢复 alice 的会话；
        // 携带的 alice 频道位置对 bob 无效，需自行订阅
        var bobPair = connectWithPositions(BOB_TOKEN, Map.of("ch1", 1L));
        assertThat(bobPair.getKey().getResumed()).isFalse();
        subscribe(bobPair.getValue(), "ch1");
        gateway.publish("ch1", "bob-only");
        assertThat(receive(bobPair.getValue()).getPayload()).isEqualTo("bob-only");

        // alice 重连恢复的是自己的会话，未确认消息不丢失
        var alicePair = connectWithPositions(ALICE_TOKEN, Map.of("ch1", 0L));
        assertThat(alicePair.getKey().getResumed()).isTrue();
        assertThat(gateway.sessions().totalCount()).isEqualTo(2);
    }

    @Test
    void subscriptionGrantedAfterConnectTakesEffect() throws Exception {
        var c = connect(ALICE_TOKEN);
        c.send(Frame.type(Frame.Type.SUBSCRIBE).setChannelId("ch2"));
        assertThat(receive(c).getCode()).isEqualTo("PERMISSION_DENIED");

        // 运行时追加授权后即可订阅
        authorizer.grant(ALICE, "ch2");
        subscribe(c, "ch2");
        gateway.publish("ch2", "now-allowed");
        assertThat(receive(c).getPayload()).isEqualTo("now-allowed");
    }

    private Map.Entry<Frame, com.github.shikejing.realtimechannelgatewayjava.transport.memory.MemoryConnection>
            connectRaw(String token) throws InterruptedException {
        var pair = com.github.shikejing.realtimechannelgatewayjava.transport.memory.MemoryTransport.pair();
        gateway.attach(pair.serverSide());
        pair.clientSide().send(Frame.type(Frame.Type.CONNECT).setToken(token));
        Frame err = pair.clientSide().pollDownlink(2, java.util.concurrent.TimeUnit.SECONDS);
        return Map.entry(err, pair.clientSide());
    }
}
