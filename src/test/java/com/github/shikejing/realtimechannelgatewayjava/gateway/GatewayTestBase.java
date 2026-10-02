package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.auth.GrantAuthorizer;
import com.github.shikejing.realtimechannelgatewayjava.auth.Identity;
import com.github.shikejing.realtimechannelgatewayjava.auth.SimpleAuthenticator;
import com.github.shikejing.realtimechannelgatewayjava.config.GatewayProperties;
import com.github.shikejing.realtimechannelgatewayjava.core.Gateway;
import com.github.shikejing.realtimechannelgatewayjava.core.ManualClock;
import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.transport.memory.MemoryConnection;
import com.github.shikejing.realtimechannelgatewayjava.transport.memory.MemoryTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 测试基类：内存通道 + 可控时钟，保证用例可重复、不依赖外部服务。
 */
abstract class GatewayTestBase {
    protected static final Identity ALICE = new Identity("alice");
    protected static final Identity BOB = new Identity("bob");
    protected static final String ALICE_TOKEN = "alice-token";
    protected static final String BOB_TOKEN = "bob-token";

    protected ManualClock clock;
    protected Gateway gateway;
    protected SimpleAuthenticator authenticator;
    protected GrantAuthorizer authorizer;
    protected GatewayProperties props;

    @BeforeEach
    void baseSetUp() {
        clock = new ManualClock(1_000_000L);
        authenticator = new SimpleAuthenticator()
                .addToken(ALICE_TOKEN, ALICE)
                .addToken(BOB_TOKEN, BOB);
        authorizer = new GrantAuthorizer();
        props = new GatewayProperties();
        props.setHeartbeatIntervalMs(1_000);
        props.setHeartbeatMissThreshold(3);
        props.setSessionRetentionMs(5_000);
        props.setChannelReplayWindowMessages(10);
        props.setConnectionBufferCapacity(64);
        props.setLoopbackPort(-1);
        gateway = Gateway.builder()
                .clock(clock)
                .properties(props)
                .authenticator(authenticator)
                .authorizer(authorizer)
                .build();
        gateway.start();
    }

    @AfterEach
    void baseTearDown() {
        gateway.shutdown();
    }

    protected MemoryConnection connect(String token) throws InterruptedException {
        return connectWithPositions(token, null).getValue();
    }

    protected Map.Entry<Frame, MemoryConnection> connectWithPositions(String token, Map<String, Long> positions)
            throws InterruptedException {
        MemoryTransport.Pair pair = MemoryTransport.pair();
        gateway.attach(pair.serverSide());
        Frame frame = Frame.type(Frame.Type.CONNECT).setToken(token);
        if (positions != null) {
            frame.setPositions(positions);
        }
        pair.clientSide().send(frame);
        Frame connected = pair.clientSide().pollDownlink(2, TimeUnit.SECONDS);
        assertThat(connected).isNotNull();
        assertThat(connected.getType()).isEqualTo(Frame.Type.CONNECTED);
        return Map.entry(connected, pair.clientSide());
    }

    protected void subscribe(MemoryConnection c, String channelId) throws InterruptedException {
        c.send(Frame.type(Frame.Type.SUBSCRIBE).setChannelId(channelId));
        Frame f = receive(c);
        assertThat(f.getType()).isEqualTo(Frame.Type.SUBSCRIBED);
        assertThat(f.getChannelId()).isEqualTo(channelId);
    }

    protected Frame receive(MemoryConnection c) throws InterruptedException {
        return c.pollDownlink(2, TimeUnit.SECONDS);
    }

    protected void assertNoMoreFrames(MemoryConnection c) throws InterruptedException {
        assertThat(c.pollDownlink(100, TimeUnit.MILLISECONDS)).isNull();
    }
}
