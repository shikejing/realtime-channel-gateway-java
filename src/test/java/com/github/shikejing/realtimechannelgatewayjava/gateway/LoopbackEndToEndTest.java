package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.auth.GrantAuthorizer;
import com.github.shikejing.realtimechannelgatewayjava.auth.Identity;
import com.github.shikejing.realtimechannelgatewayjava.auth.SimpleAuthenticator;
import com.github.shikejing.realtimechannelgatewayjava.config.GatewayProperties;
import com.github.shikejing.realtimechannelgatewayjava.core.Gateway;
import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.transport.loopback.LoopbackClient;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 本地回环端到端：真实 Socket 链路（127.0.0.1 临时端口），
 * 验证连接、订阅、推送、心跳与关闭全链路。
 */
class LoopbackEndToEndTest {

    @Test
    void loopbackLinkWorksEndToEnd() throws Exception {
        GatewayProperties props = new GatewayProperties();
        props.setLoopbackPort(0);
        props.setHeartbeatIntervalMs(10_000);
        SimpleAuthenticator auth = new SimpleAuthenticator().addToken("dev-token", new Identity("dev"));
        GrantAuthorizer authz = new GrantAuthorizer().grant(new Identity("dev"), "**");

        Gateway gateway = Gateway.builder()
                .properties(props)
                .authenticator(auth)
                .authorizer(authz)
                .build();
        gateway.start();
        try (LoopbackClient client = new LoopbackClient("127.0.0.1", gateway.actualLoopbackPort())) {
            client.connect();

            client.send(Frame.type(Frame.Type.CONNECT).setToken("dev-token"));
            Frame connected = client.receive(2, TimeUnit.SECONDS);
            assertThat(connected.getType()).isEqualTo(Frame.Type.CONNECTED);
            assertThat(connected.getResumed()).isFalse();

            client.send(Frame.type(Frame.Type.SUBSCRIBE).setChannelId("ch1"));
            assertThat(client.receive(2, TimeUnit.SECONDS).getType()).isEqualTo(Frame.Type.SUBSCRIBED);

            gateway.publish("ch1", "hello-loopback");
            Frame message = client.receive(2, TimeUnit.SECONDS);
            assertThat(message.getType()).isEqualTo(Frame.Type.MESSAGE);
            assertThat(message.getChannelId()).isEqualTo("ch1");
            assertThat(message.getPayload()).isEqualTo("hello-loopback");

            // 心跳往返
            client.send(Frame.type(Frame.Type.PING));
            assertThat(client.receive(2, TimeUnit.SECONDS).getType()).isEqualTo(Frame.Type.PONG);

            // 确认位置上行
            client.send(Frame.type(Frame.Type.ACK).setChannelId("ch1").setSeq(1L));

            // 退订
            client.send(Frame.type(Frame.Type.UNSUBSCRIBE).setChannelId("ch1"));
            Frame unsub = client.receive(2, TimeUnit.SECONDS);
            assertThat(unsub.getType()).isEqualTo(Frame.Type.UNSUBSCRIBED);
            assertThat(unsub.getChannelId()).isEqualTo("ch1");
        } finally {
            gateway.shutdown();
        }

        Thread.sleep(200);
        assertThat(Thread.getAllStackTraces().keySet().stream()
                .map(Thread::getName)
                .filter(n -> n.startsWith("loopback-"))
                .findAny()).isEmpty();
    }
}
