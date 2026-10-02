package com.github.shikejing.realtimechannelgatewayjava.config;

import com.github.shikejing.realtimechannelgatewayjava.auth.Authenticator;
import com.github.shikejing.realtimechannelgatewayjava.auth.Authorizer;
import com.github.shikejing.realtimechannelgatewayjava.auth.GrantAuthorizer;
import com.github.shikejing.realtimechannelgatewayjava.auth.Identity;
import com.github.shikejing.realtimechannelgatewayjava.auth.SimpleAuthenticator;
import com.github.shikejing.realtimechannelgatewayjava.core.Gateway;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 网关装配。默认提供开发模式的身份与授权实现：
 * token 为 "dev-token" 的客户端以 dev 身份连接，可访问所有频道。
 * 生产部署替换 Authenticator / Authorizer 实现即可。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfig {

    @Bean
    @ConditionalOnMissingBean(Authenticator.class)
    public Authenticator authenticator() {
        return new SimpleAuthenticator().addToken("dev-token", new Identity("dev"));
    }

    @Bean
    @ConditionalOnMissingBean(Authorizer.class)
    public Authorizer authorizer() {
        return new GrantAuthorizer().grant(new Identity("dev"), "**");
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public Gateway gateway(GatewayProperties props, Authenticator authenticator, Authorizer authorizer) {
        // 独立运行时默认绑定本地回环临时端口（127.0.0.1）
        if (props.getLoopbackPort() < 0) {
            props.setLoopbackPort(0);
        }
        return Gateway.builder()
                .properties(props)
                .authenticator(authenticator)
                .authorizer(authorizer)
                .build();
    }
}
