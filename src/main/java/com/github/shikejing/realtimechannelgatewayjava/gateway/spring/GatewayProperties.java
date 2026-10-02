package com.github.shikejing.realtimechannelgatewayjava.gateway.spring;

import com.github.shikejing.realtimechannelgatewayjava.gateway.GatewayConfig;
import com.github.shikejing.realtimechannelgatewayjava.gateway.SlowConsumerPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 网关配置项（application.properties 前缀 gateway.*）。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private long heartbeatIntervalMs = 10_000;
    private long heartbeatTimeoutMs = 30_000;
    private long sessionRetentionMs = 60_000;
    private int replayWindowSize = 1_000;
    private int reorderWindow = 64;
    private int outboundQueueCapacity = 256;
    private SlowConsumerPolicy slowConsumerPolicy = SlowConsumerPolicy.DISCONNECT;
    /** 令牌 -> 身份主体。 */
    private Map<String, String> tokens = new HashMap<>();
    /** 身份主体 -> 可访问频道列表。 */
    private Map<String, List<String>> grants = new HashMap<>();

    public GatewayConfig toGatewayConfig() {
        return new GatewayConfig(heartbeatIntervalMs, heartbeatTimeoutMs, sessionRetentionMs,
                replayWindowSize, reorderWindow, outboundQueueCapacity, slowConsumerPolicy);
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public long getHeartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    public void setHeartbeatTimeoutMs(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public long getSessionRetentionMs() {
        return sessionRetentionMs;
    }

    public void setSessionRetentionMs(long sessionRetentionMs) {
        this.sessionRetentionMs = sessionRetentionMs;
    }

    public int getReplayWindowSize() {
        return replayWindowSize;
    }

    public void setReplayWindowSize(int replayWindowSize) {
        this.replayWindowSize = replayWindowSize;
    }

    public int getReorderWindow() {
        return reorderWindow;
    }

    public void setReorderWindow(int reorderWindow) {
        this.reorderWindow = reorderWindow;
    }

    public int getOutboundQueueCapacity() {
        return outboundQueueCapacity;
    }

    public void setOutboundQueueCapacity(int outboundQueueCapacity) {
        this.outboundQueueCapacity = outboundQueueCapacity;
    }

    public SlowConsumerPolicy getSlowConsumerPolicy() {
        return slowConsumerPolicy;
    }

    public void setSlowConsumerPolicy(SlowConsumerPolicy slowConsumerPolicy) {
        this.slowConsumerPolicy = slowConsumerPolicy;
    }

    public Map<String, String> getTokens() {
        return tokens;
    }

    public void setTokens(Map<String, String> tokens) {
        this.tokens = tokens;
    }

    public Map<String, List<String>> getGrants() {
        return grants;
    }

    public void setGrants(Map<String, List<String>> grants) {
        this.grants = grants;
    }
}
