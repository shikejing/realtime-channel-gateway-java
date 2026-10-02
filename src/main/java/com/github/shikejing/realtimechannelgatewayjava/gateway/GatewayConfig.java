package com.github.shikejing.realtimechannelgatewayjava.gateway;

/**
 * 网关配置。
 */
public record GatewayConfig(
        long heartbeatIntervalMs,
        long heartbeatTimeoutMs,
        long sessionRetentionMs,
        int replayWindowSize,
        int reorderWindow,
        int outboundQueueCapacity,
        SlowConsumerPolicy slowConsumerPolicy) {

    public static GatewayConfig defaults() {
        return new GatewayConfig(
                10_000,   // heartbeatIntervalMs
                30_000,   // heartbeatTimeoutMs
                60_000,   // sessionRetentionMs
                1_000,    // replayWindowSize
                64,       // reorderWindow
                256,      // outboundQueueCapacity
                SlowConsumerPolicy.DISCONNECT);
    }
}
