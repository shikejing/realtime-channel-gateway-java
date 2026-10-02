package com.github.shikejing.realtimechannelgatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {
    private long heartbeatIntervalMs = 5_000;
    private long heartbeatMissThreshold = 3;
    private long sessionRetentionMs = 30_000;
    private int channelReplayWindowMessages = 1_000;
    private int connectionBufferCapacity = 256;
    private int loopbackPort = 0;

    public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) { this.heartbeatIntervalMs = heartbeatIntervalMs; }
    public long getHeartbeatMissThreshold() { return heartbeatMissThreshold; }
    public void setHeartbeatMissThreshold(long heartbeatMissThreshold) { this.heartbeatMissThreshold = heartbeatMissThreshold; }
    public long getSessionRetentionMs() { return sessionRetentionMs; }
    public void setSessionRetentionMs(long sessionRetentionMs) { this.sessionRetentionMs = sessionRetentionMs; }
    public int getChannelReplayWindowMessages() { return channelReplayWindowMessages; }
    public void setChannelReplayWindowMessages(int channelReplayWindowMessages) { this.channelReplayWindowMessages = channelReplayWindowMessages; }
    public int getConnectionBufferCapacity() { return connectionBufferCapacity; }
    public void setConnectionBufferCapacity(int connectionBufferCapacity) { this.connectionBufferCapacity = connectionBufferCapacity; }
    public int getLoopbackPort() { return loopbackPort; }
    public void setLoopbackPort(int loopbackPort) { this.loopbackPort = loopbackPort; }
}
