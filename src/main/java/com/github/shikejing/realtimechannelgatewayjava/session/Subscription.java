package com.github.shikejing.realtimechannelgatewayjava.session;

/**
 * 会话对某频道的订阅状态。
 */
public final class Subscription {
    private final String channelId;
    private volatile long lastAck;
    /** 该频道是否因重放越界而需要重新同步。 */
    private volatile boolean resyncRequired;

    public Subscription(String channelId) {
        this.channelId = channelId;
    }

    public String channelId() { return channelId; }
    public long lastAck() { return lastAck; }
    public boolean isResyncRequired() { return resyncRequired; }
    public void setResyncRequired(boolean resyncRequired) { this.resyncRequired = resyncRequired; }

    /** 推进确认位置，只接受单调递增。 */
    public boolean advanceAck(long seq) {
        if (seq > lastAck) {
            lastAck = seq;
            return true;
        }
        return false;
    }
}
