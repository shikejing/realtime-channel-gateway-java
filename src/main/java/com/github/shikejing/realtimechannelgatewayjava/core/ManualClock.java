package com.github.shikejing.realtimechannelgatewayjava.core;

/**
 * 手动推进的时钟，测试中可精确控制心跳判定和会话保留窗口。
 */
public final class ManualClock implements Clock {
    private volatile long now;
    public ManualClock(long startMillis) { this.now = startMillis; }
    @Override public long nowMillis() { return now; }
    public void advance(long millis) { now += millis; }
    public void set(long millis) { now = millis; }
}
