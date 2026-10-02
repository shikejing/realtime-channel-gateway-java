package com.github.shikejing.realtimechannelgatewayjava.gateway.clock;

public class SystemClock implements Clock {
    @Override
    public long millis() {
        return System.currentTimeMillis();
    }
}
