package com.github.shikejing.realtimechannelgatewayjava.core;

public final class SystemClock implements Clock {
    public static final SystemClock INSTANCE = new SystemClock();
    private SystemClock() {}
    @Override public long nowMillis() { return System.currentTimeMillis(); }
}
