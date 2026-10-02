package com.github.shikejing.realtimechannelgatewayjava.core;

/**
 * 可注入的时钟，便于测试中使用可控时钟。
 */
public interface Clock {
    long nowMillis();
}
