package com.github.shikejing.realtimechannelgatewayjava.gateway.clock;

/** 可替换的时钟，测试用可控时钟保证可重复。 */
public interface Clock {
    long millis();
}
