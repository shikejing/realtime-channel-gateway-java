package com.github.shikejing.realtimechannelgatewayjava.gateway;

/** 慢连接待发缓冲超限时的既定策略。 */
public enum SlowConsumerPolicy {
    /** 直接断开该连接，原因 SLOW_CONSUMER，会话进入保留窗口可重连恢复。 */
    DISCONNECT,
    /** 丢弃积压并下发 ResyncRequired，连接保持，进入可重新同步状态。 */
    DROP_AND_RESYNC
}
