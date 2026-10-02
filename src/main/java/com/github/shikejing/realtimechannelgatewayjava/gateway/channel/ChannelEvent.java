package com.github.shikejing.realtimechannelgatewayjava.gateway.channel;

/**
 * 事件产生方提交的原始事件。
 *
 * @param eventId   全局唯一事件标识，用于去重
 * @param sourceSeq 产生方侧序号，用于乱序归一化
 * @param payload   业务负载
 */
public record ChannelEvent(String eventId, long sourceSeq, String payload) {
}
