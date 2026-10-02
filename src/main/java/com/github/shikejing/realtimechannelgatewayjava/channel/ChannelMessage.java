package com.github.shikejing.realtimechannelgatewayjava.channel;

/**
 * 频道内一条已归一化的消息。
 *
 * @param seq       频道内单调递增序号（投递顺序）
 * @param eventId   事件唯一标识，用于去重
 * @param channelId 所属频道
 * @param payload   消息内容
 * @param eventTime 事件产生时间
 */
public record ChannelMessage(long seq, String eventId, String channelId, String payload, long eventTime) {
}
