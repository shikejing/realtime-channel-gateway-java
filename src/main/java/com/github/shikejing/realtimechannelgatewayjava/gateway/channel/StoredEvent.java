package com.github.shikejing.realtimechannelgatewayjava.gateway.channel;

/**
 * 落入频道日志后的事件，seq 为频道内单调递增的投递序号。
 */
public record StoredEvent(long seq, ChannelEvent event) {
}
