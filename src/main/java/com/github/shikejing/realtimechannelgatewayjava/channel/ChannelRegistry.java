package com.github.shikejing.realtimechannelgatewayjava.channel;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

public final class ChannelRegistry {
    private final ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();
    private volatile IntSupplier replayWindowMessages = () -> 1_000;

    /** 延迟读取窗口配置，使配置变更对后续创建的频道生效。 */
    public ChannelRegistry replayWindowMessages(IntSupplier replayWindowMessages) {
        this.replayWindowMessages = replayWindowMessages;
        return this;
    }

    public Channel getOrCreate(String channelId) {
        return channels.computeIfAbsent(channelId, id -> new Channel(id, replayWindowMessages.getAsInt()));
    }

    public Channel get(String channelId) { return channels.get(channelId); }
    public Collection<Channel> channels() { return channels.values(); }
}
