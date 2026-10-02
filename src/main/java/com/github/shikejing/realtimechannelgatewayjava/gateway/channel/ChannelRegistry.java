package com.github.shikejing.realtimechannelgatewayjava.gateway.channel;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 频道注册表：按需创建频道（日志 + 归一化器），
 * 将原始事件归一化、落日志，并按频道内产生顺序通知监听器。
 *
 * <p>每个频道一把锁：归一化、追加、通知在同一临界区内完成，
 * 保证同一频道的通知顺序与日志顺序一致；监听器必须非阻塞。
 */
public class ChannelRegistry {

    private final int replayWindowSize;
    private final int reorderWindow;
    private final ConcurrentHashMap<String, ChannelState> channels = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Consumer<StoredEventWithChannel>> listeners =
            new CopyOnWriteArrayList<>();

    public ChannelRegistry(int replayWindowSize, int reorderWindow) {
        this.replayWindowSize = replayWindowSize;
        this.reorderWindow = reorderWindow;
    }

    /** 事件产生方入口：归一化后落日志并触发投递。 */
    public void publish(String channel, ChannelEvent raw) {
        ChannelState state = channels.computeIfAbsent(channel, c ->
                new ChannelState(new ChannelLog(c, replayWindowSize), new EventNormalizer(reorderWindow)));
        synchronized (state) {
            List<ChannelEvent> normalized = state.normalizer.accept(raw);
            for (ChannelEvent e : normalized) {
                long seq = state.log.append(e);
                StoredEventWithChannel wrapped =
                        new StoredEventWithChannel(channel, new StoredEvent(seq, e));
                for (Consumer<StoredEventWithChannel> listener : listeners) {
                    listener.accept(wrapped);
                }
            }
        }
    }

    /** 注册新事件监听器（网关投递层）。监听器在频道锁内被调用，必须非阻塞。 */
    public void addListener(Consumer<StoredEventWithChannel> listener) {
        listeners.add(listener);
    }

    public ChannelLog log(String channel) {
        ChannelState state = channels.get(channel);
        return state == null ? null : state.log;
    }

    public record StoredEventWithChannel(String channel, StoredEvent event) {
    }

    private static final class ChannelState {
        final ChannelLog log;
        final EventNormalizer normalizer;

        ChannelState(ChannelLog log, EventNormalizer normalizer) {
            this.log = log;
            this.normalizer = normalizer;
        }
    }
}
