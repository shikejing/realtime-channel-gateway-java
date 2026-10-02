package com.github.shikejing.realtimechannelgatewayjava.channel;

import com.github.shikejing.realtimechannelgatewayjava.session.Session;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 频道：维护有限窗口的归一化消息日志与订阅者集合。
 *
 * <p>归一化语义：事件携带频道内单调序号 seq，乱序到达先进入待缓冲，
 * 连续后再投递；重复事件（eventId 相同或 seq 已处理）直接丢弃。
 * 投递顺序与 seq 顺序一致，与到达顺序无关。
 */
public final class Channel {
    private final String channelId;
    private final int replayWindowMessages;

    /** 乱序到达的消息，按 seq 排序缓存。 */
    private final TreeMap<Long, ChannelMessage> pending = new TreeMap<>();
    /** 已连续投递、仍在重放窗口内的消息。 */
    private final Deque<ChannelMessage> log = new ArrayDeque<>();
    /** 窗口内（含待缓冲）事件标识，用于去重；窗口淘汰时同步移除。 */
    private final Set<String> seenEventIds = new HashSet<>();
    private final Set<Session> subscribers = new HashSet<>();

    private long nextSeq = 1;

    Channel(String channelId, int replayWindowMessages) {
        this.channelId = channelId;
        this.replayWindowMessages = replayWindowMessages;
    }

    public String channelId() { return channelId; }

    /**
     * 发布一条带显式序号的事件。重复事件返回空列表；乱序事件在连续后一次性返回多条。
     */
    public synchronized List<ChannelMessage> publish(String eventId, long seq, String payload) {
        if (eventId != null && seenEventIds.contains(eventId)) {
            return List.of();
        }
        if (seq < nextSeq || pending.containsKey(seq)) {
            return List.of();
        }
        pending.put(seq, new ChannelMessage(seq, eventId, channelId, payload, System.currentTimeMillis()));
        return drainContiguous();
    }

    /** 发布一条自动分配序号的事件（按调用顺序投递）。 */
    public synchronized List<ChannelMessage> publish(String eventId, String payload) {
        return publish(eventId, nextSeq, payload);
    }

    private List<ChannelMessage> drainContiguous() {
        List<ChannelMessage> delivered = new ArrayList<>();
        while (true) {
            ChannelMessage m = pending.remove(nextSeq);
            if (m == null) {
                break;
            }
            if (m.eventId() != null) {
                seenEventIds.add(m.eventId());
            }
            log.addLast(m);
            delivered.add(m);
            nextSeq++;
        }
        trimWindow();
        return delivered;
    }

    private void trimWindow() {
        while (log.size() > replayWindowMessages) {
            ChannelMessage old = log.removeFirst();
            if (old.eventId() != null) {
                seenEventIds.remove(old.eventId());
            }
        }
    }

    /** 窗口内最早一条消息的 seq；窗口为空时返回下一个将产生的 seq。 */
    public synchronized long oldestSeq() {
        ChannelMessage first = log.peekFirst();
        return first == null ? nextSeq : first.seq();
    }

    /** 最新一条已投递消息的 seq；窗口为空时返回 nextSeq - 1。 */
    public synchronized long newestSeq() {
        ChannelMessage last = log.peekLast();
        return last == null ? nextSeq - 1 : last.seq();
    }

    public synchronized long nextSeq() { return nextSeq; }

    /**
     * 重放 lastAck 之后、仍在窗口内的消息。
     *
     * @return 可重放的消息（按 seq 升序）；若确认点已超出窗口下界，返回 null，
     *         调用方应通知客户端需要重新同步。
     */
    public synchronized List<ChannelMessage> replayFrom(long lastAck) {
        if (lastAck < oldestSeq() - 1) {
            return null;
        }
        List<ChannelMessage> out = new ArrayList<>();
        for (ChannelMessage m : log) {
            if (m.seq() > lastAck) {
                out.add(m);
            }
        }
        return out;
    }

    public synchronized boolean addSubscriber(Session session) { return subscribers.add(session); }
    public synchronized boolean removeSubscriber(Session session) { return subscribers.remove(session); }
    public synchronized Set<Session> subscribers() { return new HashSet<>(subscribers); }
    public synchronized int subscriberCount() { return subscribers.size(); }

    // 仅用于测试断言
    synchronized Map<Long, ChannelMessage> pendingView() { return Map.copyOf(pending); }
}
