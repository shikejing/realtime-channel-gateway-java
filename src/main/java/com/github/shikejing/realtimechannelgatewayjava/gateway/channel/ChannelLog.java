package com.github.shikejing.realtimechannelgatewayjava.gateway.channel;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * 单频道追加日志：为归一化后的事件分配单调递增序号（从 1 开始），
 * 只保留有限重放窗口，内存占用与运行时间无关。
 */
public class ChannelLog {

    private final String channel;
    private final int replayWindowSize;
    /** 窗口内事件，队首为 earliest。 */
    private final Deque<StoredEvent> window = new ArrayDeque<>();
    private long latestSeq;

    public ChannelLog(String channel, int replayWindowSize) {
        if (replayWindowSize < 1) {
            throw new IllegalArgumentException("replayWindowSize must be >= 1");
        }
        this.channel = channel;
        this.replayWindowSize = replayWindowSize;
        this.latestSeq = 0;
    }

    public String channel() {
        return channel;
    }

    /** 追加事件并返回分配的序号。 */
    public synchronized long append(ChannelEvent event) {
        latestSeq++;
        window.addLast(new StoredEvent(latestSeq, event));
        while (window.size() > replayWindowSize) {
            window.pollFirst();
        }
        return latestSeq;
    }

    /**
     * 读取 (fromSeqExclusive, latest] 区间的事件，最多 max 条。
     * 调用方需先用 earliestSeq 判断起点是否仍在窗口内。
     */
    public synchronized List<StoredEvent> readFrom(long fromSeqExclusive, int max) {
        List<StoredEvent> out = new ArrayList<>();
        for (StoredEvent e : window) {
            if (e.seq() > fromSeqExclusive) {
                out.add(e);
                if (out.size() >= max) {
                    break;
                }
            }
        }
        return out;
    }

    public synchronized long latestSeq() {
        return latestSeq;
    }

    /** 当前窗口内最小可用序号；早于该序号 - 1 的确认位置无法完整重放。 */
    public synchronized long earliestSeq() {
        StoredEvent first = window.peekFirst();
        return first == null ? latestSeq + 1 : first.seq();
    }

    /** 窗口中是否包含 seq。 */
    public synchronized boolean contains(long seq) {
        return seq >= earliestSeq() && seq <= latestSeq;
    }
}
