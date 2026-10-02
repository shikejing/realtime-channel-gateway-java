package com.github.shikejing.realtimechannelgatewayjava.gateway.channel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 将底层可能重复、乱序的原始事件流归一化为严格有序、无重复的流。
 *
 * <p>去重：按 eventId 与 sourceSeq 双重去重，保留有限窗口，内存有界。
 * 乱序：按 sourceSeq 在有限窗口内重排；窗口被填满时强制输出最小序号，
 * 容忍产生方永久空洞，保证流不会卡死。
 */
public class EventNormalizer {

    private final int reorderWindow;
    /** 期望的下一个 sourceSeq。 */
    private long expectedSeq;
    /** 乱序缓冲：sourceSeq -> event。 */
    private final TreeMap<Long, ChannelEvent> pending = new TreeMap<>();
    /** 最近见过的 eventId，用于去重；LRU 淘汰，容量有界。 */
    private final LinkedHashMap<String, Boolean> seenIds;

    public EventNormalizer(int reorderWindow) {
        if (reorderWindow < 1) {
            throw new IllegalArgumentException("reorderWindow must be >= 1");
        }
        this.reorderWindow = reorderWindow;
        this.expectedSeq = 1;
        int dedupeCapacity = reorderWindow * 4;
        this.seenIds = new LinkedHashMap<>(dedupeCapacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > dedupeCapacity;
            }
        };
    }

    /**
     * 接收一个原始事件，返回本次可归一化输出的事件（按 sourceSeq 升序，可能为空）。
     */
    public synchronized List<ChannelEvent> accept(ChannelEvent raw) {
        List<ChannelEvent> out = new ArrayList<>();
        if (raw == null || raw.eventId() == null) {
            return out;
        }
        // 重复事件（底层重发）：直接丢弃。
        if (seenIds.put(raw.eventId(), Boolean.TRUE) != null) {
            return out;
        }
        if (raw.sourceSeq() < expectedSeq) {
            // 该位置已输出过，属于底层重复投递。
            return out;
        }
        if (raw.sourceSeq() == expectedSeq) {
            out.add(raw);
            expectedSeq++;
            drainContiguous(out);
        } else {
            // 乱序到达，先缓存；putIfAbsent 语义防止同序号覆盖。
            pending.putIfAbsent(raw.sourceSeq(), raw);
            if (pending.size() > reorderWindow) {
                // 窗口已满：产生方可能跳号，强制输出最小序号以免卡死。
                flushLowest(out);
            }
        }
        return out;
    }

    private void drainContiguous(List<ChannelEvent> out) {
        while (!pending.isEmpty() && pending.firstKey() == expectedSeq) {
            out.add(pending.pollFirstEntry().getValue());
            expectedSeq++;
        }
    }

    private void flushLowest(List<ChannelEvent> out) {
        while (pending.size() > reorderWindow / 2 && !pending.isEmpty()) {
            var entry = pending.pollFirstEntry();
            out.add(entry.getValue());
            expectedSeq = entry.getKey() + 1;
            drainContiguous(out);
        }
    }
}
