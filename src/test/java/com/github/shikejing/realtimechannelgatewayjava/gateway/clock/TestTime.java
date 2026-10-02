package com.github.shikejing.realtimechannelgatewayjava.gateway.clock;

import java.util.PriorityQueue;

/**
 * 可控时钟 + 手动调度器：时间只在测试显式推进时流动，
 * 到期任务按时间顺序同步执行，保证测试可重复。
 */
public class TestTime implements Clock, Scheduler {

    private static final class Task implements Comparable<Task> {
        final Runnable runnable;
        long nextRunMs;
        final long periodMs; // <=0 表示一次性
        boolean cancelled;

        Task(Runnable runnable, long nextRunMs, long periodMs) {
            this.runnable = runnable;
            this.nextRunMs = nextRunMs;
            this.periodMs = periodMs;
        }

        @Override
        public int compareTo(Task o) {
            return Long.compare(nextRunMs, o.nextRunMs);
        }
    }

    private long nowMs;
    private final PriorityQueue<Task> tasks = new PriorityQueue<>();
    private boolean shutdown;

    public TestTime(long startMs) {
        this.nowMs = startMs;
    }

    @Override
    public synchronized long millis() {
        return nowMs;
    }

    @Override
    public synchronized Cancellable scheduleAtFixedRate(Runnable task, long initialDelayMs, long periodMs) {
        Task t = new Task(task, nowMs + initialDelayMs, periodMs);
        tasks.add(t);
        return () -> t.cancelled = true;
    }

    @Override
    public synchronized Cancellable scheduleOnce(Runnable task, long delayMs) {
        Task t = new Task(task, nowMs + delayMs, 0);
        tasks.add(t);
        return () -> t.cancelled = true;
    }

    /** 推进时间并同步执行所有到期任务。 */
    public void advanceBy(long deltaMs) {
        advanceTo(nowMs() + deltaMs);
    }

    public void advanceTo(long targetMs) {
        while (true) {
            Task next;
            synchronized (this) {
                if (tasks.isEmpty() || tasks.peek().nextRunMs > targetMs) {
                    nowMs = targetMs;
                    return;
                }
                next = tasks.poll();
                nowMs = next.nextRunMs;
            }
            if (!next.cancelled) {
                next.runnable.run();
                if (next.periodMs > 0 && !next.cancelled) {
                    synchronized (this) {
                        next.nextRunMs += next.periodMs;
                        tasks.add(next);
                    }
                }
            }
        }
    }

    private synchronized long nowMs() {
        return nowMs;
    }

    @Override
    public synchronized void shutdown() {
        shutdown = true;
        tasks.clear();
    }

    public synchronized boolean isShutdown() {
        return shutdown;
    }
}
