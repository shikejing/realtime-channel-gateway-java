package com.github.shikejing.realtimechannelgatewayjava.gateway.clock;

/** 可调度的后台任务抽象，停机时统一回收，不遗留线程。 */
public interface Scheduler {
    Cancellable scheduleAtFixedRate(Runnable task, long initialDelayMs, long periodMs);

    Cancellable scheduleOnce(Runnable task, long delayMs);

    /** 停止调度器并等待在途任务结束。 */
    void shutdown();
}
