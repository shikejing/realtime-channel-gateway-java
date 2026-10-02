package com.github.shikejing.realtimechannelgatewayjava.gateway.clock;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** 基于 ScheduledExecutorService 的真实调度器。 */
public class SystemScheduler implements Scheduler {

    private final ScheduledExecutorService executor;

    public SystemScheduler(String threadName) {
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public Cancellable scheduleAtFixedRate(Runnable task, long initialDelayMs, long periodMs) {
        var future = executor.scheduleAtFixedRate(task, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
        return () -> future.cancel(false);
    }

    @Override
    public Cancellable scheduleOnce(Runnable task, long delayMs) {
        var future = executor.schedule(task, delayMs, TimeUnit.MILLISECONDS);
        return () -> future.cancel(false);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
