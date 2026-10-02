package com.github.shikejing.realtimechannelgatewayjava.core;

import com.github.shikejing.realtimechannelgatewayjava.auth.Authenticator;
import com.github.shikejing.realtimechannelgatewayjava.auth.Authorizer;
import com.github.shikejing.realtimechannelgatewayjava.channel.Channel;
import com.github.shikejing.realtimechannelgatewayjava.channel.ChannelMessage;
import com.github.shikejing.realtimechannelgatewayjava.channel.ChannelRegistry;
import com.github.shikejing.realtimechannelgatewayjava.config.GatewayProperties;
import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.session.Session;
import com.github.shikejing.realtimechannelgatewayjava.session.SessionRegistry;
import com.github.shikejing.realtimechannelgatewayjava.session.SessionState;
import com.github.shikejing.realtimechannelgatewayjava.transport.Connection;
import com.github.shikejing.realtimechannelgatewayjava.transport.nio.LoopbackGatewayServer;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 网关核心：会话、频道、鉴权、投递与生命周期管理。
 *
 * <p>线程模型：
 * <ul>
 *   <li>事件产生方调用 publish，仅写入频道日志并扇入各连接的有界待发缓冲，不阻塞；</li>
 *   <li>单条 drain 线程负责把待发缓冲写入传输层，慢连接只影响自己的队列；</li>
 *   <li>单条调度线程负责心跳判定与会话保留窗口清理，测试中可手动触发。</li>
 * </ul>
 */
public final class Gateway {
    private final GatewayProperties properties;
    private final Clock clock;
    private final Authenticator authenticator;
    private final Authorizer authorizer;
    private final ChannelRegistry channels = new ChannelRegistry();
    private final SessionRegistry sessions = new SessionRegistry();
    private final AtomicLong eventIds = new AtomicLong();

    private LoopbackGatewayServer loopbackServer;
    private Thread drainThread;
    private ScheduledExecutorService scheduler;
    private volatile boolean running;
    private volatile boolean drainPaused;

    Gateway(Builder b) {
        this.properties = b.properties;
        this.clock = b.clock;
        this.authenticator = b.authenticator;
        this.authorizer = b.authorizer;
        this.channels.replayWindowMessages(b.properties::getChannelReplayWindowMessages);
    }

    public void start() {
        running = true;
        drainThread = new Thread(this::drainLoop, "gateway-drain");
        drainThread.setDaemon(true);
        drainThread.start();

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gateway-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::runScheduledTasks,
                properties.getHeartbeatIntervalMs(), properties.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);

        if (properties.getLoopbackPort() >= 0) {
            loopbackServer = new LoopbackGatewayServer(this, properties.getLoopbackPort());
            loopbackServer.start();
        }
    }

    public void shutdown() {
        if (!running) {
            return;
        }
        running = false;

        // 1) 先停止推送，再关闭连接，保证客户端能收到可区分的关闭原因
        for (Session s : sessions.all()) {
            if (s.state() == SessionState.ACTIVE || s.state() == SessionState.RESYNC_REQUIRED) {
                s.setState(SessionState.CLOSED);
                if (s.connection() != null) {
                    s.connection().close(CloseReason.SHUTDOWN);
                }
            }
        }
        // 2) 停止接收新连接
        if (loopbackServer != null) {
            loopbackServer.stop();
            loopbackServer = null;
        }
        // 3) 停止后台线程，不遗留任务
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        if (drainThread != null) {
            drainThread.interrupt();
            try {
                drainThread.join(3_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            drainThread = null;
        }
        // 4) 清理保留会话与订阅引用
        for (Session s : sessions.all()) {
            detachSubscriptions(s);
            sessions.remove(s);
        }
    }

    // ---------------- 事件入口 ----------------

    public void publish(String channelId, String payload) {
        Channel channel = channels.getOrCreate(channelId);
        List<ChannelMessage> delivered = channel.publish("evt-" + eventIds.incrementAndGet(), payload);
        fanout(channel, delivered);
    }

    public void publish(String channelId, String eventId, long seq, String payload) {
        Channel channel = channels.getOrCreate(channelId);
        List<ChannelMessage> delivered = channel.publish(eventId, seq, payload);
        fanout(channel, delivered);
    }

    private void fanout(Channel channel, List<ChannelMessage> messages) {
        if (messages.isEmpty()) {
            return;
        }
        for (Session session : channel.subscribers()) {
            if (session.state() == SessionState.CLOSED) {
                continue;
            }
            var sub = session.subscription(channel.channelId());
            if (sub == null || sub.isResyncRequired()) {
                continue;
            }
            for (ChannelMessage m : messages) {
                enqueueMessage(session, m);
            }
        }
        wakeDrain();
    }

    /**
     * 把消息放入连接的有界待发缓冲。缓冲超限则让该连接进入可重新同步状态，
     * 不阻塞事件产生方，也不影响其他连接。
     */
    void enqueueMessage(Session session, ChannelMessage m) {
        // 仅向活跃连接投递；保留/关闭/重同步等待中的会话不入队，
        // 断线期间的消息在重连后通过重放补发
        if (session.state() != SessionState.ACTIVE) {
            return;
        }
        Frame frame = Frame.type(Frame.Type.MESSAGE)
                .setChannelId(m.channelId())
                .setSeq(m.seq())
                .setEventId(m.eventId())
                .setPayload(m.payload());
        if (!session.offerOutbound(frame)) {
            session.setState(SessionState.RESYNC_REQUIRED);
            Channel channel = channels.get(m.channelId());
            session.connection().send(Frame.type(Frame.Type.RESYNC_REQUIRED)
                    .setChannelId(m.channelId())
                    .setOldestSeq(channel == null ? null : channel.oldestSeq())
                    .setNewestSeq(channel == null ? null : channel.newestSeq())
                    .setReason("slow_consumer"));
        }
    }

    // ---------------- 投递线程 ----------------

    private void drainLoop() {
        while (running) {
            if (drainPaused) {
                LockSupport.parkNanos(1_000_000L);
                continue;
            }
            boolean progressed = false;
            for (Session s : sessions.all()) {
                if (s.state() == SessionState.CLOSED
                        || s.state() == SessionState.RETAINED
                        || s.connection() == null) {
                    continue;
                }
                Frame frame = s.pollOutbound();
                if (frame != null) {
                    try {
                        s.connection().send(frame);
                    } catch (Exception e) {
                        connectionClosed(s, CloseReason.NORMAL);
                    }
                    progressed = true;
                }
            }
            if (!progressed) {
                LockSupport.parkNanos(5_000_000L);
            }
        }
    }

    /** 测试钩子：暂停/恢复投递线程，使缓冲超限场景可重复。 */
    public void pauseDrain() { drainPaused = true; }
    public void resumeDrain() { drainPaused = false; wakeDrain(); }

    void wakeDrain() {
        Thread t = drainThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    // ---------------- 心跳与保留窗口（可由测试手动触发） ----------------

    public void runScheduledTasks() {
        checkHeartbeats();
        expireRetainedSessions();
    }

    private void checkHeartbeats() {
        long now = clock.nowMillis();
        long timeoutMs = properties.getHeartbeatIntervalMs() * properties.getHeartbeatMissThreshold();
        for (Session s : sessions.activeSessions()) {
            if (now - s.lastActivityMillis() > timeoutMs) {
                s.connection().close(CloseReason.HEARTBEAT_TIMEOUT);
            }
        }
    }

    private void expireRetainedSessions() {
        long now = clock.nowMillis();
        for (Session s : List.copyOf(sessions.all())) {
            if (s.state() == SessionState.RETAINED && s.retainedUntilMillis() <= now) {
                detachSubscriptions(s);
                sessions.remove(s);
            }
        }
    }

    // ---------------- 连接生命周期 ----------------

    void connectionClosed(ConnectionHandler handler, CloseReason reason) {
        connectionClosed(handler.session(), reason);
    }

    private void connectionClosed(Session session, CloseReason reason) {
        if (session == null) {
            return;
        }
        if (session.state() == SessionState.ACTIVE || session.state() == SessionState.RESYNC_REQUIRED) {
            sessions.retain(session, clock.nowMillis() + properties.getSessionRetentionMs());
        }
    }

    private void detachSubscriptions(Session session) {
        for (var sub : session.subscriptions()) {
            Channel channel = channels.get(sub.channelId());
            if (channel != null) {
                channel.removeSubscriber(session);
            }
        }
    }

    public void attach(Connection connection) {
        new ConnectionHandler(this, connection);
    }

    public int actualLoopbackPort() {
        return loopbackServer == null ? -1 : loopbackServer.actualPort();
    }

    public ChannelRegistry channels() { return channels; }
    public SessionRegistry sessions() { return sessions; }
    public Clock clock() { return clock; }
    public GatewayProperties properties() { return properties; }
    public Authenticator authenticator() { return authenticator; }
    public Authorizer authorizer() { return authorizer; }
    public long heartbeatIntervalMs() { return properties.getHeartbeatIntervalMs(); }
    public long retentionMs() { return properties.getSessionRetentionMs(); }
    public int bufferCapacity() { return properties.getConnectionBufferCapacity(); }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private GatewayProperties properties = new GatewayProperties();
        private Clock clock = SystemClock.INSTANCE;
        private Authenticator authenticator;
        private Authorizer authorizer;

        public Builder properties(GatewayProperties p) { this.properties = p; return this; }
        public Builder clock(Clock c) { this.clock = c; return this; }
        public Builder authenticator(Authenticator a) { this.authenticator = a; return this; }
        public Builder authorizer(Authorizer a) { this.authorizer = a; return this; }

        public Gateway build() {
            if (authenticator == null) throw new IllegalStateException("authenticator required");
            if (authorizer == null) throw new IllegalStateException("authorizer required");
            return new Gateway(this);
        }
    }
}
