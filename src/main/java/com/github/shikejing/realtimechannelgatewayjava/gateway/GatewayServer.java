package com.github.shikejing.realtimechannelgatewayjava.gateway;

import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.AuthException;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Authenticator;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Authorizer;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.ChannelOperation;
import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Identity;
import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelEvent;
import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelLog;
import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.ChannelRegistry;
import com.github.shikejing.realtimechannelgatewayjava.gateway.channel.StoredEvent;
import com.github.shikejing.realtimechannelgatewayjava.gateway.clock.Cancellable;
import com.github.shikejing.realtimechannelgatewayjava.gateway.clock.Clock;
import com.github.shikejing.realtimechannelgatewayjava.gateway.clock.Scheduler;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ErrorCode;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.session.Session;
import com.github.shikejing.realtimechannelgatewayjava.gateway.session.SessionStore;
import com.github.shikejing.realtimechannelgatewayjava.gateway.transport.ConnectionHandle;
import com.github.shikejing.realtimechannelgatewayjava.gateway.transport.ConnectionListener;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 网关核心：传输无关。管理连接、会话、订阅、投递、心跳与停机。
 *
 * <p>并发模型：
 * <ul>
 *   <li>每条连接一个有界待发队列 + 一个发送循环（慢连接只影响自身）；</li>
 *   <li>同一频道的订阅变更、重放与事件扇出在同一把频道锁内完成，
 *       保证单频道投递顺序与产生顺序一致，上行控制消息不会打乱投递顺序；</li>
 *   <li>事件产生方只做非阻塞入队，永不被慢连接拖住。</li>
 * </ul>
 */
public class GatewayServer implements ConnectionListener, AutoCloseable {

    /** 单条连接的上下文。 */
    private static final class ConnectionContext {
        final ConnectionHandle handle;
        final BlockingQueue<ServerMessage> outbound;
        volatile Session session;
        volatile long lastSeenMs;
        volatile boolean closeRequested;
        volatile CloseReason closeReason = CloseReason.SERVER_SHUTDOWN;

        ConnectionContext(ConnectionHandle handle, int capacity, long nowMs) {
            this.handle = handle;
            this.outbound = new ArrayBlockingQueue<>(capacity);
            this.lastSeenMs = nowMs;
        }
    }

    private final GatewayConfig config;
    private final Authenticator authenticator;
    private final Authorizer authorizer;
    private final Clock clock;
    private final Scheduler scheduler;
    private final SessionStore sessions;
    private final ChannelRegistry channels;

    private final Map<String, ConnectionContext> connections = new ConcurrentHashMap<>();
    private final Map<String, ConnectionContext> activeBySession = new ConcurrentHashMap<>();
    /** 每频道一把锁：订阅变更 / 重放 / 扇出的互斥与排序。 */
    private final Map<String, Object> channelLocks = new ConcurrentHashMap<>();
    private final ExecutorService senderExecutor;
    private final java.util.concurrent.atomic.AtomicInteger activeSenders =
            new java.util.concurrent.atomic.AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Cancellable heartbeatTask;

    public GatewayServer(GatewayConfig config,
                         Authenticator authenticator,
                         Authorizer authorizer,
                         Clock clock,
                         Scheduler scheduler) {
        this.config = config;
        this.authenticator = authenticator;
        this.authorizer = authorizer;
        this.clock = clock;
        this.scheduler = scheduler;
        this.sessions = new SessionStore(clock, config.sessionRetentionMs());
        this.channels = new ChannelRegistry(config.replayWindowSize(), config.reorderWindow());
        this.channels.addListener(this::fanout);
        this.senderExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "gateway-sender");
            t.setDaemon(true);
            return t;
        });
        this.heartbeatTask = scheduler.scheduleAtFixedRate(
                this::heartbeatTick, config.heartbeatIntervalMs(), config.heartbeatIntervalMs());
    }

    /** 传输层接入新连接。 */
    public void acceptConnection(ConnectionHandle connection) {
        if (!running.get()) {
            connection.close(CloseReason.SERVER_SHUTDOWN);
            return;
        }
        ConnectionContext ctx = new ConnectionContext(
                connection, config.outboundQueueCapacity(), clock.millis());
        connections.put(connection.id(), ctx);
        senderExecutor.submit(() -> senderLoop(ctx));
    }

    /** 事件产生方入口：非阻塞。 */
    public void publish(String channel, ChannelEvent raw) {
        if (running.get()) {
            channels.publish(channel, raw);
        }
    }

    // ------------------------------------------------------------------
    // 上行控制消息
    // ------------------------------------------------------------------

    @Override
    public void onMessage(ConnectionHandle connection, ClientMessage message) {
        ConnectionContext ctx = connections.get(connection.id());
        if (ctx == null || ctx.closeRequested) {
            return;
        }
        ctx.lastSeenMs = clock.millis();
        try {
            if (message instanceof ClientMessage.Hello hello) {
                handleHello(ctx, hello);
            } else if (message instanceof ClientMessage.Subscribe sub) {
                handleSubscribe(ctx, sub);
            } else if (message instanceof ClientMessage.Unsubscribe unsub) {
                handleUnsubscribe(ctx, unsub);
            } else if (message instanceof ClientMessage.Ack ack) {
                handleAck(ctx, ack);
            } else if (message instanceof ClientMessage.Ping ping) {
                enqueue(ctx, new ServerMessage.Pong(ping.clientTime()));
            } else if (message instanceof ClientMessage.Bye) {
                closeConnection(ctx, CloseReason.CLIENT_BYE);
            }
        } catch (RuntimeException e) {
            enqueue(ctx, new ServerMessage.Error(ErrorCode.INTERNAL.name(), "internal error"));
        }
    }

    private void handleHello(ConnectionContext ctx, ClientMessage.Hello hello) {
        if (ctx.session != null) {
            enqueue(ctx, new ServerMessage.Error(ErrorCode.BAD_REQUEST.name(), "already authenticated"));
            return;
        }
        Identity identity;
        try {
            identity = authenticator.authenticate(hello.token());
        } catch (AuthException e) {
            enqueue(ctx, new ServerMessage.Error(ErrorCode.AUTH_FAILED.name(), e.getMessage()));
            closeConnection(ctx, CloseReason.AUTH_FAILED);
            return;
        }
        if (hello.sessionId() != null) {
            resumeSession(ctx, hello, identity);
        } else {
            createSession(ctx, identity);
        }
    }

    private void createSession(ConnectionContext ctx, Identity identity) {
        SessionStore.CreateResult result = sessions.create(identity);
        // 同身份旧会话被顶替：若仍有连接在线，踢掉旧连接，避免重复计入。
        if (result.evicted() != null) {
            ConnectionContext old = activeBySession.remove(result.evicted().id());
            if (old != null) {
                closeConnection(old, CloseReason.SESSION_TAKEN_OVER);
            }
        }
        bind(ctx, result.session());
        enqueue(ctx, new ServerMessage.Welcome(result.session().id(), result.session().resumeToken(), false));
    }

    private void resumeSession(ConnectionContext ctx, ClientMessage.Hello hello, Identity identity) {
        Session session;
        try {
            session = sessions.resume(hello.sessionId(), hello.resumeToken());
        } catch (SessionStore.ResumeException e) {
            enqueue(ctx, new ServerMessage.Error(e.code().name(), e.getMessage()));
            return;
        }
        // 恢复令牌只证明会话归属；身份必须与令牌身份一致，防止冒用。
        if (!session.identity().subject().equals(identity.subject())) {
            enqueue(ctx, new ServerMessage.Error(ErrorCode.AUTH_FAILED.name(), "identity mismatch"));
            return;
        }
        // 同一会话的旧连接若仍在线，踢掉，保证同一时刻只有一条有效连接。
        ConnectionContext old = activeBySession.get(session.id());
        if (old != null && old != ctx) {
            closeConnection(old, CloseReason.SESSION_TAKEN_OVER);
        }
        bind(ctx, session);
        enqueue(ctx, new ServerMessage.Welcome(session.id(), session.resumeToken(), true));
        // 按客户端上报的各频道确认位置重放；未上报的频道用服务端记录的位置。
        for (String channel : session.subscribedChannels()) {
            Long clientAck = hello.lastAcked() == null ? null : hello.lastAcked().get(channel);
            long fromSeq = clientAck != null ? clientAck
                    : (session.ackedSeq(channel) != null ? session.ackedSeq(channel) : 0L);
            replay(ctx, channel, fromSeq);
        }
    }

    private void bind(ConnectionContext ctx, Session session) {
        ctx.session = session;
        session.touch(clock.millis());
        activeBySession.put(session.id(), ctx);
    }

    private void handleSubscribe(ConnectionContext ctx, ClientMessage.Subscribe sub) {
        Session session = requireSession(ctx);
        if (session == null) {
            return;
        }
        // 先鉴权：越权订阅直接拒绝，不触碰频道任何状态，避免泄露。
        var decision = authorizer.authorize(session.identity(), sub.channel(), ChannelOperation.SUBSCRIBE);
        if (!decision.allowed()) {
            enqueue(ctx, new ServerMessage.SubscribeRejected(
                    sub.channel(), ErrorCode.FORBIDDEN.name(), decision.reason()));
            return;
        }
        Object lock = channelLocks.computeIfAbsent(sub.channel(), c -> new Object());
        synchronized (lock) {
            ChannelLog log = channels.log(sub.channel());
            long latest = log == null ? 0 : log.latestSeq();
            long fromSeq;
            if (sub.lastAckedSeq() == null) {
                fromSeq = latest; // 默认从最新开始，不重放历史。
            } else {
                fromSeq = Math.max(0, sub.lastAckedSeq());
            }
            session.addSubscription(sub.channel(), fromSeq);
            enqueue(ctx, new ServerMessage.Subscribed(sub.channel(), fromSeq));
            replayLocked(ctx, sub.channel(), log, fromSeq);
        }
    }

    private void handleUnsubscribe(ConnectionContext ctx, ClientMessage.Unsubscribe unsub) {
        Session session = requireSession(ctx);
        if (session == null) {
            return;
        }
        var decision = authorizer.authorize(session.identity(), unsub.channel(), ChannelOperation.UNSUBSCRIBE);
        if (!decision.allowed()) {
            enqueue(ctx, new ServerMessage.Error(ErrorCode.FORBIDDEN.name(), decision.reason()));
            return;
        }
        Object lock = channelLocks.computeIfAbsent(unsub.channel(), c -> new Object());
        synchronized (lock) {
            session.removeSubscription(unsub.channel());
            enqueue(ctx, new ServerMessage.Unsubscribed(unsub.channel()));
        }
    }

    private void handleAck(ConnectionContext ctx, ClientMessage.Ack ack) {
        Session session = requireSession(ctx);
        if (session == null) {
            return;
        }
        var decision = authorizer.authorize(session.identity(), ack.channel(), ChannelOperation.ACK);
        if (!decision.allowed()) {
            enqueue(ctx, new ServerMessage.Error(ErrorCode.FORBIDDEN.name(), decision.reason()));
            return;
        }
        session.ack(ack.channel(), ack.seq());
    }

    private Session requireSession(ConnectionContext ctx) {
        Session session = ctx.session;
        if (session == null) {
            enqueue(ctx, new ServerMessage.Error(ErrorCode.BAD_REQUEST.name(), "hello first"));
        }
        return session;
    }

    // ------------------------------------------------------------------
    // 投递
    // ------------------------------------------------------------------

    /** 频道新事件扇出：在频道锁内按产生顺序入队到各订阅连接。 */
    private void fanout(ChannelRegistry.StoredEventWithChannel swe) {
        if (!running.get()) {
            return;
        }
        Object lock = channelLocks.computeIfAbsent(swe.channel(), c -> new Object());
        synchronized (lock) {
            for (ConnectionContext ctx : connections.values()) {
                Session s = ctx.session;
                if (s != null && !ctx.closeRequested && s.ackedSeq(swe.channel()) != null) {
                    enqueue(ctx, new ServerMessage.Event(
                            swe.channel(), swe.event().seq(), swe.event().event().payload()));
                }
            }
        }
    }

    /** 重放 (fromSeq, latest]；窗口不足时给出明确的重新同步结论。 */
    private void replay(ConnectionContext ctx, String channel, long fromSeq) {
        Object lock = channelLocks.computeIfAbsent(channel, c -> new Object());
        synchronized (lock) {
            replayLocked(ctx, channel, channels.log(channel), fromSeq);
        }
    }

    private void replayLocked(ConnectionContext ctx, String channel, ChannelLog log, long fromSeq) {
        if (log == null) {
            return; // 频道尚无事件，无需重放。
        }
        long earliest = log.earliestSeq();
        if (fromSeq < earliest - 1) {
            enqueue(ctx, new ServerMessage.ResyncRequired(
                    channel, earliest, "ack position outside replay window"));
            return;
        }
        for (StoredEvent e : log.readFrom(fromSeq, config.replayWindowSize())) {
            enqueue(ctx, new ServerMessage.Event(channel, e.seq(), e.event().payload()));
        }
    }

    /**
     * 非阻塞入队。队列满时按既定策略处理慢连接：
     * DISCONNECT 断开；DROP_AND_RESYNC 清空积压并通知重新同步。
     */
    private boolean enqueue(ConnectionContext ctx, ServerMessage msg) {
        if (ctx.closeRequested) {
            return false;
        }
        if (ctx.outbound.offer(msg)) {
            return true;
        }
        onSlowConsumer(ctx, msg);
        return false;
    }

    private void onSlowConsumer(ConnectionContext ctx, ServerMessage dropped) {
        if (config.slowConsumerPolicy() == SlowConsumerPolicy.DISCONNECT) {
            closeConnection(ctx, CloseReason.SLOW_CONSUMER);
        } else {
            ctx.outbound.clear();
            String channel = dropped instanceof ServerMessage.Event e ? e.channel() : "";
            ChannelLog log = channel.isEmpty() ? null : channels.log(channel);
            long earliest = log == null ? 0 : log.earliestSeq();
            ctx.outbound.offer(new ServerMessage.ResyncRequired(
                    channel, earliest, "slow consumer: backlog dropped"));
        }
    }

    // ------------------------------------------------------------------
    // 关闭与清理（所有路径汇聚于此）
    // ------------------------------------------------------------------

    /** 发起关闭：标记后由发送循环完成最后的 Closed 通知与传输关闭。 */
    private void closeConnection(ConnectionContext ctx, CloseReason reason) {
        ctx.closeReason = reason;
        ctx.closeRequested = true;
    }

    @Override
    public void onClosed(ConnectionHandle connection, CloseReason reason) {
        ConnectionContext ctx = connections.remove(connection.id());
        if (ctx == null) {
            return;
        }
        ctx.closeReason = reason;
        ctx.closeRequested = true;
        Session session = ctx.session;
        if (session != null) {
            activeBySession.remove(session.id(), ctx);
            // 会话进入保留窗口；已被顶替/过期的会话不受影响。
            sessions.detach(session.id());
        }
    }

    /** 发送循环：独占该连接的下行写出，慢连接只阻塞本线程。 */
    private void senderLoop(ConnectionContext ctx) {
        activeSenders.incrementAndGet();
        try {
            while (!ctx.closeRequested) {
                ServerMessage msg = ctx.outbound.poll(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    ctx.handle.send(msg);
                }
            }
            // 收尾前排空残留消息（如关闭前的 Error 通知），保证客户端能收到。
            ServerMessage rest;
            while ((rest = ctx.outbound.poll()) != null) {
                ctx.handle.send(rest);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // 统一收尾：尽力通知客户端可区分的关闭原因，再关闭传输。
            if (ctx.handle.isOpen()) {
                ctx.handle.send(new ServerMessage.Closed(ctx.closeReason.name()));
                ctx.handle.close(ctx.closeReason);
            }
            activeSenders.decrementAndGet();
        }
    }

    // ------------------------------------------------------------------
    // 心跳与过期
    // ------------------------------------------------------------------

    private void heartbeatTick() {
        long now = clock.millis();
        for (ConnectionContext ctx : connections.values()) {
            if (now - ctx.lastSeenMs > config.heartbeatTimeoutMs()) {
                closeConnection(ctx, CloseReason.HEARTBEAT_TIMEOUT);
            }
        }
        sessions.expireDetached();
    }

    // ------------------------------------------------------------------
    // 停机
    // ------------------------------------------------------------------

    public SessionStore sessions() {
        return sessions;
    }

    public ChannelRegistry channels() {
        return channels;
    }

    public int connectionCount() {
        return connections.size();
    }

    /** 仍在运行的发送循环数；停机后应归零，用于验证不遗留后台线程。 */
    public int activeSenderCount() {
        return activeSenders.get();
    }

    /** 停机：停止接收新事件与新连接，通知全部客户端关闭原因，回收后台任务与线程。 */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        heartbeatTask.cancel();
        for (ConnectionContext ctx : connections.values()) {
            closeConnection(ctx, CloseReason.SERVER_SHUTDOWN);
        }
        senderExecutor.shutdown();
        try {
            if (!senderExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                senderExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            senderExecutor.shutdownNow();
        }
        scheduler.shutdown();
    }
}
