package com.github.shikejing.realtimechannelgatewayjava.session;

import com.github.shikejing.realtimechannelgatewayjava.auth.Identity;
import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.transport.Connection;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 会话：绑定可校验身份，维护订阅关系、各频道确认位置与有界待发缓冲。
 *
 * <p>一条连接同一时刻只属于一个会话；会话在连接断开后进入保留状态，
 * 由重连恢复，从而保证同一客户端不会被重复计入。
 */
public final class Session {
    private final String sessionId;
    private final Identity identity;
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final BlockingQueue<Frame> outbound;

    private volatile SessionState state = SessionState.ACTIVE;
    private volatile Connection connection;
    private volatile long lastActivityMillis;
    private volatile long retainedUntilMillis;

    public Session(String sessionId, Identity identity, int bufferCapacity) {
        this.sessionId = sessionId;
        this.identity = identity;
        this.outbound = new LinkedBlockingQueue<>(bufferCapacity);
    }

    public String sessionId() { return sessionId; }
    public Identity identity() { return identity; }
    public SessionState state() { return state; }
    public Connection connection() { return connection; }

    public void bind(Connection connection) {
        this.connection = connection;
        this.state = SessionState.ACTIVE;
    }

    public void setState(SessionState state) { this.state = state; }

    public Subscription subscribe(String channelId, Long lastAck) {
        Subscription sub = subscriptions.computeIfAbsent(channelId, Subscription::new);
        if (lastAck != null) {
            sub.advanceAck(lastAck);
        }
        return sub;
    }

    public void unsubscribe(String channelId) {
        subscriptions.remove(channelId);
    }

    public Subscription subscription(String channelId) {
        return subscriptions.get(channelId);
    }

    public Collection<Subscription> subscriptions() {
        return subscriptions.values();
    }

    public boolean offerOutbound(Frame frame) { return outbound.offer(frame); }
    public Frame pollOutbound() { return outbound.poll(); }
    public int outboundSize() { return outbound.size(); }

    public void touch(long nowMillis) { this.lastActivityMillis = nowMillis; }
    public long lastActivityMillis() { return lastActivityMillis; }

    public void retainUntil(long deadlineMillis) {
        this.retainedUntilMillis = deadlineMillis;
        this.state = SessionState.RETAINED;
    }

    public long retainedUntilMillis() { return retainedUntilMillis; }

    /** 仅用于断言：当前订阅的频道标识。 */
    public java.util.Set<String> subscribedChannels() { return subscriptions.keySet(); }

    @Override public String toString() {
        return "Session[" + sessionId + "/" + identity + "/" + state + "]";
    }
}
