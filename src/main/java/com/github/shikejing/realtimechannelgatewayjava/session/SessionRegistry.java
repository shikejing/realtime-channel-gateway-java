package com.github.shikejing.realtimechannelgatewayjava.session;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话注册表：按身份保证同一时刻只有一个活跃会话，
 * 保留窗口内的会话可由重连恢复。
 */
public final class SessionRegistry {
    private final ConcurrentHashMap<String, Session> byId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Session> activeByIdentity = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Session> retainedByIdentity = new ConcurrentHashMap<>();

    public void activate(Session session) {
        byId.put(session.sessionId(), session);
        activeByIdentity.put(session.identity().principal(), session);
    }

    public Optional<Session> findActiveByIdentity(String principal) {
        return Optional.ofNullable(activeByIdentity.get(principal));
    }

    public Optional<Session> findRetainedByIdentity(String principal) {
        return Optional.ofNullable(retainedByIdentity.get(principal));
    }

    /** 连接断开：活跃会话转入保留状态。 */
    public void retain(Session session, long deadlineMillis) {
        if (session.state() == SessionState.RETAINED || session.state() == SessionState.CLOSED) {
            return;
        }
        activeByIdentity.remove(session.identity().principal(), session);
        retainedByIdentity.put(session.identity().principal(), session);
        session.retainUntil(deadlineMillis);
    }

    /** 保留超时或顶号清理：移除会话。 */
    public void remove(Session session) {
        activeByIdentity.remove(session.identity().principal(), session);
        retainedByIdentity.remove(session.identity().principal(), session);
        byId.remove(session.sessionId(), session);
        session.setState(SessionState.CLOSED);
    }

    public Collection<Session> all() { return byId.values(); }
    public Collection<Session> activeSessions() { return activeByIdentity.values(); }
    public int activeCount() { return activeByIdentity.size(); }
    public int retainedCount() { return retainedByIdentity.size(); }
    public int totalCount() { return byId.size(); }
}
