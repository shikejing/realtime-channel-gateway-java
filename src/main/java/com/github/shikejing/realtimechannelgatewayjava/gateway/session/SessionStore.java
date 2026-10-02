package com.github.shikejing.realtimechannelgatewayjava.gateway.session;

import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Identity;
import com.github.shikejing.realtimechannelgatewayjava.gateway.clock.Clock;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ErrorCode;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话存储：创建、按身份去重、保留窗口内可恢复、超时过期。
 * 所有共享状态由 ConcurrentHashMap 保护，无数据竞争。
 */
public class SessionStore {

    /** 重连恢复失败，携带可区分的原因码。 */
    public static class ResumeException extends RuntimeException {
        private final ErrorCode code;

        public ResumeException(ErrorCode code, String message) {
            super(message);
            this.code = code;
        }

        public ErrorCode code() {
            return code;
        }
    }

    /** 创建结果：新会话 + 被顶替失效的同身份旧会话（可能为 null）。 */
    public record CreateResult(Session session, Session evicted) {
    }

    private final Clock clock;
    private final long retentionMs;
    private final ConcurrentHashMap<String, Session> byId = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();

    public SessionStore(Clock clock, long retentionMs) {
        this.clock = clock;
        this.retentionMs = retentionMs;
    }

    /**
     * 创建新会话；同一身份的旧会话若存在则失效并移除，
     * 保证同一客户端不被重复计入、不留下多个同时有效的会话。
     */
    public CreateResult create(Identity identity) {
        Session evicted = null;
        // 使同身份旧会话失效。
        for (Session s : byId.values()) {
            if (s.identity().subject().equals(identity.subject())
                    && s.state() != Session.State.EXPIRED) {
                s.state(Session.State.EXPIRED);
                byId.remove(s.id(), s);
                evicted = s;
            }
        }
        Session session = new Session(randomHex(16), identity, randomHex(32), clock.millis());
        byId.put(session.id(), session);
        return new CreateResult(session, evicted);
    }

    public Session find(String sessionId) {
        return sessionId == null ? null : byId.get(sessionId);
    }

    /** 断线：会话进入 DETACHED，保留窗口内可恢复。 */
    public void detach(String sessionId) {
        Session s = byId.get(sessionId);
        if (s != null && s.state() == Session.State.ACTIVE) {
            s.state(Session.State.DETACHED);
            s.touch(clock.millis());
        }
    }

    /**
     * 重连恢复：校验 resumeToken，会话回到 ACTIVE。
     * 身份与令牌不匹配、会话不存在或已过期都会被拒绝，且不可冒用。
     */
    public Session resume(String sessionId, String resumeToken) {
        Session s = byId.get(sessionId);
        if (s == null || s.state() == Session.State.EXPIRED) {
            throw new ResumeException(ErrorCode.SESSION_EXPIRED, "session expired or unknown");
        }
        if (resumeToken == null || !constantTimeEquals(s.resumeToken(), resumeToken)) {
            throw new ResumeException(ErrorCode.RESUME_TOKEN_MISMATCH, "resume token mismatch");
        }
        s.state(Session.State.ACTIVE);
        s.touch(clock.millis());
        return s;
    }

    /** 过期清理：超过保留窗口的 DETACHED 会话转为 EXPIRED 并移除。 */
    public int expireDetached() {
        long now = clock.millis();
        int expired = 0;
        for (Session s : byId.values()) {
            if (s.state() == Session.State.DETACHED && now - s.lastActiveMs() >= retentionMs) {
                s.state(Session.State.EXPIRED);
                if (byId.remove(s.id(), s)) {
                    expired++;
                }
            }
        }
        return expired;
    }

    public void remove(String sessionId) {
        byId.remove(sessionId);
    }

    public Collection<Session> all() {
        return List.copyOf(byId.values());
    }

    /** 当前处于 ACTIVE 或 DETACHED 的有效会话数。 */
    public int activeCount() {
        int n = 0;
        for (Session s : byId.values()) {
            if (s.state() != Session.State.EXPIRED) {
                n++;
            }
        }
        return n;
    }

    private String randomHex(int bytes) {
        byte[] buf = new byte[bytes];
        random.nextBytes(buf);
        return HexFormat.of().formatHex(buf);
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        byte[] x = a.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] y = b.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (x.length != y.length) {
            return false;
        }
        int r = 0;
        for (int i = 0; i < x.length; i++) {
            r |= x[i] ^ y[i];
        }
        return r == 0;
    }
}
