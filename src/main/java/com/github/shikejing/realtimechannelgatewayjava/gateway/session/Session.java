package com.github.shikejing.realtimechannelgatewayjava.gateway.session;

import com.github.shikejing.realtimechannelgatewayjava.gateway.auth.Identity;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话：绑定身份与一组订阅关系。断线后在保留窗口内保持 DETACHED，可被重连恢复。
 */
public class Session {

    public enum State {ACTIVE, DETACHED, EXPIRED}

    private final String id;
    private final Identity identity;
    private final String resumeToken;
    private final long createdAtMs;
    private volatile State state;
    private volatile long lastActiveMs;
    private final Map<String, Long> ackedSeqByChannel = new ConcurrentHashMap<>();

    public Session(String id, Identity identity, String resumeToken, long createdAtMs) {
        this.id = id;
        this.identity = identity;
        this.resumeToken = resumeToken;
        this.createdAtMs = createdAtMs;
        this.state = State.ACTIVE;
        this.lastActiveMs = createdAtMs;
    }

    public String id() {
        return id;
    }

    public Identity identity() {
        return identity;
    }

    public String resumeToken() {
        return resumeToken;
    }

    public long createdAtMs() {
        return createdAtMs;
    }

    public State state() {
        return state;
    }

    public void state(State newState) {
        this.state = newState;
    }

    public long lastActiveMs() {
        return lastActiveMs;
    }

    public void touch(long nowMs) {
        this.lastActiveMs = nowMs;
    }

    public Set<String> subscribedChannels() {
        return ackedSeqByChannel.keySet();
    }

    public void addSubscription(String channel, long ackedSeq) {
        ackedSeqByChannel.put(channel, ackedSeq);
    }

    public void removeSubscription(String channel) {
        ackedSeqByChannel.remove(channel);
    }

    public Long ackedSeq(String channel) {
        return ackedSeqByChannel.get(channel);
    }

    public void ack(String channel, long seq) {
        ackedSeqByChannel.computeIfPresent(channel, (c, cur) -> Math.max(cur, seq));
    }
}
