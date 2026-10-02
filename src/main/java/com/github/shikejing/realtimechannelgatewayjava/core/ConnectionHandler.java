package com.github.shikejing.realtimechannelgatewayjava.core;

import com.github.shikejing.realtimechannelgatewayjava.auth.Identity;
import com.github.shikejing.realtimechannelgatewayjava.channel.Channel;
import com.github.shikejing.realtimechannelgatewayjava.channel.ChannelMessage;
import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.session.Session;
import com.github.shikejing.realtimechannelgatewayjava.session.SessionState;
import com.github.shikejing.realtimechannelgatewayjava.session.Subscription;
import com.github.shikejing.realtimechannelgatewayjava.transport.Connection;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单条连接的协议处理与状态机。所有上行控制消息都在这里归一处理，
 * 与数据投递线程隔离，不影响任何频道的投递顺序。
 */
final class ConnectionHandler implements Connection.Listener {
    private static final AtomicLong SESSION_IDS = new AtomicLong();

    private final Gateway gateway;
    private final Connection connection;
    private volatile Session session;

    ConnectionHandler(Gateway gateway, Connection connection) {
        this.gateway = gateway;
        this.connection = connection;
        connection.setListener(this);
    }

    Session session() { return session; }

    @Override public void onUplink(Frame frame) {
        Session s = this.session;
        if (s != null) {
            s.touch(gateway.clock().nowMillis());
        }
        switch (frame.getType()) {
            case CONNECT -> handleConnect(frame);
            case SUBSCRIBE -> handleSubscribe(frame);
            case UNSUBSCRIBE -> handleUnsubscribe(frame);
            case ACK -> handleAck(frame);
            case PING -> connection.send(Frame.type(Frame.Type.PONG));
            case RESYNC -> handleResync(frame);
            default -> sendError("INVALID_FRAME", "unsupported frame type: " + frame.getType());
        }
    }

    @Override public void onClose(CloseReason reason) {
        gateway.connectionClosed(this, reason);
    }

    /** 服务端主动关闭（顶号、停机、心跳超时），走与客户端断开相同的清理路径。 */
    void close(CloseReason reason) {
        connection.close(reason);
    }

    // ---------------- CONNECT 与重连恢复 ----------------

    private void handleConnect(Frame frame) {
        if (session != null) {
            sendError("ALREADY_CONNECTED", "session already established");
            return;
        }
        Optional<Identity> authenticated = gateway.authenticator().authenticate(frame.getToken());
        if (authenticated.isEmpty()) {
            connection.send(Frame.type(Frame.Type.ERROR)
                    .setCode("AUTH_FAILED").setMessage("authentication failed"));
            close(CloseReason.AUTH_FAILED);
            return;
        }
        Identity identity = authenticated.get();
        long now = gateway.clock().nowMillis();

        // 同身份已有活跃连接：顶掉旧连接，旧会话保留并由本次连接恢复，
        // 保证同一客户端只有一个有效会话、订阅关系不丢
        Session previous = gateway.sessions().findActiveByIdentity(identity.principal()).orElse(null);
        if (previous != null) {
            previous.connection().close(CloseReason.REPLACED);
            gateway.sessions().retain(previous, now + gateway.retentionMs());
        }

        Session retained = gateway.sessions().findRetainedByIdentity(identity.principal()).orElse(null);
        Session s;
        boolean resumed;
        if (retained != null && retained.retainedUntilMillis() > now) {
            s = retained;
            resumed = true;
        } else {
            if (retained != null) {
                gateway.sessions().remove(retained);
            }
            s = new Session("s-" + SESSION_IDS.incrementAndGet(), identity, gateway.bufferCapacity());
            resumed = false;
        }
        s.bind(connection);
        s.touch(now);
        gateway.sessions().activate(s);
        this.session = s;

        if (frame.getPositions() != null) {
            frame.getPositions().forEach((channelId, ack) -> {
                Subscription sub = s.subscription(channelId);
                if (sub != null) {
                    sub.advanceAck(ack);
                }
            });
        }
        // 先确认连接建立，再投递重放消息与重同步结论
        connection.send(Frame.type(Frame.Type.CONNECTED)
                .setSessionId(s.sessionId())
                .setResumed(resumed)
                .setHeartbeatIntervalMs(gateway.heartbeatIntervalMs())
                .setRetentionMs(gateway.retentionMs()));
        if (resumed) {
            for (Subscription sub : s.subscriptions()) {
                replayForSubscription(s, sub);
            }
        }
        gateway.wakeDrain();
    }

    // ---------------- 订阅 / 退订 / 确认 ----------------

    private void handleSubscribe(Frame frame) {
        Session s = requireSession();
        if (s == null) {
            return;
        }
        String channelId = frame.getChannelId();
        if (channelId == null) {
            sendError("INVALID_FRAME", "channelId required");
            return;
        }
        if (!gateway.authorizer().canSubscribe(s.identity(), channelId)) {
            // 越权拒绝：给出可区分原因，不建立订阅、不投递任何内容
            connection.send(Frame.type(Frame.Type.ERROR)
                    .setCode("PERMISSION_DENIED")
                    .setChannelId(channelId)
                    .setMessage("subscribe denied for channel: " + channelId));
            return;
        }
        Channel channel = gateway.channels().getOrCreate(channelId);
        long startAck = frame.getLastAck() != null ? frame.getLastAck() : channel.newestSeq();
        Subscription sub = s.subscribe(channelId, startAck);
        channel.addSubscriber(s);
        connection.send(Frame.type(Frame.Type.SUBSCRIBED)
                .setChannelId(channelId)
                .setOldestSeq(channel.oldestSeq())
                .setNewestSeq(channel.newestSeq()));
        replayForSubscription(s, sub);
        gateway.wakeDrain();
    }

    private void handleUnsubscribe(Frame frame) {
        Session s = requireSession();
        if (s == null) {
            return;
        }
        String channelId = frame.getChannelId();
        if (channelId == null) {
            sendError("INVALID_FRAME", "channelId required");
            return;
        }
        s.unsubscribe(channelId);
        Channel channel = gateway.channels().get(channelId);
        if (channel != null) {
            channel.removeSubscriber(s);
        }
        connection.send(Frame.type(Frame.Type.UNSUBSCRIBED).setChannelId(channelId));
    }

    private void handleAck(Frame frame) {
        Session s = requireSession();
        if (s == null) {
            return;
        }
        String channelId = frame.getChannelId();
        Long seq = frame.getSeq();
        if (channelId == null || seq == null) {
            sendError("INVALID_FRAME", "channelId and seq required for ack");
            return;
        }
        Subscription sub = s.subscription(channelId);
        if (sub != null) {
            sub.advanceAck(seq);
        }
    }

    // ---------------- 重新同步 ----------------

    private void handleResync(Frame frame) {
        Session s = requireSession();
        if (s == null) {
            return;
        }
        if (frame.getPositions() != null) {
            frame.getPositions().forEach((channelId, ack) -> {
                Subscription sub = s.subscription(channelId);
                if (sub != null) {
                    sub.advanceAck(ack);
                }
            });
        }
        boolean allClear = true;
        for (Subscription sub : s.subscriptions()) {
            Channel channel = gateway.channels().get(sub.channelId());
            if (channel == null) {
                continue;
            }
            List<ChannelMessage> replay = channel.replayFrom(sub.lastAck());
            if (replay == null) {
                sub.setResyncRequired(true);
                allClear = false;
                sendResyncRequired(channel, "replay_window_exceeded");
            } else {
                sub.setResyncRequired(false);
            }
        }
        // 所有频道都可续传后再恢复投递，避免重放消息被慢连接状态丢弃
        if (allClear) {
            s.setState(SessionState.ACTIVE);
        }
        for (Subscription sub : s.subscriptions()) {
            Channel channel = gateway.channels().get(sub.channelId());
            if (channel == null) {
                continue;
            }
            List<ChannelMessage> replay = channel.replayFrom(sub.lastAck());
            if (replay != null) {
                for (ChannelMessage m : replay) {
                    gateway.enqueueMessage(s, m);
                }
            }
        }
        if (allClear) {
            connection.send(Frame.type(Frame.Type.RESYNC_OK));
        }
        gateway.wakeDrain();
    }

    // ---------------- 投递辅助 ----------------

    private void replayForSubscription(Session s, Subscription sub) {
        Channel channel = gateway.channels().get(sub.channelId());
        if (channel == null) {
            return;
        }
        List<ChannelMessage> replay = channel.replayFrom(sub.lastAck());
        if (replay == null) {
            sub.setResyncRequired(true);
            sendResyncRequired(channel, "replay_window_exceeded");
            return;
        }
        for (ChannelMessage m : replay) {
            gateway.enqueueMessage(s, m);
        }
    }

    private void sendResyncRequired(Channel channel, String reason) {
        connection.send(Frame.type(Frame.Type.RESYNC_REQUIRED)
                .setChannelId(channel.channelId())
                .setOldestSeq(channel.oldestSeq())
                .setNewestSeq(channel.newestSeq())
                .setReason(reason));
    }

    private Session requireSession() {
        Session s = this.session;
        if (s == null) {
            sendError("NOT_CONNECTED", "connect first");
        }
        return s;
    }

    private void sendError(String code, String message) {
        connection.send(Frame.type(Frame.Type.ERROR).setCode(code).setMessage(message));
    }
}
