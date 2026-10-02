package com.github.shikejing.realtimechannelgatewayjava.gateway.transport;

import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ClientMessage;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.gateway.protocol.ServerMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 进程内内存通道：一对连接端点，客户端侧供测试直接驱动。
 * 关闭幂等，且无论哪侧发起关闭都会回调 onClosed，走同一条清理路径。
 */
public final class InMemoryTransport {

    private InMemoryTransport() {
    }

    /** 客户端测试侧端点。 */
    public interface ClientEndpoint {
        void send(ClientMessage message);

        /** 取走并返回目前收到的全部下行消息。 */
        List<ServerMessage> drainReceived();

        List<ServerMessage> received();

        void disconnect();

        boolean isOpen();
    }

    /** 一条已建立的内存连接：服务端句柄 + 客户端测试端点。 */
    public record Connection(ConnectionHandle serverHandle, ClientEndpoint client) {
    }

    /** 建立一对连接，serverListener 为网关入口。 */
    public static Connection connect(ConnectionListener serverListener) {
        InMemoryHandle handle = new InMemoryHandle(serverListener);
        return new Connection(handle, new InMemoryClientEndpoint(handle, serverListener));
    }

    private static final class InMemoryHandle implements ConnectionHandle {
        private final String id = UUID.randomUUID().toString();
        private final ConnectionListener listener;
        private final List<ServerMessage> inbox = new CopyOnWriteArrayList<>();
        private final AtomicBoolean open = new AtomicBoolean(true);

        InMemoryHandle(ConnectionListener listener) {
            this.listener = listener;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public void send(ServerMessage message) {
            if (open.get()) {
                inbox.add(message);
            }
        }

        @Override
        public void close(CloseReason reason) {
            if (open.compareAndSet(true, false)) {
                listener.onClosed(this, reason);
            }
        }

        @Override
        public boolean isOpen() {
            return open.get();
        }

        List<ServerMessage> inbox() {
            return inbox;
        }
    }

    private static final class InMemoryClientEndpoint implements ClientEndpoint {
        private final InMemoryHandle handle;
        private final ConnectionListener serverListener;

        InMemoryClientEndpoint(InMemoryHandle handle, ConnectionListener serverListener) {
            this.handle = handle;
            this.serverListener = serverListener;
        }

        @Override
        public void send(ClientMessage message) {
            if (handle.isOpen()) {
                serverListener.onMessage(handle, message);
            }
        }

        @Override
        public List<ServerMessage> drainReceived() {
            List<ServerMessage> out = new ArrayList<>(handle.inbox());
            handle.inbox().clear();
            return out;
        }

        @Override
        public List<ServerMessage> received() {
            return List.copyOf(handle.inbox());
        }

        @Override
        public void disconnect() {
            handle.close(CloseReason.CLIENT_BYE);
        }

        @Override
        public boolean isOpen() {
            return handle.isOpen();
        }
    }
}
