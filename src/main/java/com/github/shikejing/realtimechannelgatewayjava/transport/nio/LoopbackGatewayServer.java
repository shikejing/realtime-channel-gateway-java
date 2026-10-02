package com.github.shikejing.realtimechannelgatewayjava.transport.nio;

import com.github.shikejing.realtimechannelgatewayjava.core.Gateway;
import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * 本地回环 NIO 服务端：绑定 127.0.0.1（端口 0 时使用临时端口），
 * 单 selector 线程处理读写，接受的连接交给网关。
 */
public final class LoopbackGatewayServer {
    private final Gateway gateway;
    private final int requestedPort;

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private Thread selectorThread;
    private volatile boolean running;
    private volatile int actualPort;

    public LoopbackGatewayServer(Gateway gateway, int port) {
        this.gateway = gateway;
        this.requestedPort = port;
    }

    public void start() {
        try {
            selector = Selector.open();
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(new InetSocketAddress("127.0.0.1", requestedPort));
            actualPort = serverChannel.socket().getLocalPort();
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            running = true;
            selectorThread = new Thread(this::loop, "loopback-selector");
            selectorThread.setDaemon(true);
            selectorThread.start();
        } catch (IOException e) {
            throw new IllegalStateException("failed to start loopback server", e);
        }
    }

    public void stop() {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
        if (serverChannel != null) {
            try { serverChannel.close(); } catch (IOException ignore) { }
        }
        if (selectorThread != null) {
            selectorThread.interrupt();
            try { selectorThread.join(2_000); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (selector != null) {
            try { selector.close(); } catch (IOException ignore) { }
        }
    }

    public int actualPort() { return actualPort; }

    private void loop() {
        while (running) {
            try {
                selector.select(200);
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        accept(key);
                    } else if (key.isReadable()) {
                        read(key);
                    } else if (key.isWritable()) {
                        write(key);
                    }
                }
                // 写事件由下行队列驱动：任一连接有待发字节即关注 OP_WRITE
                for (SelectionKey k : selector.keys()) {
                    if (k.isValid() && k.attachment() instanceof NioConnection conn
                            && !conn.writeQueue().isEmpty()) {
                        k.interestOps(k.interestOps() | SelectionKey.OP_WRITE);
                    }
                }
            } catch (IOException | RuntimeException e) {
                if (!running) {
                    break;
                }
                // 单连接异常不应终止 selector 循环；连接级异常在 read/write 内处理
            }
        }
        // 停机前最后刷新一次，确保 CLOSE 帧等已入队帧送达对端
        for (SelectionKey key : selector.keys()) {
            if (key.isValid() && key.attachment() instanceof NioConnection conn
                    && !conn.writeQueue().isEmpty()) {
                try {
                    write(key);
                } catch (IOException ignore) {
                    // 忽略停机阶段的写出异常
                }
            }
        }
    }

    private void accept(SelectionKey key) throws IOException {
        SocketChannel channel = serverChannel.accept();
        if (channel == null) {
            return;
        }
        channel.configureBlocking(false);
        NioConnection connection = new NioConnection(channel, selector::wakeup);
        channel.register(selector, SelectionKey.OP_READ, connection);
        gateway.attach(connection);
    }

    private void read(SelectionKey key) throws IOException {
        NioConnection conn = (NioConnection) key.attachment();
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buf = ByteBuffer.allocate(64 * 1024);
        int n;
        try {
            n = channel.read(buf);
        } catch (IOException e) {
            closeConnection(conn, key, CloseReason.NORMAL);
            return;
        }
        if (n == -1) {
            closeConnection(conn, key, CloseReason.NORMAL);
            return;
        }
        buf.flip();
        for (Frame frame : conn.decoder().decode(buf)) {
            if (conn.listener() != null) {
                conn.listener().onUplink(frame);
            }
        }
        if (conn.closeRequested() && conn.writeQueue().isEmpty()) {
            closeConnection(conn, key, conn.closeReason());
        } else if (!conn.writeQueue().isEmpty()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private void write(SelectionKey key) throws IOException {
        NioConnection conn = (NioConnection) key.attachment();
        SocketChannel channel = (SocketChannel) key.channel();
        while (!conn.writeQueue().isEmpty()) {
            ByteBuffer bb = conn.writeQueue().peek();
            int n = channel.write(bb);
            if (n == 0) {
                break;
            }
            if (!bb.hasRemaining()) {
                conn.writeQueue().poll();
            }
        }
        if (conn.writeQueue().isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            if (conn.closeRequested()) {
                closeConnection(conn, key, conn.closeReason());
            }
        }
    }

    private void closeConnection(NioConnection conn, SelectionKey key, CloseReason reason) {
        if (!conn.markClosed()) {
            return;
        }
        key.cancel();
        try {
            conn.channel().close();
        } catch (IOException ignore) {
        }
        if (conn.listener() != null) {
            conn.listener().onClose(reason);
        }
    }
}
