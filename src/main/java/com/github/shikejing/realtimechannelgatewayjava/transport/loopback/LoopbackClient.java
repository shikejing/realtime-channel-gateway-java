package com.github.shikejing.realtimechannelgatewayjava.transport.loopback;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.protocol.FrameCodec;
import com.github.shikejing.realtimechannelgatewayjava.transport.nio.FrameDecoder;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 本地回环客户端：通过 Socket 连接 127.0.0.1 网关，提供阻塞式收发 API。
 * 读线程在 close 时一并停止，不遗留后台线程。
 */
public final class LoopbackClient implements Closeable {
    private final String host;
    private final int port;
    private final BlockingQueue<Frame> downlink = new LinkedBlockingQueue<>();

    private Socket socket;
    private Thread readerThread;
    private volatile boolean closed;

    public LoopbackClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        readerThread = new Thread(this::readLoop, "loopback-client-reader");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    private void readLoop() {
        try (InputStream in = socket.getInputStream()) {
            FrameDecoder decoder = new FrameDecoder();
            byte[] buf = new byte[8192];
            while (!closed) {
                int n = in.read(buf);
                if (n == -1) {
                    break;
                }
                ByteBuffer bb = ByteBuffer.wrap(buf, 0, n);
                for (Frame frame : decoder.decode(bb)) {
                    downlink.offer(frame);
                }
            }
        } catch (IOException e) {
            if (!closed) {
                downlink.offer(Frame.type(Frame.Type.CLOSE).setReason("connection_lost"));
            }
        }
    }

    public synchronized void send(Frame frame) throws IOException {
        if (closed || socket == null || socket.isClosed()) {
            throw new IOException("client closed");
        }
        socket.getOutputStream().write(FrameCodec.encode(frame));
        socket.getOutputStream().flush();
    }

    /** 阻塞等待一个下行帧，超时返回 null。 */
    public Frame receive(long timeout, TimeUnit unit) throws InterruptedException {
        return downlink.poll(timeout, unit);
    }

    @Override public void close() {
        closed = true;
        if (socket != null) {
            try { socket.close(); } catch (IOException ignore) { }
        }
        if (readerThread != null) {
            readerThread.interrupt();
        }
    }
}
