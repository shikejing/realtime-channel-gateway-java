package com.github.shikejing.realtimechannelgatewayjava.transport.nio;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.protocol.FrameCodec;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.transport.Connection;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于本地回环 NIO SocketChannel 的连接实现。
 * 下行帧进入写队列，由 selector 线程写出；关闭帧入队后随数据一起刷出。
 */
public final class NioConnection implements Connection {
    private final SocketChannel channel;
    private final Runnable wakeup;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    private final FrameDecoder decoder = new FrameDecoder();
    private volatile Listener listener;
    private volatile boolean closeRequested;
    private volatile CloseReason closeReason;

    NioConnection(SocketChannel channel, Runnable wakeup) {
        this.channel = channel;
        this.wakeup = wakeup;
    }

    @Override public void send(Frame frame) {
        if (!open.get()) {
            return;
        }
        writeQueue.offer(ByteBuffer.wrap(FrameCodec.encode(frame)));
        wakeup.run();
    }

    @Override public void close(CloseReason reason) {
        if (!open.get()) {
            return;
        }
        closeReason = reason;
        closeRequested = true;
        send(Frame.type(Frame.Type.CLOSE).setReason(reason.wire()));
        wakeup.run();
    }

    @Override public boolean isOpen() { return open.get(); }
    @Override public String remoteAddress() { return channel.socket().getRemoteSocketAddress().toString(); }
    @Override public void setListener(Listener listener) { this.listener = listener; }

    boolean markClosed() { return open.compareAndSet(true, false); }
    boolean closeRequested() { return closeRequested; }
    CloseReason closeReason() { return closeReason; }
    Queue<ByteBuffer> writeQueue() { return writeQueue; }
    FrameDecoder decoder() { return decoder; }
    SocketChannel channel() { return channel; }
    Listener listener() { return listener; }
}
