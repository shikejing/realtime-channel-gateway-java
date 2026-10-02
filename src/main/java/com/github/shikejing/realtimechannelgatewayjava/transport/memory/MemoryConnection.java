package com.github.shikejing.realtimechannelgatewayjava.transport.memory;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;
import com.github.shikejing.realtimechannelgatewayjava.transport.Connection;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 内存通道连接：用有界队列模拟一条双向连接，测试中可重复、可控。
 *
 * <p>不对称语义：
 * <ul>
 *   <li>服务端侧 send：下行帧经 downlinkSink 进入客户端接收队列；</li>
 *   <li>客户端侧 send：上行帧经 uplinkSink 投递给服务端监听者；</li>
 *   <li>服务端关闭：CLOSE 帧送达客户端，同时服务端监听者收到 onClose；</li>
 *   <li>客户端关闭：不产生上行帧，仅通知服务端监听者 onClose。</li>
 * </ul>
 */
public class MemoryConnection implements Connection {
    private final BlockingQueue<Frame> downlink = new LinkedBlockingQueue<>();
    private final AtomicBoolean open = new AtomicBoolean(true);

    private volatile Listener listener;
    private volatile Consumer<Frame> downlinkSink;
    private volatile Consumer<Frame> uplinkSink;
    private volatile Consumer<CloseReason> closeNotifier;

    @Override public void send(Frame frame) {
        if (!open.get()) {
            return;
        }
        Consumer<Frame> sink = downlinkSink != null ? downlinkSink : uplinkSink;
        if (sink != null) {
            sink.accept(frame);
        } else {
            downlink.offer(frame);
        }
    }

    @Override public void close(CloseReason reason) {
        if (open.compareAndSet(true, false)) {
            if (downlinkSink != null) {
                // 服务端侧关闭：CLOSE 帧送达客户端
                downlinkSink.accept(Frame.type(Frame.Type.CLOSE).setReason(reason.wire()));
            }
            Listener l = listener;
            if (l != null) {
                l.onClose(reason);
            }
            if (closeNotifier != null) {
                closeNotifier.accept(reason);
            }
        }
    }

    @Override public boolean isOpen() { return open.get(); }
    @Override public String remoteAddress() { return "memory:loopback"; }
    @Override public void setListener(Listener listener) { this.listener = listener; }

    void setDownlinkSink(Consumer<Frame> downlinkSink) { this.downlinkSink = downlinkSink; }
    void setUplinkSink(Consumer<Frame> uplinkSink) { this.uplinkSink = uplinkSink; }
    void setCloseNotifier(Consumer<CloseReason> closeNotifier) { this.closeNotifier = closeNotifier; }

    /** 对端连接关闭时，通知本端监听者。 */
    void notifyPeerClose(CloseReason reason) {
        Listener l = listener;
        if (l != null) {
            l.onClose(reason);
        }
    }

    /** 对端连接发来的上行帧投递给本端监听者。 */
    void deliverUplink(Frame frame) {
        Listener l = listener;
        if (l != null && open.get()) {
            l.onUplink(frame);
        }
    }

    /** 对端连接发来的下行帧进入本端接收队列；收到 CLOSE 帧时本端同步关闭。 */
    void deliverDownlink(Frame frame) {
        if (frame.getType() == Frame.Type.CLOSE) {
            open.set(false);
        }
        downlink.offer(frame);
    }

    public Frame pollDownlink(long timeout, TimeUnit unit) throws InterruptedException {
        return downlink.poll(timeout, unit);
    }
}
