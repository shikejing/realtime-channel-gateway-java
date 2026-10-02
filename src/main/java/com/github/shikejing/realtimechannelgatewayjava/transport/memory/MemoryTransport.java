package com.github.shikejing.realtimechannelgatewayjava.transport.memory;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.transport.Connection;

import java.util.concurrent.TimeUnit;

/**
 * 内存传输工厂：创建一对内存连接，服务端侧交给网关，客户端侧留在测试中。
 */
public final class MemoryTransport {

    private MemoryTransport() {}

    public static Pair pair() {
        MemoryConnection serverSide = new MemoryConnection();
        MemoryConnection clientSide = new MemoryConnection();
        // 服务端下行 -> 客户端接收队列
        serverSide.setDownlinkSink(clientSide::deliverDownlink);
        // 客户端上行 -> 服务端监听者
        clientSide.setUplinkSink(serverSide::deliverUplink);
        // 客户端关闭 -> 服务端监听者直接感知（不产生上行帧）
        clientSide.setCloseNotifier(serverSide::notifyPeerClose);
        return new Pair(serverSide, clientSide);
    }

    /**
     * @param serverSide 交给网关的服务端连接
     * @param clientSide 测试持有的客户端连接
     */
    public record Pair(Connection serverSide, MemoryConnection clientSide) {
        public Frame receive(long timeout, TimeUnit unit) throws InterruptedException {
            return clientSide.pollDownlink(timeout, unit);
        }
    }
}
