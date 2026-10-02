package com.github.shikejing.realtimechannelgatewayjava.transport;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.session.CloseReason;

/**
 * 传输层连接抽象：网关核心只依赖此接口，测试用内存通道，生产用本地回环 NIO。
 */
public interface Connection {

    /** 发送下行帧；连接已关闭时静默失败或抛出，由实现决定。 */
    void send(Frame downlinkFrame);

    /** 关闭连接并把关闭原因送达对端。 */
    void close(CloseReason reason);

    boolean isOpen();

    String remoteAddress();

    void setListener(Listener listener);

    interface Listener {
        void onUplink(Frame frame);
        void onClose(CloseReason reason);
    }
}
