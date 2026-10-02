package com.github.shikejing.realtimechannelgatewayjava.session;

public enum SessionState {
    /** 连接活跃，正常投递 */
    ACTIVE,
    /** 待发缓冲超限或重放越界，已暂停投递，等待客户端重新同步 */
    RESYNC_REQUIRED,
    /** 连接已断开，会话在保留窗口内等待重连 */
    RETAINED,
    /** 会话已结束（关闭或保留超时） */
    CLOSED
}
