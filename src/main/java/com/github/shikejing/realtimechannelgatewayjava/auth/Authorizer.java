package com.github.shikejing.realtimechannelgatewayjava.auth;

/**
 * 逐频道鉴权：订阅等上行控制按连接身份校验权限。
 */
public interface Authorizer {
    boolean canSubscribe(Identity identity, String channelId);
}
