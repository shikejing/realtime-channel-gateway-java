package com.github.shikejing.realtimechannelgatewayjava.gateway.auth;

/** 按连接身份对逐频道操作鉴权。 */
public interface Authorizer {

    record Decision(boolean allowed, String reason) {
        public static Decision allow() {
            return new Decision(true, null);
        }

        public static Decision deny(String reason) {
            return new Decision(false, reason);
        }
    }

    Decision authorize(Identity identity, String channel, ChannelOperation operation);
}
