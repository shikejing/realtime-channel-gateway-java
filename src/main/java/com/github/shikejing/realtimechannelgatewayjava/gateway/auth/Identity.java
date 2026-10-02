package com.github.shikejing.realtimechannelgatewayjava.gateway.auth;

import java.util.Map;
import java.util.Set;

/**
 * 连接绑定的可校验身份。
 *
 * @param subject    身份主体（用户/客户端标识）
 * @param attributes 附加声明，供鉴权器使用
 */
public record Identity(String subject, Map<String, String> attributes) {

    public Identity {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    public static Identity of(String subject) {
        return new Identity(subject, Map.of());
    }
}
