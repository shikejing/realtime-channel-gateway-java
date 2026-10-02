package com.github.shikejing.realtimechannelgatewayjava.auth;

/**
 * 连接绑定的可校验身份。
 */
public record Identity(String principal) {
    @Override public String toString() { return principal; }
}
