package com.github.shikejing.realtimechannelgatewayjava.gateway.auth;

/** 将客户端令牌校验为身份。 */
public interface Authenticator {
    Identity authenticate(String token) throws AuthException;
}
