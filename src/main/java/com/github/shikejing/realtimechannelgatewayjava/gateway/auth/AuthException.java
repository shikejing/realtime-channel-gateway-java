package com.github.shikejing.realtimechannelgatewayjava.gateway.auth;

/** 身份校验失败。 */
public class AuthException extends Exception {
    public AuthException(String message) {
        super(message);
    }
}
