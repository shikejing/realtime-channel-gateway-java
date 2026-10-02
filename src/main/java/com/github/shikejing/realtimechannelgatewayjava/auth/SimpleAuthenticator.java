package com.github.shikejing.realtimechannelgatewayjava.auth;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于共享密钥 token 的身份校验实现。
 */
public final class SimpleAuthenticator implements Authenticator {
    private final Map<String, Identity> tokenToIdentity = new ConcurrentHashMap<>();

    public SimpleAuthenticator() {}

    public SimpleAuthenticator addToken(String token, Identity identity) {
        tokenToIdentity.put(token, identity);
        return this;
    }

    @Override public Optional<Identity> authenticate(String token) {
        return Optional.ofNullable(token).map(tokenToIdentity::get);
    }
}
