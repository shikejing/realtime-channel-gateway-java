package com.github.shikejing.realtimechannelgatewayjava.auth;

import java.util.Optional;

/**
 * 身份校验：客户端连接时携带凭证，服务端校验后返回身份。
 */
public interface Authenticator {
    Optional<Identity> authenticate(String token);
}
