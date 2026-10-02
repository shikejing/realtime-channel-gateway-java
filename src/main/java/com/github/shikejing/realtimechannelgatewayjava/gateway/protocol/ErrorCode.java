package com.github.shikejing.realtimechannelgatewayjava.gateway.protocol;

/** 可区分的错误原因码。 */
public enum ErrorCode {
    AUTH_FAILED,
    FORBIDDEN,
    SESSION_NOT_FOUND,
    SESSION_EXPIRED,
    RESUME_TOKEN_MISMATCH,
    CHANNEL_UNKNOWN,
    BAD_REQUEST,
    INTERNAL
}
