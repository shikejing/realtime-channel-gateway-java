package com.github.shikejing.realtimechannelgatewayjava.core;

public class GatewayException extends RuntimeException {
    public GatewayException(String message) { super(message); }
    public GatewayException(String message, Throwable cause) { super(message, cause); }
}
