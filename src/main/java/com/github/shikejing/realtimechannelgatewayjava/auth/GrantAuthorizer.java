package com.github.shikejing.realtimechannelgatewayjava.auth;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于授权集合的鉴权实现，支持精确匹配与前缀通配（如 "room.*"）。
 */
public final class GrantAuthorizer implements Authorizer {
    private final Map<Identity, Set<String>> grants = new ConcurrentHashMap<>();

    public GrantAuthorizer grant(Identity identity, String channelPattern) {
        grants.computeIfAbsent(identity, k -> ConcurrentHashMap.newKeySet()).add(channelPattern);
        return this;
    }

    @Override public boolean canSubscribe(Identity identity, String channelId) {
        Set<String> set = grants.get(identity);
        if (set == null) return false;
        for (String pattern : set) {
            if (pattern.equals(channelId)) return true;
            if (pattern.endsWith(".*") && channelId.startsWith(pattern.substring(0, pattern.length() - 2) + ".")) return true;
            if (pattern.equals("**")) return true;
        }
        return false;
    }
}
