package com.github.shikejing.realtimechannelgatewayjava.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 帧编解码：长度前缀（4 字节大端）+ UTF-8 JSON。
 */
public final class FrameCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FrameCodec() {}

    public static byte[] encode(Frame frame) {
        try {
            byte[] json = MAPPER.writeValueAsBytes(frame);
            byte[] out = new byte[4 + json.length];
            out[0] = (byte) (json.length >>> 24);
            out[1] = (byte) (json.length >>> 16);
            out[2] = (byte) (json.length >>> 8);
            out[3] = (byte) json.length;
            System.arraycopy(json, 0, out, 4, json.length);
            return out;
        } catch (IOException e) {
            throw new IllegalStateException("frame encode failed", e);
        }
    }

    public static Frame decode(byte[] json) {
        try {
            return MAPPER.readValue(json, Frame.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("frame decode failed", e);
        }
    }

    public static Frame decodeJson(byte[] json, int off, int len) {
        return decode(new String(json, off, len, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8));
    }
}
