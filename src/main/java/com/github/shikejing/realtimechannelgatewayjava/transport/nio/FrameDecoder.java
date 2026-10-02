package com.github.shikejing.realtimechannelgatewayjava.transport.nio;

import com.github.shikejing.realtimechannelgatewayjava.protocol.Frame;
import com.github.shikejing.realtimechannelgatewayjava.protocol.FrameCodec;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 流式帧解码器：累积字节，按 4 字节长度前缀切出完整帧。
 */
public final class FrameDecoder {
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    public synchronized List<Frame> decode(ByteBuffer in) {
        while (in.hasRemaining()) {
            buffer.write(in.get());
        }
        List<Frame> out = new ArrayList<>();
        byte[] all = buffer.toByteArray();
        int off = 0;
        while (all.length - off >= 4) {
            int len = ((all[off] & 0xFF) << 24)
                    | ((all[off + 1] & 0xFF) << 16)
                    | ((all[off + 2] & 0xFF) << 8)
                    | (all[off + 3] & 0xFF);
            if (len < 0 || len > 16 * 1024 * 1024) {
                throw new IllegalArgumentException("invalid frame length: " + len);
            }
            if (all.length - off - 4 < len) {
                break;
            }
            byte[] json = Arrays.copyOfRange(all, off + 4, off + 4 + len);
            out.add(FrameCodec.decode(json));
            off += 4 + len;
        }
        buffer.reset();
        if (off < all.length) {
            buffer.write(all, off, all.length - off);
        }
        return out;
    }
}
