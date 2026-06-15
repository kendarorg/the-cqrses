package org.kendar.cqrses.pg;

import org.kendar.cqrses.di.GlobalRegistry;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentCalculator {
    private static final AtomicInteger segments = new AtomicInteger(3);
    public static void setSegments(int segmentsCount) {
        // The segment count is frozen at setup: changing it after events are written
        // misroutes their replay. Fail-fast once started (grill item 1).
        GlobalRegistry.assertNotStarted("SegmentCalculator.setSegments");
        segments.set(segmentsCount);
    }

    public static int getSegments() {
        return segments.get();
    }

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final int SEED = 0;

    /**
     * Maps the string form of {@code value} to a stable segment in [0, SEGMENTS - 1] using MurmurHash3 (x86_32).
     */
    public static int calculateSegment(Object value) {
        var stringified = value.toString();
        int hash = murmur3(stringified.getBytes(StandardCharsets.UTF_8));
        return Math.floorMod(hash, getSegments());
    }

    private static int murmur3(byte[] data) {
        int h = SEED;
        int length = data.length;
        int roundedEnd = length & ~0x03; // largest multiple of 4 <= length

        for (int i = 0; i < roundedEnd; i += 4) {
            int k = (data[i] & 0xff)
                    | ((data[i + 1] & 0xff) << 8)
                    | ((data[i + 2] & 0xff) << 16)
                    | ((data[i + 3] & 0xff) << 24);
            k *= C1;
            k = Integer.rotateLeft(k, 15);
            k *= C2;

            h ^= k;
            h = Integer.rotateLeft(h, 13);
            h = h * 5 + 0xe6546b64;
        }

        int k = 0;
        switch (length & 0x03) {
            case 3:
                k = (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k |= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k |= (data[roundedEnd] & 0xff);
                k *= C1;
                k = Integer.rotateLeft(k, 15);
                k *= C2;
                h ^= k;
        }

        h ^= length;
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);
        return h;
    }
}
