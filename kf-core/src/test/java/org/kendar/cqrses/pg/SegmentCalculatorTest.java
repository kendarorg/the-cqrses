package org.kendar.cqrses.pg;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentCalculatorTest {

    @Test
    void segmentsConstantIsThree() {
        assertEquals(3, SegmentCalculator.getSegments());
    }

    @Test
    void segmentAlwaysWithinRange() {
        for (int i = 0; i < 5000; i++) {
            int seg = SegmentCalculator.calculateSegment("aggregate-" + i);
            assertTrue(seg >= 0 && seg < SegmentCalculator.getSegments(),
                    "segment out of range: " + seg + " for input " + i);
        }
    }

    @Test
    void uuidInputsStayWithinRange() {
        for (int i = 0; i < 2000; i++) {
            int seg = SegmentCalculator.calculateSegment(UUID.randomUUID());
            assertTrue(seg >= 0 && seg < SegmentCalculator.getSegments());
        }
    }

    @Test
    void isDeterministicForSameInput() {
        String value = "0f2c1b7a-1111-2222-3333-444455556666";
        int first = SegmentCalculator.calculateSegment(value);
        for (int i = 0; i < 100; i++) {
            assertEquals(first, SegmentCalculator.calculateSegment(value));
        }
    }

    @Test
    void usesStringFormSoEqualStringsCollide() {
        UUID id = UUID.randomUUID();
        assertEquals(SegmentCalculator.calculateSegment(id),
                SegmentCalculator.calculateSegment(id.toString()));
    }

    @Test
    void emptyStringHashesToAValidSegment() {
        int seg = SegmentCalculator.calculateSegment("");
        assertTrue(seg >= 0 && seg < SegmentCalculator.getSegments());
    }

    @Test
    void shortInputsExerciseTailMixing() {
        // 1, 2 and 3 byte inputs exercise the switch fall-through tail of murmur3.
        assertTrue(SegmentCalculator.calculateSegment("a") >= 0);
        assertTrue(SegmentCalculator.calculateSegment("ab") >= 0);
        assertTrue(SegmentCalculator.calculateSegment("abc") >= 0);
    }

    @Test
    void distributesAcrossAllBuckets() {
        Set<Integer> buckets = new HashSet<>();
        for (int i = 0; i < 5000; i++) {
            buckets.add(SegmentCalculator.calculateSegment("key-" + i));
        }
        // A reasonable hash should spread 5000 distinct keys across all SEGMENTS buckets.
        assertEquals(SegmentCalculator.getSegments(), buckets.size(),
                "poor distribution, only used " + buckets.size() + " buckets");
    }
}
