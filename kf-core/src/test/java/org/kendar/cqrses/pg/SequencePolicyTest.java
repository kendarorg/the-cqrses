package org.kendar.cqrses.pg;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.AggregateIdentifier;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the three {@link SequencePolicy} implementations. {@link NullSequencePolicy}
 * returns a fixed sequence id; {@link PerAggregateSequencePolicy} and
 * {@link PerSegmentSequencePolicy} reflect over the {@code @AggregateIdentifier}
 * field — the latter mapping the id to {@code <group>:seg:<segment>}.
 */
class SequencePolicyTest {

    // --- NullSequencePolicy ----------------------------------------------------

    @Test
    void nullPolicyReturnsConstantForAnyInput() {
        var p = new NullSequencePolicy();
        assertEquals("NullSequencePolicy", p.getSequenceId(new Object()));
        assertEquals("NullSequencePolicy", p.getSequenceId("anything"));
    }

    @Test
    void nullPolicyIgnoresArgumentEvenWhenNull() {
        assertEquals("NullSequencePolicy", new NullSequencePolicy().getSequenceId(null));
    }

    // --- PerSegmentSequencePolicy ---------------------------------------------

    @Test
    void perSegmentMapsIdToGroupAndSegment() {
        var c = new WithStringId();
        c.id = "agg-42";
        int expectedSeg = SegmentCalculator.calculateSegment("agg-42");
        assertTrue(expectedSeg >= 0 && expectedSeg < SegmentCalculator.getSegments());

        var p = new PerSegmentSequencePolicy();
        // Defaults to the "default" group.
        assertEquals("default", p.getGroup());
        assertEquals("default:seg:" + expectedSeg, p.getSequenceId(c));
    }

    @Test
    void perSegmentSetGroupChangesPrefix() {
        var c = new WithStringId();
        c.id = "agg-42";
        int expectedSeg = SegmentCalculator.calculateSegment("agg-42");

        var p = new PerSegmentSequencePolicy();
        p.setGroup("orders");
        assertEquals("orders", p.getGroup());
        assertEquals("orders:seg:" + expectedSeg, p.getSequenceId(c));
    }

    @Test
    void perSegmentThrowsWhenNoAnnotatedField() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> new PerSegmentSequencePolicy().getSequenceId(new WithoutId()));
        assertTrue(ex.getMessage().contains("No @AggregateIdentifier"));
    }

    @Test
    void perSegmentThrowsWhenIdValueIsNull() {
        var c = new WithStringId();
        c.id = null;
        assertThrows(IllegalArgumentException.class,
                () -> new PerSegmentSequencePolicy().getSequenceId(c));
    }

    @Test
    void perSegmentThrowsWhenIdValueIsBlank() {
        var c = new WithStringId();
        c.id = "    ";
        assertThrows(IllegalArgumentException.class,
                () -> new PerSegmentSequencePolicy().getSequenceId(c));
    }

    // --- PerAggregateSequencePolicy -------------------------------------------

    static class WithUuidId {
        @AggregateIdentifier
        UUID id;
    }

    static class WithStringId {
        @AggregateIdentifier
        String id;
    }

    static class WithoutId {
        String name = "x";
    }

    @Test
    void perAggregateReturnsStringFormOfUuid() {
        var c = new WithUuidId();
        c.id = UUID.randomUUID();
        assertEquals(c.id.toString(), new PerAggregateSequencePolicy().getSequenceId(c));
    }

    @Test
    void perAggregateTrimsSurroundingWhitespace() {
        var c = new WithStringId();
        c.id = "   agg-42   ";
        assertEquals("agg-42", new PerAggregateSequencePolicy().getSequenceId(c));
    }

    @Test
    void perAggregateThrowsWhenNoAnnotatedField() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> new PerAggregateSequencePolicy().getSequenceId(new WithoutId()));
        assertTrue(ex.getMessage().contains("No @AggregateIdentifier"));
    }

    @Test
    void perAggregateThrowsWhenIdValueIsNull() {
        var c = new WithStringId();
        c.id = null;
        assertThrows(IllegalArgumentException.class,
                () -> new PerAggregateSequencePolicy().getSequenceId(c));
    }

    @Test
    void perAggregateThrowsWhenIdValueIsBlank() {
        var c = new WithStringId();
        c.id = "    ";
        assertThrows(IllegalArgumentException.class,
                () -> new PerAggregateSequencePolicy().getSequenceId(c));
    }

    @Test
    void perAggregateThrowsOnNullArgument() {
        // The null guard dereferences the argument while building its message,
        // so a null argument surfaces as a NullPointerException.
        assertThrows(NullPointerException.class,
                () -> new PerAggregateSequencePolicy().getSequenceId(null));
    }
}
