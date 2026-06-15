package org.kendar.cqrses.spring;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.springframework.mock.env.MockEnvironment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test #1: the {@link KfSegmentEnvironmentPostProcessor} sets {@code getSegments()} from
 * {@code kf.segments} (the only hook guaranteed to run before any bean), and fails fast on a
 * missing/invalid mandatory property.
 */
class KfSegmentEnvironmentPostProcessorTest {

    private final KfSegmentEnvironmentPostProcessor processor = new KfSegmentEnvironmentPostProcessor();

    @Test
    void setsSegmentsFromProperty() {
        SegmentCalculator.setSegments(3); // reset to the static default
        MockEnvironment env = new MockEnvironment()
                .withProperty("kf.segments", "7")
                .withProperty("kf.liveness.port", "8090");

        processor.postProcessEnvironment(env, null);

        assertEquals(7, SegmentCalculator.getSegments());
    }

    @Test
    void failsFastWhenSegmentsMissing() {
        MockEnvironment env = new MockEnvironment().withProperty("kf.liveness.port", "8090");
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> processor.postProcessEnvironment(env, null));
        assertEquals(true, ex.getMessage().contains("kf.segments"));
    }

    @Test
    void failsFastWhenSegmentsInvalid() {
        MockEnvironment env = new MockEnvironment()
                .withProperty("kf.segments", "not-a-number")
                .withProperty("kf.liveness.port", "8090");
        assertThrows(IllegalStateException.class, () -> processor.postProcessEnvironment(env, null));
    }

    @Test
    void failsFastWhenLivenessPortMissing() {
        MockEnvironment env = new MockEnvironment().withProperty("kf.segments", "5");
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> processor.postProcessEnvironment(env, null));
        assertEquals(true, ex.getMessage().contains("kf.liveness.port"));
    }
}
