package org.kendar.cqrses.spring.observability;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.spring.KfProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Binding of the new kf.observability.* instrumentation properties. */
class KfObservabilityPropertiesTest {

    private KfProperties bind(Map<String, String> props) {
        return new Binder(new MapConfigurationPropertySource(props))
                .bind("kf", KfProperties.class)
                .orElseGet(KfProperties::new);
    }

    @Test
    void defaultsAreAllOff() {
        var kf = bind(Map.of());
        assertFalse(kf.getObservability().isExtendedMetrics());
        assertFalse(kf.getObservability().getTrace().isEnabled());
        assertEquals(100, kf.getObservability().getTrace().getSampleEvery());
        assertEquals(10000, kf.getObservability().getTrace().getBufferCapacity());
        assertEquals(5000, kf.getObservability().getLag().getSampleIntervalMs());
    }

    @Test
    void kebabCasePropertiesBind() {
        var kf = bind(Map.of(
                "kf.observability.extended-metrics", "true",
                "kf.observability.trace.enabled", "true",
                "kf.observability.trace.sample-every", "10",
                "kf.observability.trace.buffer-capacity", "500",
                "kf.observability.lag.sample-interval-ms", "1000"));
        assertTrue(kf.getObservability().isExtendedMetrics());
        assertTrue(kf.getObservability().getTrace().isEnabled());
        assertEquals(10, kf.getObservability().getTrace().getSampleEvery());
        assertEquals(500, kf.getObservability().getTrace().getBufferCapacity());
        assertEquals(1000, kf.getObservability().getLag().getSampleIntervalMs());
    }
}
