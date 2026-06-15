package org.kendar.cqrses.spring;

import org.kendar.cqrses.pg.SegmentCalculator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Sets {@link SegmentCalculator#setSegments(int)} from {@code kf.segments} during
 * {@code prepareEnvironment} — before the {@link org.springframework.context.ApplicationContext}
 * and any bean exists. This is the only hook guaranteed to run before anything reads
 * {@link SegmentCalculator#getSegments()} (which feeds the murmur hash in
 * {@code ProcessingGroupsManager} and becomes the cluster's partition count {@code N}).
 *
 * <p>{@code kf.segments} and {@code kf.liveness.port} are both <b>mandatory</b>: the static default
 * of {@code 3} must never be silently inherited in a real deployment. Absent or invalid values fail
 * fast here, before any partition-sensitive state is built. {@code kf.liveness.port} is validated
 * (not stored) here for symmetry; it is consumed later at cluster {@code start(...)} via
 * {@link KfProperties}.
 *
 * <p>Registered in {@code META-INF/spring.factories} (that hook still uses {@code spring.factories},
 * not {@code AutoConfiguration.imports}).
 */
public class KfSegmentEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        SegmentCalculator.setSegments(requireInt(environment, "kf.segments"));
        // Validate presence/shape now; the value is bound again via KfProperties for cluster start().
        requireInt(environment, "kf.liveness.port");
    }

    private static int requireInt(ConfigurableEnvironment environment, String key) {
        String raw = environment.getProperty(key);
        if (raw == null || raw.isBlank()) {
            throw new IllegalStateException(
                    "Required property '" + key + "' is missing. kf-spring refuses to silently inherit a default; "
                            + "set it explicitly in application.yml / properties.");
        }
        try {
            int value = Integer.parseInt(raw.trim());
            if (value <= 0) {
                throw new IllegalStateException(
                        "Property '" + key + "' must be a positive integer but was '" + raw + "'.");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Property '" + key + "' must be a positive integer but was '" + raw + "'.", e);
        }
    }

    @Override
    public int getOrder() {
        // After config-data processing so values from application.yml / profiles are visible.
        return Ordered.LOWEST_PRECEDENCE;
    }
}
