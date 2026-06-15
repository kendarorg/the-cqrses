package org.kendar.cqrses.spring;

import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.CommandInterceptor;
import org.kendar.cqrses.annotations.Projection;
import org.kendar.cqrses.annotations.Saga;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Classpath scanner for kf's four class-level discovery annotations:
 * {@link Aggregate}, {@link Saga}, {@link Projection}, {@link CommandInterceptor}. These drive
 * {@code GlobalRegistry}'s {@code classRegistry} routing (and, for aggregates, event-applier
 * registration). Method-level {@code @CommandHandler}/{@code @EventHandler} live inside these types.
 */
final class KfHandlerScanner {

    private KfHandlerScanner() {
    }

    /**
     * Resolves the scan roots: explicit {@code kf.scan.base-packages} if non-empty, else Spring's
     * {@link AutoConfigurationPackages} (the package of the {@code @SpringBootApplication}).
     */
    static List<String> resolveBasePackages(List<String> configured, ListableBeanFactory beanFactory) {
        if (configured != null && !configured.isEmpty()) {
            return new ArrayList<>(configured);
        }
        if (AutoConfigurationPackages.has(beanFactory)) {
            return AutoConfigurationPackages.get(beanFactory);
        }
        return List.of();
    }

    /** Scans {@code basePackages} and returns every type carrying one of the four kf annotations. */
    static List<Class<?>> scan(List<String> basePackages, ClassLoader classLoader) {
        var provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AnnotationTypeFilter(Aggregate.class));
        provider.addIncludeFilter(new AnnotationTypeFilter(Saga.class));
        provider.addIncludeFilter(new AnnotationTypeFilter(Projection.class));
        provider.addIncludeFilter(new AnnotationTypeFilter(CommandInterceptor.class));

        Set<Class<?>> found = new LinkedHashSet<>();
        for (String basePackage : basePackages) {
            for (var candidate : provider.findCandidateComponents(basePackage)) {
                String className = candidate.getBeanClassName();
                if (className == null) continue;
                found.add(ClassUtils.resolveClassName(className, classLoader));
            }
        }
        return new ArrayList<>(found);
    }
}
