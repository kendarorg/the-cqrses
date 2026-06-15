package org.kendar.pfm.config;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * Creates the app's read-model tables ({@code pfm-schema.sql}) at startup. Runs in {@code @PostConstruct}
 * — i.e. before the kf-spring {@code KfBootstrap} {@code SmartLifecycle} starts the buses and long
 * before any command can flow — so the projection handlers always find their tables. The framework's
 * own tables are created separately by the starter's {@code SchemaInitializer}.
 */
@Component
public class ReadModelSchema {

    private final DataSource dataSource;

    public ReadModelSchema(@Qualifier("kf-datasource") DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void initialize() {
        ResourceDatabasePopulator populator =
                new ResourceDatabasePopulator(new ClassPathResource("pfm-schema.sql"));
        populator.execute(dataSource);
    }
}
