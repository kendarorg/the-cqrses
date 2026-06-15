package org.kendar.pfm.config;

import jakarta.annotation.PostConstruct;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * Creates the app's read-model tables ({@code pfm-schema.sql}) at startup. Runs in
 * {@code @PostConstruct} — i.e. while beans are still being constructed, before Axon's event
 * processors are started by their {@code SmartLifecycle} — so the projection handlers always find
 * their tables. Axon's own tables are created by Hibernate ({@code ddl-auto=update}) on the same
 * datasource.
 */
@Component
public class ReadModelSchema {

    private final DataSource dataSource;

    public ReadModelSchema(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void initialize() {
        ResourceDatabasePopulator populator =
                new ResourceDatabasePopulator(new ClassPathResource("pfm-schema.sql"));
        populator.execute(dataSource);
    }
}
