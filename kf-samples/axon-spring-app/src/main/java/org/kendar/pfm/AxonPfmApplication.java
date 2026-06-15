package org.kendar.pfm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Axon counterpart of {@code PfmApplication}: the same personal-finance demo, built on open-source
 * Axon Framework (server-less — shared MySQL only, no Axon Server). Boots the Axon Spring Boot
 * starter, which auto-wires the JPA event/token/dead-letter stores around the standard
 * {@code spring.datasource}, and registers the {@code @Aggregate} / {@code @EventHandler} beans.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class AxonPfmApplication {

    public static void main(String[] args) {
        SpringApplication.run(AxonPfmApplication.class, args);
    }
}
