package org.kendar.pfm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Personal Finance Manager — a runnable demo of the kf framework end-to-end through the kf-spring
 * starter (kf-core-db single-node backend; kf-cluster present but disabled). The starter's
 * {@code KfAutoConfiguration} is picked up automatically; it scans {@code org.kendar.pfm} for the
 * aggregate/projections and wires the JDBC stores + buses around the {@code kf-datasource} bean.
 */
@SpringBootApplication
public class PfmApplication {

    public static void main(String[] args) {
        SpringApplication.run(PfmApplication.class, args);
    }
}
