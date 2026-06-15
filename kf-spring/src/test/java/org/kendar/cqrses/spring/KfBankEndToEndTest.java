package org.kendar.cqrses.spring;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.spring.bank.AccountAggregate;
import org.kendar.cqrses.spring.bank.AuditService;
import org.kendar.cqrses.spring.bank.BalanceProjection;
import org.kendar.cqrses.spring.bank.Deposit;
import org.kendar.cqrses.spring.bank.OpenAccount;
import org.kendar.cqrses.utils.UUIDGenerator;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import javax.sql.DataSource;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test #3: end-to-end H2 ({@code MODE=MySQL}) bank slice driven through the auto-wired stack. A
 * command is sent through the real {@link CommandBus}; the {@link BalanceProjection} (a live Spring
 * bean bridged into {@code GlobalRegistry}) reacts on a lane thread and is updated. The
 * {@link AuditService} collaborator is a {@code @Lazy} bean, so a non-zero construction count before
 * the first command proves {@link KfBootstrap}'s pre-warm pulled it off the dispatch path.
 *
 * <p>Driven via {@link SpringApplicationBuilder} (not {@code @SpringBootTest}) so the
 * {@code kf.segments} / {@code kf.liveness.port} default properties are deterministically visible to
 * the {@link KfSegmentEnvironmentPostProcessor} during {@code prepareEnvironment} — exercising the
 * full real startup path, including the segment-setting hook.
 */
class KfBankEndToEndTest {

    @Test
    void commandDrivesProjectionAndPreWarmPulledTheCollaborator() {
        AuditService.resetConstructions();
        GlobalRegistry.clear();

        SpringApplicationBuilder app = new SpringApplicationBuilder(TestApp.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "kf.segments=4",
                        "kf.liveness.port=8099",
                        "kf.processing-groups=balances",
                        "kf.scan.base-packages=org.kendar.cqrses.spring.bank");

        try (ConfigurableApplicationContext ctx = app.run()) {
            // The post-processor set the segment count from kf.segments before any bean.
            assertThat(SegmentCalculator.getSegments()).isEqualTo(4);

            // Pre-warm constructed the @Lazy collaborator during bootstrap — before any dispatch.
            assertThat(AuditService.CONSTRUCTIONS.get())
                    .as("pre-warm must construct the collaborator off the dispatch path")
                    .isEqualTo(1);

            CommandBus commandBus = ctx.getBean(CommandBus.class);
            BalanceProjection projection = ctx.getBean(BalanceProjection.class);
            AuditService audit = ctx.getBean(AuditService.class);

            UUID account = UUIDGenerator.newUuid();
            commandBus.sendSync(new OpenAccount(account));
            commandBus.sendSync(new Deposit(account, 100));

            awaitUntil(() -> projection.balanceOf(account) == 100);

            assertThat(projection.balanceOf(account)).isEqualTo(100);
            assertThat(audit.recordedCount())
                    .as("the projection's collaborator was invoked on the Deposited event")
                    .isGreaterThanOrEqualTo(1);
            // No second AuditService was built on a lane thread.
            assertThat(AuditService.CONSTRUCTIONS.get()).isEqualTo(1);
        }
    }

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("Condition did not become true within 5s");
    }

    @Configuration
    @Import(KfAutoConfiguration.class)
    static class TestApp {

        private static final AtomicInteger DB_SEQ = new AtomicInteger();

        @Bean("kf-datasource")
        DataSource kfDataSource() {
            JdbcDataSource ds = new JdbcDataSource();
            ds.setURL("jdbc:h2:mem:kfspring_e2e_" + DB_SEQ.incrementAndGet()
                    + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
            return ds;
        }

        /** Live bean: scanner finds {@code @Projection} + a bean exists → register(class, bean). */
        @Bean
        BalanceProjection balanceProjection() {
            return new BalanceProjection();
        }

        /**
         * {@code @Lazy} collaborator: absent pre-warm it would be built on a lane thread at first
         * dispatch; {@link KfBootstrap}'s pre-warm forces its construction during setup instead.
         * Not kf-annotated, so it is pulled via the fallback resolver, not the scanner.
         */
        @Bean
        @Lazy
        AuditService auditService() {
            return new AuditService();
        }

        // AccountAggregate is intentionally NOT a bean: kf instantiates it per-id, so the scanner
        // registers it via GlobalRegistry.register(class). Referenced to document the contract.
        @SuppressWarnings("unused")
        private static final Class<?> AGGREGATE = AccountAggregate.class;
    }
}
