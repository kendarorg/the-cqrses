package org.kendar.pfm.config;

import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.jpa.JpaTokenStore;
import org.axonframework.serialization.Serializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Replaces Axon's auto-configured {@link TokenStore} with a {@link JpaTokenStore} whose claim
 * {@code owner} is this node's stable {@code pfm.cluster.node-id} (e.g. {@code node1}) instead of the
 * default {@code pid@hostname}. That makes {@code token_entry.owner} directly map to a node, so the
 * cluster IT (and any operator) can read segment ownership straight from the table — the analog of kf
 * reading {@code cluster_assignments.owner_node}.
 *
 * <p>The claim timeout — how long a node's segment claim may go un-renewed before a peer steals it —
 * is the central handoff-timing knob (Axon's analog of kf's lease/staleness window). Kept at Axon's
 * 10s default here and mirrored by the IT's wait budgets.
 *
 * <p>Leaf dependencies only ({@link EntityManagerProvider}, {@link Serializer}) → no cycle with
 * Axon's event-processing configurer that consumes the token store.
 */
@Configuration
public class AxonTokenStoreConfig {

    /** Mirrors Axon's default {@code axon.eventhandling.tokenstore.claim-timeout}. */
    private static final Duration CLAIM_TIMEOUT = Duration.ofSeconds(10);

    @Bean
    public TokenStore tokenStore(EntityManagerProvider entityManagerProvider,
                                 @Qualifier("serializer") Serializer serializer,
                                 PfmProperties props) {
        JpaTokenStore.Builder builder = JpaTokenStore.builder()
                .entityManagerProvider(entityManagerProvider)
                .serializer(serializer)
                .claimTimeout(CLAIM_TIMEOUT);
        String nodeId = props.getCluster().getNodeId();
        if (nodeId != null && !nodeId.isBlank()) {
            builder.nodeId(nodeId);
        }
        return builder.build();
    }
}
