package org.kendar.cqrses.repositories;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DefaultDb;
import org.kendar.cqrses.db.SchemaInitializer;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Docker-gated fairness check for {@link JdbcEventStore#loadSegmentsTail} on a real
 * MySQL server — the one behaviour H2 (MODE=MySQL) can no longer prove. The MySQL
 * branch caps each segment to its fair share with a {@code ROW_NUMBER()} window so a
 * backlogged low-numbered segment cannot consume the whole batch and starve colder
 * segments; the cheap H2 global {@code LIMIT} would return only segment 0 here.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcEventStoreFairnessIT {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>(DockerImageName.parse("mysql:8.4"));

    private Db db;
    private JdbcEventStore store;

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        GlobalRegistry.register(MessageSerializer.class, new JacksonMessageSerializer());

        MysqlDataSource ds = new MysqlDataSource();
        ds.setUrl(MYSQL.getJdbcUrl());
        ds.setUser(MYSQL.getUsername());
        ds.setPassword(MYSQL.getPassword());
        db = new DefaultDb(ds);
        new SchemaInitializer(db).initialize();
        // fresh data per method (tables are CREATE IF NOT EXISTS, so just truncate)
        db.execute("DELETE FROM event_entry");
        db.execute("DELETE FROM segment_counter");

        store = new JdbcEventStore(db);
    }

    private static InternalMessage msg(UUID aggregateId, long version) {
        InternalMessage m = new InternalMessage();
        Context c = new Context();
        c.setAggregateId(aggregateId);
        c.setAggregateVersion(version);
        c.setType("E");
        m.setContext(c);
        m.setPayload(new byte[]{1});
        return m;
    }

    private static UUID aggInSegment(int segment) {
        while (true) {
            UUID u = UUIDGenerator.newUuid();
            if (SegmentCalculator.calculateSegment(u) == segment) return u;
        }
    }

    private void seed(int segment, int count) {
        UUID agg = aggInSegment(segment);
        List<InternalMessage> batch = new ArrayList<>(count);
        for (int v = 0; v < count; v++) batch.add(msg(agg, v));
        store.appendEvents(batch);
    }

    @Test
    void mysqlPerSegmentCapKeepsColdSegmentsFromStarving() {
        assertTrue(SegmentCalculator.getSegments() >= 3,
                "this fairness scenario assumes at least 3 segments");

        // Segment 0 is heavily backlogged; segments 1 and 2 are cold.
        seed(0, 30);
        seed(1, 5);
        seed(2, 5);

        int limit = 10;
        List<InternalMessage> batch =
                store.loadSegmentsTail(Map.of(0, -1L, 1, -1L, 2, -1L), limit);

        assertEquals(limit, batch.size(), "the batch is bounded by the overall limit");

        int[] perSegment = new int[3];
        for (InternalMessage m : batch) {
            perSegment[SegmentCalculator.calculateSegment(m.getContext().getAggregateId())]++;
        }

        // The cheap global LIMIT (H2 path) would return 10 rows all from segment 0.
        // The window cap (perSegment = ceil(10/3) = 4) must leave room for the cold ones.
        assertTrue(perSegment[0] <= 4,
                "the backlogged segment must not exceed its fair per-segment share, was " + perSegment[0]);
        assertTrue(perSegment[1] > 0, "cold segment 1 must make progress, not be starved");
        assertTrue(perSegment[2] > 0, "cold segment 2 must make progress, not be starved");
    }
}
