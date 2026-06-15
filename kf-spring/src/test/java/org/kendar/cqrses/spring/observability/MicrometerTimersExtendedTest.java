package org.kendar.cqrses.spring.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.observability.AppendPhase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The extended-metrics gate and the new meters: append-phase histogram timers,
 * in-flight / pump-lag gauges (registered once, updated in place), nudge counters.
 */
class MicrometerTimersExtendedTest {

    @Test
    void extendedMetricsOffRegistersNothingNew() {
        var registry = new SimpleMeterRegistry();
        var timers = new MicrometerTimers(registry, "n1", false);
        timers.onAppendPhase(AppendPhase.LOCK, 1000);
        timers.onAppendInFlight(0, +1);
        timers.onPumpNudge(false);
        timers.onPumpLag("ledger", 0, 5);
        assertNull(registry.find("kf.append.phase").timer());
        assertNull(registry.find("kf.append.inflight").gauge());
        assertNull(registry.find("kf.pump.nudge").counter());
        assertNull(registry.find("kf.pump.lag").gauge());
        // baseline meters still work
        timers.onCommandHandled("g", "Cmd", 1000, true);
        assertNotNull(registry.find("kf.command.handle").timer());
    }

    @Test
    void appendPhaseTimerTaggedByPhaseAndNode() {
        var registry = new SimpleMeterRegistry();
        var timers = new MicrometerTimers(registry, "n1", true);
        timers.onAppendPhase(AppendPhase.LOCK, 1_000_000);
        timers.onAppendPhase(AppendPhase.COMMIT, 2_000_000);
        var lock = registry.find("kf.append.phase").tags("phase", "lock", "node", "n1").timer();
        var commit = registry.find("kf.append.phase").tags("phase", "commit", "node", "n1").timer();
        assertNotNull(lock);
        assertNotNull(commit);
        assertEquals(1, lock.count());
    }

    @Test
    void inFlightGaugeTracksDeltasInPlace() {
        var registry = new SimpleMeterRegistry();
        var timers = new MicrometerTimers(registry, "n1", true);
        timers.onAppendInFlight(3, +1);
        timers.onAppendInFlight(3, +1);
        timers.onAppendInFlight(3, -1);
        var gauge = registry.find("kf.append.inflight").tags("segment", "3", "node", "n1").gauge();
        assertNotNull(gauge);
        assertEquals(1.0, gauge.value());
        assertEquals(1, registry.find("kf.append.inflight").gauges().size(),
                "gauge must be registered once, not per call");
    }

    @Test
    void pumpLagGaugeSetsLatestValue() {
        var registry = new SimpleMeterRegistry();
        var timers = new MicrometerTimers(registry, "n1", true);
        timers.onPumpLag("ledger", 2, 10);
        timers.onPumpLag("ledger", 2, 4);
        var gauge = registry.find("kf.pump.lag")
                .tags("group", "ledger", "segment", "2", "node", "n1").gauge();
        assertNotNull(gauge);
        assertEquals(4.0, gauge.value());
    }

    @Test
    void forwardMetersAreNotGatedByExtendedFlag() {
        // Forwarding meters belong to the baseline set: a cluster running with
        // forwarding on must see them without opting into extended metrics.
        var registry = new SimpleMeterRegistry();
        var timers = new MicrometerTimers(registry, "n1", false);
        timers.onCommandForwarded("RecordOperation", "n2", true, 5_000_000, true);
        timers.onForwardFallback("RecordOperation", "no-route");
        timers.onForwardServed("RecordOperation", true, 3_000_000, true);
        timers.onRoutingRefreshed(12, 3, 1_000_000);

        var forward = registry.find("kf.command.forward")
                .tags("type", "RecordOperation", "target", "n2", "sync", "true", "ok", "true", "node", "n1")
                .timer();
        assertNotNull(forward);
        assertEquals(1, forward.count());
        assertEquals(1.0, registry.find("kf.command.forward.fallback")
                .tags("type", "RecordOperation", "reason", "no-route").counter().count());
        assertNotNull(registry.find("kf.command.forward.serve").tags("sync", "true").timer());
        assertNotNull(registry.find("kf.routing.refresh").timer());
        assertEquals(12.0, registry.find("kf.routing.assignments").gauge().value());
        assertEquals(3.0, registry.find("kf.routing.forwardable.nodes").gauge().value());
    }

    @Test
    void nudgeCounterSplitsByDeferred() {
        var registry = new SimpleMeterRegistry();
        var timers = new MicrometerTimers(registry, "n1", true);
        timers.onPumpNudge(false);
        timers.onPumpNudge(false);
        timers.onPumpNudge(true);
        assertEquals(2.0, registry.find("kf.pump.nudge").tags("deferred", "false").counter().count());
        assertEquals(1.0, registry.find("kf.pump.nudge").tags("deferred", "true").counter().count());
    }
}
