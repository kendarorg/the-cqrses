package org.kendar.cqrses.cluster;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Keeps the node's {@code cluster_nodes.last_heartbeat} fresh on a {@link ClusterConfig#HEARTBEAT}
 * cadence, plus a GC fast-path: a {@code GarbageCollectorMXBean} notification longer than
 * {@link ClusterConfig#GC_PAUSE_THRESHOLD} triggers an immediate re-upsert, so a stop-the-world
 * pause does not push the node past the staleness window and get it falsely declared dead.
 * <p>
 * Any DB failure here is swallowed — the node then relies on the liveness {@code /alive} endpoint
 * as the leader's fallback proof-of-life.
 */
public class HeartbeatService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatService.class.getName());

    private final Db db;
    private final String nodeId;
    private final String host;
    private final int livenessPort;
    private final int forwardPort;
    private final ClusterClock clock;

    private ScheduledExecutorService scheduler;
    private final List<GcRegistration> registrations = new ArrayList<>();

    public HeartbeatService(Db db, String nodeId, String host, int livenessPort, ClusterClock clock) {
        this(db, nodeId, host, livenessPort, 0, clock);
    }

    public HeartbeatService(Db db, String nodeId, String host, int livenessPort, int forwardPort, ClusterClock clock) {
        this.db = db;
        this.nodeId = nodeId;
        this.host = host;
        this.livenessPort = livenessPort;
        this.forwardPort = forwardPort;
        this.clock = clock;
    }

    public void start() {
        heartbeatOnce();
        registerGcListeners();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-heartbeat");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::heartbeatOnce,
                ClusterConfig.HEARTBEAT, ClusterConfig.HEARTBEAT, TimeUnit.MILLISECONDS);
    }

    /**
     * Upsert this node's row to {@code last_heartbeat = now}. Update-first, insert-if-absent.
     * Test seam (package-visible); DB errors are swallowed.
     */
    void heartbeatOnce() {
        long now = clock.now();
        try {
            int updated = db.update(
                    "UPDATE cluster_nodes SET last_heartbeat = ?, host = ?, liveness_port = ?, forward_port = ? WHERE node_id = ?",
                    now, host, livenessPort, forwardPort, nodeId);
            if (updated == 0) {
                db.insertInto("cluster_nodes")
                        .set("node_id", nodeId)
                        .set("host", host)
                        .set("liveness_port", livenessPort)
                        .set("forward_port", forwardPort)
                        .set("last_heartbeat", now)
                        .ignore()
                        .execute();
            }
        } catch (DbException e) {
            LOGGER.debug("heartbeat upsert swallowed: {}", e.getMessage());
        }
    }

    private void registerGcListeners() {
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc instanceof NotificationEmitter emitter) {
                GcListener listener = new GcListener();
                emitter.addNotificationListener(listener, null, null);
                registrations.add(new GcRegistration(emitter, listener));
            }
        }
    }

    private record GcRegistration(NotificationEmitter emitter, NotificationListener listener) {
    }

    private final class GcListener implements NotificationListener {
        @Override
        public void handleNotification(Notification notification, Object handback) {
            if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION
                    .equals(notification.getType())) {
                return;
            }
            try {
                GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo
                        .from((CompositeData) notification.getUserData());
                maybeHeartbeatForGcPause(info.getGcInfo().getDuration());
            } catch (RuntimeException e) {
                LOGGER.debug("GC notification handling swallowed: {}", e.getMessage());
            }
        }
    }

    /**
     * GC fast-path core (package-visible test seam): a pause longer than
     * {@link ClusterConfig#GC_PAUSE_THRESHOLD} triggers an immediate heartbeat re-upsert. Any DB
     * failure inside {@link #heartbeatOnce()} is swallowed there.
     */
    void maybeHeartbeatForGcPause(long durationMillis) {
        if (durationMillis > ClusterConfig.GC_PAUSE_THRESHOLD) {
            heartbeatOnce();
        }
    }

    public void stop() {
        for (GcRegistration reg : registrations) {
            try {
                reg.emitter().removeNotificationListener(reg.listener());
            } catch (Exception ignored) {
                // listener may already be gone; nothing to do
            }
        }
        registrations.clear();
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
