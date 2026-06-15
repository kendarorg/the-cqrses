# kf-cluster — partition-lifecycle layer

A generic **partition-lifecycle** layer on top of `kf-core-db`'s `Db`. The unit
of distribution is the integer **partition / item id** `0..N-1`; the cluster's
*sole* guarantee is: **at most one live node owns each partition at any moment.**
It adds *placement*, not durability — durability stays in the `kf-core-db`
stores' OCC.

Dependency direction is strict: **kf-cluster → kf-core**, never the reverse.
No Hazelcast. All time columns are `BIGINT` epoch millis.

## Components (`org.kendar.cqrses.cluster`)

| Class | Role |
|-------|------|
| `ClusterNode` / `ClusterNodeBuilder` | Public entry point; assembles + starts the services. `release(itemId)` is the callback that clears a lease once a pump has truly drained. |
| `HeartbeatService` | Writes this node's `cluster_nodes.last_heartbeat` row. |
| `LeaderService` | The single elected leader loop: membership-stability gate, dead-node confirmation via HTTP `GET /alive`, minimal-movement rebalance (`Assignment.compute`), epoch-fenced UPDATEs, best-effort `POST /notify` poke, dead-row GC. |
| `WorkerService` | Polls `cluster_assignments` for partitions assigned to this node; claims the lease and runs `ItemProcessor.process(itemId)`; on reassignment calls `stopProcess(itemId)`. |
| `Liveness` | The HTTP server hosting `/alive` and `/notify`. |
| `LeaderLock` (SPI) + `DbLeaderLock` | Leader election. `DbLeaderLock` is the default DB-backed lease; `ClusterNodeBuilder.leaderLock(...)` swaps in Ratis / ZooKeeper / etc. |
| `ItemProcessor` | The app's plug-in: `process(int)` (long-lived pump per partition — **must not return**) + `stopProcess(int)` (cooperative wind-down). |
| `SegmentItemProcessor` | The kf-cluster ↔ kf-core adapter for event sourcing: `itemId == segment`, driven through `kf-core`'s `SegmentOwnership` SPI. |
| `Assignment` | `compute(N, members, current)` → minimal-movement target assignment. |
| `ClusterSchema` | Seeds the five tables. |
| `ClusterConfig` / `ClusterClock` | Named timing constants + clock. |
| `rows.*` | Row DTOs + `RowMappers` for the cluster tables. |

## Tables

Five tables seeded by `ClusterSchema.init`:
`cluster_nodes`, `cluster_assignments`, `cluster_leader_lock`,
`cluster_leader_health`, `cluster_config`.

## Configuration

Timing is in `ClusterConfig` (constants, epoch-millis spans — margins are
intentionally wide so NTP skew / a GC pause can't trip a false death):

| Constant | Default | Meaning |
|----------|---------|---------|
| `HEARTBEAT` | 3 s | Heartbeat re-stamp cadence. |
| `STALENESS_WINDOW` | 9 s | Unseen-longer-than → death candidate (subject to `/alive`). |
| `GC_PAUSE_THRESHOLD` | 4 s | Longer pause → immediate out-of-band heartbeat. |
| `LEADER_TICK` / `WORKER_TICK` | 5 s | Reconcile cadences. |
| `LEASE` | 30 s | Processing-lease lifetime. |
| `LEASE_RENEW` | 10 s | Lease-renew cadence. |
| `MEMBERSHIP_STABILIZE` | 10 s | Membership must hold this long before a normal rebalance. |
| `MAX_INSTABILITY` | 30 s | Cap on how long flapping can starve a rebalance. |
| `LIVENESS_TIMEOUT` | 2 s | Per-`/alive`-probe timeout. |
| `LIVENESS_FAIL_TICKS` | 2 | Consecutive failed probes before a stale node is confirmed dead. |
| `LEADER_LOCK_LEASE` | 15 s | Leader-lock lease lifetime. |
| `NODE_GC_STALE` | 60 s | Dead `cluster_nodes` rows staler than this are pruned. |

Per-node runtime values (node id, advertised host, liveness port, segment count,
leader-lock impl, `ItemProcessor`) are supplied through `ClusterNodeBuilder`. In
a Spring app these come from `kf.cluster.*` / `kf.liveness.port` (see `kf-spring`).

## Leader tick & worker lifecycle

See the **Cluster topology** section of the root `CROSS_CUTTING.md` for the full leader
tick (lease → live set → dead-node confirmation → stability gate → minimal-move
assignment → notify/GC) and the crash / DB-blind / lost-leadership failure modes.
The key safety property: a gaining node cannot start a second pump until the
original calls back `ClusterNode.release(itemId)` (graceful) or the lease times
out via `lease_until` (crash).

## Build & test

```bash
mvn -pl kf-cluster -am test
```

Unit tests use H2. The full multi-JVM cluster behaviour is exercised by
`kf-samples/kf-cluster-it` (Testcontainers, Docker-gated).
