# Kendar Framework

A lightweight CQRS / Event-Sourcing framework for **Java 25**.

Built because the existing options (Axon et al.) gate real features — multi-node
distribution, replay, dead-letter handling — behind paid tiers. Kendar Framework
gives you the same building blocks with no licence wall and no mandatory DI
container: it runs as a plain library, or wired through Spring Boot, or
distributed across a cluster, all from the same kernel.

> The previous design (the `kf-es-*` tree) is preserved under `olds/` for
> reference only. **It is no longer authoritative.** Do not depend on, import
> from, or modify anything under `olds/`. The current modules are everything
> outside that directory.

## Requirements & technology

- **Java 25**
- **Maven** multi-module build
- **Jackson** — message serialisation / deserialisation (`MessageSerializer`,
  upcasting)
- **Logback / SLF4J** — logging
- **H2** in MySQL compatibility mode (`MODE=MySQL`) — the JDBC stack's test
  database; the same SQL runs against a real MySQL server. H2 is a test-scope
  dependency only.

## Modules

Read them in this order. Dependency direction is strict: every module depends on
`kf-core`, and `kf-cluster → kf-core` (never the other way).

| Module | What it is |
|--------|------------|
| **`kf-core`** | The kernel. Annotations (`@Aggregate`, `@Command`, `@Event`, `@CommandHandler`, `@EventHandler`, `@Projection`, `@Saga`, …), the abstract bus base (`Bus`, `CommandBus`, `EventBus`, `Context`), the static service locator (`GlobalRegistry`), store contracts (`EventStore`, `SagaStore`, `CheckpointStore`, `DlqStore`), processing-group machinery (`ProcessingGroupsManager`, `SegmentProcessor`, `SegmentCalculator`), saga association, the `Scheduler` SPI + `Sleeper` backoff, Jackson serialisation + upcasting, the thin JDBC `Db` wrapper, and the `cluster.spi.SegmentOwnership` SPI. **The only module the others always depend on.** Depends on Jackson + Logback (H2 only for tests). |
| **`kf-core-memory`** | In-process implementations: `InMemoryCommandBus`, `InMemoryEventBus`, `InMemoryEventStore`, `InMemorySagaStore`, `InMemoryDlqStore`, `InMemoryCheckpointStore`, `InMemoryScheduler`. For tests, single-node demos, and the in-memory contract suites. Not used by cluster deployments. |
| **`kf-core-db`** | JDBC implementations against H2 in MySQL mode — all DDL/DML in the H2 ∩ MySQL intersection. Plain `java.sql` behind the `Db` wrapper (no JPA / Hibernate / Spring). Provides `JdbcCommandBus`, `JdbcEventBus`, `JdbcEventStore`, `JdbcSagaStore`, `JdbcDlqStore`, `JdbcCheckpointStore`, `JdbcScheduler`, `JdbcProcessingGroup(sManager)`, and the `SchemaInitializer`. Single-node durability via per-aggregate OCC + a `UNIQUE(aggregate_id, sequence)` backstop. |
| **`kf-cluster`** | Generic partition-lifecycle layer on top of `kf-core-db`'s `Db`. The unit of distribution is the integer partition id `0..N-1`; the sole guarantee is **at most one live node owns each partition at any moment.** DB-backed leader election (`LeaderService`, pluggable `LeaderLock`), `HeartbeatService`, `WorkerService`, and `Liveness` HTTP server. No Hazelcast. The app plugs in via `ItemProcessor`; `SegmentItemProcessor` is the kf-cluster ↔ kf-core adapter for the event-sourcing case. |
| **`kf-spring`** | Spring Boot auto-configuration that wires the `kf-core-db` stack and an optional `kf-cluster` node. The app supplies a `@Bean("kf-datasource") DataSource`; every infra bean is `@ConditionalOnMissingBean`. Binds the `kf.*` namespace (`kf.segments`, `kf.cluster.*`, `kf.liveness.port`, …) and classpath-scans for handlers. |
| **`kf-samples/kf-spring-app`** | A personal-finance / bank sample (`PfmApplication`) wiring `kf-spring` end-to-end: `Account` aggregate, commands/events, projections, DLQ policy + controller, a `/cluster/*` control API, and a web layer. |
| **`kf-samples/kf-cluster-it`** | Heavyweight Testcontainers ITs: three real JVMs of `kf-spring-app` against one shared MySQL, driven through the `/cluster/*` control API. Docker-gated, all `*IT`. |
| **`kf-integration`** | Cross-implementation contract suites. The bank scenario, its DLQ behaviour, and the cluster-pull tail are written once against an `IntegrationBackend` seam and re-run against `InMemoryBackend` and `JdbcBackend`. |

## Architecture

```
                      kf-samples (kf-spring-app, kf-cluster-it)
                                     │
        ┌────────────────────────────┴────────────────────────────┐
        │                        kf-spring                          │
        │   Spring Boot auto-config: wires kf-core-db + kf-cluster   │
        └───────────────┬──────────────────────────┬───────────────┘
                        │                          │
        ┌───────────────▼──────────┐   ┌───────────▼──────────────┐
        │        kf-cluster         │   │   kf-core-memory          │
        │  leader election +        │   │  in-process buses/stores  │
        │  partition distribution   │   │  (tests / single node)    │
        └───────────────┬──────────┘   └───────────┬──────────────┘
                        │                          │
        ┌───────────────▼──────────┐               │
        │        kf-core-db         │               │
        │  JDBC buses/stores over   │               │
        │  H2-in-MySQL-mode / MySQL │               │
        └───────────────┬──────────┘               │
                        │                          │
        ┌───────────────▼──────────────────────────▼──────────────┐
        │                          kf-core                          │
        │  annotations · Bus/CommandBus/EventBus · GlobalRegistry   │
        │  store contracts · processing groups · sagas · Scheduler  │
        │  Jackson serialisation · Db wrapper · SegmentOwnership SPI │
        └──────────────────────────────────────────────────────────┘
```

## Key concepts

- **Command** — an intent to change state. Routed to exactly one aggregate /
  command handler. Command-side durability is optimistic-concurrency: a
  per-aggregate lock plus a `UNIQUE(aggregate_id, sequence)` backstop.
- **Event** — a fact that something happened. Appended to the aggregate's stream
  and published to zero or more handlers.
- **Aggregate** (`@Aggregate`) — a consistency boundary, reconstructed by
  replaying its event stream. A stored snapshot, when present, short-circuits the
  replay from version 0 to `snapshot.aggregateVersion + 1` (best-effort, opt-in;
  there is no automatic snapshot scheduler).
- **Projection** (`@Projection`) — an event-side read-model builder.
- **Saga** (`@Saga`) — a multi-step process coordinated via association values.
- **Context** — carries correlation / version metadata through a request. A
  `Context.aggregateVersion == -1` means "assign the next version on append".
- **Processing groups, segments, lanes** — `SegmentCalculator` hashes each
  message's aggregate id (or saga association) into one of `SEGMENTS` partitions.
  A processing group is a named group of homogeneous handlers; each group has up
  to `SEGMENTS` lanes. Per-aggregate ordering is preserved because every message
  for one aggregate hashes to the same segment → the same lane. **The segment
  count is frozen at setup** — changing it after events are written misroutes
  their replay.
- **DLQ (dead-letter queue)** — when a handler fails, the group's
  `DlqEnqueuePolicy` decides whether to skip, block, or park the failed message
  in a `DlqStore` for operator resolution (`DlqManager`).

## Clustering (in brief)

The cluster adds *placement*, not durability. The leader's tick loop is the sole
source of assignment changes: acquire an epoch-fenced lease, compute the
heartbeat-live membership, confirm dead nodes via a `GET /alive` probe, wait for
a stability gate, then write a minimal-movement assignment (`Assignment.compute`)
with epoch-guarded UPDATEs. Each node's `WorkerService` claims its partitions and
runs the app's `ItemProcessor.process(itemId)` pump; on reassignment it calls
`stopProcess(itemId)` and the lease is held until the app calls back
`ClusterNode.release(itemId)` — which is what guarantees there is never a second
pump for a partition.

Under the cluster the event-side pump is **at-least-once**: checkpoints commit
*after* dispatch, so a crash or handoff re-processes the straddling event on the
new owner. **Projections must be idempotent** (UPSERT / SET / track last-applied
`segment_seq`); the framework does not de-duplicate.

## Lifecycle

The framework has two phases that **must not overlap**:

1. **Setup** — register dependencies in `GlobalRegistry`, subscribe handlers on
   the buses, configure interceptors and per-group policy, call
   `SegmentCalculator.setSegments(N)`, and start the `Scheduler`. No
   `send` / `publish` in flight.
2. **Runtime** — begins implicitly on the first `send` / `publish`. From then on
   the topology (handler maps, per-group policy, `GlobalRegistry` bindings, the
   `SEGMENTS` value) is **frozen**.

The frozen-topology rule is **per-node, not per-cluster**: nodes may join, leave,
and rebalance, but every handler stays subscribed to the same buses — the cluster
only changes which node's bus sees which partition's traffic.

## Building

```bash
mvn clean verify
```

Unit tests (`*Test`) run under Surefire; integration tests (`*IT`) under
Failsafe. `kf-samples/kf-cluster-it` is Docker-gated (Testcontainers / MySQL) and
only runs when a Docker daemon is reachable.

## Logging & observability

Logging is SLF4J over Logback. Every framework logger lives under the
`org.kendar.cqrses` namespace, so a single line scopes framework verbosity
without touching the rest of your application:

```xml
<logger name="org.kendar.cqrses" level="DEBUG"/>
```

### SQL trace logging

`DefaultDb` (the JDBC `Db` wrapper used by `kf-core-db` and `kf-cluster`) can log
**every statement it runs with the `?` placeholders interpolated** into an
approximate, copy-pasteable SQL string (see `SqlApproximator`). Enable the
`org.kendar.cqrses.db` logger at `DEBUG`:

```xml
<logger name="org.kendar.cqrses.db" level="DEBUG"/>
```

The call site is guarded by `Logger.isDebugEnabled()`, so when the logger is at
`INFO` or above the trace costs nothing — leave it out (or at `INFO`) in
production and switch it to `DEBUG` only while diagnosing query behaviour.

## Further reading

- **`CLAUDE.md`** — the authoritative design notes: module contracts, cluster
  topology, intentional trade-offs, and the full lifecycle rules.
- **`docs/tricks.md`** — non-obvious implementation patterns (per-aggregate
  locking, the `-1` version sentinel, segment-tail reads, …). The *what* and
  *why* are authoritative; cross-check class / method names against the current
  `kf-core` tree, as parts of it still reference the legacy `kf-es-*` names.
