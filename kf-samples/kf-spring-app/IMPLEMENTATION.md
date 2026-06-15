# kf-spring-app — Implementation notes

How this sample wires the **kf framework** end-to-end through the `kf-spring` starter. The
[README](README.md) covers what the app *is* and how to run it; this file explains the
framework-related machinery: the CQRS/event-sourcing model, how Spring beans bridge into the
framework's static registry, the DLQ policy, the cluster-control plumbing, and the configuration
knobs that drive it all.

If you are new to the framework, read the root [`CROSS_CUTTING.md`](../../CROSS_CUTTING.md) first — it is the
authoritative design overview of all the `kf-*` modules. This document is the worked example.

## What the starter does for you

`PfmApplication` is a bare `@SpringBootApplication`. Everything below is auto-configured by
`kf-spring`'s `KfAutoConfiguration`, which is picked up off the classpath:

- **Stores + buses.** The single-node `kf-core-db` stack (`JdbcEventStore`, `JdbcSagaStore`,
  `JdbcDlqStore`, `JdbcCheckpointStore`, `JdbcScheduler`, `JdbcCommandBus`, `JdbcEventBus`,
  `ProcessingGroupsManager`) is constructed around the app-provided `kf-datasource` bean. Every
  infra bean is `@ConditionalOnMissingBean`, so any one can be overridden.
- **Schema.** The starter's `SchemaInitializer` creates the framework's own tables
  (`event_entry`, `processor_checkpoint`, `dlq_item`, saga/scheduler tables, and — when the cluster
  is on — the five `cluster_*` tables). The app's read tables are separate (see below).
- **Handler discovery.** `KfHandlerScanner` walks `kf.scan.base-packages` for `@Aggregate`,
  `@Projection`, `@Saga`, `@CommandInterceptor`, `@Schedulable` and registers each into
  `GlobalRegistry`.
- **Segments.** `KfSegmentEnvironmentPostProcessor` reads `kf.segments` from the environment
  *before* context refresh and calls `SegmentCalculator.setSegments(...)` — it must run before any
  store/bus is built, because the segment count is frozen at setup.
- **Lifecycle.** `KfBootstrap` (a late-phase `SmartLifecycle`) starts the buses, and — when the
  cluster is enabled — the `ClusterNode`.

## The one bean you must provide: `kf-datasource`

`config/DataSourceConfig` supplies the mandatory `@Bean("kf-datasource") DataSource`. The framework
deliberately keeps its database segregated behind this qualifier rather than grabbing the app's
default `DataSource`. Here the app and the framework happen to share **one** H2 (`MODE=MySQL`)
database — the framework's tables and the app's `pfm_*` read tables live side by side — and the same
bean backs the read-model `JdbcTemplate`. A real app could point `kf-datasource` at a separate
database.

The URL is bound from `pfm.datasource.url` (file-backed H2 in MySQL mode), so the event store and
read model survive restarts.

## Domain: CQRS / event sourcing

```
HTTP write ──▶ CommandBus.sendSync ──▶ Account (aggregate) ──▶ event ──▶ EventStore
                                                                  │
                                                                  ▼  (event-side pump)
                                                       Projection ──▶ pfm_* read tables
HTTP read  ◀─────────────────────────────────────────── read store (direct query)
```

There is **no QueryBus** in the framework — reads query the projections directly.

| Layer | Classes | Notes |
|-------|---------|-------|
| Aggregate | `domain.Account` (+ `domain.OpType`) | One event stream per user, keyed by a deterministic `userId`. |
| Commands | `commands.RegisterUser`, `commands.RecordOperation` | |
| Events | `events.UserRegistered`, `events.OperationRecorded` | |
| Projections | `read.UserProjection` → `read.UserReadStore`; `read.LedgerProjection` → `read.OperationReadStore` | `read.Uuids` / `read.Uuids.fromBytes` helpers. |

### The aggregate

`Account` is **not** a Spring bean — kf instantiates it per-id, so the scanner registers it via
`GlobalRegistry.register(Class)` (no instance). It holds only what it needs to guard command
invariants (`registered`); the running balance lives in the read model, not in aggregate state.

Note the two roles of `@EventHandler` *on the aggregate*: `EventApplyer.apply(this, event)` folds the
event into aggregate state during command handling **and** appends it to the store. `on(OperationRecorded)`
is an intentional no-op — it must exist so `EventApplyer` can fold the event, even though no invariant
depends on past operations.

`RegisterUser` is idempotent (returns early if already `registered`), so the `/api/login` flow can
re-issue it safely.

### The projections are idempotent — and they must be

Both projections are **live Spring beans** annotated `@Projection`. Because a bean of the type
exists, the scanner bridges them via `GlobalRegistry.register(Class, bean)` (instance form) instead
of having the framework new them up. They are stateless (all state in the DB), which matters because
a projection singleton in a multi-lane group is invoked **concurrently** by up to `SEGMENTS` lane
threads, each handling a disjoint set of aggregates.

`pfm-schema.sql` makes the read model **insert-ignore on the primary key** (`op_id` for operations,
`user_id` for users). This idempotency is load-bearing: the cluster event-side pump is
**at-least-once** (it advances the durable `processor_checkpoint` *after* dispatch, so a crash or
handoff between "handler ran" and "checkpoint committed" re-processes the straddling event on the new
owner). UPSERT / `INSERT IGNORE` absorbs that re-dispatch so totals never double-count.

The read schema is written in the **H2 ∩ MySQL intersection** so the identical DDL runs on H2
(`MODE=MySQL`, demo) and on real MySQL (the `kf-cluster-it` run). One non-obvious point: the secondary
index is declared *inline* in `CREATE TABLE` rather than as `CREATE INDEX IF NOT EXISTS`, because
MySQL supports `IF NOT EXISTS` on `CREATE TABLE` but not on `CREATE INDEX` — an inline index rides
the table's own `IF NOT EXISTS` and stays idempotent on both backends.

`config/ReadModelSchema` runs `pfm-schema.sql` in `@PostConstruct`, i.e. before `KfBootstrap` starts
the buses and long before any command can flow, so the projection handlers always find their tables.

## DLQ policy: don't silently drop failed projections

By default the framework's event-side policy returns `ignore()` on a handler failure. Combined with
the at-least-once pump advancing its checkpoint after every dispatch, that turns any transient
projection-handler failure (e.g. a row-lock timeout under heavy flood + rebalance) into **permanent,
silent read-model data loss**: the checkpoint has moved past the event and nothing re-applies it.

This sample replaces that with a real DLQ:

- `dlq/EnqueueOnErrorDlqPolicy` — `enqueue()` when the handler threw, `doNotEnqueue()` otherwise.
- `dlq/ProjectionDlqPolicyConfig` — an `EventBusCustomizer` that installs the policy on every
  `kf.processing-groups` group, paired with a `PerAggregateSequencePolicy`. The customizer runs
  *before* the EventBus topology freezes (`ProcessingGroupsManager#start`), so the lanes pick it up.
  Per-aggregate keying means a dead letter blocks head-of-line only for *its* aggregate; every other
  aggregate keeps projecting.

Draining the DLQ (`dlq/DlqControl` + `dlq/DlqController`, `POST /dlq/retry-all`) re-runs every PENDING
dead letter at least once through a node-local `LocalDlqManager`. Because the `DlqStore` SPI keys
lookups by `sequenceId` (no "list all pending" method), `DlqControl` enumerates the shared `dlq_item`
table directly and retries each row in per-aggregate FIFO order. Re-running is safe and repeatable —
the read model is insert-ignore. Any single node can drain the whole queue, since all nodes carry the
same projection handlers and write to the same read model. This endpoint is what the cluster
consistency IT calls to drain stranded events before its final verdict.

## Cluster control: stop/restart a node's cluster part without killing the JVM

`kf-cluster` is on the classpath but **disabled** in the default config (`kf.cluster.enabled=false`)
— single-node `kf-core-db` is the correct demo mode. The `kf-cluster-it` integration run flips it on
via environment overrides against a shared MySQL.

`cluster/ClusterControl` + `cluster/ClusterController` expose runtime control over **this node's**
cluster part so a test (or operator) can stop and restart cluster participation without taking the
JVM down — the container stays up, ports stay mapped, a debugger stays attached.

```
GET  /cluster/status  -> 200 {enabled, running, nodeId, segments}
POST /cluster/stop     -> 200 {enabled, running:false}
POST /cluster/start    -> 200 {enabled, running:true}
```

Key behaviours:

- **No-op when not in cluster.** When `kf.cluster.enabled=false` the `ClusterNode`,
  `SegmentProcessor` and `ItemProcessor` beans are absent (the starter gates them on the flag), so
  every endpoint is an inert success reporting `enabled=false`. The control injects them as
  `ObjectProvider<...>` and tolerates their absence.
- **`stop()` halts both halves.** It calls `ClusterNode.stop()` (worker/heartbeat/leader/liveness —
  which releases the leader lock and lets peers see the node leave) **and**
  `SegmentProcessor.stopAll()` (the event-side pull pump). Both are required: stopping the node alone
  would leave the pump dispatching events other nodes have taken over, duplicating work.
- **`start()` re-joins.** `ClusterNode.start(segments, livenessPort, itemProcessor)` reconstructs its
  services; the worker tick re-claims this node's assigned segments and the `SegmentProcessor`
  rebuilds its worker map. The default `DbLeaderLock` is stateless and re-acquirable, so a
  `stop()`→`start()` cycle on the same beans is safe.
- **Initial state.** `KfBootstrap` has already started the node at boot; `ClusterControl` listens for
  `ApplicationReadyEvent` (which fires after) and sets `running = enabled` so its view matches reality
  from the first request.

For the cluster-wide semantics (leader election, partition distribution, at-least-once pull pump,
handoff), see [`CROSS_CUTTING.md`](../../CROSS_CUTTING.md) and the [`kf-cluster-it` README](../kf-cluster-it/README.md).

## Configuration reference (`src/main/resources/application.yml`)

```yaml
server:
  port: 8080
kf:
  segments: 4                 # partition/segment count; set pre-context by the EnvironmentPostProcessor, frozen at setup
  liveness:
    port: 8070                # mandatory in the starter; only consumed when the cluster is enabled
  cluster:
    enabled: false            # kf-cluster on the classpath but OFF — single-node demo mode
  scan:
    base-packages: org.kendar.pfm   # where KfHandlerScanner looks for @Aggregate/@Projection/...
  processing-groups:          # the projection groups that get an EventBus policy
    - users
    - ledger
pfm:
  datasource:
    url: jdbc:h2:file:./data/pfm;MODE=MySQL;DB_CLOSE_DELAY=-1   # file-backed → survives restarts
logging:
  level:
    org.kendar.cqrses: INFO
    org.kendar.cqrses.pg.SegmentProcessor: DEBUG          # pump DISPATCH/SKIP diagnostics
    org.kendar.cqrses.pg.ProcessingGroupsManager: DEBUG
```

Under `kf-cluster-it` the same app is pointed at a shared **MySQL** container with
`kf.cluster.enabled=true`, `kf.segments=6`, and a per-node `kf.cluster.node-id`/`host`, all via
`KF_*` environment variables — **command-line / env overrides take precedence over the yml**.

## Map of source files

| Package | Responsibility |
|---------|----------------|
| `org.kendar.pfm` | `PfmApplication` — the Boot entrypoint (everything else is auto-wired). |
| `config` | `DataSourceConfig` (the `kf-datasource` bean + read `JdbcTemplate`), `ReadModelSchema` (creates `pfm_*` tables). |
| `domain`, `domain.commands`, `domain.events` | The event-sourced model: `Account` aggregate, commands, events, `OpType`. |
| `read` | Projections (`UserProjection`, `LedgerProjection`) and their read stores; `Uuids` helper. |
| `dlq` | DLQ policy (`EnqueueOnErrorDlqPolicy`, `ProjectionDlqPolicyConfig`) + drain control (`DlqControl`, `DlqController`). |
| `cluster` | Runtime cluster-part control (`ClusterControl`, `ClusterController`). |
| `web`, `web.dto` | `FinanceController` (`/api/*`) and request/response DTOs; static UI under `resources/static`. |
