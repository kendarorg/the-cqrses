# Setting up Kendar Framework with Spring Boot

`kf-spring` is a Spring Boot 3.4 auto-configuration starter that wires the
`kf-core-db` single-node stack (event store, saga store, DLQ, scheduler,
command/event buses) and, optionally, a `kf-cluster` node — with one import and
a handful of `kf.*` properties.

It is the **only** Kendar module that touches Spring. `kf-core`, `kf-core-db`,
and `kf-cluster` stay pure POJO / `java.sql` / JDK-only.

---

## 1. Add the dependency

```xml
<dependency>
    <groupId>org.kendar.cqrses</groupId>
    <artifactId>kf-spring</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

This transitively pulls in `kf-core`, `kf-core-db`, and `kf-cluster`. Auto-config
is registered via
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`,
so no `@Import` or `@EnableKendar` is needed — a plain `@SpringBootApplication`
is enough.

```java
@SpringBootApplication
public class MyApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }
}
```

---

## 2. Provide the framework DataSource (required)

The framework keeps **its** database (event store, saga store, checkpoints,
scheduler) segregated from your application's own data sources. You must declare
a `DataSource` bean with the qualifier **`kf-datasource`**. Without it, context
startup fails.

```java
@Configuration
public class DataSourceConfig {

    @Bean("kf-datasource")
    public DataSource kfDataSource(@Value("${myapp.datasource.url}") String url) {
        return DataSourceBuilder.create().url(url).build();
    }
}
```

The framework targets **H2 in MySQL-compatibility mode** or a real MySQL server.
The framework tables are created automatically at startup by `SchemaInitializer`
(invoked when the `Db` bean is built).

You may point this DataSource at the same physical database as your read-model
tables (the sample app does), or keep them separate.

> Tip: an H2 file URL for local dev:
> `jdbc:h2:file:./data/app;MODE=MySQL;DB_CLOSE_DELAY=-1`

---

## 3. Configure the YAML

All framework configuration lives under the `kf.*` namespace, bound by
`KfProperties` (`@ConfigurationProperties("kf")`). Two of those values
(`kf.segments`, `kf.liveness.port`) are special: they are also read directly
from the environment *before* the Spring context exists (see §6), so they must
resolve from plain `application.yml` / env vars — not from a `@Bean`-supplied
property source.

### 3.1 Full annotated `application.yml`

This single block shows **every** `kf.*` property at once. In practice you only
need the few marked *mandatory*; the rest have working defaults.

```yaml
kf:
  # ── Core (mandatory) ───────────────────────────────────────────────
  # Partition / segment count. Hashes every aggregate id (or saga association
  # value) into 0..N-1. Drives lanes (single node) and partitions (cluster).
  # READ BEFORE THE CONTEXT and FROZEN at setup — changing it after events are
  # written misroutes their replay. Pick once for the life of the data. Default 3.
  segments: 4

  liveness:
    # Port for the cluster's HTTP /alive + /notify server. MANDATORY and
    # validated pre-context even in single-node mode (where it is not bound).
    # Default 8070.
    port: 8070

  # ── Handler discovery ──────────────────────────────────────────────
  scan:
    # Classpath roots scanned for @Aggregate / @Saga / @Projection /
    # @CommandInterceptor. List form or comma-separated single line.
    # Empty (default) → falls back to the @SpringBootApplication package.
    base-packages:
      - com.example.myapp.domain
      - com.example.myapp.read

  # ── Event-side processing groups ───────────────────────────────────
  # Group names that receive the default EventBus policy (DLQ + sequence).
  # These are the `group=` values on your @Projection / @Saga classes.
  # Empty (default) → no group gets the default policy preinstalled; use an
  # EventBusCustomizer bean (see §8) for anything non-default.
  processing-groups:
    - users
    - ledger

  # ── Cluster (all optional; off by default) ─────────────────────────
  cluster:
    # Master switch. false (default) = single-node JDBC, the correct
    # out-of-the-box mode. true = join a cluster against a shared DB.
    enabled: false
    # Stable identity so a restarted node re-adopts its rows.
    # null/unset (default) → a random UUID is generated each boot.
    node-id: node-1
    # Host advertised to peers for /alive + /notify probes.
    # null/unset (default) → auto-detected at cluster start.
    host: 10.0.0.11
    # Projection dispatch threads PER processing group. 1 (default) = one
    # worker polls all this node's owned segments in a single wide read.
    # N>1 fans dispatch across N segment-partitioned slots for CPU-heavy
    # projections; per-aggregate ordering still holds (a segment always maps
    # to the same slot). Has no effect in single-node mode.
    dispatch-concurrency: 1
```

### 3.2 Property reference

| Property | Type | Default | Mandatory | Meaning |
|---|---|---|---|---|
| `kf.segments` | int | `3` | **yes** | Partition/lane count. Read pre-context (§6) and **frozen at setup** — changing it after events exist misroutes replay. Must be a positive integer. |
| `kf.liveness.port` | int | `8070` | **yes** | Port for the cluster `/alive` + `/notify` HTTP server. Validated pre-context even single-node; only actually bound when the cluster is enabled. Must be a positive integer. |
| `kf.scan.base-packages` | List&lt;String&gt; | `@SpringBootApplication` package | no | Roots scanned for handler classes (§4). YAML list or comma-separated. |
| `kf.processing-groups` | List&lt;String&gt; | empty | no | Event-side group names that get the default `EventBus` policy. Should match the `group=` on your `@Projection` / `@Saga`. |
| `kf.cluster.enabled` | boolean | `false` | no | `false` = single-node JDBC; `true` = cluster node against a shared DB. |
| `kf.cluster.node-id` | String | random UUID | no | Stable node identity across restarts. |
| `kf.cluster.host` | String | auto-detected | no | Host advertised to peers for `/alive` + `/notify`. |
| `kf.cluster.dispatch-concurrency` | int | `1` | no | Projection dispatch threads per processing group. `N>1` fans out into N segment-partitioned slots (ordering preserved). No effect single-node. |

> **Mandatory + fail-fast.** `kf.segments` and `kf.liveness.port` are read and
> validated by `KfSegmentEnvironmentPostProcessor` *before any bean is created*.
> If either is missing or not a positive integer, startup aborts immediately —
> defaults are intentionally **not** silently inherited at the pre-context stage,
> so you must declare both even though `KfProperties` lists fallback values.

### 3.3 Application-side properties you also need

The framework requires **you** to supply the database; the URL is your own
property, not a `kf.*` one. The sample app exposes it as `pfm.datasource.url`
and binds it in the `@Bean("kf-datasource")` method (§2):

```yaml
# Your namespace — referenced by your DataSourceConfig, not by the framework.
pfm:
  datasource:
    # File-backed H2 in MySQL mode survives restarts; use a real MySQL URL
    # in production / cluster mode.
    url: jdbc:h2:file:./data/app;MODE=MySQL;DB_CLOSE_DELAY=-1
```

Standard Spring Boot properties apply as usual and interact with the framework:

```yaml
server:
  port: 8080          # your web layer (independent of kf.liveness.port)

logging:
  level:
    org.kendar.cqrses: INFO
    # Per-event projection-pump diagnostics (DISPATCH / SKIP, no-op reasons).
    org.kendar.cqrses.pg.SegmentProcessor: DEBUG
    org.kendar.cqrses.pg.ProcessingGroupsManager: DEBUG
```

### 3.4 Overriding via environment variables

Because `kf.*` is bound through Spring's relaxed binding, every property has an
environment-variable form — useful for containers and the cluster tests, where
each node differs only by a few values. The mapping replaces `.`/`-` with `_`
and upper-cases:

| YAML | Environment variable |
|---|---|
| `kf.segments` | `KF_SEGMENTS` |
| `kf.liveness.port` | `KF_LIVENESS_PORT` |
| `kf.cluster.enabled` | `KF_CLUSTER_ENABLED` |
| `kf.cluster.node-id` | `KF_CLUSTER_NODE_ID` |
| `kf.cluster.host` | `KF_CLUSTER_HOST` |
| `kf.cluster.dispatch-concurrency` | `KF_CLUSTER_DISPATCH_CONCURRENCY` |
| `server.port` | `SERVER_PORT` |
| `pfm.datasource.url` (app's own) | `PFM_DATASOURCE_URL` |

This is exactly how the 3-node `kf-cluster-it` configures its containers — one
image, per-node env only:

```text
KF_CLUSTER_ENABLED=true
KF_SEGMENTS=4
KF_LIVENESS_PORT=8070
KF_CLUSTER_NODE_ID=node-1     # node-2, node-3 on the other containers
KF_CLUSTER_HOST=node-1        # network alias reachable by peers
PFM_DATASOURCE_URL=jdbc:mysql://db:3306/pfm?...   # shared MySQL
SERVER_PORT=8080
```

> Note: command-line args and environment variables sit **above**
> `application.yml` in Spring Boot's property precedence — an env var or
> `--kf.segments=8` overrides the YAML value (and is seen by the pre-context
> post-processor too).

---

## 4. Write your handlers

Handlers are discovered by classpath scan (`KfHandlerScanner`) over
`kf.scan.base-packages`. Four class-level annotations are picked up:
`@Aggregate`, `@Saga`, `@Projection`, `@CommandInterceptor`.

### Aggregates (not Spring beans)

The framework instantiates an aggregate per id and replays its events — do **not**
make it a `@Component`.

```java
@Aggregate(group = "accounts")
public class Account {
    public boolean registered;

    @CommandHandler
    public void handle(RegisterUser cmd) {
        if (registered) return;
        EventApplyer.apply(this, new UserRegistered(cmd.userId, cmd.username));
    }

    @EventHandler
    public void on(UserRegistered ignored) {
        registered = true;     // fold event into local state
    }
}
```

### Projections (Spring beans allowed)

A projection **may** be a `@Component`, so it can take Spring collaborators.
Handler method parameters beyond the event are resolved from Spring first, then
from `GlobalRegistry`.

```java
@Component
@Projection(group = "users")
public class UserProjection {

    @EventHandler
    public void on(UserRegistered e, UserReadStore store) {
        store.upsert(e.userId, e.username);   // store injected as a collaborator
    }
}
```

> **Projections must be idempotent.** The event-side pump is at-least-once
> (checkpoint committed after dispatch), so design read-model writes as
> UPSERT / SET / last-applied-seq tracking. The framework does not de-duplicate.

### Read-model schema

Create your own read-model tables *before* event dispatch begins. Doing it in a
`@PostConstruct` works, because `KfBootstrap` runs as a very late
`SmartLifecycle` (after all `@PostConstruct`):

```java
@Component
public class ReadModelSchema {
    private final DataSource dataSource;   // your read-model DataSource
    @PostConstruct
    public void initialize() {
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql"))
            .execute(dataSource);
    }
}
```

---

## 5. Send commands / read state

Inject the auto-wired `CommandBus`. There is **no QueryBus** — query your
projections (read stores) directly.

```java
@RestController
@RequestMapping("/api")
public class FinanceController {
    private final CommandBus commandBus;
    private final UserReadStore users;

    public FinanceController(CommandBus commandBus, UserReadStore users) {
        this.commandBus = commandBus;
        this.users = users;
    }

    @PostMapping("/login")
    public LoginResponse login(@RequestBody LoginRequest req) {
        UUID userId = Uuids.userId(req.username());
        if (!users.exists(userId)) {
            commandBus.sendSync(new RegisterUser(userId, req.username()));
        }
        return new LoginResponse(userId.toString(), req.username());
    }

    @GetMapping("/summary")
    public Summary summary(@RequestParam String username) {
        return users.summary(Uuids.userId(username));   // read straight from projection
    }
}
```

---

## 6. How it starts up (what happens under the hood)

1. **Pre-context** — `KfSegmentEnvironmentPostProcessor` reads `kf.segments`
   and calls `SegmentCalculator.setSegments(N)` *before* the context refreshes.
   This must happen before any store, bus, or `ProcessingGroupsManager` is built.
   `kf.liveness.port` is validated here too.
2. **Bean creation** — `KfAutoConfiguration` declares every infrastructure bean
   (`Db`, `MessageSerializer`, `UpcastersManager`, `EventStore`, `SagaStore`,
   `DlqStore`, `CheckpointStore`, `Scheduler`, `CommandBus`, `EventBus`,
   `SegmentProcessor`, plus the cluster/local segment driver). The framework
   tables are created when the `Db` bean is built.
3. **Lifecycle start** — `KfBootstrap` (a late `SmartLifecycle`) registers all
   infra into `GlobalRegistry`, scans and registers handler classes, sets a
   Spring fallback resolver for handler collaborators, starts the buses (event
   side runs in **pull mode**), starts the scheduler, then starts either the
   `ClusterNode` (cluster on) or the `LocalSegmentOwner` (cluster off).
4. **Shutdown** — the reverse, each step isolated so one failure doesn't block
   the others.

---

## 7. Running as a cluster

Single-node is the default and needs nothing extra. To run several JVMs against
one shared MySQL with at-most-one-owner-per-partition guarantees, enable the
cluster and point every node at the same database.

### 7.1 Standard cluster `application.yml`

A complete config for a cluster node. This is the **shared** part — identical on
every node; only the three per-node values (`kf.cluster.node-id`,
`kf.cluster.host`, and usually `server.port`) differ, and those are best
overridden per node via environment variables (§3.4).

```yaml
server:
  # HTTP port for your own web layer. Distinct from kf.liveness.port below.
  port: 8080

kf:
  # Partition count. MUST be identical on every node and never changed once
  # events exist. Pick comfortably above the node count (e.g. 16 partitions
  # for a 3-node cluster) so rebalancing spreads load evenly.
  segments: 16

  liveness:
    # Port for this node's cluster /alive + /notify server. Must be reachable
    # by the other nodes at kf.cluster.host:8070.
    port: 8070

  scan:
    base-packages: com.example.myapp

  processing-groups:
    - users
    - ledger

  cluster:
    enabled: true
    # Stable, UNIQUE per node — set via KF_CLUSTER_NODE_ID per container/host.
    # A restarted node with the same id re-adopts its rows cleanly.
    node-id: ${KF_CLUSTER_NODE_ID:node-1}
    # Hostname/IP the OTHER nodes use to reach this node's liveness server.
    # Must be routable across the cluster (container alias, service DNS, or IP).
    host: ${KF_CLUSTER_HOST:node-1}
    # Bump above 1 only for CPU-heavy projections (see §3.2).
    dispatch-concurrency: 1

# The framework's shared database — the SAME physical DB for all nodes.
# Use a real MySQL (or H2 in MySQL mode over TCP); a per-node H2 file URL
# does NOT work for a cluster — nodes must see one another's rows.
myapp:
  datasource:
    url: jdbc:mysql://db-host:3306/kf?useSSL=false&serverTimezone=UTC

logging:
  level:
    org.kendar.cqrses: INFO
    # Useful while validating handoff; drop to INFO once stable.
    org.kendar.cqrses.pg.SegmentProcessor: INFO
    org.kendar.cqrses.cluster: INFO
```

> The `${KF_CLUSTER_NODE_ID:node-1}` syntax takes the env var when present and
> falls back to `node-1` otherwise — so the YAML is shared and each container
> just sets `KF_CLUSTER_NODE_ID` / `KF_CLUSTER_HOST` (§3.4).

### 7.2 Per-node differences

Everything above is identical across nodes **except**:

| Per-node value | node-1 | node-2 | node-3 |
|---|---|---|---|
| `KF_CLUSTER_NODE_ID` | `node-1` | `node-2` | `node-3` |
| `KF_CLUSTER_HOST` | `node-1` | `node-2` | `node-3` |
| `SERVER_PORT` (if co-located) | `8080` | `8081` | `8082` |

(`KF_CLUSTER_HOST` is whatever address peers can reach — a Docker network alias,
Kubernetes service name, or the host IP.)

### 7.3 Rules of the cluster

- **Same `kf.segments` everywhere**, fixed for the life of the data.
- **One shared database** — every node's `kf-datasource` points at it. The
  cluster coordinates through five `cluster_*` tables there.
- **Unique `kf.cluster.node-id`** per node; **reachable `host:liveness.port`**
  for the leader's `/alive` + `/notify` probes.
- One node is elected leader and assigns the `0..N-1` partitions; ownership
  rebalances automatically as nodes join, leave, or crash. Handler
  registrations on each JVM never change — only *which* node sees *which*
  partition's traffic.
- Your read model must stay idempotent (the event pump is at-least-once and a
  partition can re-process its straddling event after a handoff).

---

## 8. Overriding framework beans

Every infrastructure bean is `@ConditionalOnMissingBean`, so you can replace any
single piece by declaring your own bean of the same type — e.g. a custom
`MessageSerializer`, `Scheduler`, `CheckpointStore`, or `EventStore`.

To tweak the `EventBus` (per-group DLQ / sequence policies) without replacing it,
provide an `EventBusCustomizer`:

```java
@Bean
public EventBusCustomizer myEventBusCustomizer() {
    return eventBus -> eventBus.setProcessingGroupPolicy(
        Bus.defaultProcessingGroupPolicyConfig("orders"));
}
```

---

## Quick checklist

- [ ] Add the `kf-spring` dependency.
- [ ] Declare a `@Bean("kf-datasource") DataSource`.
- [ ] Set `kf.segments` and `kf.liveness.port` (mandatory).
- [ ] Set `kf.scan.base-packages` (or rely on the `@SpringBootApplication` package).
- [ ] List your event-side `kf.processing-groups`.
- [ ] Write `@Aggregate` / `@Projection` / `@Saga` classes; make projections idempotent.
- [ ] Create read-model tables before dispatch (`@PostConstruct`).
- [ ] Inject `CommandBus` to write; query projections to read.
- [ ] (Optional) set `kf.cluster.enabled: true` and per-node identity to cluster.
