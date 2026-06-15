# kf-core — the kernel

The kernel of the Kendar Framework: every other module depends on `kf-core`, and
`kf-core` depends on nothing inside the framework. It is a plain library — no
Spring, no Guice, no CDI — built for **Java 25**. Runtime dependencies are
**Jackson** (serialisation) and **Logback / SLF4J** (logging); **H2** is
test-scope only.

`kf-core` defines *contracts and shared behaviour*, not storage. It contains the
annotations, the abstract bus, the service locator, the store **interfaces**
(implemented by `kf-core-memory` / `kf-core-db`), the processing-group /
segment / saga machinery, serialisation + upcasting, a thin JDBC wrapper, and
the SPI the cluster module drives.

## Package map (`org.kendar.cqrses`)

| Package | What lives there |
|---------|------------------|
| `annotations` | The full annotation set: `@Aggregate`, `@Command`, `@Event`, `@CommandHandler`, `@EventHandler`, `@Projection`, `@Saga`, `@SagaHandler`, `@SagaStart`, `@SagaId`, `@AggregateIdentifier`, `@AggregateVersion`, `@CommandInterceptor`, `@CreationPolicy`, `@UpcasterSpec`, `@Schedulable`, `@Schedule`. |
| `bus` | The abstract bus base. `Bus` (shared dispatch state + policy map), `CommandBus` (aggregate / interceptor subscription + rehydration), `EventBus` (saga association + `SagaStore` wiring), `Context`, `InternalMessage`, `EventApplyer`. Concrete buses live in the memory/db modules. |
| `di` | `GlobalRegistry` — the static service locator (handler maps, `@AggregateIdentifier` cache, `Class → TargetType` map, optional `fallbackResolver`); `TargetType`. |
| `dlq` | Dead-letter machinery: `DlqStore`, `DlqManager`, `LocalDlqManager`, `DlqItem`, `DlqItemStatus`, `DlqEnqueuePolicy`, `DlqEnqueueDecision(Result)`. |
| `repositories` | Store **contracts** + bases: `EventStore`, `SagaStore`, `CheckpointStore`, `BaseEventStore`, `BaseSagaStore`, `AggregateSnapshot`, `SagaInstance`. |
| `pg` | Processing-group / segment engine: `ProcessingGroupsManager`, `ProcessingGroup`, `SegmentProcessor`, `SagaSegmentWorker`, `SagaResolver`, `SegmentCalculator`, `LaneWork`, `LocalSegmentOwner`, and the sequence policies (`NullSequencePolicy`, `PerAggregateSequencePolicy`, `PerSegmentSequencePolicy`, `SequencePolicy`). |
| `saga` | Saga association: `AssociationValue`, `PropertyAccessor`, `JavaBeanPropertyAccessor`, `SagaManager`. |
| `scheduler` | `Scheduler` SPI (closure one-shot + durable named-task overload) and `Sleeper` adaptive backoff. |
| `serialization` | `MessageSerializer`, `JacksonMessageSerializer`, `Upcaster`, `UpcastersManager`. |
| `db` | Thin JDBC wrapper: `Db`, `DefaultDb`, `ConnectionStorage`, `InsertBuilder`, `UpdateBuilder`, `RowMapper`, `UuidBytes`, `SqlApproximator`, `DbException`. No JPA / Hibernate. |
| `cluster.spi` | `SegmentOwnership` — the SPI the cluster module calls to learn which segments this node owns (implemented by `SegmentProcessor`). |
| `exceptions` | Typed framework exceptions (`OptimisticConcurrencyException`, `InvalidHandlerException`, `ProcessingGroupStoppedException`, …). |
| `utils` | `ReflectionUtils`, `TriConsumer`, `UUIDGenerator`. |

## Configuration

`kf-core` has **no external configuration file** of its own beyond
`src/main/resources/logback.xml` (default logging). Everything is configured
*programmatically* during the **setup phase**:

- **Segment count** — `SegmentCalculator.setSegments(N)`. **Frozen at setup**;
  changing it after events are written misroutes their replay. Default `3`.
- **Per-group policy** — `Bus.ProcessingGroupPolicyConfig` couples a group name
  with a `DlqEnqueuePolicy` and a `SequencePolicy`, keyed by group (not handler).
- **Serialisation** — register a `MessageSerializer` (default
  `JacksonMessageSerializer`) and any `Upcaster`s via `UpcastersManager`.
- **Service wiring** — register dependencies + subscribe handler classes/lambdas
  on the buses through `GlobalRegistry`. `GlobalRegistry.start()/stop()` walk and
  start/stop the `CommandBus` and `EventBus`.

> **Lifecycle rule (see root `CLAUDE.md`).** Setup and runtime must not overlap.
> Once the first `send`/`publish` fires, the topology — handler maps, per-group
> policy, interceptor chain, `GlobalRegistry` bindings, `SEGMENTS` — is **frozen**.

## Intentional trade-offs

Reflection-based handler discovery, a static `GlobalRegistry`, an abstract `Bus`
with shared state, best-effort snapshots, and the `-1` aggregate-version
sentinel are all **deliberate** — see the "Intentional trade-offs" section of
the root `CLAUDE.md` before "fixing" any of them.

## Build & test

```bash
mvn -pl kf-core -am test
```

Tests use H2 + JUnit 5. JDK-interface dependencies (`Connection`, `DataSource`)
are tested via the **template-method** pattern (`Db.connection()` is overridable)
rather than `--add-opens` or Mockito mock-maker overrides.
