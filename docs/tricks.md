# Non-obvious patterns in the Kendar Framework

This file documents the implementation tricks a Java developer would get
wrong if they relied on Java intuition alone. Read it together with
`CLAUDE.md` (which is authoritative on module layout, frozen-topology
rules, lifecycle, and segment semantics).

Whenever the code does something non-obvious — sentinel values, locking
patterns, reflection ordering, cluster-fencing — there should be an
entry here. When you change one of those patterns, update this file in
the same commit.

Module legend (per `CLAUDE.md`):

- `kf-core` — kernel: annotations, buses, `ProcessingGroupsManager`,
  `EventStore`/`SagaStore`/`DlqStore`/`CheckpointStore` interfaces,
  `Scheduler` interface, `GlobalRegistry`, `BaseEventStore`,
  `SegmentCalculator`, `SegmentProcessor`, `SegmentOwnership` SPI,
  `Db` wrapper, in-process Sleeper.
- `kf-core-memory` — `InMemoryEventStore`, `InMemorySagaStore`,
  `InMemoryDlqStore`, `InMemoryCheckpointStore`, `InMemoryScheduler`,
  `InMemoryCommandBus`, `InMemoryEventBus`.
- `kf-core-db` — JDBC stores (H2 ∩ MySQL): `JdbcEventStore`,
  `JdbcSagaStore`, `JdbcDlqStore`, `JdbcCheckpointStore`,
  `JdbcScheduler`, `JdbcCommandBus`, `JdbcEventBus`,
  `JdbcProcessingGroup`, `JdbcProcessingGroupsManager`,
  `SchemaInitializer`.
- `kf-cluster` — partition lifecycle: `ClusterNode`, `WorkerService`,
  `LeaderService`, `LeaderLock`/`DbLeaderLock`, `HeartbeatService`,
  `Liveness`, `Assignment`, `SegmentItemProcessor`, `ClusterSchema`.
- `kf-spring` — autoconfig: `KfAutoConfiguration`, `KfBootstrap`,
  `KfHandlerScanner`, `KfSegmentEnvironmentPostProcessor`,
  `KfProperties`, `NoopItemProcessor`.

---

## 1. Aggregate command path

### 1.1 `aggregateVersion == -1` is a dual-purpose sentinel

`Context.aggregateVersion` initialises to `-1`
(`kf-core/.../bus/Context.java:21`). The same value means two
different things depending on direction:

- **Incoming command / event being appended** — "assign next on
  append". `appendEvents` reads the current per-aggregate max and
  replaces `-1` with `currentMax+1` *under the per-aggregate lock*
  before persisting. The stored row never carries `-1`.
- **`loadEvents(aggregateId, fromVersion=-1L)`** — "load all events
  for this aggregate, no version filter". Used by `BaseEventStore`
  during rehydrate after the snapshot lookup
  (`kf-core/.../repositories/BaseEventStore.java:110` calls
  `loadEvents(aggregateId, lastSnapshottedEvent)` where
  `lastSnapshottedEvent` is `-1` when no snapshot exists).

A stored event always has a concrete positive version. Passing `-1`
into `appendEvents` for an explicit-versioned command silently turns
it into an unversioned append; never call `bus.send(cmd, -1)` if you
actually meant "version 0". The framework's `send(cmd)` (no version)
deliberately defaults to `-1` (`CommandBus.java:35`).

### 1.2 Per-aggregate in-process lock + UNIQUE constraint backstop

Two-layer optimistic concurrency:

1. **In-process per-aggregate lock.** Both stores serialise append +
   version assignment for one aggregate inside one JVM:
   - `InMemoryEventStore` — `streamLocks` map at
     `kf-core-memory/.../repositories/InMemoryEventStore.java:28`,
     `synchronized (lock)` covers read-currentMax + assign + insert.
   - `JdbcEventStore` — `streamLocks` map at
     `kf-core-db/.../repositories/JdbcEventStore.java:43`. Note the
     `synchronized (lock)` is currently *commented out* in
     `appendForAggregate` (`JdbcEventStore.java:90`); the in-JVM
     serialisation now relies on `segment_counter`'s
     `SELECT ... FOR UPDATE` row lock instead (see 1.3).

2. **`UNIQUE (aggregate_id, sequence)` constraint.** The durable
   backstop on the JDBC side — a duplicate-sequence insert (a second
   JVM, or a bug in the in-process layer) violates the unique index
   and the driver throws either
   `SQLIntegrityConstraintViolationException` or a SQLState class
   `23` error. Both are translated to
   `OptimisticConcurrencyException`
   (`JdbcEventStore.java:155-168`). The DDL declares the index in
   `SchemaInitializer.java:56` (H2) and `:173` (MySQL).

The relationship: lock-then-write inside one JVM is the fast path
that almost always wins; the UNIQUE backstop is what makes the
cross-JVM single-node-DB story correct without any application-level
fencing. **Cross-JVM serialisation (the old advisory-lock guarantee)
is *not* provided by `JdbcEventStore`.** The OCC retry loop in
`ProcessingGroup.invokeCommandSync` (see 1.6) is what keeps a
synchronous command correct under contention; the cluster module's
single-owner-per-segment is what keeps the event/async path correct.

### 1.3 `segment_counter` reservation in one transaction

`JdbcEventStore` does **not** call `MAX(segment_seq)` and add one —
that races. Instead it keeps a `segment_counter(segment, next_seq)`
table, one row per segment, and reserves a contiguous block under
`SELECT ... FOR UPDATE`
(`JdbcEventStore.java:197-213`):

1. `INSERT IGNORE INTO segment_counter VALUES (segment, 0)` to
   guarantee the row exists.
2. `SELECT next_seq FROM segment_counter WHERE segment = ? FOR
   UPDATE` — takes a row lock + reads the reserved start.
3. Insert N events with `segSeq = nextSeq..nextSeq+N-1`.
4. `UPDATE segment_counter SET next_seq = ? WHERE segment = ?` —
   advance by N.

All in the same transaction. An OCC rollback (1.2) rolls the counter
back too — no hole in `segment_seq`. The gap-free property is what
makes `processor_checkpoint.last_seq` a clean high-water-mark with
no gap tracking (see 2.3). DDL in `SchemaInitializer.java:69` /
`:179`.

### 1.4 Rehydrate: snapshot + tail replay through `EventApplyer.apply`

`BaseEventStore.loadAggregate`
(`kf-core/.../repositories/BaseEventStore.java:97`) is the only
rehydration path. The flow is:

1. `aggregateType.getDeclaredConstructor().newInstance()` — a no-arg
   constructor is mandatory (`MissingAggregateConstructorException`
   otherwise).
2. `loadSnapshot(aggregateId)` — best-effort. If present, look up a
   cached `setSnapshot(<SnapshotType>)` method on the aggregate via
   `findSnapshotSetter` and invoke it. Absence of that method when
   a snapshot exists is a hard error
   (`MissingAggregateSnapshotHandlerException`).
3. `loadEvents(aggregateId, lastSnapshottedEvent)` — events with
   `version > lastSnapshottedEvent`, or all events if no snapshot
   (the `-1` sentinel from 1.1).
4. For each event, sort by `aggregateVersion`, run
   `upcastersManager.upcast(msg)` (see 10.2), then call
   `EventApplyer.apply(instance, event, msg.getContext())` to fold
   it onto the aggregate (`BaseEventStore.java:149-167`).

The fold dispatches through the per-aggregate event-handler map
registered eagerly in `GlobalRegistry.registerAggregateEventHandlers`
(`GlobalRegistry.java:67-111`). Looking up the event class is by
**simple name** match against the registered handlers' key set
(`BaseEventStore.java:152`) — a renamed event class without an
upcaster (10.2) throws at replay time. There is no fallback.

**Same code path on command write.** When an aggregate calls
`EventApplyer.apply(this, event)` inside a `@CommandHandler`, the
applier (a) invokes the matching `@EventHandler` so in-memory state
stays consistent and (b) appends the event to the per-command
`ThreadLocal` buffer (`EventApplyer.java:36`,
`begin`/`drain`/`apply`). The bus's `dispatchCommand`
(`ProcessingGroup.java:175-187`) brackets the handler invocation
with `begin` / `drain` — successful handlers persist + publish the
drained events; thrown handlers discard the buffer.

The buffer lives in a `ThreadLocal` for the same reason
`SagaManager.ENDED` does (5.3): the dispatcher invokes the handler
on its own worker thread, and the aggregate has no other way to
reach the surrounding dispatch context without changing the
`@CommandHandler` method signature. **Only the bus is allowed to
bracket a command with `begin()` / `drain()`** — those are
package-private. Rehydrate calls `apply(instance, event, ctx)` with
no buffer active and the call only folds, doesn't record.

### 1.5 `@AggregateVersion` field is reflected and assigned per fold

If the aggregate declares a field annotated `@AggregateVersion` (any
of `long`/`Long`/`int`/`Integer`),
`BaseEventStore.findVersionField` caches the field per class
(`BaseEventStore.java:54-67`) and writes the current event's
`aggregateVersion` into it after every fold
(`BaseEventStore.java:164-166`). The initial value at
load-from-snapshot time is the snapshot's `aggregateVersion`
(`:114`). Absence of the field is fine — caching uses the
`NO_VERSION_FIELD` sentinel `Field` (which is just a self-reference
to `BaseEventStore.snapshotSetterCache`) so `computeIfAbsent` never
returns null.

The cache uses sentinel `Field`/`Method` objects (`NO_SETTER`
declared as `BaseEventStore.findSnapshotSetter`,
`BaseEventStore.java:38-44`) so a class that has no setter / no
version field is encoded as "found nothing" rather than re-reflecting
on every load. **Do not** put real entries with those keys.

### 1.6 Synchronous command OCC retry loop

`CommandBus.sendSync` propagates exceptions to the caller (no DLQ
absorption) — that is the synchronous contract. The two-nodes-one-
aggregate race (or a stale expected version) surfaces as
`OptimisticConcurrencyException`; the bus retries it up to
`MAX_COMMAND_OCC_RETRIES = 8` with a `COMMAND_OCC_BACKOFF_MS = 5L`
backoff (`ProcessingGroup.java:341-386`,
`invokeCommandSync`). Each attempt calls `bus.findTarget` again,
which reloads the aggregate and recomputes the next version against
the winner's just-committed event. After the budget the violation
propagates to the caller.

`invokeCommandSync` is **only** used for `sendSync`. The async
`invokeConsumers` path (`ProcessingGroup.java:221-299`) keeps the
DLQ semantics: failures route through the group's `DlqEnqueuePolicy`
(see 4.x) and an IGNORE policy silently drops; a `sendSync` failure
would otherwise be absorbed and the HTTP caller would receive a
false ack. `ProcessingGroupsManager.sendSync` routes commands to
`invokeCommandSync` and events to `invokeConsumers`
(`ProcessingGroupsManager.java:156-176`).

### 1.7 `findTarget` routes by class registry, not by handler class

`Bus.findTarget` is split across `CommandBus.findTarget`
(`CommandBus.java:55-65`) and `EventBus.findTarget`
(`EventBus.java:82-92`). Both delegate to
`GlobalRegistry.getTargetType(registration.handlerClass())` to
discover what kind of target to resolve:

- `AGGREGATE` → `rehydrateAggregate` (always reloads from the
  event store; respects `@CommandHandler(creationPolicy=...)` — see
  1.8).
- `COMMAND_HANDLER` → `GlobalRegistry.get(handlerClass)` (the
  registered singleton instance — only `@CommandInterceptor` is
  classified this way today).
- `PROJECTION` → `GlobalRegistry.get(handlerClass)` (singleton).
- `SAGA` → `loadSagaInstance` (correlation lookup; may instantiate
  on `@SagaStart`, see 5.4).

`TargetType` is populated in `GlobalRegistry.register` from the
class-level annotation (`GlobalRegistry.java:48-65`). The handler
class itself doesn't carry a "kind" — it's the annotation that
decides. This is what lets `register(type)` work for aggregates
(framework-instantiated, never a Spring bean) and `register(type,
instance)` work for projections/interceptors (Spring beans) with
the same dispatch code.

### 1.8 `CreationPolicy` is checked *after* the aggregate is loaded

`CommandBus.rehydrateAggregate`
(`CommandBus.java:104-121`):

1. Load events for the aggregate id (returns empty
   `Optional<aggregateInstance>` only when both no events and no
   snapshot — `BaseEventStore.loadAggregate` always returns an
   `Optional.of(...)` whenever a stream exists, even with no
   `@EventHandler` to fold).
2. Read `@CommandHandler.creationPolicy` from the method.
3. If `CREATE_IF_NOT_EXISTS && aggregate.isEmpty()` or
   `ALWAYS_CREATE`, return a fresh `newInstance()` (no fold).
4. Else return `aggregate.orElse(null)` (which can be null only if
   loading produced an `Optional.empty()` — uncommon, since even an
   aggregate with no events returns a constructed-but-empty
   instance).

`ALWAYS_CREATE` deliberately ignores any existing aggregate — useful
for replay/reset commands. The default `NEVER_CREATE` returns the
loaded aggregate; if the stream is empty *and* you used
`NEVER_CREATE`, the dispatcher hits the null-target branch and
throws `InvalidHandlerException("Cannot find stored item ...")`
(`ProcessingGroup.java:244-248`).

### 1.9 `InvocationTargetException` unwrapping at every `Method.invoke`

`Method.invoke` wraps every thrown exception as
`InvocationTargetException`. The framework unwraps it at three
places so handler exceptions look like their original type to
upstream catch blocks:

- `Bus.registerMethod` for command/event handlers
  (`Bus.java:124-128`).
- `GlobalRegistry.registerAggregateEventHandlers` for the
  aggregate event-applier
  (`GlobalRegistry.java:90-94`).
- `UpcastersManager.applyHop` for upcaster methods
  (`UpcastersManager.java:121-125`, wraps the inner cause in
  `SerializerException`).

All three follow the same pattern: rethrow `RuntimeException` and
`Error` as-is; wrap a checked cause in a `RuntimeException`. Without
this you'd see `OptimisticConcurrencyException` as
`InvocationTargetException` in the OCC retry loop and the retry
would never match.

### 1.10 Field caching with explicit "null" sentinels

`commandAggregateIdCache` (`GlobalRegistry.java:29`,
`getFieldAnnotatedWith`) and the per-class
`versionFieldCache`/`snapshotSetterCache`/`sagaIdFieldCache` caches
all store sentinels for "no such field/method found":

- `ReflectionUtils.NULL_FIELD` is a real `Field` reference (a
  private holder class, `ReflectionUtils.java:18-25`), used by
  `BaseSagaStore.sagaIdFieldCache` and
  `GlobalRegistry.commandAggregateIdCache`.
- `BaseEventStore.NO_SETTER` is a `Method` reference to
  `BaseEventStore.findSnapshotSetter` itself
  (`BaseEventStore.java:38-44`).
- `BaseEventStore.NO_VERSION_FIELD` is a `Field` reference to
  `BaseEventStore.snapshotSetterCache`
  (`BaseEventStore.java:46-52`).

Why: `ConcurrentHashMap.computeIfAbsent` cannot store `null`. A
miss in a "found / not found" cache would otherwise either keep
re-reflecting or have to use a separate negative-cache structure.
The sentinel-encoded miss means one map and one lookup. Don't
register a real class under those sentinel fields.

### 1.11 Aggregate id extraction is cached by command class

`Bus.extractAggregateId(command)` delegates to
`GlobalRegistry.getFieldAnnotatedWith(command.getClass(),
AggregateIdentifier.class)` which caches the resolved `Field` (or
the `NULL_FIELD` sentinel) per command class
(`GlobalRegistry.java:281-298`). The cache is keyed by class, not
by class+annotation — `getFieldAnnotatedWith` is currently used
**only** for `@AggregateIdentifier` in the dispatch path. If you
ever call it with a different annotation, you'd hit cross-talk —
note the recursive `getFieldAnnotatedWith(clazz, annotation)` at
the end uses the cache regardless of the annotation argument.

---

## 2. Event store + replay

### 2.1 Three tail-read APIs on `EventStore`

`EventStore` (`kf-core/.../repositories/EventStore.java:11`)
distinguishes three kinds of polling:

- `loadSegmentTail(int segment, long afterSeq, int limit)` —
  one segment, all event types. Single-segment poller.
- `loadSegmentsTail(Map<Integer, Long> afterSeqBySegment, int
  limit)` — many segments in one wide read. The cluster
  projection pump uses this so a node owning K segments doesn't
  do K queries. JDBC implements it as `OR`-of-ANDs
  (`JdbcEventStore.java:238-259`) with `ORDER BY segment,
  segment_seq LIMIT ?`.
- `loadSegmentTypeTail(int sourceSegment, Set<String>
  eventTypes, long afterSeq, int limit)` — one source segment,
  filtered to a set of event simple-names, exact order. The saga
  k-way merger uses this — see 5.6.

All three return messages with `segmentSeq` and `createdAt`
populated; the regular `loadEvents` path leaves those at the
defaults (`-1L` and `0L`). The default interface methods
`UnsupportedOperationException` — every durable/pollable store has
to override.

### 2.2 In-memory tail read returns *copies*, not the stored objects

`InMemoryEventStore.copyForTail`
(`InMemoryEventStore.java:172-186`) constructs a fresh
`InternalMessage` with a fresh `Context` for every tail read.
Without this, the tail-read result would alias the live stream
list — and the dispatcher would inadvertently mutate the stored
`Context` (e.g. when `upcast` rewrites `Context.version`). The
JDBC store doesn't have this problem (every row is materialised
by `tailMapper`, `JdbcEventStore.java:215-228`); the in-memory
mirror has to do the same explicitly.

### 2.3 `processor_checkpoint` and the gap-free `segment_seq`

`CheckpointStore`
(`kf-core/.../repositories/CheckpointStore.java:16`) stores one
row per `(processingGroup, segment, sourceSegment)`:

- A **projection group** writes one row per owned segment
  (`sourceSegment == segment`).
- A **saga group** writes `SEGMENTS` rows per owned segment — one
  per merged source stream (see 5.6).

Because `segment_seq` is gap-free (1.3), the cursor is a clean
high-water-mark — no gap tracking, no "I saw 1..5 then 8".

The JDBC implementation enforces monotonicity in SQL:
`ON DUPLICATE KEY UPDATE last_seq = GREATEST(last_seq,
VALUES(last_seq))`
(`JdbcCheckpointStore.java:34`). A lagging old owner cannot
regress a new owner's cursor during an overlap window. The
in-memory store does the same via `merge(..., Math::max)`
(`InMemoryCheckpointStore.java:25`).

**`reset` deliberately allows going backward** — that is the
operator-replay path (`SegmentProcessor.replay`,
`SegmentProcessor.java:206-234`). `save` is monotonic, `reset` is
not. Don't use `reset` for anything else.

### 2.4 Snapshots are best-effort and never auto-scheduled

`storeSnapshot` exists; the framework never calls it. Aggregates
implement `Snapshotable<S>` (see `setSnapshot` lookup at 1.4) only
to support the *read* side — if a snapshot row happens to be
there, the rehydrate uses it. Writing snapshots is the
application's job (typically a periodic call from a job, or an
explicit decision after some event count).

The version stamped onto the snapshot is the **current event
stream max under the per-aggregate lock**
(`InMemoryEventStore.java:226-239`,
`JdbcEventStore.java:314-333`). Reading the max under the lock is
what prevents a torn `(payload, version)` pair if a concurrent
append bumps the stream between the read and the write.

Snapshot setter discovery is by *signature*: a single `public void
setSnapshot(<SnapshotType>)` method on the aggregate
(`BaseEventStore.findSnapshotSetter`,
`:84-95`). The parameter type names the snapshot class — that's
what `MessageSerializer.deserialize` is called with.

### 2.5 `EventStorePartitionContext` ThreadLocal for bare reads

`BaseEventStore.loadAggregate` calls
`EventStorePartitionContext.setGroup(groupOf(aggregateType))` and
clears it in `finally`
(`BaseEventStore.java:99` + `:169-171`). The in-memory store keys
its partition map on `(group, segment)`
(`InMemoryEventStore.java:248`), so a "bare" load (no aggregate
class) — e.g. a unit test calling `loadEvents(aggregateId)` —
needs to know which group's partition to look in.

The ThreadLocal is set by `loadAggregate` automatically. Direct
bare-read callers (tests, mostly) must `setGroup(...)` themselves
before calling `loadEvents` / `loadSnapshot` if their store is
partitioned by group. The JDBC store ignores it because the SQL
already filters by `aggregate_id` regardless of group.

### 2.6 Segment math: MurmurHash3 over the stringified id, then floorMod

`SegmentCalculator.calculateSegment(Object value)`
(`kf-core/.../pg/SegmentCalculator.java:23`) takes
`value.toString().getBytes(UTF-8)`, hashes via MurmurHash3 (x86_32,
seed `0`), and returns `Math.floorMod(hash, SEGMENTS)`.

Three things to know:

- **`Math.floorMod`, not `%`.** Negative hash values map to a
  positive segment index. `% SEGMENTS` would produce `-1..SEGMENTS-1`
  for half the inputs and crash array-indexed lane access.
- **`toString()`-based.** A UUID stringifies to its canonical
  36-char dashed form; `Integer.toString(42)` to `"42"`. If you
  change `value.toString()` on an aggregate-id wrapper class, you
  break segment routing for already-stored events. Don't.
- **`SEGMENTS` is process-static and **must not** change at
  runtime.** See 2.7.

### 2.7 `SegmentCalculator.setSegments` MUST run before any bus/store/PGM build

`SEGMENTS` is read by:

- `SegmentCalculator.calculateSegment` — segment routing for
  appends, tail reads, projection/saga dispatch.
- `ProcessingGroupsManager.start` — lane array sizing
  (`ProcessingGroupsManager.java:80-86`).
- `SegmentProcessor` — caches `segments` in its constructor
  (`SegmentProcessor.java:104`), used for the saga k-way merge
  (`segments` rows per owned segment, `SagaSegmentWorker.run`).
- `kf-spring` — passed as the cluster's `N` (partition count) at
  `ClusterNode.start(itemCount, ...)`
  (`KfBootstrap.java:141`).

Changing `SEGMENTS` after any of those have read it produces
silently wrong routing. In Spring, the value is set in
`KfSegmentEnvironmentPostProcessor.postProcessEnvironment`
(`kf-spring/.../KfSegmentEnvironmentPostProcessor.java:28`) — that
runs during `prepareEnvironment`, before any bean exists and well
before the `ProcessingGroupsManager` constructor. **Don't** add a
new `@Bean` that calls `SegmentCalculator.setSegments` — the order
is not guaranteed against `JdbcEventStore`/`JdbcEventBus`. The
post-processor is the only correct hook.

For pure-Java setups, call `setSegments` before any
`new InMemoryEventStore()` / `new InMemoryCommandBus(...)` / etc.

`kf.segments` is **mandatory** in Spring — the post-processor
refuses a missing or non-positive value
(`KfSegmentEnvironmentPostProcessor.java:34-52`). Same for
`kf.liveness.port`. The static default of `3` in `SegmentCalculator`
is for tests only and must never be silently inherited in a real
deployment.

### 2.8 Replay rewinds the checkpoint, not the lane

`SegmentProcessor.replay(groups, fromSeq, includeSagas)`
(`SegmentProcessor.java:206-234`) is the operator-initiated rebuild
path. Per group:

- **Projection** — under the worker's `gate` (so it cannot be
  mid-dispatch), `checkpointStore.reset(group, seg, seg, fromSeq -
  1)` (the `-1` is because `afterSeq` is strictly-greater) and
  drop the in-memory `cursor` map; the next poll re-tails from the
  reset value
  (`ProjectionGroupWorker.reset`, `SegmentProcessor.java:327-335`).
- **Saga** — stop the current saga worker for the owned segment,
  reset all `SEGMENTS` source checkpoints, restart it
  (`SegmentProcessor.replaySaga`, `:236-252`).

`includeSagas=false` skips saga groups. **Re-driving a saga re-fires
its side effects** — re-emitted commands flow through the
command-side OCC, but downstream of that, a refunded payment is
refunded twice. Saga replay is opt-in only.

A replay is naturally partitioned: each node only replays its
*currently owned* segments. The cluster pull pump is doing the
work; the rebalance / leader is unchanged. If you replay while
ownership is moving, the gaining node picks up from the reset
checkpoint when it claims (see 7.5).

---

## 3. Buses and processing groups

### 3.1 `Bus.subscribedClasses` dedupes by class identity

`Bus.register(class)` is the idempotent entry point
(`Bus.java:56-62`). Re-registering the same class is a no-op —
`subscribedClasses.contains` short-circuits and nothing is mutated
in `consumers` / `messageClasses` / `eventsForProcessingGroups`.
`GlobalRegistry.register(class)` is built on top of this and is
also idempotent (`GlobalRegistry.java:48-65` — only first-time
registration calls `autoSubscribe`).

This is important for restart paths: `Framework.start/stop` cycles
re-register handlers; the bus must not throw "already subscribed".

### 3.2 Per-handler-class duplicate-method check

`Bus.analyzeMethods` enforces "at most one
`@CommandHandler`/`@EventHandler`/`@SagaHandler` per (class, message
type)" (`Bus.java:64-93`). A second method on the same class for
the same message throws
`InvalidRegistrationException("DUPLICATE @...")`. The check uses a
local `seenByMessageType` map, not a process-wide one — two
*different* aggregates handling the same command in the same
processing group is also a fail-fast situation, but that surfaces
later from `storeMethod` allowing both to be registered (they then
share a lane and both fire; the framework deliberately permits
this because two aggregates *legitimately* may want to react to
the same broadcast event).

`GlobalRegistry.registerAggregateEventHandlers` enforces the same
single-handler rule for aggregate `@EventHandler` folds
(`GlobalRegistry.java:101-109`) — the per-aggregate event-applier
map is `(eventClass -> applier)` with `putIfAbsent` and a hard
error on duplicate.

### 3.3 `findTarget` calls `GlobalRegistry.get` per dispatch, not at registration

`Bus.registerMethod` builds a `TriConsumer<target, message,
context>` that, **at dispatch time**, calls `method.invoke(target,
params)` where `params[0]` is the message and `params[i>0]` are
pulled from `GlobalRegistry.get(classParams[i])` per call
(`Bus.java:106-115`). A `Context` parameter is a special-case
(`if (classParams[i] == Context.class)`).

Why: handler-method collaborators are looked up by type at
dispatch time, so a Spring bean swap (or a `fallbackResolver` that
returns a fresh instance per call) is reflected on the next
dispatch. The `KfBootstrap.preWarmCollaborators`
(`KfBootstrap.java:183-208`) pre-warms the cache so no bean is
constructed on a dispatch thread.

**Pre-warm is exempt from the frozen-topology rule.** It only
`putIfAbsent`s into the registry's collaborator cache; it doesn't
touch `consumers`, `processingGroupPolicies`, or `classRegistry`
(see `CLAUDE.md`'s "lifecycle" section).

### 3.4 `ProcessingGroupPolicyConfig` is per-(bus, group), set during setup

`Bus.setProcessingGroupPolicy(config)` registers a tuple of
`(group, DlqEnqueuePolicy, SequencePolicy)` in
`processingGroupPolicies` (`Bus.java:37-42`). This must be called
**before** the handlers are registered for that group —
`Bus.analyzeMethods` reads `processingGroupPolicies.get(group)` to
stamp the policy onto each `Registration`
(`Bus.java:64-93`, `Bus.storeMethod` field `policyConfig`).

If you register the handler first, the policy is the default one
(`Bus.defaultProcessingGroupPolicyConfig`,
`Bus.java:197-208`) — IGNORE DLQ + `NullSequencePolicy`. The
handler is then **stuck on the default until restart**, because
`subscribedClasses.contains` blocks re-subscribing
(3.1).

There's a special-case auto-bind: if `policyConfig.sequencePolicy()`
is a `PerSegmentSequencePolicy`, the bus stamps the group name onto
the policy at `setProcessingGroupPolicy` time
(`Bus.java:38-40`). You can construct a `PerSegmentSequencePolicy()`
bare; the framework fills in the group.

### 3.5 `SequencePolicy` defines the head-of-line block granularity

`SequencePolicy.getSequenceId(message)` produces the string the
DLQ keys head-of-line blocking on (4.2). Three implementations:

- **`NullSequencePolicy`** (`kf-core/.../pg/NullSequencePolicy.java`) —
  returns `null`. The DLQ check
  (`dlqStore.hasBlockedItems(sequenceId=null)`) returns false in
  the in-memory store but throws NPE in JDBC; effectively this
  disables head-of-line blocking. Used for the default group when
  the app hasn't picked a policy. **Don't combine with a
  `DlqEnqueuePolicy` that actually enqueues** — you'd be writing
  rows that never block anything.
- **`PerAggregateSequencePolicy`** —
  `aggregateId.toString().trim()`. A failure on aggregate X
  blocks every subsequent event for aggregate X in this group,
  but other aggregates flow freely
  (`PerAggregateSequencePolicy.java:11`).
- **`PerSegmentSequencePolicy`** — `group + ":seg:" +
  segment(aggregateId)`. A failure blocks the **entire lane**
  (every aggregate hashing to that segment). Coarser, but matches
  the actual unit of cluster ownership — when a node loses
  segment S, every blocked aggregate inside it gets handed off
  with the segment. (`PerSegmentSequencePolicy.java`)

**Sagas must NOT use `PerSegmentSequencePolicy`.** Its
`getSequenceId(event)` hashes `event.aggregateId`, but a saga's
lane is keyed by `segment(sagaId)` — those are different segments
in general. Use `NullSequencePolicy` or
`PerAggregateSequencePolicy` (with the saga's correlation as the
"aggregate" id) instead.

### 3.6 `consumers` map is `processingGroup -> messageClass -> List<Registration>`

`Bus.consumers`
(`Bus.java:27`) is the dispatch index. Look-up order at dispatch:

1. `InternalMessage.context.type` (the simple-name string) →
   `bus.messageClasses.get(type)` → `Class<?>`
   (`Bus.java:136-138`).
2. `consumers.get(group).get(messageClass)` → `List<Registration>`
   (`ProcessingGroupsManager.dispatchProjection`,
   `:235-265` shows the exact path).

The list is iterated in **registration order**
(`ArrayList.add`). Don't depend on order across different
`@Aggregate`s — two classes registering for the same command in
the same group will be invoked in scan order, which is filesystem-
or classloader-dependent.

`messageClasses` is populated lazily on first registration of a
type (`Bus.storeMethod`, `:169-171`) — there is no upfront list of
all messages a bus could see. A message arriving with a `Context.type`
that nobody has subscribed to gets `getMessageClass(type) == null`,
and the dispatcher silently skips it. This is the desired behaviour
for the cluster pull pump: every group reads every event in its
owned segments and filters out the types it doesn't subscribe to
(`SegmentProcessor.ProjectionGroupWorker.run`,
`SegmentProcessor.java:366-394` calls `dispatchProjection`, which
no-ops on unknown types).

### 3.7 Saga / projection group homogeneity check

`ProcessingGroupsManager.start` enforces that an event-side group
is either *all saga* or *all projection*
(`ProcessingGroupsManager.java:62-78`). A mixed group throws
`InvalidRegistrationException`. The reason is structural: a saga
group runs a single `SagaResolver` thread that demuxes events to
the right saga lane via correlation, while a projection group
runs lanes directly; the two dispatch shapes can't share a queue.

This is enforced at `start()`, not at registration — you can
register a saga and a projection in the same group, but the bus
will refuse to start. Catch it in tests.

### 3.8 Sync command bypasses the lane queue

`ProcessingGroupsManager.sendSync`
(`:156-176`) invokes the handler synchronously on the caller's
thread via the `lanes[0]` `ProcessingGroup`. **It uses `lanes[0]`
regardless of the command's aggregate id** — the lane is a tactical
choice for the `policy` reference; the *handler* is found by class
in `consumers.get(group).get(messageClass)`, and OCC retry is
handled by `invokeCommandSync`. There is no segment routing for
synchronous commands because they don't queue.

This means a `sendSync` from your HTTP handler always blocks on
the HTTP thread until the aggregate write commits — which is what
HTTP's "the request succeeded" contract requires.

### 3.9 `Bus.send` from inside a `@CommandHandler` enqueues to lanes

When an aggregate emits an event via `EventApplyer.apply(this,
ev)`, the bus's `dispatchCommand`
(`ProcessingGroup.java:175-187`) calls
`persistAndPublishEmitted(emitted, ctx)` *after* the handler
returns successfully. That helper does two things, in order:

1. `eventStore.appendEvents(envelopes)` — durable persistence,
   under the per-aggregate lock.
2. For each event in declaration order,
   `eventBus.send(eventInstance, assignedVersion)` — pushes onto
   the event bus, which fans out to subscribed processing groups.

The `aggregateVersion` stamped onto the published event is the one
assigned by the store (which may be `currentMax+1` from the `-1`
sentinel, or the explicit version the command supplied —
`ProcessingGroup.persistAndPublishEmitted`, `:142-166`).

A throwing handler discards `EventApplyer.drain()` and nothing
reaches step 1. There is **no transactional outbox** today —
append-then-publish runs sequentially in the bus. A crash between
append and publish leaves the events durable but unpublished;
projections pick them up on next start (in cluster mode, via
`SegmentProcessor`'s pull pump). In single-node push mode, the
events are *lost to projections* across a crash. That is an
acknowledged trade-off, and is why cluster pull mode is the
correct default for anything serious.

### 3.10 `Bus.clear()` doesn't undo `subscribe`

`Bus.clear()` is implemented per-subclass; in
`InMemoryCommandBus`/`InMemoryEventBus`/`JdbcCommandBus`/`JdbcEventBus`
it delegates to `handler.clear()` which just drains the lane queues
(`ProcessingGroupsManager.clear`, `:278-287`). `consumers`,
`subscribedClasses`, and `messageClasses` are **not** cleared — a
restart inside the same JVM goes through `Framework.stop()` ->
`Framework.start()` which clears `GlobalRegistry` (so the bus
itself becomes unreachable and gets garbage-collected), not
`bus.clear()`.

If you `reset` a bus while traffic is in flight, you can drop
messages mid-batch. The setup/runtime phase contract (`CLAUDE.md`)
exists specifically to forbid this.

### 3.11 `Bus.getConsumers` exposes a read-only view for operator tools

`LocalDlqManager` (4.5) needs to re-invoke handlers without
mutating the topology. `Bus.getConsumers(group)`
(`Bus.java:147-149`) returns the frozen `messageClass -> List<Registration>`
map directly. Don't write to it. If you do, you've violated the
frozen-topology rule and the next dispatch sees partial state.

### 3.12 `JdbcProcessingGroup` brackets each dispatch in a transaction

`JdbcProcessingGroup` extends `ProcessingGroup` and overrides the
empty `transactionStart` / `transactionEnd` / `transactionRollback`
hooks
(`kf-core-db/.../pg/JdbcProcessingGroup.java`). At dispatch time
(`ProcessingGroup.invokeConsumers`, `:241`/`:280-282`/`:297`):

- `transactionStart` — if no connection is bound to the thread,
  bind one from `DefaultDb.connection()` with `autoCommit=false`;
  remember `ownsConnection = true`.
- `transactionEnd` — `commit`, then either close+unbind (if we
  bound it) or just restore autocommit (if an outer caller had
  already bound one).
- `transactionRollback` — same shape but `rollback` first.

`JdbcEventStore.appendEvents` inside the handler **detects** the
thread-bound connection
(`JdbcEventStore.java:99-103`,
`ownConn = conn != ConnectionStorage.get()`): if the boundary
already has one, the append runs as part of the boundary's
transaction and leaves commit/rollback to the boundary. Closing
or committing it here would tear the boundary's connection out
from under it (closed connection still latched to the thread).
Only ad-hoc-connection appends (synchronous command with no
boundary) own the lifecycle.

The single-node-JDBC `ProcessingGroupsManager` is
`JdbcProcessingGroupsManager` (`kf-core-db/.../pg/JdbcProcessingGroupsManager.java`)
which only overrides `createProcessingGroup` to return
`JdbcProcessingGroup` instead of the base class. The in-memory
manager uses the base `ProcessingGroup` whose tx hooks are empty.

---

## 4. DLQ

### 4.1 Per-group `DlqEnqueuePolicy`, four possible outcomes

`DlqEnqueuePolicy.shouldEnqueue(message, error)` returns a
`DlqEnqueueDecisionResult` whose decision is one of:

- `ENQUEUE` / `REQUEUE` — `shouldEnqueue() == true`. The failed
  message is written to the DLQ and starts blocking its sequence.
- `EVICT` — `shouldEvict() == true`. The DLQ head for this
  sequence is popped (`dlqStore.evictFirst(sequenceId)`); the
  sequence is now unblocked.
- `IGNORE` — `shouldIgnore() == true`. The failure is silently
  dropped (logged as `WARN`,
  `ProcessingGroup.java:264-271`). This is the default policy
  (`Bus.defaultProcessingGroupPolicyConfig`,
  `:201-208`).
- `DO_NOT_ENQUEUE` — none of the above. The failure propagates;
  for an async dispatch, the lane just logs the error and moves
  on.

`shouldEnqueue` is also called with `error=null` on the "already
blocked" path (see 4.2). That's the only `null` call the policy
sees — and the `AlwaysEnqueueOnError` helper inside
`LocalDlqManager` returns `doNotEnqueue()` for it
(`LocalDlqManager.java:218-224`).

### 4.2 Head-of-line gate runs *before* dispatch

`ProcessingGroup.invokeConsumers`
(`:221-299`) opens with:

```
sequenceId = policy.sequencePolicy().getSequenceId(message);
if (dlqStore.hasBlockedItems(sequenceId)) {
    addToDlq(sequenceId, toSend, null);   // null error
    return;
}
```

A sequence with a pending DLQ item routes every subsequent message
in that sequence straight to the DLQ — no handler invocation. That
keeps per-aggregate (or per-segment) ordering: the dropped events
queue up in DLQ in arrival order via `JdbcDlqStore.ordinal` (4.6) /
in-memory FIFO.

The "no error" path is why `DlqItem.errorMessage` is set to
`"Blocked by processing group ..."` and `errorClass`/`stackTrace`
are null (`ProcessingGroup.addToDlq`,
`:312-339`). Don't read `e.getMessage()` blindly when processing
these — check for the synthetic "blocked" message.

`sendSync` does NOT go through this gate — the OCC retry loop in
`invokeCommandSync` (1.6) bypasses head-of-line blocking entirely.
That's intentional: a synchronous caller already has the right to
fail loud.

### 4.3 `Context` is persisted with Java serialization

DLQ items capture the full failure context so the manager can
rebuild an `InternalMessage` on retry/redispatch
(`ProcessingGroup.addToDlq`, `:325`,
`item.setProcessingContext(toSend.getContext())`).

`Context` implements `Serializable` (`Context.java:17`) specifically
because `JdbcDlqStore` persists it with `ObjectOutputStream`, not
through the registered Jackson `MessageSerializer`
(`JdbcDlqStore.serializeContext`,
`:52-72`). Reason: the default Jackson `ObjectMapper` cannot
serialise `Context.timestamp` (an `Instant`) without the JSR-310
module, and the framework refuses to require an opt-in module just
for DLQ persistence. Java serialization handles `Instant` natively.

Implication: changing the `Context` field shape breaks DLQ
deserialisation across restarts. Add a `serialVersionUID` and an
explicit `readObject` if you have to evolve it.

### 4.4 `DlqItem.equals` is `(id, sequenceId, processingGroup)`

`DlqItem.equals` is overridden to compare only those three fields
(`DlqItem.java:18-22`). This matters in `InMemoryDlqStore.removeItem`
(`InMemoryDlqStore.java:72-79`), which uses
`queue.remove(item)` to find the right entry **wherever it sits**
in the FIFO — not just the head. The custom equals is what makes
that O(n) traversal find the right thing.

A typo-equivalence: if you mutate the `id` of a DLQ item after
`addItem`, you also break removal. Don't.

### 4.5 `LocalDlqManager` uses a *throwaway* `ProcessingGroup` for retry

`LocalDlqManager.retryItem`
(`LocalDlqManager.java:80-108`) cannot use the live worker for
retry — the live worker's `hasBlockedItems` gate (4.2) would route
the retry straight back to the DLQ while the item is still in the
store. So:

1. Build a fresh `ProcessingGroup` for the item's group, backed by
   a `CapturingDlqStore` (whose `hasBlockedItems` is `false`, so
   dispatch actually runs) and an `AlwaysEnqueueOnError` policy
   (so a re-failure is captured instead of being swallowed by the
   group's real IGNORE policy)
   (`LocalDlqManager.java:205-212`).
2. Invoke it synchronously via `pg.invokeConsumers(message, ...)`.
3. If `capturing.captured == null` — success: `store.removeItem`
   the original DLQ row, the sequence is unblocked.
4. Else — re-failure: bump `retryCount`, set
   `lastRetryError{Message,Class,StackTrace}`, write the row back
   `PENDING` (FIFO position preserved by `updateItem`, 4.6), throw
   `DlqRetryFailedException`.

The throwaway PG runs `commandSide=false` unconditionally
(`LocalDlqManager.java:209`) and the ctor rejects a `CommandBus`
(`:55-59`). **v1 is event-side only.** Re-running a command would
re-emit its events and double-write the event store; the framework
has no way to suppress that. If you need command-side DLQ retry,
do it at the application level by re-issuing a fresh command.

### 4.6 `JdbcDlqStore.ordinal` preserves FIFO across updates

`updateItem` updates every mutable field **except `ordinal`**
(`JdbcDlqStore.java:188-203`). The in-memory equivalent is a
rebuild-queue trick: iterate the queue and replace the entry in
place, never `remove()+add()`
(`InMemoryDlqStore.java:56-69`).

This is what makes a failed retry (which calls `updateItem` to
bump `retryCount` and update `lastRetry*`) keep its head-of-line
position. A naive UPDATE-with-NOW-ordinal would move the entry to
the tail and break FIFO inside a multi-item sequence.

`ordinal` is assigned on `addItem` as `MAX(ordinal) + 1` per
`sequence_id` (`JdbcDlqStore.java:107-109`). The
`idx_dlq_seq (sequence_id, ordinal)` index serves `listItems`,
`evictFirst`, and `hasBlockedItems`.

### 4.7 `JdbcDlqStore` keeps an in-JVM identity cache

`JdbcDlqStore.identityCache`
(`:43`) maps `DlqItem.id -> DlqItem`. `getItem` / `listItems`
return the **same** instance that was `addItem`-ed, and
`updateStatus` mutates that live instance
(`:179-183`). Reason: an integration test (or operator tool)
holds a reference to the letter through the framework's
status-transition lifecycle (PENDING → RETRYING → RESOLVED /
DISMISSED). Without the identity cache the test would have to
re-`getItem` after every transition.

After a restart there are no live references; the cache is rebuilt
lazily as `getItem` rehydrates rows
(`:166-176`). Don't rely on the cache being warm if you've never
called `getItem` on a particular id.

### 4.8 "Already blocked" is signalled by `Throwable error == null`

This is repeated for clarity. Three flavours of `addToDlq`
exist (`ProcessingGroup.addToDlq`,
`:312-339`):

- Normal failure: `e != null`. `errorMessage` / `errorClass` /
  `stackTrace` are set; `lastRetry*` is null on first failure.
- Head-of-line block: `e == null`. `errorMessage = "Blocked by
  processing group ..."`, `errorClass = null`,
  `stackTrace = null`.
- Capture during retry: `AlwaysEnqueueOnError` returns
  `enqueue()` only when `error != null`
  (`LocalDlqManager.java:220-223`). A "blocked" call inside the
  retry's throwaway PG (which shouldn't happen because
  `CapturingDlqStore.hasBlockedItems` is false) would return
  `doNotEnqueue()`.

---

## 5. Sagas

### 5.1 Saga lookup uses two indexes; correlation wins

`InMemorySagaStore.loadSagaByCorrelationId`
(`:122-139`) consults `correlationIndex.get((type, value))` first;
this is a node-local global `(type, value) -> sagaId` map updated
on every `storeSaga`. JDBC's equivalent is `saga_correlation` keyed
by `(type, corr_value)`
(`JdbcSagaStore.java:114-125`).

The returned `SagaInstance` has its `correlationId` field set to
the value the caller looked up, **not** the saga's canonical
correlation id
(`InMemorySagaStore.java:130-138`,
`JdbcSagaStore.java:124`). This is because a saga can carry several
correlation values (e.g. order id + payment id + booking id) and
the consumer side shouldn't have to know which one matched.

### 5.2 Stale correlations are dropped on every `storeSaga`

A saga can mutate its correlation values during its lifecycle
(e.g. `paymentId` is null on start, gets set on
`PaymentInitiated`). On every `storeSaga`, the stored *owned set*
is diffed against the new set and stale entries are removed
(`InMemorySagaStore.storeSaga`,
`:67-84`, `JdbcSagaStore.storeSaga`, `:74-90`).

Without this, a `paymentId` value left over from a previous saga
lifecycle could keep shadowing the actual saga later created with
that id. JDBC reads the *old* set from `saga_correlation` (not from
a heap map) so it survives a restart — the in-memory
`ownedBySegment` map is the heap equivalent.

### 5.3 `SagaManager.ENDED` is a ThreadLocal flag set inside the handler

Same shape as `EventApplyer.BUFFER` (1.4). Inside a
`@SagaHandler`, the saga calls `SagaManager.endSaga()`; after the
handler returns normally, the dispatcher reads
`SagaManager.isSagaEnded()`. If true *and* `@Saga(deleteAfterCompletion
= true)`, the dispatcher deletes the saga from the store; else it
writes it back (`ProcessingGroup.dispatchEvent`,
`:195-211`).

`ProcessingGroup.dispatchEvent` `finally`s a `SagaManager.clear()`
so the flag never leaks across invocations — even on a thrown
handler.

### 5.4 `@SagaStart` instantiates inline; otherwise correlation miss → null

`EventBus.loadSagaInstance`
(`:141-166`) handles a saga event with no correlation match:

- If the receiving method is `@SagaStart`: instantiate a fresh
  saga via the no-arg constructor and let the handler populate
  `@SagaId` and association fields. The dispatcher writes it back
  via `sagaStore.storeSaga` after the handler returns
  (`ProcessingGroup.dispatchEvent`, `:199-207`).
- Else: return `null`. The dispatcher's null-target check
  (`ProcessingGroup.java:244-248`) sees `targetType == SAGA` and
  silently skips
  (`continue`) — the event was simply not for any saga of this
  type.

A non-`@SagaStart` saga handler for an event with no correlation
match is a no-op. Don't worry that a missing correlation throws —
it doesn't.

### 5.5 `@SagaId` is assigned *inside* the create handler

The flow for a saga create:

1. The bus correlates and finds no existing saga.
2. The handler is `@SagaStart` → `loadSagaInstance` returns a
   bare instance.
3. The handler runs and assigns `this.sagaId = generate()` plus
   any correlation properties (`paymentId`, etc.).
4. The dispatcher reads back via `extractSagaId(saga)` and stores.

This is also why creates **cannot** be routed by `segment(sagaId)`
up front: the `sagaId` doesn't exist yet. `SagaResolver` runs
creates **inline on the resolver thread** via a thread-less
`createPg`
(`SagaResolver.java:99-105`), serialised across all of that group's
creates on one node. Updates are routed normally — find sagaId via
correlation, hash to a lane, enqueue.

The cluster pull mode's equivalent (`SagaResolver.resolveForSegment`,
`:115-154`) runs both create and update **synchronously on the
saga worker's thread**. The update is gated by
`segment(sagaId) == ownedSegment`; the create is gated by
`segment(event.aggregateId) == ownedSegment`. Exactly one owner per
segment ⇒ no duplicate creation.

### 5.6 Cluster saga workers k-way merge `SEGMENTS` source streams

A saga in `segment(sagaId) == s` can correlate to events from
aggregates in **any** segment. So
`SagaSegmentWorker`
(`kf-core/.../pg/SagaSegmentWorker.java`), one per owned `(group,
segment)` (unlike projections, which collapse to one worker per
group, see 7.4), reads all `SEGMENTS` source segments and merges
them by `createdAt`:

1. Per source segment `k`, refill from
   `EventStore.loadSegmentTypeTail(k, types, cp[k], batchLimit)`
   when its buffer is drained (`refillEmptyBuffers`,
   `:142-150`).
2. Candidate = the buffered head with the smallest `createdAt`
   (tie → lowest `k`)
   (`:87-95`).
3. Low-watermark check: `min(head.createdAt | now - CLOCK_SKEW_MS
   if empty)`. The candidate must be `<= watermark` before it's
   dispatched — otherwise a lagging/empty source might still
   produce an earlier event and break order
   (`:101-115`).
4. Dispatch via `resolver.resolveForSegment(m, ownedSegment)`,
   then advance per-source checkpoint.

`CLOCK_SKEW_MS = 200L` and `ERROR_BACKOFF_MS = 500L` are tuned for
loosely-synced NTP clocks. Cross-node merge order is approximate;
within a single source segment the order is exact (gap-free
`segment_seq`).

This is the only place "approximate global" ordering shows up in
the framework. It matches the
no-cross-aggregate-ordering stance — a saga doesn't get a strict
total order across aggregates; it gets "best-effort
`createdAt`-order across the watermark".

A saga handler exception is caught at the worker level and logged
(`:118-125`) so one bad event can't kill the worker (which would
stall the whole `(group, segment)`). A *store* exception is also
caught — it backs off and retries from the last checkpoint
(`:128-138`).

---

## 6. Scheduler

### 6.1 Scheduler dual API: closure + durable annotation

`Scheduler` (`kf-core/.../scheduler/Scheduler.java:14`) declares
two overloads:

- `schedule(Instant when, Runnable task)` — closure-based.
  `InMemoryScheduler` implements it; `JdbcScheduler` throws
  `UnsupportedOperationException` ("a Runnable cannot be
  persisted").
- `schedule(Instant when, String taskName, Object params)` —
  durable. `InMemoryScheduler` throws (default impl);
  `JdbcScheduler` persists a row in `scheduled_task`.

The durable overload looks up `taskName` against the registered
`@Schedulable` beans' `@Schedule("taskName")` methods. On fire,
`params` is deserialised to the method's declared single parameter
type via the registered `MessageSerializer` and the method invoked
reflectively (`JdbcScheduler.fire`,
`:202-212`).

**`taskName` must already be registered before `schedule(...)` is
called** — `JdbcScheduler.schedule`
(`:66-87`) checks `tasks.containsKey(taskName)` and throws if not.
The scan is done at `start()`
(`:99-100`, `scanSchedulables`,
`:131-157`). New `@Schedulable` beans registered *after* `start`
are invisible — another instance of the frozen-topology rule.

A scheduled task whose `taskName` is *fired* on a node that
doesn't know it logs a WARN and leaves the row PENDING for a node
that does
(`:202-208`). Useful for partial deployments where node A knows a
new task definition and node B doesn't yet.

### 6.2 `JdbcScheduler` exactly-once OCC pick

`JdbcScheduler.runDueTasks`
(`:175-200`):

1. `SELECT id, version, task_name, params_json FROM scheduled_task
   WHERE status = 'PENDING' AND execution_time <= ? ORDER BY
   execution_time`.
2. For each due row:
   `UPDATE scheduled_task SET status='PICKED', picked_by=?,
   version=? WHERE id=? AND version=? AND status='PENDING'`.
   Only the JVM whose `version` matches wins. Lose → `continue`.
3. Win → `fire(row)`, then `DELETE FROM scheduled_task WHERE id =
   ?`.
4. If `fire` throws, **the row is left `PICKED`** for inspection
   (`:195-198`). DLQ-for-tasks is out of scope; a stuck `PICKED`
   row is a known artefact.

`status` and `version` together make the pick atomic. Multiple
nodes polling the same DB will see the same due set but only one
will win the CAS per row.

### 6.3 `InMemoryScheduler.stop` joins on the tick thread

`InMemoryScheduler.stop`
(`:78-100`) interrupts the tick thread and `join(TICK_MS * 2)` —
~200ms cap. A long-running task on the tick thread (tasks run on
the tick thread; that's an in-memory simplification) can block
the join; on timeout a WARN is logged and the thread is
abandoned.

Implication: any work that scheduled task was about to do on the
buses is **orphaned** because the buses are reset right after.
Quiesce scheduled tasks before stop if their work matters. The
join timeout is a safety net.

Calling `schedule` on a stopped scheduler throws
`IllegalStateException` (`:42-44`) — otherwise tasks queued on a
stopped scheduler would never fire.

---

## 7. Cluster

### 7.1 N integer partitions, single leader, no Hazelcast

`kf-cluster` replaces the old PostgreSQL + Hazelcast design with:

- N integer partitions (`itemId` 0..N-1; `cluster_config.item_count`
  seeded once, validated on every node start —
  `ClusterSchema.seedAndValidateItemCount`,
  `:69-78`). A node started with a different `N` than the seeded
  value refuses to start.
- A single leader at any time, fenced by an epoch on
  `cluster_leader_lock`
  (`ClusterSchema.java:45-50`,
  `DbLeaderLock.java:32-66`).
- DB as the only coordination substrate (H2 ∩ MySQL portable SQL).

The cluster's **sole guarantee**: exactly one live node runs
`ItemProcessor.process(i)` for partition `i` at a time
(`ItemProcessor.java:13-29`,
`ClusterNode.java:17-19`). How to find and process the work for
partition `i` is the application's concern. The
`SegmentItemProcessor` adapter (7.6) is what plugs this into the
kf-core projection/saga pump.

### 7.2 Epoch fencing on `cluster_leader_lock.epoch`

`DbLeaderLock.acquire`
(`:32-52`) does **one** UPDATE that *both* claims the row and
bumps the epoch:

```
UPDATE cluster_leader_lock
   SET owner_node = ?, epoch = epoch + 1, lease_until = ?
 WHERE id = 1 AND (owner_node IS NULL OR owner_node = ? OR lease_until < ?)
```

If the row is unclaimed, expired, or already ours: win,
`epoch += 1`. The returned epoch is a strictly-monotonic fencing
token — a revived old leader always carries a lower token than the
current one.

Every leader-tick write is fenced by epoch:
`UPDATE cluster_assignments SET owner_node = ?, epoch = ? WHERE
item_id = ? AND epoch < ?`
(`LeaderService.java:219-222`). A stale leader's lower-epoch write
is rejected — the row already has a higher epoch.

**Fail-safe.** Any DB error in the leader/lock path returns "I do
not hold the lock" (`-1` from `acquire`, `false` from `isHeld`);
the lease expires and the cluster is briefly leaderless. A stall,
never a split brain.

### 7.3 owner_node / lease_holder split — the no-double-pump core

`cluster_assignments` has two owner columns:

- `owner_node` — the leader's *intent*. Flipped at handoff.
- `lease_holder` + `lease_until` — the *actual* live pump. Renewed
  by the current `WorkerService` every tick
  (`WorkerService.java:102-107`).

The handoff dance:

1. Leader flips `owner_node` from A to B
   (`LeaderService.rebalance`, `:189-238`, epoch-fenced UPDATE).
2. A's worker tick sees `owner_node != self` and calls
   `processor.stopProcess(itemId)`
   (`WorkerService.java:92-99`).
3. A's pump cooperatively winds down (the application's pace),
   while A keeps renewing `lease_holder = A`. **B's claim CAS
   fails** because the lease is still live
   (`WorkerService.java:80-83`).
4. When A's app has truly stopped, it calls
   `ClusterNode.release(itemId)`
   (`ClusterNode.java:81-85`), which clears the lease
   (`WorkerService.release`,
   `:120-136`).
5. B's next tick wins the claim CAS, starts its pump.

There is no window where both A and B are pumping. The lease
renewal is the safety net: a crashed A's lease expires after
`ClusterConfig.LEASE` (30s) and B claims; an alive A keeps the
lease for as long as the app needs to drain.

### 7.4 Heartbeat + `/alive` probe + stability gate

Three layers of liveness in `LeaderService.leaderTick`
(`:77-114`):

- **Heartbeat**: a node not seen in
  `STALENESS_WINDOW` (9s) is a death candidate
  (`HeartbeatService.heartbeatOnce`, `:67-85`; default cadence
  `HEARTBEAT = 3s`). The GC fast-path
  (`maybeHeartbeatForGcPause`, `:122-126`) re-stamps immediately
  after a `GC_PAUSE_THRESHOLD = 4s` pause so a stop-the-world
  doesn't trip a false death.
- **`/alive` probe**: for any *owner* that dropped out of the
  heartbeat set, the leader pings `GET http://host:port/alive`
  (`LeaderService.deadNodeConfirmation`,
  `:116-149`,
  `probeAlive` template method at `:248-263`). A node that still
  answers /alive is **spared** — alive but DB-blind. Below
  `LIVENESS_FAIL_TICKS = 2` consecutive probe failures, the node
  is also spared.
- **Stability gate**: a membership change must persist for
  `MEMBERSHIP_STABILIZE = 10s` before a rebalance. Continuous
  instability for `MAX_INSTABILITY = 30s` forces a rebalance over
  *continuously-present* nodes
  (`LeaderService.stabilityGate`, `:151-187`).

The gate's `continuous` set is the **intersection** of all live
sets observed during an instability episode — only nodes that
have been there throughout get reassigned to.

### 7.5 Minimal-movement assignment

`Assignment.compute`
(`kf-cluster/.../cluster/Assignment.java:30-105`) is pure (no I/O).
Algorithm:

1. Partition current holdings by node — keep entries whose owner
   is still in `members`. Everything else goes into the
   `orphans` list.
2. Compute capacity per node: `base = n / count`, `rem = n %
   count`. The `+1` slots go to **the members already carrying
   the most** (so heavily-loaded nodes shed the least —
   minimal-movement).
3. Shed over-capacity items back into the orphan pool, highest
   item ids first (deterministic across reruns).
4. Fill under-capacity members from the orphan pool, **least-
   loaded first** — so a brand-new node (load 0) fills before
   nodes already near capacity. A 3→4 node change moves only the
   new node's fair share. The opposite of `i mod liveCount`,
   which would remap almost every partition on a single
   membership change.

Determinism comes from `Comparator.naturalOrder()` ties everywhere
and from sorting `members` lexicographically. Same `(n, members,
current)` inputs always yield the same target.

### 7.6 `SegmentItemProcessor` bridges itemId↔segment

`SegmentItemProcessor`
(`kf-cluster/.../cluster/SegmentItemProcessor.java`) is the kf-core
↔ kf-cluster adapter. The contract:
**`itemId == segment`**. Owning partition `i` means owning segment
`i` of every event-side processing group.

`process(itemId)` → `ownership.claimSegment(itemId)` which **blocks
the cluster pump thread** until released — `SegmentProcessor.claimSegment`
(`:112-132`) builds the per-segment park latch and `await()`s. The
cluster pump must not return under normal operation
(`ItemProcessor.process` contract), so the park is correct.

`stopProcess(itemId)` → `ownership.releaseSegment(itemId, () ->
node.release(itemId))`. The release runs **off the caller's
thread** via a daemon
(`SegmentProcessor.releaseSegment`, `:154-159`); the
`onDrained = node.release(itemId)` callback fires only after the
kf-core barrier confirms in-flight dispatch has finished
(`SegmentProcessor.drain`, `:161-193`). That sequencing is what
guarantees the gaining node cannot start a second pump until this
one has truly stopped (see 7.3 step 4).

Dependency direction is preserved: kf-cluster → kf-core, never the
other way. `SegmentOwnership`
(`kf-core/.../cluster/spi/SegmentOwnership.java`) is the SPI; the
`onDrained` callback is how kf-core signals "I have truly stopped"
without knowing about `ClusterNode.release`.

### 7.7 Live `ownedSegments` set as the dispatch-time gate

`SegmentProcessor`
(`kf-core/.../pg/SegmentProcessor.java`) keeps a
`ConcurrentHashMap.newKeySet()` of currently-owned segments
(`:72`). The projection pump's wide read
(`EventStore.loadSegmentsTail`) may return events for segments
just-released; before each dispatch the worker recomputes
`SegmentCalculator.calculateSegment(aggregateId)` and **skips** if
the segment is no longer owned (or not this slot's,
`:374-381`).

This is the *live* ownership gate. It gates *starting* a
dispatch, never aborts a running handler. That is what makes
`releaseSegment` a simple "remove from set, let any in-flight
dispatch finish via the gate barrier, then clear the lease"
instead of a per-`(group, segment)` thread join.

The gate barrier is the `synchronized (gate)` block around the
poll-and-dispatch batch
(`ProjectionGroupWorker.run`, `:351-395`,
`barrier()` at `:320-324`). Acquiring the gate in `drain` proves
no batch is in flight.

Per-aggregate ordering is preserved by the segment math: one
aggregate → one segment → one owner → one worker thread. The
intra-group dispatch parallelism (`dispatchConcurrency`) keys on
`Math.floorMod(seg, dispatchConcurrency)` so a segment always maps
to the same slot — order still holds.

### 7.8 Projection workers are one-per-group; saga workers stay one-per-(group, segment)

This is the single-PG-per-node optimisation (see
`plans/singlePgForNode.md`). Under cluster pull mode:

- **Projection** — one `ProjectionGroupWorker` per `(group,
  dispatchSlot)` polls `loadSegmentsTail` for **all** owned
  segments in one wide read
  (`SegmentProcessor.java:135-145`,
  `ensureProjectionWorkers`). Started once; the worker survives
  segment claims/releases and just gates on the live ownedSegments
  set. With `dispatchConcurrency = 1` (the default), one thread per
  group dispatches everything.
- **Saga** — still one `SagaSegmentWorker` per owned `(group,
  segment)` (`startSagaWorker`, `:147-152`). Each saga lane
  k-way-merges `SEGMENTS` source streams per owned target
  segment — that can't collapse into a single wide read.

Started/stopped on claim/release of individual segments. Stopping
joins on `JOIN_TIMEOUT_MS = 10s`; longer-running saga handlers can
get interrupted.

### 7.9 At-least-once: checkpoint AFTER dispatch

The pump's per-event sequence
(`SegmentProcessor.ProjectionGroupWorker.run`,
`:383-393`) is:

1. `eventManager.dispatchProjection(group, seg, m)` — invoke the
   handler.
2. `cursor.put(seg, m.getSegmentSeq())` — update in-memory cursor.
3. `checkpointStore.save(...)` — durable high-water-mark.

A crash or segment handoff between step 1 and step 3
re-processes the straddling event on the new owner. **Projections
must be idempotent under replay** — UPSERT, `SET x=v`, or track
last-applied `segment_seq`. The framework does not provide
exactly-once dispatch for the event side.

This is also true for an operator `replay(...)` (2.8). Saga groups
re-fire side effects on replay and are excluded by default.

### 7.10 Pull mode is a flip flipped before `start()`

`ProcessingGroupsManager.setPullMode(true)` makes:

- `start(consumers)` build lanes + saga resolvers **but spawn no
  threads**
  (`ProcessingGroupsManager.java:89-105` — the `if (!pullMode)`
  guards both the lane thread and the resolver thread).
- `send(pgs, msg)` a no-op
  (`ProcessingGroupsManager.java:114-119`). The local push from
  `dispatchCommand → eventBus.send` is silently dropped; events
  reach lanes only via the `SegmentProcessor` poller.

Command side stays push regardless. The flip happens in
`KfBootstrap.start`
(`KfBootstrap.java:120-128`) **before** `GlobalRegistry.start()`
calls `bus.start()`.

If `kf.cluster.enabled=true` but `eventBus.getHandler()` returns
null (e.g. a test double), the warning logs and event-side dispatch
**duplicates across nodes**
(`KfBootstrap.java:125-128`). This is also the case if the app
supplies a `NoopItemProcessor`
(`kf-spring/.../spring/NoopItemProcessor.java`) — every node's
projection workers will run all segments. The framework refuses to
silently do the right thing here; it's a real configuration choice.

### 7.11 GC fast-path on long pauses

`HeartbeatService` registers a `NotificationListener` on every
`GarbageCollectorMXBean` (`:87-95`,
`registerGcListeners`). On a GC notification longer than
`GC_PAUSE_THRESHOLD = 4s`, it triggers an immediate
heartbeat re-upsert
(`maybeHeartbeatForGcPause`, `:122-126`). Without this, a 6s GC on
a 9s staleness window would push the node past the threshold and
get it falsely declared dead.

DB failures are swallowed at `:82-84` — heartbeat is best-effort.
The `/alive` endpoint is the leader's backstop proof-of-life
(7.4).

### 7.12 `Liveness` server holds no app references

`Liveness`
(`kf-cluster/.../cluster/Liveness.java`) is a JDK-only `HttpServer`
that holds **no reference to `Db` or any app component** — only a
`Runnable` reconcile callback (the worker tick). `GET /alive` is
served from a fixed in-memory body
(`ALIVE_BODY`, `:33`). Even if the main app / GC / DB are stuck,
`/alive` keeps answering.

`POST /notify` returns 200 immediately and dispatches the
reconcile to a **separate single-thread executor**
(`notifyExecutor`, `:78-86`) so the HTTP layer never blocks. Two
executors: `httpExecutor` for the request itself, `notifyExecutor`
for the reconcile.

This separation is what allows the dead-node confirmation probe
to give a meaningful signal even when the suspect's worker tick is
hung.

---

## 8. Lifecycle

### 8.1 Spring autoconfig ordering

`KfAutoConfiguration` is `@ConditionalOnMissingBean` everywhere, so
the app can override any single bean. The ordered registration
sequence runs in `KfBootstrap.start`
(`kf-spring/.../spring/KfBootstrap.java:91-149`):

1. (Pre-context) `KfSegmentEnvironmentPostProcessor` sets
   `SegmentCalculator.setSegments(kf.segments)` from the
   environment (2.7 reasoning).
2. Buses → registry **first** (because
   `GlobalRegistry.autoSubscribe` casts `registry.get(CommandBus
   /EventBus)` with no null check —
   `GlobalRegistry.java:271-278`).
3. Serializer, UpcastersManager, Db, EventStore, SagaStore,
   DlqStore, Scheduler.
4. `setFallbackResolver(type -> context.getBeanProvider(type).getIfAvailable())`
   — handler-method collaborator resolution via Spring,
   cached-back into the registry.
5. Scan + register handlers. Bean-backed ones get `register(class,
   instance)`; bare ones (aggregates, sagas) get `register(class)`
   (`scanAndRegisterHandlers`, `:152-170`).
6. Pre-warm collaborators (3.3).
7. If cluster enabled, flip event-side
   `ProcessingGroupsManager.setPullMode(true)` **before**
   `GlobalRegistry.start()` (7.10).
8. `GlobalRegistry.start()` → `bus.start()` builds lanes.
9. Scheduler start.
10. Cluster `ClusterNode.start(N=segments, livenessPort, processor)`.

Reverse order in `stop` (`:210-240`), each step in its own
try/catch so one failure can't block the others.

### 8.2 `MessageSerializer` and `UpcastersManager` are registered eagerly

`KfAutoConfiguration.kfMessageSerializer`
(`KfAutoConfiguration.java:67-74`) **eagerly** calls
`GlobalRegistry.register(MessageSerializer.class, serializer)` at
bean construction time, not in `KfBootstrap.start`. Reason:
`JdbcEventStore`'s constructor (via `BaseEventStore`) reads
`GlobalRegistry.get(UpcastersManager.class)` at construction
(`BaseEventStore.java:34-36`), which happens during Spring context
refresh — before `KfBootstrap.start` runs.

`KfBootstrap.start` re-registers them (idempotent). The
`kfUpcastersManager` bean does the same eagerness for
`UpcastersManager` (`KfAutoConfiguration.java:81`).

The `kfEventStore` bean has `UpcastersManager` as a constructor
parameter purely to force the Spring DI ordering — the
upcastersManager has to exist before `JdbcEventStore`'s ctor reads
from the registry.

### 8.3 `GlobalRegistry.register(class, instance)` populates supertypes

`GlobalRegistry.register(type, instance)`
(`:142-171`) puts the instance under the declared `type` key, then
also under every supertype + interface
(`ReflectionUtils.getAllSupertypes`,
`:109-122`) with `putIfAbsent`. A `JdbcEventStore` registered
under `EventStore.class` becomes also resolvable by
`get(BaseEventStore.class)`, by `get(JdbcEventStore.class)`, etc.

Explicit registrations always win over derived ones
(`putIfAbsent`). First instance registered for a shared interface
wins over later ones — order matters; register the canonical impl
first.

`Framework.stop()`'s walk uses `GlobalRegistry.allInstances()`
(`:221-231`), which dedupes by identity. Each distinct instance
appears once, regardless of how many keys it's registered under.

### 8.4 Frozen-topology rule is per-node, not per-cluster

What is frozen at runtime: handler maps, policy maps, sequence
policies, `GlobalRegistry` bindings — i.e. the dispatch topology.

What is **not** frozen: which partition each node owns. Cluster
rebalances change `cluster_assignments.owner_node` and the
`ownedSegments` set inside `SegmentProcessor`. The buses are
untouched — every handler stays subscribed; the cluster only
changes which node's bus sees which messages.

Don't conflate the two. A rebalance is not a topology change at
the bus level.

The cluster's dynamic state — `ownedSegments`, projection-worker
cursors, saga-worker per-source checkpoints, epoch maps — all
lives inside `SegmentProcessor` and the cluster services
(registered as singletons into `GlobalRegistry` during setup, then
mutated freely at runtime). The bus handler maps are never
mutated after `bus.start()`.

### 8.5 `GlobalRegistry.clear()` drops the fallback resolver

`clear()` is called by `KfBootstrap.stop` (8.1 step 10). It also
drops `fallbackResolver` — which captures the wiring context (the
Spring `ApplicationContext`). A stale resolver surviving a clear
would fire `get()` misses against a torn-down context, throwing
`"ApplicationContext has been closed already"` on the next setup
phase in the same JVM. That symptom leaked across
`ApplicationContextRunner` runs before the explicit clear was
added (`GlobalRegistry.java:246-260`).

### 8.6 `Framework.stop()` then `start()` must be re-startable

The cluster integration tests need to spin up a fresh cluster per
test against a shared MySQL without forking a JVM, so the rule is:
after `stop()`, a subsequent `start()` must re-create the deadline
pool, clear the stopped flag, and the framework is usable again.
`Stoppable` implementations must therefore also be re-startable
rather than single-shot.

The high-level entry points are `GlobalRegistry.start` /
`GlobalRegistry.stop` (`GlobalRegistry.java:35-42`), which call
`bus.start()` / `bus.stop()`. Both are idempotent across multiple
cycles, and `ProcessingGroupsManager.start(consumers)` drops
previously-built lanes/resolvers before rebuilding
(`ProcessingGroupsManager.java:54-56`). Spring's `KfBootstrap`
brackets this with the registration / clear sequence in `start` /
`stop`.

---

## 9. Testing strategy: template method over `--add-opens`

When a class under test depends on JDK interfaces
(`javax.sql.DataSource`, `java.sql.Connection`, HttpServer
handlers, etc.) that Mockito's inline mock maker cannot instrument
on JDK 25+, **do not** reach for `--add-opens` JVM flags or a
global mock-maker override. Instead:

1. Remove `final` from the class.
2. Route all JDK-interface acquisition through a single
   overridable method (e.g. `connection()`, `probeAlive(host,
   port)`, `poke(host, port)`).
3. In tests, subclass the class and override the method to return
   an in-memory implementation (H2 for JDBC, a stub HTTP response,
   etc.) or to throw the error under test.

Examples in the tree:

- `Db.connection()` is the template seam for JDBC. Happy-path
  tests pass an H2 `JdbcDataSource`; error tests pass an
  anonymous subclass whose `connection()` throws `SQLException`
  (`kf-core/.../db/Db.java:64-68`,
  `DefaultDb.java:217-232`).
- `LeaderService.probeAlive` and `LeaderService.poke` are
  template seams for the cluster's HTTP probe/poke
  (`kf-cluster/.../cluster/LeaderService.java:248-283`).
- `HeartbeatService.heartbeatOnce` and
  `HeartbeatService.maybeHeartbeatForGcPause` are package-visible
  test seams (`HeartbeatService.java:67-126`).

No `--add-opens`, no `mockito-extensions/org.mockito.plugins.MockMaker`
override, no Mockito at all for those tests.

`WorkerService.workerTick` is the same: package-visible test
seam, also serialised against `release` via `synchronized (lock)`
so the scheduler and a poke cannot race
(`WorkerService.java:64-113`).

---

## 10. Serialization

### 10.1 Jackson is the default; upcasters operate on `JsonNode`

`JacksonMessageSerializer`
(`kf-core/.../serialization/JacksonMessageSerializer.java`)
implements `MessageSerializer<String, JsonNode>` — the first type
param is the textual format, the second is the intermediate
node representation used by upcasters.

`deserializeToIntermediate(byte[])` returns a `JsonNode` for
`UpcastersManager.applyHop` to mutate
(`UpcastersManager.java:115-128`). The upcaster method receives a
`JsonNode` (or narrower `ObjectNode`) and returns the rewritten
node; the manager re-serialises via `serializer.serialize(upcasted)`.

The DLQ uses Java serialization for `Context` (4.3) precisely
because the default Jackson `ObjectMapper` doesn't have
`JavaTimeModule` registered. If you swap in a custom `ObjectMapper`
with `JavaTimeModule`, DLQ persistence still uses Java
serialization — it doesn't go through the `MessageSerializer`.

### 10.2 Upcaster discovery is at construction, by `@UpcasterSpec`

`UpcastersManager` is built once at setup with the list of
`Upcaster` beans. It scans each bean for `@UpcasterSpec`-annotated
methods, validates `(JsonNode) -> JsonNode` signatures
(`:130-138`), and indexes the hops by lowercased origin type
name
(`:65-83`).

At runtime, `upcast(msg)` runs a fixpoint loop: while some hop
matches the message's current `(type, version)`, apply the
**widest** one
(`:98-110`,
`if (chosen == null || h.to > chosen.to) chosen = h`). Versions
strictly increase each hop (validation at `:62-66`), so the loop
always terminates.

- `from` is matched against `Context.version`.
- `origin` is matched against `Context.type` case-insensitively,
  **simple name only**. The origin class may have been deleted —
  `origin` is a `String`, not a `Class<?>` (`UpcasterSpec.java:34`).
- `to` defaults to `from + 1` (single step). A direct long-jump
  shortcut declared with `to = 7` wins over a `to = 2` step at
  the same `from`.
- `target` (default `""`) lets a hop rename `Context.type` on
  rewrite (`UpcasterSpec.java:51`).

If no `Upcaster` beans are registered, `UpcastersManager` builds
the empty hops map and `upcast` short-circuits
(`:85-90`).

### 10.3 `UpcastersManager.upcast` mutates the message in place

The same `InternalMessage` instance is returned with payload +
version rewritten
(`:106-111`,
`if (changed) msg.setPayload(payload)`). Callers must not rely on
identity comparisons before/after upcast.

This is why the in-memory tail read returns a copy (2.2) — without
the copy, upcast would mutate the stored stream's `InternalMessage`
in place and corrupt the next reader.

---

## 11. Misc gotchas

### 11.1 `Sleeper` is production-validated adaptive backoff

`kf-core/.../scheduler/Sleeper.java` is a `wait`-based sleep
(`synchronized (obj) { obj.wait(timeoutMillis) }`,
`:21-35`) used inside the framework's hot paths
(`ProcessingGroup.start` polls + `Sleeper.yield()`,
`SegmentProcessor` retry backoff). It is intentionally not
`Thread.sleep` — the `wait`/`notify` shape is what lets the spin
be cooperative under load.

The user's per-project memory marks this as
"production-validated adaptive backoff for proxy hot paths; do
not rewrite or flag as buggy".

`Sleeper.yield()` is `Thread.onSpinWait()`. Cheap, JIT-friendly,
and the right thing inside a tight poll loop.

### 11.2 `ConnectionStorage` is per-thread; long-lived loops accumulate state

`DefaultDb.connection()`
(`:217-232`) reuses the thread-bound connection if present;
otherwise opens a fresh one from the `DataSource` per call. The
`Db.update`/`query`/etc. methods close the *fresh* connection
before returning, via `release()`
(`:49-56`,
`if (conn != ConnectionStorage.get()) conn.close()`).

Long-lived loop threads (the cluster pull pump, the worker/leader/
heartbeat schedulers) **must not** keep a bound connection across
poll iterations. A bound connection with `autoCommit = false` left
latched to the thread (a) leaks a pool slot and (b), on MySQL,
pins a stale REPEATABLE READ snapshot so newly-committed rows
become invisible.

`JdbcProcessingGroup.finishTransaction`
(`:70-77`) is the standard pattern: restore autocommit, then if we
own the connection (we bound it), close it and unbind the thread.
The boundary-bound case (an outer caller already bound one) does
**not** close — the outer caller owns the lifecycle.

`ConnectionStorage.bind` refuses to nest
(`:37-49`,
`throw new IllegalStateException("a connection is already bound")`).
There is no transaction propagation; nested transactions are out
of scope.

### 11.3 UUIDs are stored as `BINARY(16)`, not `CHAR(36)`

`UuidBytes.toBytes/fromBytes`
(`kf-core/.../db/UuidBytes.java`) packs a UUID into 16 raw bytes
(big-endian most-significant, then least-significant long). Storing
as `CHAR(36)` would waste 20+ bytes per row and lose lexicographic
sortability of the binary form.

Every JDBC row mapper that reads a UUID column calls
`UuidBytes.fromBytes(rs.getBytes(...))`; every parameterised insert
calls `UuidBytes.toBytes(uuid)`. If you add a UUID column, follow
the convention.

### 11.4 `Bus.extractAggregateId` returns null for command without `@AggregateIdentifier`

`Bus.extractAggregateId(command)`
(`:44-54`) returns null when the command has no
`@AggregateIdentifier` field. `CommandBus.buildContext`
(`:22-32`) sets `context.aggregateId = extractAggregateId(...)`
unconditionally — null is allowed for command-side interceptors
that don't target any particular aggregate
(`ProcessingGroupsManager.send`,
`:128-138` defaults to segment 0 for command-side null aggregate
id; event-side null aggregate id throws).

An *event* with no `@AggregateIdentifier` and no parent command
context throws in `ProcessingGroup.buildEventEnvelope`
(`:113-121`) — that's the right thing because events that don't
belong to any aggregate cannot be stored.

### 11.5 `InsertBuilder.ignore()` emits `INSERT IGNORE`

`kf-core/.../db/InsertBuilder.java:37-39` — the H2 ∩ MySQL portable
spelling of "duplicate-key insert is a no-op". The old
PostgreSQL `ON CONFLICT DO NOTHING` is not used because PostgreSQL
is no longer a target. `INSERT IGNORE` works in both MySQL and
H2's MySQL-compatibility mode.

### 11.6 SchemaInitializer picks H2 vs MySQL by JDBC product name

`SchemaInitializer.isMysql()`
(`:298-305`) reads `Connection.getMetaData().getDatabaseProductName()`
and matches `"mysql"` case-insensitively. H2 reports `"H2"` even
in `MODE=MySQL`, so the detection cleanly separates the
embedded test/single-node store from a real MySQL server.

The dialects diverge only at **secondary index creation**: H2
supports standalone `CREATE INDEX IF NOT EXISTS`; MySQL doesn't
(it's a parse error). MySQL gets `INDEX name (cols)` declared
inline in `CREATE TABLE IF NOT EXISTS`, which rides the table's
own idempotency.

`event_entry`'s `UNIQUE (segment, segment_seq)` already serves
the projection tail poll on MySQL, so the MySQL DDL omits the
separate `idx_event_tail_seg` H2 ships (`:174`).

### 11.7 Indexed column widths must stay under InnoDB's 3072-byte cap

MySQL's InnoDB caps an index key at 3072 bytes; with `utf8mb4` (4
bytes/char) that's 768 chars per composite index. H2's
`MODE=MySQL` does NOT enforce this — it only surfaces on a real
server.

That's why `saga_correlation`'s `type` and `corr_value` are
`VARCHAR(255)`
(`SchemaInitializer.java:111-117`,
`:218-225`): the PK `(type, corr_value)` would otherwise be
`(512+512)×4 = 4096` bytes; at 255 each it's a comfortable
`(255+255)×4 = 2040`. Keep indexed `VARCHAR` widths small enough
that every PK/UNIQUE/INDEX stays under 3072 bytes.

### 11.8 Saga handler `keyName` defaults to `associationProperty`

`@SagaHandler` (and `@SagaStart`) have two attributes:
`associationProperty` (the bean property to read off the event)
and `keyName` (the index key). If `keyName` is empty, it defaults
to `associationProperty`
(`EventBus.buildSpec`, `:48-57`,
`BaseSagaStore.buildCorrelationSpecs`, `:46-60`).

Use `keyName` only when two different events expose the same
correlation under different property names (e.g.
`PaymentInitiated.paymentId` vs `PaymentRefunded.refundId` both
correlating against a shared `paymentRef` key). Otherwise leave
it default.

### 11.9 `PropertyAccessor` cache is global, per `(type, property)`

`JavaBeanPropertyAccessor.CACHE`
(`kf-core/.../saga/JavaBeanPropertyAccessor.java:9`) is a
process-wide `(type, property) -> Accessor` map. Cached entries
are either a `MethodAccessor` (a JavaBean-style `getProperty()`
getter), a `FieldAccessor` (direct field access fallback), or the
`Accessor.MISSING` sentinel.

The accessor instance itself is also cached per type:
`EventBus.ACCESSOR_CACHE`
(`:24`) holds one instance per `Class<? extends PropertyAccessor>`,
created via `ReflectionUtils.instantiateAccessor`
(`:124-132`). If you write a custom `PropertyAccessor`, it needs
a no-arg constructor.

### 11.10 `Framework.stop`'s walk vs registered key

When the user registers a bus as `register(CommandBus.class,
new InMemoryCommandBus(...))`, the bus is found by
`get(CommandBus.class)` AND by `get(InMemoryCommandBus.class)` AND
by `get(Bus.class)` (8.3). The teardown walk uses
`GlobalRegistry.allInstances()` and dedupes by identity, so the
right `stop()` is called once per distinct instance regardless of
how many keys it's registered under.

### 11.11 GC fast-path heartbeat: don't depend on it

The GC fast-path
(`HeartbeatService.maybeHeartbeatForGcPause`,
`:122-126`) is best-effort. A `GarbageCollectorMXBean` notification
arrives **after** the pause ends; if the pause was longer than
`STALENESS_WINDOW`, the leader has *already* declared the node a
death candidate (and the `/alive` probe is the next backstop).
The fast-path covers the gap between pauses just shy of
`STALENESS_WINDOW` and the regular `HEARTBEAT` cadence; longer
than that, you rely on `/alive`.

The notification handler is registered on every
`GarbageCollectorMXBean` that implements `NotificationEmitter`
(`HeartbeatService.java:87-95`). Removed on `stop`
(`:128-135`).

## 12. Observability SPI

### 12.1 `kf-core` carries no metrics dependency — the SPI is a static holder

Hot-path timing goes through `org.kendar.cqrses.observability`:
`ObservabilityInterface` (8 semantic methods + `onSqlExecuted`),
the no-op `NullObservability`, and the `Observability` static
holder (`get()` / `set()`). The holder mirrors the
`EventApplyer` / `SagaManager` static-accessor pattern already on
the dispatch path: a single `volatile` read, no `GlobalRegistry`
map lookup per message. The default is `NullObservability`, so an
uninstrumented build (and every `kf-core` / `kf-core-db` test) pays
only that volatile read plus the `System.nanoTime()` bracketing at
each call site. **No Micrometer in `kf-core`.**

The methods are *semantic*, not a generic `time(name, …)`, so each
maps 1:1 to a meter name and the call sites read as documentation:
`onCommandHandled`, `onAggregateRehydrated`, `onEventsAppended`,
`onEventDispatched` (projection), `onSagaDispatched`,
`onSegmentTailRead`, `onCheckpointSaved`, `onDlqEnqueued`,
`onSqlExecuted`.

### 12.2 Where the call sites are

- **Command handle** — `ProcessingGroup.dispatchCommand`, bracketing
  only the handler `accept` (before the `EventApplyer.drain()` /
  append) so `onCommandHandled` and `onEventsAppended` stay disjoint.
- **Projection / saga dispatch** — `ProcessingGroup.dispatchEvent`
  (the single chokepoint both push and pull-mode projection dispatch
  funnel through; saga `resolveForSegment` also routes here via
  `invokeConsumers`, so the saga worker does **not** re-emit
  `onSagaDispatched` — that would double-count).
- **Aggregate rehydrate** — `BaseEventStore.loadAggregate` (delegates
  to a timed inner method carrying the replayed-event count).
- **Events append** — `ProcessingGroup.persistAndPublishEmitted`.
- **Segment tail read + checkpoint** — `SegmentProcessor`
  (projection worker) and `SagaSegmentWorker` (per-source tail).
- **DLQ enqueue** — `ProcessingGroup.addToDlq`.

### 12.3 SQL is timed by category, not by query

`DefaultDb` routes all seven JDBC methods through one
`inConnection(sql, op)` helper that brackets the whole operation
(connection acquire + statement) and reports `onSqlExecuted` keyed by
`SqlCategory.of(sql)` — a low-cardinality `verb:table` label
(`select:event_entry`, `insert:dlq_item`, …). This separates raw DB
time from handler time when chasing a bottleneck without exploding
meter cardinality (no per-statement labels). A cheap token scan finds
the table after `FROM` / `INTO` / `UPDATE`; anything unrecognised
collapses to `verb:other`.

### 12.4 Micrometer + Prometheus live in `kf-spring` / the sample

`kf-spring`'s `MicrometerTimers` implements the SPI over a
`MeterRegistry`; `KfObservabilityAutoConfiguration` is
`@ConditionalOnClass(MeterRegistry)` (Micrometer is an **optional**,
non-transitive dependency of `kf-spring`) and falls back to
`NullObservability` when no registry bean exists.
`KfBootstrap.start()` installs it via `Observability.set(...)` before
any dispatch; `stop()` restores the no-op. Latency timers
(`kf.command.handle`, `kf.events.append`, `kf.segment.tail.read`,
`kf.sql.execute`) publish a percentile histogram; per-segment
dispatch timers stay plain to bound cardinality. Every meter carries
a `node` tag.

The `kf-spring-app` sample pulls `spring-boot-starter-actuator` +
`micrometer-registry-prometheus` and exposes `/actuator/prometheus`.
The `kf-cluster-it` harness runs a `prom/prometheus` Testcontainer on
the shared Docker network (scraping `node1..3:8080` by alias) and
`AbstractClusterIT.captureMetrics(phase, windowMs)` queries it via
PromQL at each flood-phase boundary, writing
`target/kf-metrics-<TestClass>.csv`.

## 13. Command results, auto-snapshots, snapshot upcasting, test fixtures

Four Axon-OSS-parity features added together; the non-obvious bits:

**`sendSync` returns the handler's result.** `Bus.registerMethod` builds
`TriFunction` invokers (not `TriConsumer`), so `method.invoke`'s return value
flows `dispatchCommand → invokeCommandSync → ProcessingGroupsManager.sendSync
→ CommandBus.sendSync`, which is `<R> R` with an unchecked cast — Axon's
`sendAndWait` shape. `void` handlers and the event side yield/discard `null`.
When a command type spans several groups (interceptor + aggregate), the *last
non-null* result wins. The async `send` still returns nothing.

**Auto-snapshot is boundary-crossing, not modulo.** With
`@Aggregate(snapshotEvery = N)`, `ProcessingGroup.maybeSnapshot` fires when
`(lastVersion + 1) / N > firstVersionOfBatch / N` — a multi-event batch that
jumps *past* a multiple-of-N boundary still triggers, where a `% N == 0` check
on the last version would miss it. Only envelopes carrying the command's own
aggregateId count (a handler may emit events for other aggregates). The
snapshot is stamped with the batch's exact last version (no re-read of the
stream max → no race with a later command), and the payload comes from
`getSnapshot()`; the `getSnapshot()`/`setSnapshot(T)` pair is validated at
registration (`GlobalRegistry.checkSnapshotContract`), before any bus
subscription happens. Failures are logged and never fail the command
(snapshots stay best-effort).

**Snapshot upcasting discards rather than misapplies.** `snapshot_entry` rows
carry `schema_version` (= `@Aggregate(version)`, default 1 — matching rows
written before the column existed) and `snapshot_type` (payload simple class
name, the `@UpcasterSpec` origin key). `BaseEventStore.upcastSnapshot` runs a
mismatched snapshot through the same `UpcastersManager` fixpoint the event
path uses; if the chain doesn't land exactly on the current revision the
snapshot is dropped with a WARN and the aggregate replays its full stream —
so bumping `version` without writing an upcaster is safe, just slower until
the next snapshot heals the row. Gotcha: the 2-arg
`storeSnapshot(id, payload)` resolves the revision from the *payload's* class
annotation; a separate snapshot DTO on a revisioned aggregate needs the
explicit `storeSnapshot(id, payload, schemaVersion, aggregateVersion)`
overload (the auto-trigger always uses it).

**Fixtures run the real pipeline with zero threads.** `kf-test`'s
`AggregateTestFixture` calls `commandBus.getHandler().setPullMode(true)`
before `start()`: the manager builds its lanes (which `sendSync` needs) but
spawns no pump threads — `sendSync` is entirely caller-thread, so the fixture
needs no teardown. `SagaTestFixture` does the same on the event side and
drives `SagaResolver.resolveForSegment(msg, 0)` directly (1 segment), the
identical code path the cluster pump uses — `@SagaStart` creation, the
correlation index, and post-handler saga persistence all run for real. Saga
handler errors can't propagate through `invokeConsumers` (they route to the
DLQ policy), so the fixture's per-group `DlqEnqueuePolicy` doubles as the
error hook: it records the raw exception and the fixture rethrows it as an
`AssertionError`. Both fixtures clear and rewire the global `GlobalRegistry`
in their constructor — one fixture at a time.

## 14. Cluster command forwarding

Commands whose target segment is owned by another node can be forwarded over a
dedicated TCP channel and executed there (`kf.cluster.forwarding.enabled`,
default off). The non-obvious bits:

**The hook lives in kf-core; the transport in kf-cluster.**
`CommandBus.sendSync/send` consult the `CommandForwarding` static holder (one
volatile read, same shape as `Observability`) right after `buildContext`;
kf-cluster's `ClusterCommandForwarder` implements the `cluster.spi
.CommandForwarder` interface — the dependency direction stays kf-cluster →
kf-core, mirroring `SegmentOwnership`. When nothing is installed the hook is a
null check.

**`sendSyncLocal` / `sendLocal` are the anti-ping-pong guard.** The receiving
node's `BusRemoteCommandExecutor` re-enters the pipeline through these bypass
entries, never through `sendSync` — two nodes with mutually-stale routing
tables would otherwise bounce a command between them forever.

**Local execution is always the safe fallback — except after delivery.**
Disabled, no route, owner==self, connect/write failure (one refresh-and-retry
first): all degrade to local dispatch, which is *correct* (per-JVM aggregate
lock + `UNIQUE(aggregate_id, sequence)` + the OCC retry loop arbitrate; the
cluster never gated command-side correctness on ownership). But once the frame
was written, a missing response is **ambiguous** — the remote may have
executed — so Wait mode throws `ForwardTimeoutException` and Ack mode only
logs: re-running locally could apply the command twice, and OCC does not
deduplicate semantically distinct appends. Never "improve" this to a fallback
without idempotency keys.

**The receiver executes unconditionally.** No "do I still own this segment"
check server-side: a NOT_OWNER bounce would add a ricochet livelock during
rebalances for zero correctness gain. Stale routing degrades to exactly the
pre-forwarding behaviour.

**Wire protocol.** Char-tagged frames (`WireCodec`): request
`'C' | 'W'/'A' | requestId i64 | bodyLen | records`, response
`'R' | requestId | 'A'/'V'/'E' | bodyLen | records`; every record is uniformly
`tag u8 | len i32 | bytes` so unknown tags ('I' traceId is reserved) skip
without decoding. The i64 `requestId` is what multiplexes ONE connection per
peer: `ForwardingClient` parks futures in a `ConcurrentHashMap<requestId,
CompletableFuture>` and a single reader vthread completes them in any order.
The request `'T'` record is the SIMPLE class name (resolved via
`Bus.getMessageClass`, the framework's canonical identity); the response value
`'T'` is an FQCN (handler results are arbitrary unregistered types →
`Class.forName`, homogeneous-classpath assumption).

**Blocking sockets + virtual threads, not NIO.** JEP 491 (JDK 24) removed
synchronized-pinning, so the server parks a cheap vthread per request — that
per-request fan-out (NOT one thread per connection) is what keeps a slow
handler from stalling the connection's read loop. Unbounded vthreads on both
sides also mean A-forwards-to-B-while-B-forwards-to-A cannot deadlock at the
pool level; the residual cycle (a handler that itself sendSyncs a
remotely-owned command) is bounded by `FORWARD_SYNC_TIMEOUT` — avoid
`sendSync` inside `@CommandHandler`.

**Routing is DB-polled, never master-mediated.** `ClusterRoutingTable` caches
`cluster_assignments` + `cluster_nodes` (only rows with `forward_port > 0`)
into two volatile immutable maps; refresh every `ROUTING_REFRESH` plus
rate-limited on-miss/on-failure. `forward_port = 0` (the column default —
probe-then-alter migration, `ADD COLUMN IF NOT EXISTS` is not in the H2∩MySQL
intersection) means "not a forward target", which is the rolling-upgrade
safety valve. `ClusterNode.start` binds the server BEFORE the heartbeat ever
advertises the port.

**Lifecycle.** `KfBootstrap` installs the forwarder right after
`node.start(...)` (still inside the SmartLifecycle phase → before first app
send) and `CommandForwarding.reset()` is the FIRST step of `stop()` (late
sends degrade to local instead of racing a closing transport); the sample
app's `ClusterControl` mirrors that pair across its runtime stop/start. The
kf-test fixtures reset the holder in their constructors.

**Registration validations added with this feature.** Aggregate-handled
commands must declare a `java.util.UUID` `@AggregateIdentifier` field
(interceptor-only commands stay exempt — they route to segment 0); a `@SagaId`
field, when present, must be `UUID`. Both fail at registration
(`InvalidRegistrationException`) instead of surfacing as a null aggregateId at
dispatch time.
