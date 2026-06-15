# kf-core-memory — in-process implementations

Pure in-process (heap) implementations of the `kf-core` store and bus contracts.
For **unit tests, single-node demos, and the in-memory backend** of the
`kf-integration` contract suites. **Not used by cluster deployments** — those use
the JDBC stack in `kf-core-db` and never depend on this module.

Depends only on `kf-core` (plus Jackson / Logback transitively).

## What it provides (`org.kendar.cqrses`)

| Class | Implements | Notes |
|-------|-----------|-------|
| `bus.InMemoryCommandBus` | `CommandBus` | Single-node **push** command bus. |
| `bus.InMemoryEventBus` | `EventBus` | Single-node **push** event bus — one thread per lane (up to `SEGMENTS` lanes per group). |
| `repositories.InMemoryEventStore` | `EventStore` | Heap-held event streams + optional snapshots. |
| `repositories.InMemorySagaStore` | `SagaStore` | Heap-held saga instances + association index. |
| `repositories.InMemoryDlqStore` | `DlqStore` | Heap-held dead letters. |
| `repositories.InMemoryCheckpointStore` | `CheckpointStore` | Heap-held processor high-water-marks. |
| `scheduler.InMemoryScheduler` | `Scheduler` | Reference closure scheduler. **Non-durable**: the named-task overload (`schedule(Instant, String, Object)`) throws `UnsupportedOperationException` — durable scheduling requires `JdbcScheduler`. |

## Configuration

None of its own. These are wired during the `kf-core` **setup phase**: construct
the store/bus instances, register them in `GlobalRegistry`, subscribe handlers,
set `SegmentCalculator.setSegments(N)`, and start via `GlobalRegistry.start()`.
All data lives on the heap and is gone when the JVM exits or the store is reset —
`Bus.clear()` resets bus dispatch state but does **not** clear store data.

## Push vs pull

The in-memory buses run **push** mode: each lane runs on its own thread and a
projection/saga singleton in a multi-lane group is invoked concurrently by up to
`SEGMENTS` lane threads (each dispatching a disjoint set of aggregates). Such
singletons must be concurrency-safe; ordering is guaranteed **within a single
aggregate / saga only**. The cluster pull mode (`SegmentProcessor`) lives in
`kf-core` and is exercised by the JDBC stack, not here.

## Build & test

```bash
mvn -pl kf-core-memory -am test
```
