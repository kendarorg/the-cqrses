# kf-core-db — JDBC implementations

JDBC implementations of the `kf-core` store and bus contracts. The durable,
production single-node backend, and the substrate the `kf-cluster` module
distributes across nodes.

All DDL/DML targets **H2 in MySQL compatibility mode** (`MODE=MySQL`) and is kept
strictly inside the **H2 ∩ MySQL intersection**, so the identical SQL runs
against a real MySQL server. Plain `java.sql` behind the `Db` wrapper from
`kf-core` — **no JPA / Hibernate / Spring** at this layer. Depends on `kf-core`
(H2 / MySQL-connector / Testcontainers are test-scope).

## What it provides (`org.kendar.cqrses`)

| Class | Implements |
|-------|-----------|
| `bus.JdbcCommandBus` | `CommandBus` |
| `bus.JdbcEventBus` | `EventBus` |
| `repositories.JdbcEventStore` | `EventStore` |
| `repositories.JdbcSagaStore` | `SagaStore` |
| `repositories.JdbcDlqStore` | `DlqStore` |
| `repositories.JdbcCheckpointStore` | `CheckpointStore` |
| `scheduler.JdbcScheduler` | `Scheduler` (durable — supports the named-task overload) |
| `pg.JdbcProcessingGroup` / `pg.JdbcProcessingGroupsManager` | processing-group pull workers |
| `db.SchemaInitializer` | creates the framework tables |

## Durability model

Single-node durability comes from the **stores themselves**, not the cluster:

- **OCC** via an in-process per-aggregate lock, plus
- a `UNIQUE(aggregate_id, sequence)` backstop in the event table.

The cluster layer (`kf-cluster`) adds **placement** on top — which node sees
which partition — **not** durability. The `-1` aggregate-version sentinel ("load
all" / "assign next on append") is handled here exactly as the `kf-core`
contract specifies.

Under the cluster, the event-side pump is **at-least-once**: the
`processor_checkpoint` high-water-mark is committed *after* dispatch, so a
crash/handoff re-processes the straddling event. Projections must be idempotent.

## Configuration

Wired during the `kf-core` **setup phase**:

1. Construct a `Db` over a `DataSource` (the wrapper's `connection()` is
   overridable — the template-method seam used by tests).
2. Run `SchemaInitializer` to create the tables (idempotent).
3. Construct the JDBC buses + stores, register them in `GlobalRegistry`,
   subscribe handlers, `SegmentCalculator.setSegments(N)`, `GlobalRegistry.start()`.

There is **no properties file** here — `kf-spring` is what reads `kf.*` and
constructs this stack for you. The DB connection itself is whatever `DataSource`
you hand the `Db`.

## Testing

Tests run against **H2 in MySQL mode** by default; Testcontainers + a real MySQL
8.x are available for parity. Because the SQL is in the H2 ∩ MySQL intersection,
the same statements pass on both. The bank parity suites in `kf-integration` run
this backend (`JdbcBackend`) side-by-side with the in-memory one.

```bash
mvn -pl kf-core-db -am test     # H2-backed
```
