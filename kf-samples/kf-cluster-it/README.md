# kf-cluster-it — 3-node Testcontainers run of `kf-spring-app`

Heavyweight integration tests that stand up **three real JVMs of `kf-samples/kf-spring-app`** (the
Personal Finance Manager demo) in cluster mode against **one shared MySQL**, on a single Docker
network. The headline test drives cluster membership **through the app's `/cluster/*` control API**,
so a node's cluster part can be stopped and restarted at runtime **without killing the JVM** — the
container stays up, all ports stay mapped, and a debugger stays attached.

These are `*IT` tests (Maven **failsafe**), **Docker-gated** (skipped when Docker is unavailable),
and run for **minutes** — that is expected. They are excluded from the normal unit pass.

> For the node application itself — its domain model, how it wires the framework, and the full
> `/cluster/*` + `/dlq/*` control surface these tests drive — see the
> [`kf-spring-app` README](../kf-spring-app/README.md) and its
> [IMPLEMENTATION.md](../kf-spring-app/IMPLEMENTATION.md).

## Why containers

`GlobalRegistry` is a process-wide static service locator, so real nodes **must** be separate JVMs.
Testcontainers gives separate processes on a shared Docker network — the faithful way to run the
real buses + `SegmentProcessor` pull pump + DB-backed leader election + inter-node liveness HTTP.
The database is the only shared substrate, so the shared store is **one MySQL 8.4 container**.

## Topology & ports

One MySQL (alias `kfdb`) + three app nodes (aliases `node1..3`), `KF_SEGMENTS=6` (⇒ 2 segments per
node when balanced). Every container port is **pinned to a fixed host port** so host-side `curl` /
debugger attach is predictable (assertions still use `getMappedPort`, so the exact numbers don't
matter to the tests):

| port | role           | node1 | node2 | node3 |
|------|----------------|-------|-------|-------|
| 8080 | app HTTP / API | 18081 | 18082 | 18083 |
| 8070 | liveness       | 18071 | 18072 | 18073 |
| 5005 | JDWP debug     | 15005 | 15006 | 15007 |

MySQL's 3306 is mapped to a random host port (`AbstractClusterIT.db()` uses `getJdbcUrl()`).

> **Fixed-port caveat:** pinned host ports mean only one cluster can run at a time. Failsafe runs IT
> classes sequentially, so this is fine for local / diagnostic runs.

## The `/cluster/*` control API (in `kf-spring-app`)

Always present; **no-op when `kf.cluster.enabled=false`** (the `ClusterNode`/`SegmentProcessor`
beans are absent, so every call is an inert success reporting `enabled:false`).

```
GET  /cluster/status  -> 200 {enabled, running, nodeId, segments}
POST /cluster/stop    -> 200 {enabled, running:false}   # node.stop() + segmentProcessor.stopAll()
POST /cluster/start   -> 200 {enabled, running:true}    # node.start(segments, livenessPort, itemProcessor)
```

`stop()` halts both leader/heartbeat/liveness/worker **and** the event-side pull pump (otherwise a
stopped node would keep dispatching events other nodes have taken over). `start()` re-joins; the
worker tick re-claims this node's assigned segments and the `SegmentProcessor` rebuilds its workers.

The flood ITs also use the app's **`POST /dlq/retry-all`** (idempotent) to drain any projection
events that were dead-lettered during a handoff before the final read-model verdict — re-applying
them is safe because the PFM read model is insert-ignore.

## Running

```bash
# Package the app jar first (the ITs build the node image from it), then run the ITs:
mvn -pl kf-samples/kf-spring-app -am package
mvn -pl kf-samples/kf-cluster-it verify

# A single scenario:
mvn -pl kf-samples/kf-cluster-it verify -Dit.test=ApiStopRestartHandoffIT
```

### Logs

Everything — the harness (`kf-cluster-it`), all three node containers (`node1..3`), and
Testcontainers — is captured to **`target/kf-cluster-it.log`** (plain UTF-8, truncated each run).
Read or tail that file rather than the live console: the live docker stream is byte-framed and can
render garbled in a terminal ("one char then blank lines"), whereas the file is written one full
line at a time. Nodes run with `SPRING_OUTPUT_ANSI_ENABLED=NEVER` so neither sink carries colour
escape codes.

```bash
tail -f kf-samples/kf-cluster-it/target/kf-cluster-it.log
```

The node image is built from the boot jar via `ImageFromDockerfile`:

```dockerfile
FROM eclipse-temurin:25-jre
COPY app.jar /app/app.jar
ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/app.jar"]
```

(If no `eclipse-temurin:25-jre` base is available, switch the base to `25-jdk` in
`AbstractClusterIT.DOCKERFILE`.)

## Attaching a debugger

Each node runs with JDWP open (`server=y,suspend=n` — don't block startup; the cluster timers must
run). Attach an IDE **Remote JVM Debug** to:

- `localhost:15005` → node1
- `localhost:15006` → node2
- `localhost:15007` → node3

To suspend a node until the debugger attaches, run with `-DDEBUG_SUSPEND=y` (and optionally
`-DDEBUG_SUSPEND_NODE=<0|1|2>`, default 0) — that node starts with `suspend=y`.

## Scenarios

| IT | what it proves |
|----|----------------|
| `ClusterFormationIT` | 3 nodes join: each `running:true`, 3 fresh heartbeats, one leader, all 6 segments owned by distinct live nodes. |
| `ApiStopRestartHandoffIT` | **Core deliverable.** `POST /cluster/stop` → node stays up but its segments reassign; load keeps flowing and the read model stays correct; `POST /cluster/start` → it rejoins and rebalances. |
| `HardKillHandoffIT` | Contrast: `container.stop()` (crash) → same reassignment + read-model correctness, proving crash recovery, not just graceful leave. |
| `FloodStopRestartConsistencyIT` | Endurance: 100 users under a continuous multi-threaded flood. **Gracefully** stop node3 (`/cluster/stop`) mid-flood, keep hammering the survivors and assert the read model converges to the exact acked totals; restart node3, flood more, then stop bombarding, wait 60s, and verify **every** user reads identically from all three nodes equal to its exact expected `net`. |
| `FloodHardKillRestartConsistencyIT` | Same scenario, but node3 is **hard-killed** (`container.stop()`) and brought back as a fresh container/JVM — proving exact convergence across a real crash + cold restart, not just a graceful leave. |

> Both flood ITs share `AbstractFloodConsistencyIT`; the subclasses only decide how node3 leaves/rejoins.
> The flood is **quiesced and node3 pulled from write rotation before** the take-down, so a crash can't
> orphan an ambiguous in-flight write (which would leave the read model permanently above the counted
> total). The remaining at-least-once re-dispatch is absorbed by the insert-ignore read model + the 60s drain.

All read-model assertions rely on the PFM read model being **idempotent** (UPSERT / `INSERT IGNORE`),
which is what absorbs the at-least-once re-dispatch window during a segment handoff.
