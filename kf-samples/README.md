# kf-samples — example apps & sample-driven integration tests

Everything sample-related lives here (not at the repo root). These are **not**
framework modules — they consume the framework and contain no production code
beyond the demo app itself.

| Module | What it is |
|--------|-----------|
| [`kf-spring-app`](kf-spring-app/README.md) | The **Personal Finance Manager** demo (`PfmApplication`) wiring `kf-spring` end-to-end: aggregate, commands/events, projections, DLQ policy + controller, a `/cluster/*` runtime control API, and a web UI. Runnable standalone; also the node image for the cluster ITs. |
| [`kf-cluster-it`](kf-cluster-it/README.md) | Heavyweight **Testcontainers** integration tests: three real JVMs of `kf-spring-app` against one shared MySQL, driving cluster membership through the app's `/cluster/*` API. Docker-gated, all `*IT` (Failsafe), runs for minutes. |
| `axon-spring-app` | **Axon 4.13 twin** of `kf-spring-app` — same PFM app, REST surface and read-model tables, built on open-source Axon Framework (server-less: shared MySQL only, no Axon Server). Projection work distributes via Axon's token-store segment claiming (`PooledStreamingEventProcessor`); per-aggregate DLQ via `SequencedDeadLetterQueue`. Emits the **same `kf.*` meters** as the kf app so the two Prometheus reports are directly comparable. |
| `axon-cluster-it` | Axon twin of `kf-cluster-it`: the **same** flood / handoff / consistency scenarios against three `axon-spring-app` JVMs + one MySQL, for a head-to-head perf comparison. Coordination assertions are retargeted to Axon's `token_entry` (segment claims; no leader); ownership is read from each node's `/cluster/segments`. |

There is no aggregator pom here — each is a leaf module of the root reactor.

```bash
# Run the demo
mvn -pl kf-samples/kf-spring-app -am spring-boot:run

# Run the cluster ITs (needs Docker; package the app jar first)
mvn -pl kf-samples/kf-spring-app -am package
mvn -pl kf-samples/kf-cluster-it verify
```

## kf ↔ Axon performance comparison

`axon-spring-app` / `axon-cluster-it` reproduce the kf demo + ITs on **open-source
Axon 4.13** (server-less, same single-MySQL substrate) so the two stacks run the
*identical* flood/handoff/consistency scenarios. The comparison plan and the full
kf↔Axon mapping (segment ownership ⇄ token claims, OCC ⇄ `domain_event_entry`
unique constraint, DLQ ⇄ `SequencedDeadLetterQueue`, `/cluster/stop` ⇄ processor
shutdown) are in [`plans/axon-comparison.md`](../plans/axon-comparison.md).

```bash
# Axon demo (H2, single node)
mvn -pl kf-samples/axon-spring-app -am spring-boot:run

# Axon cluster ITs (needs Docker; package the boot jar first)
mvn -pl kf-samples/axon-spring-app -am package
mvn -pl kf-samples/axon-cluster-it verify
```

**Reading the results.** Both stacks emit the same `kf.*` Micrometer meters
(command/append/dispatch/tail-read/SQL latency, DLQ rate); each IT writes a
per-scenario report (`target/kf-metrics-*.md` vs `target/axon-metrics-*.md`) plus
the load generator's acked-ops/s throughput in the logs. Compare those side by
side. One substrate caveat: Axon's event store is JPA/Hibernate while kf is plain
JDBC, and the Axon SQL timer (datasource-proxy) has millisecond resolution vs kf's
nanosecond `Db` timing — so treat `sql.execute` deltas as coarse.
