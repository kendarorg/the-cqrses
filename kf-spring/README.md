# kf-spring — Spring Boot starter

Spring Boot auto-configuration that wires the `kf-core-db` stack and an optional
`kf-cluster` node, so an application gets the whole framework by adding one
dependency and a `DataSource` bean. Depends on `kf-core`, `kf-core-db`,
`kf-cluster` and `spring-boot-autoconfigure` (Spring Boot parent: see root pom).

Every infra bean is `@ConditionalOnMissingBean`, so **any single piece can be
overridden** by the application.

> **Setup, configuration, and usage live in [INSTRUCTIONS.md](INSTRUCTIONS.md).**
> This README covers only the module's structure and scope.

## Components (`org.kendar.cqrses.spring`)

| Class | Role |
|-------|------|
| `KfAutoConfiguration` | The auto-config entry point (registered via `META-INF/spring/...AutoConfiguration.imports` + `spring.factories`). Declares the infra beans. |
| `KfBootstrap` | Owns the ordered registration + lifecycle (setup phase → start). |
| `KfHandlerScanner` | Classpath-scans for `@Aggregate` / `@Projection` / `@Saga` / `@CommandInterceptor` / `@Schedulable` and registers them. |
| `KfSegmentEnvironmentPostProcessor` | Reads `kf.segments` from the `Environment` **before context refresh** and calls `SegmentCalculator.setSegments(...)` — it must run before any store / bus / `ProcessingGroupsManager` is constructed. |
| `KfProperties` | Binds the `kf.*` namespace. |
| `EventBusCustomizer` | Hook for customising the `EventBus` (per-group policy etc.). |

## Scope

- **In scope:** Spring Boot auto-configuration of the `kf-core-db` single-node
  stack and an optional `kf-cluster` node, classpath handler discovery, the
  pre-context segment setup, and lifecycle management. The app gets the whole
  framework from one dependency plus a `@Bean("kf-datasource") DataSource`.
- **Out of scope:** the framework runtime itself (lives in `kf-core` /
  `kf-core-db` / `kf-cluster`), and the application's own domain, web layer, and
  read-model data sources. The framework DB is kept segregated from app data
  sources.
- **Configuration surface:** the `kf.*` namespace, bound by `KfProperties` —
  see [INSTRUCTIONS.md](INSTRUCTIONS.md) for the full property reference and
  example `application.yml` (single-node and cluster).

## Lifecycle

`KfBootstrap` does the entire **setup phase** during context startup (register in
`GlobalRegistry`, subscribe scanned handlers, set segments via the
post-processor, start the buses + optional cluster node) and tears it down on
shutdown. The frozen-topology rule applies per node: once traffic flows, handler
registrations don't change; only cluster *assignment* (which node sees which
partition) is dynamic.

## Build

```bash
mvn -pl kf-spring -am test
```

See `kf-samples/kf-spring-app` for an end-to-end wiring example.
