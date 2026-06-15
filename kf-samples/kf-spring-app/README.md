# kf-spring-app — Personal Finance Manager sample

A small, runnable **Personal Finance Manager (PFM)** demo: log in by username, record money-in /
money-out operations against tags, and read back per-tag and overall summaries. A tiny web UI is
served from `/`.

It is also the node image driven by the `kf-cluster-it` integration tests through its `/cluster/*`
control API.

Under the hood it is a Spring Boot app wired to the **kf framework** (CQRS / event sourcing) via the
`kf-spring` starter. **You do not need to know any of that to run it** — for how the framework
machinery is wired, the domain model, the DLQ policy, and the cluster-control plumbing, see
**[IMPLEMENTATION.md](IMPLEMENTATION.md)**.

## Run

```bash
mvn -pl kf-samples/kf-spring-app -am spring-boot:run
```

Then open **http://localhost:8080/**.

Or package the boot jar (also what the cluster ITs build their node image from) and run it directly:

```bash
mvn -pl kf-samples/kf-spring-app -am package
java -jar kf-samples/kf-spring-app/target/*.jar
```

State is stored in a file-backed H2 database under `./data/`, so it survives restarts. Delete that
directory for a clean slate.

## HTTP API

The web UI uses these; you can also call them directly with `curl`.

| Method & path | Purpose |
|---------------|---------|
| `POST /api/login` | `{username}` → log in, registering the user on first sight. |
| `POST /api/operations` | `{username, type: IN\|OUT, amount, tag}` → record an operation (amount must be positive). |
| `GET /api/summary?username=` | Overall in/out/net totals. |
| `GET /api/summary/by-tag?username=` | Totals grouped by tag. |
| `GET /api/operations?username=` | The 50 most recent operations. |

```bash
curl -s localhost:8080/api/login -H 'content-type: application/json' -d '{"username":"alice"}'
curl -s localhost:8080/api/operations -H 'content-type: application/json' \
  -d '{"username":"alice","type":"IN","amount":1200,"tag":"salary"}'
curl -s 'localhost:8080/api/summary?username=alice'
```

### Operational endpoints

- `GET /cluster/status`, `POST /cluster/stop`, `POST /cluster/start` — runtime cluster control. In the
  default config the cluster is **disabled**, so these report `enabled:false` and do nothing. They
  exist for the `kf-cluster-it` tests. See [IMPLEMENTATION.md](IMPLEMENTATION.md#cluster-control-stoprestart-a-nodes-cluster-part-without-killing-the-jvm).
- `POST /dlq/retry-all` — re-run any dead-lettered projection events. See
  [IMPLEMENTATION.md](IMPLEMENTATION.md#dlq-policy-dont-silently-drop-failed-projections).

## Configuration

Defaults live in `src/main/resources/application.yml` (server port `8080`, file-backed H2, cluster
off). Override any `kf.*` / `pfm.*` property the usual Spring Boot way — command-line args and
environment variables take precedence over the yml. The full annotated config is documented in
[IMPLEMENTATION.md](IMPLEMENTATION.md#configuration-reference-srcmainresourcesapplicationyml).

## Dependencies

`kf-spring` (which pulls in the whole framework: `kf-core`, `kf-core-db`, `kf-cluster`),
`spring-boot-starter-web`, `spring-boot-starter-jdbc`, H2 + the MySQL connector.
