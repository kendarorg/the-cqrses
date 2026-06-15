# kf-integration — cross-implementation contract suites

The framework's **behaviour contract**, written **once** and re-run against
**every** store/bus implementation. Contains **no production code** — only the
shared scenario classes and the per-backend bindings, all under `src/test`.

The point: the in-memory stack (`kf-core-memory`) and the JDBC stack
(`kf-core-db`) must be observably **the same framework**. A single abstract
scenario asserts the behaviour; each backend supplies the seam.

Test-scope dependencies: `kf-core`, `kf-core-memory`, `kf-core-db`, H2, JUnit 5.

## The seam

`IntegrationBackend` is the abstraction every scenario is written against — it
hands the scenario a fully-wired command bus, event bus, and stores without the
scenario knowing whether they are heap- or JDBC-backed.

| Abstract scenario | What it proves |
|-------------------|----------------|
| `AbstractBankLedgerTest` | The end-to-end bank scenario: open account, deposit, withdraw, transfer-via-saga, projected balances. |
| `AbstractBankDlqTest` | DLQ behaviour under a failing handler. |
| `AbstractClusterPullTest` | The cluster **pull-mode** event tail (`SegmentProcessor` semantics). |

Each is subclassed per backend:

| Backend | Binding | Concrete tests |
|---------|---------|----------------|
| **memory** | `InMemoryBackend` | `InMemoryBankLedgerTest`, `InMemoryBankDlqTest`, `InMemoryClusterPullTest` |
| **jdbc** | `JdbcBackend` (H2 in MySQL mode) | `JdbcBankLedgerTest`, `JdbcBankDlqTest`, `JdbcClusterPullTest` |

## The bank domain (`...integration.bank`)

A self-contained CQRS/ES domain used by the scenarios: `AccountAggregate`,
`TransferAggregate`, commands (`OpenAccount`, `Deposit`, `Withdraw`,
`RequestTransfer`), events (`AccountOpened`, `Deposited`, `Withdrawn`,
`WithdrawRejected`, `TransferRequested`), the `TransferSaga`, and read-side
handlers (`BalanceProjection`, `FraudMonitor`).

## Configuration

None — the backends are constructed in code. To add a new implementation,
implement `IntegrationBackend` and subclass each `Abstract*Test`.

## Build

```bash
mvn -pl kf-integration -am test
```
