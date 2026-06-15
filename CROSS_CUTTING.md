# Event Sourcing Module

## Architecture

Axon Framework with CQRS/ES pattern.

### Aggregates (`domain/aggregate/`)
- `ProductAggregate`
- Identified by `ProductCode`
- Use `@Aggregate`, `@CommandHandler`, `@EventSourcingHandler`
- Apply events to change state — never mutate state directly

### Sagas (`domain/workflow/`)
- `ReservationSaga`, `ConfirmationSaga`
- Use `@Saga`, `@StartSaga`, `@SagaEventHandler`
- Orchestrate cross-aggregate processes — no business logic ownership

### Commands (`domain/api/command/`)
- `ReserveProduct`, `ConfirmProduct`, etc.
- Represent intent to change state

### Events (`domain/api/event/`)
- `ProductReserved`, `ProductConfirmed`, etc.
- Immutable facts; persisted to Axon event store; consumed by projections

### Projections (`application/read/`)
- `TransportationProductProjection`, `ProcessesStatusProjection`
- Read-only views built from events (query side of CQRS)

### Domain Services (`domain/service/`)
- `ReserveIntegrationService`, `ConfirmIntegrationService`
- Integration points with external providers

## Conventions

- Commands = intent, events = facts
- Aggregates apply events, never directly mutate state
- Sagas coordinate processes, don't own business logic
- Projections are read-only
