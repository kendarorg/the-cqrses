Over the last few weeks I built **Kendar Framework** - a lightweight CQRS / Event-Sourcing kernel for Java 25. The code is open and on GitHub: **https://github.com/kendarorg/the-cqrses**.

The motivation was simple. The mature options - AxonIQ above all - gate the features you actually need in production - multi-node distribution, event replay, dead-letter handling - behind paid tiers. The goal was to build a genuinely open-source equivalent of the AxonIQ stack that is **stable, reliable, and usable** - not a toy or a proof of concept, but the same building blocks you'd reach for in real systems, with no licence wall and no mandatory DI container: something that runs as a plain library, or wired through Spring Boot, or distributed across a cluster, all from one kernel.

I'll describe what the framework covers below. But the more interesting story - and the reason I'm writing this as an article rather than a quick post - is what those few weeks taught me about where AI coding assistants genuinely accelerate you, and where they quietly work against you. CQRS/ES turned out to be an almost perfect stress test for that question, because the whole system *is* cross-cutting concerns, and cross-cutting concerns are exactly where today's assistants struggle.

## What the framework covers

Kendar Framework is a strict, one-directional stack of modules. Everything depends on the kernel, and nothing depends back. That direction is the spine of everything that follows.

- **The kernel** holds pure contracts and shared behaviour - no storage, no Spring. It defines the annotations you build a domain with, the command and event buses, the store interfaces, schema-evolution upcasting, durable scheduling, and the segment engine that hashes each aggregate to a fixed partition so its messages always stay in order.
- **The in-memory module** is a heap-backed implementation for tests, demos, and single-node use.
- **The JDBC module** is the durable single-node backend. Plain SQL, no JPA or Hibernate, written to run unchanged on both H2 and MySQL. Durability comes from optimistic concurrency on each aggregate's event stream.
- **The cluster module** distributes the work across nodes with one guarantee: at most one live node owns each partition at any moment. It does leader election, heartbeats, liveness checks, and minimal-movement rebalancing - adding *placement*, not durability. Delivery is at-least-once, so read models must be idempotent.
- **The Spring Boot starter** wires the whole stack from a single dependency and a datasource, with every piece overridable.
- **The contract suites** are the behaviour of the framework written once and re-run against both the in-memory and JDBC backends, proving they are observably the same framework. (Hold this thought - it's the hero of the second half.)

In feature terms: aggregates with snapshot-accelerated replay, sagas for multi-step processes, read-model projections, dead-letter queues with skip/block/park policies, durable scheduling, schema evolution, optimistic-concurrency commands, and true multi-node distribution with safe handoff - as a library, under Spring, or clustered, from one kernel.

## Why this is a brutal test for AI assistance

A CRUD app is a pile of mostly independent endpoints. CQRS/ES is the opposite: it's a small number of **invariants that thread through every module at once.**

- **Ordering** - every message for one aggregate must hash to the same segment and the same lane, or per-aggregate ordering breaks.
- **Concurrency** - command durability rides on the per-aggregate lock *and* the `UNIQUE(aggregate_id, sequence)` backstop *and* the OCC retry loop, together.
- **Distribution** - a gaining node may not start a second pump for a partition until the losing node calls back `release(itemId)`, or the lease expires. That single rule is what makes the cluster correct.
- **Delivery semantics** - checkpoints commit *after* dispatch, so a handoff re-processes the straddling event. That's deliberate: it's why projections must be idempotent.
- **Lifecycle** - setup and runtime must never overlap. On the first `send`/`publish` the entire topology - handler maps, policies, the segment count - is **frozen**.

None of these lives in one file. Each is a property of how five modules interact. Get one wrong and it won't fail a unit test - it fails as a duplicated event on the *second* node, three rebalances later, under load.

## Where the AI stopped being useful

I went in expecting to move fast. For the leaf work, I did: the JDBC wrapper, the Jackson upcaster, the adaptive `Sleeper` backoff, the boilerplate of the sample bank app, the DTOs and row mappers. Bounded problems with a clear local contract - AI is genuinely excellent here, and a large fraction of the codebase came together this way.

Then I hit the cross-cutting core, and the productivity curve inverted.

The problem is that **an LLM optimises locally.** Ask it to "make checkpointing safe" and it will cheerfully add a lock and commit the checkpoint *before* dispatch - code that looks correct and passes the test in front of it, while silently converting at-least-once into a lost-event bug on handoff. Ask it to "clean up segment routing" and it'll tweak the hash or switch `floorMod` back to `%`, not knowing that the segment count is frozen and that `toString()`-based hashing is load-bearing for every event already on disk. Ask it to "simplify the sentinel handling" and it'll remove the `-1` aggregate-version sentinel that means two different things depending on direction.

These aren't hallucinations. They're *plausible, well-written, locally-correct changes that violate an invariant the model can't see* - because the invariant lives in the interaction between modules, not in the function it was handed. The context that makes the code correct is precisely the context that doesn't fit in the prompt window, and often isn't written down anywhere except one engineer's head.

So I wrote most of the core by hand. Not out of purism - out of economics. For the parts where I had to hold the whole distributed-ordering-and-recovery model in mind at once, a confidently-wrong suggestion costs *more* to detect and unwind than the code would have cost to write from scratch. The assistant's speed advantage goes negative the moment verification becomes harder than authorship.

## Modularisation as a survival strategy

The thing that actually made this tractable wasn't a cleverer prompt. It was **brutal modularity** - and it turned out to be the lever that lets AI back in safely.

The strict, one-way dependency graph means each module has a small, explicit surface. `kf-cluster` knows nothing about event sourcing except through one adapter (`SegmentItemProcessor`). `kf-core-db` knows nothing about Spring. `kf-core` knows nothing about *any* storage. And the `kf-integration` contract suites pin the behaviour down once, independently of implementation.

Here's the payoff: **a modular boundary is the only thing that can shrink a cross-cutting concern down to something safely delegable.** Once the question stops being "is this checkpoint logic globally correct?" and becomes "does `JdbcBackend` pass the exact same contract suite `InMemoryBackend` already passes?", the invariant is enforced by the *seam*, not by the model's understanding. The boundary converts an unverifiable global property into a verifiable local one - and a verifiable local task is exactly what an assistant is good at.

So the relationship inverts in a useful way. The better the architecture, the *more* of it you can hand to an AI, because more of it is locally checkable. Bad modularity doesn't just hurt human maintainers; it removes the only mechanism that would have made AI assistance trustworthy on the hard parts.

## What I'd tell anyone building something with real invariants

1. **Use AI for the leaves, not the spine.** Bounded, well-specified, locally-verifiable code is its sweet spot. Distributed invariants that span modules are not.
2. **A passing test is not a correct change.** On cross-cutting code, the dangerous failures are the ones no existing test exercises. The model will pass the test and break the system, confidently.
3. **Invest in seams before speed.** A contract suite that runs against multiple implementations is worth more than any prompt. It is what lets you trust *anyone's* code - human or model.
4. **Someone still has to own the invariant.** There is no substitute yet for one person understanding how ordering, concurrency, distribution, and recovery interact. The assistant can type. It cannot, yet, own the property that makes the typing correct.

Kendar Framework came together quickly *because* the split was honest: the hard 20% was written slowly, by hand, with the whole picture in view - and the easy 80% was genuinely accelerated by tooling, safely, because the architecture made every boundary explicit and every boundary checkable.

The lesson generalises well beyond CQRS. **AI assistance scales with the quality of your boundaries.** If you want to go fast with it on a serious system, the highest-leverage work is the architecture you do *before* you ever open the chat window.

*Kendar Framework is a CQRS/Event-Sourcing library for Java 25 - library, Spring Boot, or clustered, from one kernel. If you've found your own line between what to write by hand and what to delegate to an assistant on systems work, I'd genuinely like to hear where you draw it.*
