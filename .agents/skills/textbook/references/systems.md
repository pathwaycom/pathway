# External systems and dependencies

Two analysis procedures: one for integrating with an external **system** (message
broker, database, service), the other for relying on a third-party **library**.
Both serve the same purpose: readers must understand our code and what it rests on.

---

# Part A. External system

Apply this when the session concerns a connector, integration, or migration from
one storage system to another. This is a full 5–10-page chapter, not a paragraph
saying “Pulsar is a broker.”

The standard: readers can design an integration from scratch, explain their
choices to colleagues, and predict where it will break under load.

## A1. What the system is

- The problem it solves and **what came before**: previous approaches and why
  they stopped being sufficient.
- Data model and basic concepts. For a broker: topic, partition, subscription,
  offset, consumer group, retention. Define every term.
- Precise guarantees: ordering (global, per partition, none), delivery
  (at-most-once / at-least-once / effectively-once and their practical meaning),
  durability (when a write is considered persistent, replica count), transactions.
- Consistency model and behavior during network partitions.

## A2. Architecture, with a diagram

**A diagram is required.** Components, placement, and data paths.

- Processes that make up the system and their responsibilities.
- Data and metadata placement; coordination via ZooKeeper, etcd, embedded Raft,
  KRaft, or another mechanism.
- Whether storage and serving are separate. Pulsar separates brokers and
  BookKeeper; Kafka brokers handle both. Explain the architectural consequences.
- The message path from producer to consumer, step by step, including when it
  reaches disk and where it is replicated.
- Scaling and what happens on node addition, node failure, and rebalancing.

When a change depends on how the client communicates with the system, follow
`networking.md` for the relevant transport and wire-protocol details. A database
integration does not automatically require an HTTP chapter: establish the actual
stack and explain only the mechanisms needed for this session.

## A3. Comparison with alternatives is required

For Pulsar, compare Kafka, NATS JetStream, and MQTT. For Postgres, compare MySQL
and storage systems suited to our workload. The aim is to explain **the axes on
which systems differ**, so readers can reason about a different set of conditions.

Compare along common axes:

| Axis | What to compare |
|---|---|
| Model | offset log, queue, pub/sub, brokerless |
| Storage | always on disk, in memory, separate compute and storage |
| Ordering | global, per partition/key, none |
| Delivery | at-most / at-least / exactly-once and their cost |
| Scaling | by partition, segment, or node; cost of adding capacity |
| Latency | typical and tail; fsync, replication, or batching as the dominant cost |
| Throughput | how it is achieved: batches, compression, zero-copy |
| Backpressure | what happens when the consumer cannot keep up |
| Multitenancy | topic isolation, quotas |
| Operations | setup, upgrades, monitoring |
| When to choose it | an honest one-line “choose this if…” |

Include **diagrams of differences**, preferably several small ones showing
contrasting models. For example: Kafka's broker-local partition log versus
Pulsar's segments distributed across bookies, and how that changes node-failure
behavior.

Discuss misleading comparisons explicitly. MQTT and Kafka address different
problems: a device protocol with QoS versus a distributed log. Explain why they
appear together rather than merely comparing throughput.

## A4. Tuning for maximum performance

Explain the mechanics behind settings rather than listing configuration lines.

- **Key controls and their physical effects**: batch size, accumulation delay
  (`linger`), compression and whose CPU performs it, client and broker buffers,
  partition count, `fsync` policy, acknowledging replicas.
- **Explicit tradeoffs**: batching trades latency for throughput; `acks=all`
  trades tail latency for durability; compression trades CPU for bandwidth;
  more partitions trade overhead and rebalance time for parallelism.
- **Settings outside the system**: `SO_RCVBUF`/`SO_SNDBUF`, `TCP_NODELAY`, NIC
  queues and interrupts, thread affinity, NUMA, I/O scheduler and filesystem,
  page cache, `dirty_ratio`.
- **Latency requirements**, where relevant: what to do when p99.9 matters;
  when batching hurts; when a dedicated receive thread is worth the resource.
- **Verification**: metrics, load scenarios, and what counts as success.
- **Our values**: current settings, their rationale, defaults deliberately kept,
  and when to reconsider them.

---

# Part B. Third-party libraries under the hood

Readers also learn operating systems, concurrency, and performance through
other people's code. Examine key dependencies rather than merely mentioning them.

Key means anything on the hot path, anything creating threads, or anything using
network or disk I/O. A JSON wrapper used only for configuration is not key.

**Read source code.** A README explains what a library does; we need how it does
it. Obtain the source, locate hot spots, and read them.

## Inspect the version actually used

- Establish the resolved dependency version and enabled features from lockfiles,
  manifests, build options, and runtime configuration. Inspect that version's
  source, including native/transitive layers when they determine the behavior.
  Do not substitute the latest upstream implementation for the one in use.
- Trace our call into the relevant internal types, functions, buffers, queues,
  callbacks, worker threads, and underlying library or OS operations. Explain
  each relevant component's role, ownership, and guarantees. Connect this path
  to the changed code and show before/after behavior where appropriate.
- Cite source snippets and line links using verified upstream commit permalinks
  where available. Record the package version, source revision, and relevant
  features. If an upstream revision cannot be established, report the exact
  package source/version inspected and the limitation; do not invent a SHA.

## Undocumented behavior and implementation-dependent optimizations

When a solution relies on an undocumented option, an internal API, or a behavior
discovered in source, explain that dependency explicitly:

- What was discovered, where it appears in the dependency source, and the exact
  option, call, or configuration path our code uses to reach it.
- Whether this is an undocumented public interface, a private/internal API, or
  an observed implementation detail. Separate supported guarantees from behavior
  that might change between versions.
- Why it changes correctness or performance: follow the mechanism through the
  relevant layers, such as batching, copies, queueing, wakeups, locking, or FFI.
  Use measurements from the session when available and label unmeasured claims.
- Preconditions, version/feature sensitivity, failure modes, and what should be
  rechecked on upgrade. Explain any existing regression coverage or fallback,
  and honestly identify missing verification.

For example, if an optimization uses an undocumented capability in `rdkafka`,
inspect the resolved crate and any underlying native client behavior relevant
to that capability. Explain how our call reaches it and why it helps. Do not
infer the mechanism solely from an option's name or a benchmark improvement.
Apply the same reasoning to any other dependency when its internals matter;
incidental libraries do not need a source-code tour.

## Questions to answer

- **High-level model**: layers, offered abstractions, and where our code enters.
- **Buffers**: location, size, allocation timing and ownership. Reused or allocated
  per message? Pooled? Copied between layers? How many times is each byte copied
  between the socket and our structure?
- **Threads**: count, creation timing, responsibilities, hidden background workers,
  communication through queues/channels/shared state. Hot-path synchronization
  primitives and their cost.
- **Coroutines and async**: whether and why they are used: many connections
  without a thread each, lower switching overhead, linear code instead of
  callbacks. Illustrative switching costs may be ~10–50 ns for a coroutine
  versus ~1–3 μs for a thread; label machine-dependent estimates as such.
  Identify the executor, suspension points, `.await` behavior, and consequences
  of a blocking call inside a coroutine.
- **Syscalls**: exact names and frequency, per connection/message/timer. `epoll`,
  `io_uring`, `writev`/`sendmsg` for batching, `MSG_ZEROCOPY`, `TCP_CORK`.
  This is real code through which to teach Unix.
- **Design reasons**: authors' tradeoffs. Fixed buffers for an unknown workload,
  per-message allocation because the API transfers ownership, one lock around
  a structure because correctness is simpler.
- **Optimization opportunities**: configuration for our workload; supplying a
  buffer, allocator, or executor to bypass a general path; whether a patch or
  fork is worthwhile. If the library is not the bottleneck, say so explicitly.
- **Hazards**: panics, hidden locks, unbounded queues, missing backpressure,
  network failure behavior, default timeouts.

## Format

Give a key library 2–4 pages: a data-path diagram through its layers, source
snippets with explanations, and a “per-message work” table listing allocations,
copies, syscalls, and lock acquisitions. Such a table makes the remaining
optimization opportunities immediately visible.

For a substantial dependency analysis, use a separately navigable chapter or
section following the reading-path rules in `writing.md`. Keep a short summary
of the mechanism, assumptions, and upgrade risks in the main change walkthrough
and code-review chapter so readers can skip the deep dive without missing
information necessary to assess the change.
