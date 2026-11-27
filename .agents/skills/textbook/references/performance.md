# Performance and predictable latency

Use this checklist to examine the session's actual code. For each applicable
topic, show the mechanism, code location, effect, measurement method, and tradeoff.
Briefly mark topics that do not apply. Preserve technical depth; anchor explanations
to actual requirements and data rather than industry names or job titles.

## 1. Latency budget and queues

- The complete event path: reception, decoding, queueing, processing,
  serialization, transmission, and acknowledgement. Separate service time
  from end-to-end latency.
- p50, p95, p99, p99.9, maximum, jitter, and outliers; observation count and
  measurement period. Explain whether the sample supports the claimed percentile.
- Little's law, arrival and service rates, utilization, queue growth near
  saturation, burst loads, and head-of-line blocking.
- Bounded queues, backpressure, admission control, load shedding, deadlines,
  cancellation, and stale events. What happens on overflow?
- Batch size and accumulation time: throughput gains versus waiting;
  separate measurements at low load, saturation, and during short bursts.

## 2. CPU and data representation

- Pipelines, out-of-order execution, IPC, execution ports, dependency chains,
  instruction latency versus throughput, instruction cache, and machine-code size.
- Branch prediction, branchless code and its limitations, SIMD, autovectorization,
  alignment, AoS/SoA, and sequential access versus pointer chasing.
- Algorithmic complexity and unfavorable inputs: searching, sorting, hashing,
  collisions, rehashing, container growth, and working-set size.
- Static/dynamic dispatch, inlining, LTO/PGO, serialization, string parsing,
  formatting, and FFI transition costs.

## 3. Memory and locality

- Stack, heap, lifetimes, and deallocation; allocations per event, pools,
  arenas, preallocation, reuse, and deferred reclamation.
- L1/L2/LLC, cache lines, coherence, true/false sharing, padding, prefetching,
  memory bandwidth versus latency, reference counters as a source of contention.
- NUMA, first touch, thread and memory placement, remote access; TLB, huge pages,
  minor/major page faults, prefaulting, `mlock`, and OS limits.
- Copies at each layer; buffer ownership, scatter/gather, zero-copy, and the cost
  of managing lifetimes. Explain each approach's limits.

## 4. Synchronization and the memory model

- Atomics, happens-before, Acquire/Release/Relaxed/SeqCst, compiler and CPU
  architecture constraints, cache-line bouncing, CAS costs under contention.
- Mutexes, spinlocks, adaptive spinning, parking, `futex`; critical-section
  duration, contention, fairness, starvation, deadlock, and priority inversion.
- SPSC/MPSC/MPMC queues, ring buffers, producer/consumer indices, ownership
  transfer, and publication. Which lock-free/wait-free progress guarantees
  have actually been proved?
- ABA, reclamation, hazard pointers, and epoch-based reclamation where relevant;
  seqlock/RCU, read consistency, and retries. Correctness comes before speed.

## 5. Scheduling and jitter sources

- Affinity, migration, SMT siblings, NUMA, oversubscription, work stealing,
  context switches, and competition with background work.
- Busy polling versus waiting, wakeup latency, timers, async executors,
  event-loop blocking, and service fairness.
- IRQ/softirq, interrupt coalescing, CPU frequency, turbo, C/P-states,
  thermal throttling, cgroup CPU quotas and throttling, memory limits.
- Core isolation and scheduling policies where needed: gains, resource costs,
  and the risk of starving other work. Settings are not universal.

## 6. Networking and I/O

- NIC → DMA → RX queue → IRQ/softirq → kernel stack → socket → application;
  the reverse transmission path, buffers, and copies at each boundary.
- RSS/RPS/XPS, queue and interrupt affinity, UDP/TCP, loss, reordering,
  retransmission, flow/congestion control, socket buffers, Nagle,
  `TCP_NODELAY`, `sendmmsg`/`recvmmsg`, `epoll`, `io_uring`, `SO_BUSY_POLL`.
- Kernel bypass, AF_XDP, DPDK, and RDMA when relevant: hardware requirements,
  buffer ownership, polling, and operational costs.
- Parsing and message formats, event sequence, gap detection, duplicates,
  reconnect recovery, and correctness of reprocessing.
- Page cache, writeback, `fsync`, write batching, journaling, and durable
  acknowledgement; the cost of required durability and recovery guarantees.

## 7. Time and trustworthy measurements

- Monotonic versus wall clocks, resolution and reading overhead, TSC and
  instruction ordering; clock synchronization, NTP/PTP, hardware timestamps.
- Account for clock error in one-way measurements across machines; identify
  where software and hardware timestamps are taken.
- Open-loop versus closed-loop generators, coordinated omission, independence
  of the load generator, histograms, warmup, steady state, and repeatability.
- Release builds, optimizer effects and `black_box`, software versions,
  CPU/NIC, topology, frequencies, data size, workload. Instrumentation overhead.
- `perf stat/record`, flamegraphs, off-CPU profiling, `perf sched`, `perf c2c`,
  `strace`, eBPF, `numastat`, `ethtool`, `ss`, `iostat`, `llvm-mca`,
  disassembly, Criterion, HDR Histogram: choose tools to test a hypothesis.
- Before/after comparisons under identical conditions, variability and confidence;
  experimentally verify the ceiling and check throughput, memory, and correctness
  regressions.

## 8. Overload and recovery

- Timeouts, bounded retries, backoff, retry storms; the effects of reconnects,
  cache warmup, and buffer recreation on tail latency.
- Receiver failure, stuck workers, full disks, memory exhaustion, network loss;
  bounded resource consumption and event ordering.
- Separating the hot path from logging, metrics, audit, and persistence: the
  cost of handing work to background tasks, queue overflow, delivery guarantees.
- Data invariants, concurrency tests, and error handling. Evaluate optimizations
  together with changes to guarantees, not just speed numbers.
