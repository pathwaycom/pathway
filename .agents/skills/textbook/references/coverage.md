# Required perspectives

Work through every axis. If one does not apply, say “not involved in this task”
in one line and continue. Do not invent content to tick a box, but do not silently
skip it either: readers must see that the question was considered.

These axes help identify topics; coverage also includes any other nontrivial
knowledge required to understand and assess the actual change. Apply the general
explanation rule in `SKILL.md` and the ladder in `writing.md` to such topics.
Select depth by their effect on behavior, correctness, failures, or performance;
avoid unrelated background merely to expand a checklist.

---

## 1. Unix and system calls

The reader's Unix knowledge is especially uneven, making this a high-priority
axis. If code enters the kernel, explain it in detail.

**Questions to answer:**

- Which syscalls occur? Name them: `read`, `write`, `recvmsg`, `sendmsg`,
  `epoll_wait`, `futex`, `mmap`, `openat`, `fsync`, `clone`, `io_uring_enter`.
- Where is the **user-space/kernel-space boundary**? Draw it.
- What does entering the kernel mean: privilege change, the `syscall` instruction,
  stack switching, and why even an otherwise empty transition has a cost
  (an illustrative estimate is ~50–100 ns; verify for the actual machine).
- **How hot is the path?** Mention a startup-only call. Analyze a per-message
  call thoroughly, count its frequency, and discuss reducing it.
- Is the call blocking? Does the thread sleep in the kernel, spin, or yield
  to the scheduler while waiting?
- How can readers observe it? `strace -c -f`, `perf trace`, `bpftrace`.
  Give a command they can copy.

**Explain from first principles whenever encountered:**

- **File descriptor**: a small integer indexing a process's open-file table;
  why sockets, files, and pipes are all FDs; what `O_NONBLOCK` means.
- **Sockets**: `socket()`/`bind()`/`listen()`/`accept()`, kernel receive/send
  buffers, `SO_RCVBUF`, `TCP_NODELAY`, Nagle's algorithm, and why a socket
  `write()` does not mean the data has been transmitted.
- **Multiplexing**: `select` → `poll` → `epoll`, level-triggered versus
  edge-triggered, why `epoll` scales differently from `select`; where
  `io_uring` fits and how shared submission/completion queues and batching
  change syscall requirements.
- **Virtual memory**: pages, page tables, MMU, TLB, minor versus major page
  faults (resident page versus disk access), why major faults disrupt hot paths;
  `mmap` versus `read`, `mlock`, huge pages.
- **Processes and threads**: what `clone` creates; shared address space and FD
  table versus per-thread stack, registers, and TLS.
- **Scheduling**: scheduler policy used by the target kernel, CFS where applicable,
  time slices, priorities, `SCHED_FIFO`, `taskset`, affinity, and cache effects
  of migration between cores.
- **Signals, `/proc`, `ulimit`, `cgroups`**, if touched by the code.

### 1.1 Network protocols, when relevant

Use `networking.md` when connection lifecycle, framing, HTTP behavior, security
handshakes, retries, delivery guarantees, or network costs help explain the
session's changes. Follow the actual protocol stack and client implementation.
Do not add a general networking tutorial merely because the project uses a
database or HTTP client. If networking is unrelated, the brief applicability
note for this axis is sufficient; omit the detailed protocol sections.

---

## 2. Concurrency and synchronization

**Examine the internals**, whether in our code, the standard library, tokio,
rayon, or another crate. Read dependency source where needed.

**Questions to answer:**

- How many threads exist, who creates them, when, and for how long?
- How is work distributed **across cores**? Evenly or unevenly? Who decides:
  our code, the runtime, or the kernel scheduler?
- Which synchronization primitives are used, and **how do they work inside**?
  `Mutex` (atomics plus `futex`, syscall-free fast path), `RwLock` (including
  why it can lose to `Mutex` with frequent writes), `Condvar`, channels
  (`mpsc`, `crossbeam`), atomics, lock-free structures, seqlock.
- **Who waits for whom, and on what?** This is the chapter's central question.
  A diagram is required.
- **Memory ordering**, if atomics appear: `Relaxed`/`Acquire`/`Release`/`SeqCst`,
  happens-before, CPU/compiler reordering, and the guarantees of each ordering.
  Explain from first principles; intuition misleads even experienced developers.
- Where can **deadlock**, **livelock**, **lock convoy**, **priority inversion**,
  or **false sharing** occur? Why do they occur or not occur in our code?
- Async: `Future` as a state machine, executors, `.await`, blocking the runtime,
  holding a synchronous `Mutex` across `.await`, and `tokio::sync::Mutex`.
- Pools: worker counts, work stealing, and task-queue organization.

**A diagram is required.** Show a thread timeline with acquire/release points
and waiting arrows. See `diagrams.md`.

---

## 3. Languages: Rust, Python, C++

Assume no prior knowledge of a language feature. Explain every mechanism used,
through a specific fragment of our code rather than in the abstract.

### 3.1 Rust — primary

**Explain as encountered:**

- **Ownership, borrowing, lifetimes**: what the borrow checker checks and why;
  exclusive `&mut`; what a move is and when it is free.
- **`Send` and `Sync`**: their precise meaning, why `Rc` is not `Send`, under
  which bounds `Arc` is `Send`, and automatic derivation.
- **`Arc`, `Rc`, `Box`, `Cow`, `RefCell`, `UnsafeCell`**: data and counter
  placement, atomic operations on clone/drop, and their hot-path cost.
- **Traits and generics**: static dispatch and monomorphization versus
  `dyn Trait` and vtables; code-size and runtime costs.
- **Iterators**: why `.iter().filter().map().sum()` can compile to the same
  code as a manual loop, and when it does not.
- **`unsafe`**: the exact manually maintained invariant, consequences of
  violation, and why the compiler cannot verify it.
- **Async**: `Future`, `Poll`, `Pin`, wakers, `async fn` state machines,
  and why blocking the executor is a problem.
- **Errors**: `Result`, `?`, `panic` versus `unwrap`, a thread panic,
  unwinding versus `abort`.
- **Compilation**: LLVM, inserted/eliminated bounds checks, `#[inline]`, LTO,
  `codegen-units`, and release builds for performance measurements.

For every mechanism, show its location in our code and the alternative behavior.

### 3.2 Python — a full part of the codebase

If the session changes Python, give it a chapter rather than a “just bindings”
footnote.

- **GIL** (Global Interpreter Lock): the lock held while executing bytecode in
  a conventional GIL-enabled interpreter. CPU-bound `threading` limitations,
  releasing the GIL for blocking I/O and in native extensions, NumPy and our
  Rust code. Explain free-threaded mode (PEP 703) and its applicability.
- **`threading` / `multiprocessing` / `asyncio`**: different models and costs.
  Processes provide parallelism at the cost of IPC and serialization.
  `asyncio` provides cooperative multitasking, usually on one event-loop thread:
  explain coroutines, `await`, and why one synchronous call can block the loop.
- **Memory management**: reference counting plus cyclic GC; why
  `sys.getrefcount` may be higher than expected; `del`; returning memory to the OS.
- **Abstraction costs**: calls, attribute access, object creation; why a Python
  list loop can be much slower than its Rust equivalent (illustratively
  ~50–100 times in some workloads), and when that matters.
- **FFI boundary**: PyO3/`pyclass`, Python → Rust transitions, who holds the GIL
  (`allow_threads` where used), data ownership, copying versus buffer pointers,
  the buffer protocol, `memoryview`, and zero-copy NumPy access.
- **Types and contracts**: annotations are not runtime checks; what mypy does
  and does not catch.

Include measurements where useful: `timeit`, `cProfile`, `py-spy` (attaches to a
running process without modifying its code).

### 3.3 C++ — the educational comparison layer

C++ is covered to broaden understanding, even when it is absent from the project.
**Where a mechanism has a C++ counterpart, add a 3–6-line aside.** Avoid a
separate chapter or a retelling of the standard; make a short comparison.

| Rust | C++ | What to emphasize |
|---|---|---|
| ownership, move | RAII, move semantics, `std::move` | moved-from C++ objects are often valid but unspecified; Rust prevents using moved values |
| `Box<T>` | `std::unique_ptr<T>` | broadly similar ownership roles |
| `Arc<T>` | `std::shared_ptr<T>` | atomic counts do not prevent races on the contained data |
| `Mutex<T>` | `std::mutex` + `lock_guard` | Rust encloses data in the lock; C++ leaves the association to the programmer |
| `Send`/`Sync` | no direct counterpart | thread safety is a convention rather than a corresponding C++ type guarantee |
| traits, generics | concepts, templates, virtual functions | monomorphization ≈ template instantiation; `dyn Trait` ≈ vtable |
| `Option<T>` | `std::optional<T>` | dereferencing an empty C++ optional can be UB |
| slices | `std::span` | Rust checks lifetimes |
| `Result` + `?` | exceptions or `std::expected` | exception cost on the hot path |
| atomics | `std::atomic` | shared model foundations derived from C++11; explain once for both |

Also cover where relevant: undefined behavior and additional C++ exposure to it,
initialization, `-O2`/`-O3`, strict aliasing, linkers, and header-only libraries.

---

## 4. Memory

- Stack or heap? Who owns the data and when is it freed?
- Allocations: count, location, hot-path involvement, allocator (system,
  jemalloc, mimalloc), illustrative per-allocation cost (~20–100 ns), and why
  fragmentation and allocator jitter can matter more than average cost.
- Structure layout: size, alignment, padding, `#[repr(C)]`, `std::mem::size_of`.
  Does it fit in a **cache line** (commonly 64 bytes; verify the target)?
- Locality: sequential access versus pointer chasing, `Vec<Struct>` versus
  `Vec<Box<Struct>>`, array of structs versus struct of arrays.
- **False sharing**: threads write different fields in one cache line and hurt
  performance. Detection and fixes: padding, `CachePadded`.
- Copies, ownership transfer, zero-copy, and its actual meaning in the kernel.
- Pools, buffer reuse, arenas, `SmallVec`.

---

## 5. Performance

Start from the task's requirements: throughput, latency, resource consumption.
Explain tradeoffs. Read the detailed mechanisms and measurement checklist in
`performance.md`; explicitly mark inapplicable topics.

- For latency-sensitive work, examine **p99, p99.9, maximum**, not only the mean.
  Assess rare delays against the actual requirements.
- **Name jitter sources**: allocation, container growth/rehash, cache misses,
  page faults, context switches, core migration, scheduling, interrupts,
  NUMA access, `futex` sleep, hot-path logging, syscalls.
- **Identify the hot path**: its functions, allocations, locks, syscalls,
  I/O, formatting; which costs fit the latency budget and what proves that.
- Give measured operation costs with conditions; clearly label estimates
  and do not present them as universal constants.
- Caches: L1/L2/L3, cache lines, prefetching, branch prediction and misprediction
  cost (illustratively ~15–20 cycles), removing unnecessary hot-path branches.

### CPU-level analysis is required for optimizations

“Thirty percent faster” does not explain an optimization. Go down to the
microarchitecture and show **why** the CPU has less work or waiting.
Numbers below illustrate orders of magnitude; check the target architecture.

- **Pipeline**: fetch, decode, execute, writeback, often with many stages
  (for example 15–20). Later instructions enter while earlier ones execute;
  disrupted flow has a cost.
- **Superscalar and out-of-order execution**: multiple instructions per cycle
  (for example 4–6), issued according to operand readiness rather than source
  order. Execution ports have specializations; a loop can saturate one port.
- **Dependency chains**: each operation waiting for the previous result defeats
  parallel execution. Unroll into independent accumulators where appropriate;
  this explains large differences between equal operation counts.
- **Branch misprediction**: pipeline recovery, illustratively ~15–20 lost cycles.
  Explain `cmov`, arithmetic versus `if`, data ordering, `likely`/`unlikely`.
- **SIMD**: one instruction over multiple data elements (SSE/AVX2/AVX-512, NEON).
  Autovectorization and obstacles: aliasing, loop branches, alignment, and
  dependencies between iterations.
- **Instruction latency versus throughput**: illustrative addition ~1 cycle,
  multiplication ~3–5, division ~20–40; independent operations can overlap.
  Inspect expensive hot-path operations rather than assuming their cost.
- **Memory hierarchy as a pipeline extension**: cache access and misses down to
  RAM; illustrative L1 access ~4 cycles and RAM access ~200–300 cycles.
  Explain locality and access patterns a prefetcher can predict.
- **Atomics and barriers in hardware**: x86 `lock` prefixes, MESI Modified state,
  cache-line ownership, and cross-core atomic increments bouncing a cache line.
  Explain the target architecture rather than assuming all CPUs behave alike.

Tools: `perf stat` (IPC, cache and branch misses), `perf record` + flamegraphs,
Godbolt for assembly, `llvm-mca` for pipeline estimates. Show assembly when it
explains the mechanism; it can be the strongest evidence in the chapter.

- Multicore execution: `taskset`, `sched_setaffinity`, `isolcpus`, busy polling
  versus `epoll_wait`; spending a core to reduce latency.
- Networking where relevant: kernel bypass (DPDK, Solarflare/Onload), TCP-stack
  overhead, `TCP_NODELAY`, `SO_BUSY_POLL`, timestamping. Measure stack costs;
  they depend on the workload and machine.
- Measurement method: `perf stat`, `perf record`, flamegraphs, Criterion, run
  counts, warmup, release mode, and `black_box` against optimizer elimination.

### Identify the ceiling — required for every benchmark

When measurements were made, the document **must** answer “what prevents further
improvement?” “We reached 200k messages per second” without a ceiling explains
neither remaining headroom nor whether further work is worthwhile.

**1. Name the limiting factor.** One specific thing, not “the system.”
Typical candidates:

| Limiting factor | What it looks like | Evidence |
|---|---|---|
| ALU / execution ports | IPC near the target's issue capacity, few cache misses | `perf stat`: high IPC, low `cache-misses` |
| dependency chain | IPC around 1 despite simple code | `llvm-mca`; independent accumulators improve performance |
| memory latency | low IPC (illustratively ~0.3–0.8), many LLC misses | `LLC-load-misses`, stalled cycles |
| memory bandwidth | adding threads stops helping | compare with `mbw`/STREAM |
| branch prediction | unordered input in a hot loop | `branch-misses` |
| locking | flat scaling with thread count | `perf lock`, `futex` time, contention profile |
| syscalls | substantial kernel time | `strace -c -f`, system time, `perf trace` |
| networking | RTT or bandwidth ceiling | more concurrency does not increase load |
| disk / `fsync` | p99 spikes align with commits | `iostat`, `biolatency`, disable fsync only in a controlled experiment |
| serial code | Amdahl's law: parallelism does not help | measure the serial fraction |
| Python GIL | one thread executes while others wait | `py-spy dump`, per-thread profiles |
| allocator | jitter, time in `malloc`/`free` | profile; compare jemalloc/mimalloc |

**2. Prove it experimentally.** Do not rely on reasoning alone.

- Scale: double threads/cores and observe whether throughput doubles.
- Remove the suspect in an isolated experiment: stub the network, disable
  `fsync`, replace a contended lock with an uncontended path. Does the ceiling move?
- Counters: IPC, misses, stalled cycles through `perf stat`.
- Synthetic limit: what can the hardware do on this operation alone?

**3. Compare with a theoretical limit.** Memory bandwidth, a 10G link
(1.25 GB/s before protocol overhead), NVMe, minimum cycles per message.
Are we at 5% or 80%? The former suggests architectural headroom; the latter
suggests further gains may be expensive and unnecessary.

**4. Name the next change and whether it is worthwhile.** For example:
“The shared queue is limited by `futex`; next try per-thread queues with work
stealing, with a hypothesized 40% improvement at the cost of more complex
balancing.” Distinguish projections from measurements. A justified decision
not to optimize further is equally useful.

**5. State what remains unexplained.** “The model predicted 3 μs, we measured
8 μs, and the difference is unresolved; examine this section with `perf record`”
is more useful than a plausible guess.

If no optimization was made, identify possible bottlenecks in the changed code
and what you would measure first.

---

## 6. Bug-fix analysis

A bug-fix session needs a full chapter; “fixed it” is insufficient.

- **Symptom**: hang, crash, incorrect data, degradation.
- **Reproduction**: stable or intermittent, and what it depends on.
- **Localization**: search process, rejected hypotheses, and why. Explain dead
  ends; they teach diagnosis.
- **Root cause**: mechanics in threads, memory, or kernel. Not “a race,” but
  “thread A read `state.len()` without locking while thread B called `push`,
  relocating the vector.”
- **Nondeterminism**: required timing, why load changes probability, and why
  a debug build may behave differently. Central for races and hangs.
- **Correctness of the fix**: the invariant now guaranteed and how.
- **Future protection**: test, assertion, type, `loom`, `TSan`, instrumentation.
- **Diagnostics**: copyable commands using `gdb -p`, `perf`, `strace`, `TSan`,
  `loom`, or thread-stack dumps.

Explain relevant classes: data races, deadlock, ABA, unsafe use-after-free,
channel overflow, lost wakeups, synchronous code blocking async runtimes,
and leaked tasks.

---

## 7. External systems and dependencies

See **`references/systems.md`** when integrating with a broker, database, or
service, or relying substantially on a third-party library.

Remember:

- **System**: purpose, architectural diagram, alternatives compared on common
  axes (Pulsar/Kafka/JetStream/MQTT and equivalents for other classes), tuning
  for performance with the mechanics behind each setting.
- **Library**: read source; buffers, copies, threads, coroutines and their purpose,
  syscalls and frequency, authors' assumptions, opportunities for our workload.
- **Source version and hidden behavior**: inspect the resolved version and
  relevant transitive/native layers. Explain undocumented capabilities or
  implementation-dependent behavior with exact source references, guarantees,
  preconditions, measurements where available, and upgrade risks. See `systems.md`.
- **Navigation**: substantial dependency internals get a linked deep dive;
  keep essential conclusions and risks in the main walkthrough so the chapter
  can be skipped without losing the basis for review. See `writing.md`.

---

## 8. Code review

The final chapter is practical and central to the document's usefulness.
The reader opens it beside the diff and reviews without reconstructing context.
**Reviewing through this chapter must be faster than reviewing the bare diff.**

- **Change map**: file → change → reason → inspection point.
- **Affected-code diagram**: classes, structs, traits/interfaces, modules, and
  their relevant relationships; use functions/modules for procedural changes.
  Include unchanged neighbors needed to explain impact. Distinguish change
  statuses and accompany the SVG with a source-location table. See Template 5
  in `diagrams.md`.
- **Clickable source references**: commit-pinned line links for snippets, the
  change map, diagram source locations, and risks. Record base/reviewed SHAs;
  link each before/after version to its own revision. Clearly label working-tree
  or unpublished code and provide local/in-document fallbacks. See
  “Source links and revisions” in `writing.md`.
- **Reading order**: where to start to understand the logic.
- **Risks with snippets**: each risky location gets a 5–25-line listing with
  path/line and an inspection point. Not “the pool has a subtlety,” but the
  actual snippet and the question. See “Code in the text” in `writing.md`.
- **Before/after** where behavior changed, not just formatting.
- **Execution walkthrough** for behavioral changes: one concrete input and
  initial state followed through the relevant internal components, calls, state
  changes, and outputs before and after. Explain each component's role and link
  steps to the diagram and source; distinguish observed behavior from inferred
  execution. See “End-to-end execution walkthrough” in `writing.md`.
- **Manual verification**: test commands, scenarios, post-deployment metrics.
- **Out of scope**: TODOs, known limitations, deferred decisions.
- **Reviewer questions**: 5–10 specific questions such as “Is a five-second
  timeout appropriate here?”
