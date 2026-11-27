# How to write

## Reader model

Picture a practicing developer with uneven CS knowledge. They may know async
Rust thoroughly but not remember what a file descriptor is. They may understand
CPU caches without knowing what `epoll` does. You cannot predict the gaps, so
explain everything.

They read the document **once, outside the session's context**, perhaps on a
phone, a week after the code was written. Everything needed for understanding
must be in the document.

Write in the language selected from the user's request according to `SKILL.md`.
The English examples here illustrate structure; translate explanatory prose,
headings, and comments you introduce into that language. Preserve actual source
code and identifiers when quoting repository snippets.

## The rule of extra explanation

Always resolve “explain or skip?” in favor of explaining.

The cost of error is asymmetric: the reader can skip an extra paragraph in
three seconds, but a missing explanation can make a whole chapter useless.
They stumble over a term, lose the thread, and skim the rest.

Explicitly mark skippable sections so that detail does not become irritating:

> **If you know how `Arc<T>` works, skip to the next heading.**

## Reading paths and skippable chapters

A long textbook must support readers with limited time as well as readers who
want the full explanation. Keep the depth and make navigation explicit.

- Near the contents, provide linked reading paths: a quick review path through
  the summary, change map, key behavior, risks, and verification; focused paths
  for particular subsystems; and the complete foundations-to-internals path.
  Link actual headings or explicit anchors in the generated HTML.
- Start substantial chapters with their purpose, a short summary, prerequisites
  linked to earlier explanations, and who benefits from reading them. State
  when they can be skipped and where to continue. Use the document's language.
- Keep detailed foundations, external-system explanations, and dependency
  internals in clearly named chapters or sections. The main walkthrough should
  retain a concise explanation of the facts needed to follow the change, with
  links to the full analysis.
- Do not make a deep-dive chapter a hidden prerequisite: before a fact is used,
  briefly state what the reader needs and link to its explanation. Keep critical
  assumptions, correctness conditions, version constraints, and risks visible
  in the main walkthrough and review checklist even when detailed proof lives
  in an optional reading path.
- Give a chapter's key takeaways and links back to the affected code or next
  relevant section. Prefer descriptive links to instructions such as “see above.”
  Make printed navigation useful with chapter/section titles as well as links.
- Verify that a reader following only the quick review path can identify what
  changed, why, the essential mechanism, important limitations, and how it was
  checked. Readers choosing the deep dives should still find the full reasoning
  and source-level detail rather than a shortened replacement.

## The explanation ladder

Explain every nontrivial point in this order:

1. **What it is**: one sentence, in plain language.
2. **Why it is needed**: the problem it solves and what would happen without it.
3. **How it works inside**: mechanics, data structures, CPU/kernel behavior.
4. **Where it appears in our code**: exact file and line.
5. **What breaks**: how incorrect use manifests itself.

Item 3 is the reason for writing. Item 5 is what the reader remembers.

Apply this ladder whenever a nontrivial concept is necessary to understand the
change, regardless of subject or whether a checklist names it. Include relevant
unchanged mechanisms and dependencies. Explain guarantees, assumptions, and
tradeoffs that affect the solution; follow prerequisite concepts far enough
that the reasoning stands on its own. Reuse an earlier explanation by linking
to it rather than repeating it.

Use the actual change to decide depth. Ask whether a reader unfamiliar with
the mechanism could assess why the solution works, its failure cases, and its
tradeoffs. Fill gaps that prevent that assessment. Omit tangents that do not
help it; the appearance of a library name alone is insufficient reason for a
general tutorial.

Example for `Mutex`:

> A mutex (short for *mutual exclusion*) allows only one thread at a time into
> a section of code. (1)
>
> Without it, two threads can modify the same variable concurrently, and the
> result depends on which gets there first. (2)
>
> Inside Linux's `std::sync::Mutex` are atomic state and the `futex` syscall.
> When uncontended, acquisition uses an atomic operation in user space without
> entering the kernel. When a thread finds the lock held, it can call
> `futex(FUTEX_WAIT)` and sleep; the kernel takes it off the CPU. Unlocking can
> call `futex(FUTEX_WAKE)` to wake a waiter. (3)
>
> In our example, `engine/src/state.rs:142` protects the subscription table. (4)
>
> Hold it on the hot path across a network operation and other threads queue
> behind it, sending p99 latency sharply upward. That is the bug discussed in
> chapter 6. (5)

## Explanation techniques

**Code first, theory second.** Anchor every theoretical aside to a repository
line: “Here we pass `buf` to a function and cannot use it afterward; why?” rather
than “Let us discuss Rust ownership.”

**Expand abbreviations.** Explain `fd`, `MMU`, `TLB`, `ABA`, `NUMA`, and `p99`
fully at first appearance.

**Numbers instead of adjectives.** Say “~100 ns versus ~2 ns, a factor of 50,”
not “slow.” Say “on every message, ~200k times per second,” not “often.” Orders
of magnitude are more useful than adjectives. Keep a rough scale in mind:
register ~0.3 ns, L1 ~1 ns, L2 ~4 ns, L3 ~15 ns, RAM ~80 ns, syscall entry/exit
~50–100 ns, context switch ~1–3 μs, SSD ~50 μs, in-rack networking ~100 μs.
These are illustrative, machine-dependent estimates, not universal constants.
If you did not measure them, clearly label them as estimates.

**Use analogies, then return to the mechanism.** After an analogy, explain what
actually exists: “…but in this case the kernel maintains a linked list of
`task_struct` objects.”

**Explain every code block.** The “Code in the text” section below matters as
much as all the other rules combined.

**Explain what did not work.** Rejected alternatives provide half the learning:
“We considered `RwLock`, but it loses for this read/write ratio; here is why.”

## Code in the text

The textbook must **speed up code review**, not replace it. A reader opens the
diff to find what to inspect, why decisions were made, and where risks lie.
Code in the text answers those questions only if presented properly.

**Rule: no snippet without an explanation.** Unexplained code is just a slower
`git diff`. The surrounding explanation supplies the value.

### Three forms

**1. Before/after**: the primary form for explaining changes.

Show two short blocks or one diff, then explain the substantive change, why the
previous version was inadequate, and what is now guaranteed.

> ```rust
> // before — engine/src/state.rs:142
> let table = self.subs.lock().unwrap();
> let result = self.client.fetch(&table.topic).await;   // lock held across await
> ```
> ```rust
> // after
> let topic = { self.subs.lock().unwrap().topic.clone() };  // lock released here
> let result = self.client.fetch(&topic).await;
> ```
> The synchronous `Mutex` is acquired before `.await`. While the task waits for
> the network, the lock remains held. The executor runs another task, which
> hits the same lock and blocks the whole thread instead of suspending. Four
> such tasks can stop a four-worker runtime. Cloning `topic` costs one
> allocation; the benefit is avoiding runtime blockage. This example explains
> the hang in chapter 8.

**2. Annotated listing**: for code where every line matters.

Put numbered markers directly in code (`// (1)`, `// (2)`) and a numbered
explanation below it. Readers can match explanations to lines without scrolling.

**3. Minimal example**: to explain a mechanism rather than our code.

Use ten lines that isolate the point: what happens when `Arc` is cloned, what
the borrow checker checks, or how `Relaxed` differs from `Acquire`. It must
compile and be verifiable; the reader may want to run it.

### End-to-end execution walkthrough

For behavioral changes, trace a concrete, representative scenario through the
affected subsystem before and after the change. A diff and a dependency diagram
need this explanation of how the parts cooperate during execution.

- Set out the input, relevant initial state, trigger, expected output, and
  correctness invariant. Keep the example small enough to follow manually.
- Walk from the caller or public entry point through the relevant internal
  classes, structs, traits, modules, and functions to the observable result.
  Explain what each component does, why it exists, and how it relates to the
  surrounding subsystem, including important unchanged components.
- At each meaningful step, show the data passed, state read or modified,
  intermediate result, and relevant primitive such as an index, queue, lock,
  transaction, or notification. Explain its purpose and guarantees where needed.
- Connect steps to the affected-code diagram and clickable source-line links.
  Add an SVG sequence or data-flow diagram when execution order or state flow
  would otherwise be hard to follow.
- Use the same scenario for before and after. Identify exactly where execution
  diverges and how the result, failure behavior, or guarantees differ. For a
  newly supported operation, describe the previous limitation rather than
  inventing an execution path that did not exist.
- Include a boundary or failure scenario where it is needed to understand
  correctness. State whether the trace comes from an observed run, a test,
  or source-level reasoning; do not present an inferred trace as measured evidence.

For example, a join change can trace one input update through key extraction,
lookups, retained state, matching, and emitted updates, where those mechanisms
actually occur in the implementation. Use real component names and source
locations. Make clear which parts change and which supply unchanged behavior.

Apply this requirement when behavior changes; unrelated documentation or
formatting work does not need an invented execution scenario.

### Format

- **Short snippets**: 5–25 lines. Split long functions into pieces and mark
  omissions explicitly: `// ... validation omitted`.
- **Always include path and line** in the first comment: `// engine/src/state.rs:142`.
- **Also provide a clickable source link outside the code block**, using the
  commit and line numbers for the exact version shown. A path in a comment alone
  is insufficient. See “Source links and revisions” below.
- **Keep only the substance.** Omit imports, error handling, and logging unless
  they are the topic.
- **Always label the code language**: `rust`, `python`, `cpp`, `bash`.
- **Assembly and tool output are also code.** Show `perf stat` output, three
  lines of assembly, or `strace -c` output in the syscall chapter; do not just
  paraphrase it.

### Source links and revisions

- Record the repository and full base/reviewed commit SHAs near the file map.
  Pin links to full commit SHAs, never moving branch names such as `main` or
  `feature/x`. For GitHub, use
  `https://github.com/<owner>/<repo>/blob/<full-sha>/<path>#L142-L156`.
  Adapt the permalink and line-anchor syntax to the actual hosting service.
- Apply this to snippets, entries in the change map, risk locations, and the
  source-location table accompanying the affected-code diagram. Keep the visible
  path and line numbers useful in the printed PDF as well.
- A before snippet links to its original revision and original path/lines;
  an after snippet links to the reviewed revision and its path/lines. Account
  for renames and deletions. Check targets against the actual revision contents;
  never reuse current line numbers for a previous version without checking.
- Use a clean repository web URL without credentials. Check that linked commits
  are available on the remote when possible. Private repositories may require
  access; do not change repository visibility or publish code to make links work.
- For uncommitted code, do not present a HEAD permalink as the changed source.
  Use an actual local file/line link supported by the environment, label it
  “working tree,” and explain that it requires the same checkout. For a portable
  HTML/PDF, also include a linked in-document source excerpt with a stable anchor
  and the file path/line numbers. For example, a Markdown heading with
  `{#source-pool-working-tree}` can be linked as `#source-pool-working-tree`.
- For a commit that is only local, report its SHA and label it unpublished;
  provide the same local/in-document fallback rather than claiming a remote
  permalink is usable. Do not commit, push, or upload merely to obtain a link
  without authorization for that action.

### Faster review

Start with the affected-code relationship diagram and its source-location table.
Then follow the reading order, before/after snippets, and risk-specific checks.

In the code-review chapter, include a snippet and one inspection point for each
risk. Readers should be able to review from that chapter while comparing it
with the diff, without reconstructing the context. An illustrative inspection
point (accompany the actual snippet with its source link):

> `engine/src/pool.rs:88`: queue capacity is the constant 1024.
> **Inspect:** what happens on overflow. The sender currently blocks; this is
> intentional backpressure, not lost messages. Check that the caller can handle
> blocking.

## Length

Aim for 20–40 PDF pages for an average task. Fewer than ten almost certainly
means insufficient explanation. The only upper limit is that every paragraph
must add something.

Uneven chapter lengths are normal: “Foundations” and “Under the hood” are usually
the longest; “TL;DR” is half a page.

## Formatting

- `#` is a chapter (starting a new PDF page), `##` a section, `###` a subsection.
- Paragraphs of 3–6 lines. Walls of text are hard to read.
- Lists for enumeration; prose for reasoning. Do not turn explanations into
  bullets: connected reasoning falls apart in a list.
- Tables for comparisons: before/after, options A/B/C, operation costs.
- Blockquotes (`>`) for important notes and warnings.
- Fenced code blocks with a language tag: `rust`, `bash`, `c`.
- Code references as `path/file.rs:142` so readers can locate them.

## Antipatterns

| Poor approach | Why it fails | Better approach |
|---|---|---|
| “As everyone knows, a mutex blocks a thread” | the reader may not know | explain from first principles |
| “We refactored the module” | conveys no information | say exactly what, why, and what improved |
| “This is a standard pattern” | teaches nothing | name it, explain its origins and purpose |
| “Optimized the hot path” | no mechanism | identify the bottleneck, measurement, and result |
| “Race conditions are possible” | alarming but unexplained | specify threads, shared data, and interleavings |
| Line-by-line paraphrase of the diff | already available in Git | explain decisions and alternatives |
| Theory detached from code | a general Rust textbook, not a session textbook | start with a repository line |
