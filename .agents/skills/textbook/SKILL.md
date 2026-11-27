---
name: textbook
description: >-
  Write an exhaustive, first-principles technical textbook about the work done in
  the current coding-agent session, and build self-contained HTML and a downloadable PDF
  locally. Use when the user explicitly requests a textbook, session documentation,
  an in-depth explanation of the changes, a PDF explaining the task, code-review
  material, or an explanation of how the changes work under the hood. Equivalent
  requests in any language also apply. The reader should learn how the system
  works, not just what changed. Do not activate without an explicit request.
---

# Textbook — a textbook about the session

## Purpose and audience

After a working session (feature, connector, optimization, bug fix), write a
document that lets the reader:

1. Understand **every** change and review the code quickly.
2. Understand **the mechanics under the hood**: what happens in Rust, in threads,
   and in the kernel.
3. **Grow as an engineer**: explain concepts from first principles rather than
   merely naming them.

The reader is a practicing developer with **uneven knowledge of computer science**.
They are capable but have gaps: some topics they know thoroughly, others they
have never learned. They can skip what they know. They **cannot** reconstruct
what you left out.

The guiding rule follows: **too much explanation is better than too little.**
When in doubt about explaining a concept, explain it.

The goal: the reader understands operating systems, networks, algorithms, memory
management, and system calls; can use Rust, Python, and C++; and can measure
performance and explain engineering decisions through concrete code.

The second, equally important practical goal: **reviewing with the textbook must
be faster than reviewing without it.** Include before/after snippets for every
meaningful change, annotated listings for every risk, and a file path and line
number for every snippet. Read the “Code in the text” section in
`references/writing.md`; do not skip it.

## Explain the knowledge the change depends on

Whenever understanding a change depends on a nontrivial mechanism, protocol,
algorithm, abstraction, data format, or system property, explain it sufficiently
for the reader to assess the decision and its correctness independently.
Apply this to every subject, including ones absent from the topic checklists,
and to relevant unchanged code and dependencies.

At first meaningful use, explain what it is, why it is needed, how it works,
where it appears in this code, its guarantees and assumptions, what can go
wrong, and how those details affect the change. Include alternatives,
measurements, or diagrams where they help evaluate the decision. Explain any
prerequisites needed to follow that reasoning, or link to their explanation
earlier in the textbook.

Choose depth by relevance: could a reader who does not know this topic explain
why the solution works, when it fails, and what tradeoff it makes? If not, add
the missing explanation. Stop when further detail no longer helps answer those
questions about this task. Merely mentioning a technology does not require a
chapter about it. The checklists help discover relevant topics; they do not
limit which topics deserve explanation or require unrelated material.

## Document language

The instructions in this skill are in English. **That does not determine the
language of the generated textbook.**

- Write the textbook in the language of the user's request that invokes it.
  An English request produces English; a French request produces French; a Polish
  request produces Polish; the same rule applies to any other language.
- An explicit request for a particular output language takes precedence.
- For a short invocation such as `$textbook` or `/textbook`, infer the language
  from the user's surrounding natural-language messages in the current session.
  Ignore the language of code, logs, quoted material, repository files, and these
  instructions when making that choice.
- If a mixed-language request has a clear main language, use it. Ask only if
  neither the request nor the session provides a reasonable language choice.
  There is no fixed Russian or English default for generated textbooks.
- Use the selected language consistently for prose, chapter headings, glossary
  explanations, diagram labels and captions, title, contents heading, download
  button, and download hint. Translate the example headings and labels below.
- Keep identifiers, APIs, commands, file paths, and established technical terms
  intact where appropriate. Expand acronyms and explain terms at first use in
  the document's language. Avoid misleading literal translations of terms such
  as `page fault` and `borrow checker`.
- Pass the selected language tag and localized interface labels to `build.py`
  as described in Step 5. Check font and PDF-renderer support for its script.

## Programming languages

The codebase is **Rust** (the engine) and **Python** (bindings, API, scripts).
Treat both as primary: if the session changes Python code, give it a full chapter,
not a paragraph saying “these are just bindings.”

**C++ is a required educational layer.** The reader develops understanding across
three languages, so wherever a mechanism has a C++ counterpart, include a short,
substantive comparison. `Arc<T>` → `std::shared_ptr<T>` (and the exact difference
in guarantees); `Mutex<T>` → `std::mutex` + `std::lock_guard` (why Rust puts data
inside the lock while C++ puts it beside the lock); `Vec` → `std::vector`;
traits → concepts and virtual tables; ownership → RAII and move semantics;
`Send`/`Sync` → “in C++, the programmer is responsible for this.” Use a 3–6-line
aside, not a separate chapter. The reader should recognize the same pattern in
C++ and understand where C++ will not protect them.

Make similar comparisons with Python where relevant: the GIL versus actual
parallelism in Rust, `threading` versus `multiprocessing`, `asyncio` versus
`tokio`, CPython reference counting versus `Arc`, and FFI call overhead.

## Discovery and invocation

This repository keeps the skill in `.agents/skills/textbook/` for Codex.
`.claude/skills/textbook` is a relative symlink to the same directory for
Claude Code. Maintain the canonical directory so both tools use the same files.
Invoke it as `$textbook` in Codex or `/textbook` in Claude Code. An explicit
natural-language request matching the description also applies. Resolve bundled
`references/` and `scripts/` paths relative to this skill's directory, not the
current working directory.

## When to run

**Only on an explicit request.** Do not offer it, run it in the background, or
write a textbook instead of answering. One session = one task = one document,
unless the user says otherwise.

---

## Step 1. Gather the material

Do this before writing a single line. You are writing about **this session**,
not Rust in general.

```bash
git status
git log --oneline "$(git merge-base HEAD origin/main)"..HEAD   # or origin/master
git diff "$(git merge-base HEAD origin/main)"...HEAD --stat
git diff "$(git merge-base HEAD origin/main)"...HEAD
```

Collect:

- **Complete diffs**: every file and every change.
- **The previous state**: read the original versions of changed sections to
  explain why they worked that way before and why they work differently now.
- **Surrounding code**: callers, implemented traits, and data ownership. Go beyond
  the diff: the reader needs to understand the system, not just the patch.
- **Session history**: what you tried, what failed, and which hypotheses were
  rejected. Dead ends explain why this particular solution was chosen.
- **Bugs**: symptom, reproduction, localization, cause, and fix.
- **Numbers**: benchmarks, profiles, `perf`, and before/after measurements, if any.
- **Source revisions**: record the full base and reviewed commit SHAs and the
  repository's clean web URL. Use the actual target branch to select the base;
  the commands above illustrate the common `origin/main` case. Identify uncommitted
  changes and whether the referenced commits are available on the remote.
  Never include credentials from a remote URL in the document.

If the session was long and some context is no longer available, state that in
the document. Do not invent it. A “What remains uncovered” section is more honest
than a plausible reconstruction.

## Step 2. Outline the contents

Draft a structure, print it in the terminal as one list, and **continue writing
immediately**. Do not wait for confirmation unless the user requested it.

Use this skeleton, adapting it to the task while keeping the order:

```text
0. TL;DR                  — what changed, in 10 lines, plus a file map
1. Task and context       — why this matters and what was wrong
2. Foundations            — concepts needed to understand the rest
3. External system        — if integrating: purpose, architecture, comparison
                            with alternatives, and performance tuning
4. Walkthrough of changes — by module, with code and explanations of why
5. Dependencies           — third-party libraries under the hood
6. Under the hood         — concurrency, Unix/syscalls, memory
7. Performance            — throughput, latency, CPU microarchitecture
8. Bugs                   — if any: nature, cause, and nondeterminism
9. Code review            — what to inspect, risks, manual verification
10. Glossary              — every term used in the document
11. Further exploration   — reading and unresolved questions
```

Chapters 3 and 5 are not optional decoration. If the session is about a Pulsar
connector, an eight-page chapter explaining Pulsar and comparing it with Kafka,
MQTT, and JetStream is required: without it, the reader cannot understand the
system the code joins.

Chapter 2 (“Foundations”) is not a formality. It is what turns a report into a
textbook. Label skippable subsections honestly: “If you know how `Arc` works,
skip this section.”

For a long textbook, add linked reading paths for quick review, focused subsystem
reading, and a complete deep dive. Start substantial chapters with a summary,
prerequisites, and guidance on whether to skip them and where to continue.
Preserve essential assumptions and risks in the main walkthrough even when a
dependency or foundations chapter is skipped. See “Reading paths and skippable
chapters” in `references/writing.md`.

## Step 3. Write

Read `references/writing.md` before the first chapter. It defines the depth of
explanation, length, and common failures.

For behavioral changes, include a concrete end-to-end execution walkthrough
before and after the change. Follow the relevant calls, internal components,
state transitions, and outputs; explain each component's role. See
“End-to-end execution walkthrough” in `references/writing.md`.

Read and explicitly work through `references/coverage.md`: Unix and system
calls, concurrency, languages (Rust/Python/C++), memory, performance down to CPU
microarchitecture, and bug-fix analysis. If an axis does not apply, say so in one
line rather than inventing content.

For performance analysis, also read `references/performance.md`. It lists
specific topics in latency, queues, memory, CPU, synchronization, networking,
measurement, and overload behavior. Let the session's code determine the depth.

If the task involves an external system (broker, database, service) or relies
substantially on a third-party library, also read `references/systems.md`. It
contains separate procedures for explaining and comparing systems, and for
examining libraries down to buffers, threads, coroutines, and system calls.
Inspect the resolved version and relevant native/transitive layers. When using
undocumented capabilities or implementation details, explain the exact source
path, mechanism, support guarantees, and upgrade risks. Give a substantial
dependency analysis its own navigable section, with key conclusions retained
in the main walkthrough.

Read `references/networking.md` when transport or application-protocol behavior
is needed to understand a change, its correctness, a failure, or its performance.
It covers TCP, HTTP, DNS/TLS, connection management, and database wire protocols.
Select only the relevant mechanisms and anchor them to the session's code.
The mere presence of a network dependency does not require a networking chapter.

Write chapters in separate files rather than keeping everything in one buffer:

```text
docs/textbook/<YYYY-MM-DD>-<slug>/
  01-tldr.md
  02-context.md
  ...
  assets/*.svg
```

## Step 4. Diagrams

Draw diagrams **by hand in SVG**. No Mermaid, ASCII art, or raster images.

Read `references/diagrams.md` for the grid, palette, fonts, and templates for
thread timelines, lock diagrams, memory layouts, packet paths through the
kernel, and state machines.

Required whenever the topic appears:

- **Changed code** → an SVG map of affected classes, structs, traits/interfaces,
  and modules, with their relationships and relevant unchanged callers/dependencies.
  For procedural code, use modules and functions. Distinguish added, modified,
  removed, and unchanged elements. See Template 5 in `references/diagrams.md`.
- **Any concurrency** → thread timeline with synchronization points.
- **Any lock** → who waits for whom, and on what.
- **Network or disk I/O** → data path with explicit user-space/kernel boundaries.
- **Protocol behavior analyzed under `references/networking.md`** → a sequence
  diagram showing the relevant exchange, waits, acknowledgements, and failures.
- **Nontrivial memory structure** → layout with bytes and cache lines.

A diagram without a caption is useless. Every diagram needs meaningful `alt`
text (used as the caption) and an explanatory paragraph nearby. Use the
textbook's language for both, including visible text inside the SVG.

## Step 5. Build and deliver

The following example is for an English-language request. For another language,
translate the title, metadata, and all three interface labels, and set `--lang`
to the corresponding BCP 47 language tag, such as `fr`, `pl`, or `ru`.

```bash
python3 <skill>/scripts/build.py --check
python3 <skill>/scripts/build.py \
  --src docs/textbook/<YYYY-MM-DD>-<slug> \
  --lang en \
  --title "Connector X: walkthrough of the changes" \
  --meta "branch feature/x · 3 commits · 2026-08-13" \
  --contents-label "Contents" \
  --download-label "Download PDF" \
  --download-hint "Read the complete textbook here or download the PDF"
```

The script joins chapters, inlines SVG, builds `textbook.html`, renders
`textbook.pdf` (WeasyPrint → Chromium → wkhtmltopdf → Pandoc+XeLaTeX, whichever
works), and creates `build/artifact.html`: the same page with a download button
and the PDF embedded as base64. For right-to-left text, also pass `--direction rtl`
and verify the selected renderer and fonts support the script.

Read `references/delivery.md` and give the user links to the actual local files
`build/artifact.html` and `build/textbook.pdf` with absolute paths.
`artifact.html` opens in a browser and includes the full textbook and a download
button that works without a server. `textbook.html` is the lighter version
without the embedded PDF.

Do not assume a publishing tool or permanent URL is available. If the user asks
for a public/private web link, use an available tool and their chosen hosting
service. If no destination is specified, finish the local build first, then
ask where to publish. Do not describe a local link as web publication.
Do not commit, push, or publish the textbook merely to deliver it without the
corresponding authorization. Update the textbook by editing source chapters
and rebuilding.

**Never pass base64 through the model.** The script embeds the PDF. If PDF
rendering fails, report the problem; the HTML remains available for browser
printing.

## Delivery checklist

- [ ] Document language follows the user's request, including diagrams and UI labels
- [ ] Every axis in `coverage.md` reviewed; irrelevant axes explicitly marked
- [ ] Every term explained at first use
- [ ] Nontrivial knowledge needed to assess the change explained, including
      relevant prerequisites and topics outside the checklists
- [ ] No “as everyone knows,” “obviously,” or “trivially”
- [ ] Every hot-path syscall named and its frequency estimated
- [ ] External system: architecture, comparison, and tuning included
- [ ] Key dependencies examined: buffers, threads, coroutines, syscalls
- [ ] Dependency analysis uses the resolved versions; undocumented behavior,
      implementation assumptions, evidence, and upgrade risks are identified
- [ ] Long documents provide linked reading paths and chapter summaries;
      essential reasoning and risks remain available when deep dives are skipped
- [ ] Where protocol behavior matters, relevant networking mechanisms and their
      correctness/performance implications explained with code and a sequence diagram
- [ ] Optimizations explained down to the CPU, beyond “it became faster”
- [ ] Measurements: ceiling identified, experimentally verified, compared with limits
- [ ] C++ and Python comparisons included where appropriate
- [ ] SVG diagram for every applicable topic in Step 4
- [ ] Affected-code relationship map included, with a legend and linked source locations
- [ ] Every snippet explained with path, line, and reasoning
- [ ] Source references are clickable commit-pinned line links; before/after
      revisions are correct and local/unpublished changes are clearly identified
- [ ] Key changes shown as before/after code, not paraphrased
- [ ] Behavioral changes illustrated by a concrete before/after execution trace,
      connected to the affected-code diagram, internal components, and source links
- [ ] Code-review chapter includes a listing and inspection point for every risk
- [ ] Bugs include causes, not just fixes
- [ ] Glossary covers every term
- [ ] `build.py` succeeded; HTML/PDF checked; embedded PDF matches the separate file
- [ ] Actual HTML/PDF links delivered; web link only after successful publication

## What to avoid

- **Paraphrasing the diff.** “Added a `retries: u32` field” is visible in `git diff`.
  Explain why `u32`, why here, and what happens on overflow.
- **Saving words.** A short textbook is a failed textbook. Thirty pages is normal.
- **Assuming knowledge.** Never, even for `Vec` or file descriptors.
- **Writing a generic tutorial.** Anchor every explanation to the session's code:
  first a repository line, then the theory around it.
- **Hiding uncertainty.** If you do not know the syscall count, write “Not checked;
  here is how to check: `strace -c -f ./bin`.”
