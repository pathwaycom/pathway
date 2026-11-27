# SVG diagrams

Draw diagrams by hand in SVG. No Mermaid, ASCII, or raster images: SVG provides
layout control, prints as vectors in PDF, and stays small.

## Rules

- One file per diagram: `assets/<short-name>.svg`.
- Insert into Markdown as `![What the diagram shows](assets/thread-timeline.svg)`.
  The `alt` text becomes the caption; make it meaningful.
- A `viewBox` is required; width ≤ 900 (the page width). Use the height needed.
- **No external resources**: fonts, images, or `<script>`. These make the artifact
  dependent on its host environment and may fail under a host's CSP or PDF renderer.
- Use system fonts: `font-family="sans-serif"` or `monospace`. Verify glyph
  coverage for the selected document language; install suitable fonts if needed.
- Text size ≥ 11, otherwise it is unreadable in PDF.
- Do not use `<title>`/`<desc>` instead of visible labels: they do not print.
- Explain every diagram in a nearby paragraph. A picture does not explain itself.
- Translate all visible labels, captions, and alt text into the document's
  language. The English templates below are examples, not a fixed output language.

## Palette

Match the document's styles:

| Role | Color |
|---|---|
| Outline, accent | `#1a4d8f` |
| Main fill | `#eef4fb` |
| Second thread / attention | `#a86400` / `#fff6e6` |
| Error, blocking | `#a3242c` / `#fbecec` |
| Success, release | `#1f7a4d` / `#e9f5ee` |
| Neutral, OS kernel | `#5b6472` / `#f0f2f5` |
| Text | `#16181d` |

Color is a redundant cue, not the only one: printing may be monochrome. Also
use dashes (`stroke-dasharray`), shapes, and labels.

## Template 1. Thread timeline

The most common and useful diagram. Time runs left to right; one row per thread.
A bar means execution, dashes mean waiting, a vertical line means synchronization.

```svg
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 860 240" width="860" height="240">
  <text x="0" y="16" font-family="sans-serif" font-size="13" font-weight="bold">Mutex acquisition: thread B waits on futex</text>

  <!-- time axis -->
  <line x1="110" y1="210" x2="840" y2="210" stroke="#5b6472" stroke-width="1"/>
  <text x="840" y="228" font-family="sans-serif" font-size="11" fill="#5b6472" text-anchor="end">time →</text>

  <!-- lane A -->
  <text x="0" y="62" font-family="sans-serif" font-size="12">thread A</text>
  <rect x="110" y="46" width="240" height="22" fill="#eef4fb" stroke="#1a4d8f"/>
  <text x="230" y="62" font-family="sans-serif" font-size="11" text-anchor="middle">holds lock</text>

  <!-- lane B -->
  <text x="0" y="122" font-family="sans-serif" font-size="12">thread B</text>
  <rect x="150" y="106" width="200" height="22" fill="#fbecec" stroke="#a3242c" stroke-dasharray="4 3"/>
  <text x="250" y="122" font-family="sans-serif" font-size="11" text-anchor="middle">asleep: futex(WAIT)</text>
  <rect x="350" y="106" width="180" height="22" fill="#fff6e6" stroke="#a86400"/>
  <text x="440" y="122" font-family="sans-serif" font-size="11" text-anchor="middle">holds lock</text>

  <!-- synchronization event -->
  <line x1="350" y1="36" x2="350" y2="200" stroke="#1f7a4d" stroke-width="1.5" stroke-dasharray="3 3"/>
  <text x="356" y="196" font-family="sans-serif" font-size="11" fill="#1f7a4d">unlock → futex(WAKE)</text>
</svg>
```

Label **exactly what** happens at synchronization, not just “sync.”

## Template 2. User-space/kernel boundary

For anything entering the kernel: networking, disk, sleep, memory.

```svg
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 300" width="800" height="300">
  <rect x="20" y="20" width="760" height="110" fill="#eef4fb" stroke="#1a4d8f"/>
  <text x="32" y="40" font-family="sans-serif" font-size="12" font-weight="bold">user space</text>

  <line x1="20" y1="150" x2="780" y2="150" stroke="#a3242c" stroke-width="2" stroke-dasharray="8 4"/>
  <text x="400" y="145" font-family="sans-serif" font-size="11" fill="#a3242c" text-anchor="middle">privilege boundary · ~50–100 ns per transition (illustrative)</text>

  <rect x="20" y="170" width="760" height="110" fill="#f0f2f5" stroke="#5b6472"/>
  <text x="32" y="190" font-family="sans-serif" font-size="12" font-weight="bold">kernel space</text>

  <!-- syscall arrow -->
  <line x1="200" y1="110" x2="200" y2="195" stroke="#1a4d8f" stroke-width="2" marker-end="url(#a)"/>
  <text x="210" y="160" font-family="monospace" font-size="11">sendmsg()</text>
  <defs>
    <marker id="a" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
      <path d="M0,0 L0,6 L7,3 z" fill="#1a4d8f"/>
    </marker>
  </defs>
</svg>
```

Define each `marker` inside the same `<svg>`; files do not share definitions.
Keep `id` unique across the whole document: all SVGs are inlined into one page,
so duplicate IDs conflict. Use a filename prefix, for example `id="sock-arrow"`.

## Template 3. Memory layout

For structures, buffers, and cache lines. Always include a byte scale.

```svg
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 160" width="800" height="160">
  <text x="0" y="16" font-family="sans-serif" font-size="13" font-weight="bold">Struct layout: fields and padding on a byte scale</text>
  <rect x="0" y="40" width="360" height="40" fill="#eef4fb" stroke="#1a4d8f"/>
  <text x="180" y="65" font-family="monospace" font-size="12" text-anchor="middle">id: u64 · price: f64</text>
  <rect x="360" y="40" width="120" height="40" fill="#fff6e6" stroke="#a86400"/>
  <text x="420" y="65" font-family="monospace" font-size="12" text-anchor="middle">padding</text>
  <line x1="0" y1="95" x2="512" y2="95" stroke="#5b6472"/>
  <text x="0" y="112" font-family="monospace" font-size="10" fill="#5b6472">0</text>
  <text x="256" y="112" font-family="monospace" font-size="10" fill="#5b6472">32</text>
  <text x="500" y="112" font-family="monospace" font-size="10" fill="#5b6472">64 (cache line)</text>
</svg>
```

Use the actual structure's measured field offsets and sizes when adapting this
schematic template; its rectangles illustrate presentation, not a Rust/C++ layout.

## Template 4. State machine

For `Future`, protocols, and connection lifecycles. Nodes are rounded rectangles;
transitions are arrows labeled with events.

Keep flow left to right or top to bottom, not a web of crossing lines. If there
are more than seven states, split it into two diagrams.

## Template 5. Affected-code relationship map

Required for code changes. Place it near the change walkthrough and reference
it from the code-review chapter. It should orient the reviewer before detailed
listings.

- Show affected classes, Rust structs/enums, traits/interfaces, and modules.
  Include relevant unchanged callers and dependencies to explain the boundary
  and impact. For procedural code, show modules and functions; do not invent
  classes. For documentation/configuration-only work, map the affected files,
  configuration consumers, or workflow jobs instead.
- Label each node with its exact identifier and kind. Distinguish added,
  modified, removed, and unchanged elements with explicit labels and a legend;
  color alone is insufficient. Keep identifiers intact and translate explanations
  and legend text into the document's language.
- Draw directional edges with precise labels: calls, contains/owns, borrows,
  implements, inherits, imports/depends on, or sends data to. Explain what each
  arrow means. Do not imply ownership from a mere call or dependency.
- Read definitions and call sites to verify the graph. Show only relationships
  supported by the source. If the change rewires the graph, provide before/after
  panels or clearly distinguish removed and added edges.
- Split a crowded graph by subsystem while keeping shared boundary nodes
  identifiable. Mark omitted context instead of implying the graph is exhaustive.
- Below the SVG, provide a table: symbol/module → role → change status → clickable
  file/line reference. Use commit-pinned links and the working-tree/unpublished
  fallbacks in `writing.md`. Removed nodes link to the previous revision.
  SVG nodes may also be clickable, but the table must remain usable in PDF
  renderers that do not preserve SVG hyperlinks.
- External navigation links to source are allowed; the diagram's rendering must
  still be self-contained, with no externally loaded images, fonts, or scripts.
- Add a caption and nearby paragraph explaining the changed relationships,
  affected callers, and the order in which to inspect the code.

## Before delivery

- [ ] Opens independently in a browser and looks as intended
- [ ] Readable in monochrome print
- [ ] All IDs unique across the entire textbook
- [ ] No external resources or scripts
- [ ] Meaningful Markdown alt text in the document's language
- [ ] Nearby paragraph explains the diagram
- [ ] Affected-code map has verified relationships, change-status legend, and
      a source-location table with clickable revision-aware links
