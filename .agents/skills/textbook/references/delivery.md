# Delivering HTML and PDF in Codex and Claude Code

## Local builds are the primary option

```bash
python3 <skill>/scripts/build.py --check
python3 <skill>/scripts/build.py \
  --src docs/textbook/<date>-<slug> \
  --lang en --title "Title" --meta "Branch and date" \
  --contents-label "Contents" \
  --download-label "Download PDF" \
  --download-hint "Read the complete textbook here or download the PDF"
```

This example is for an English request. For any other language, use its language
tag and translate the title, metadata, contents label, download label, and hint.
The agent selects the language from the user's request as specified in `SKILL.md`;
the builder receives it explicitly and does not infer it from Markdown.
For right-to-left languages, pass `--direction rtl` and verify layout and fonts.

Outputs in `<src>/build/`:

- `textbook.html`: a self-contained readable page with styles and diagrams.
- `textbook.pdf`: a separate PDF for viewing and sharing.
- `artifact.html`: the same page with an embedded PDF and download button.

Provide Markdown links to HTML and PDF using absolute paths, for example
`[Textbook HTML](/absolute/path/build/artifact.html)`.
Explain that these are files on the machine running the coding agent. In a remote
environment, users can retrieve them through its file interface, SFTP, or scp.
The CLI does not provide a graphical file viewer; open HTML in a browser and
PDF in a compatible viewer. Preview capabilities depend on the client.
Do not invent sandbox links or cloud URLs the environment has not supplied.

## Web links on request

Do not assume the coding-agent environment provides an artifact-hosting service.
If the session actually provides a file-upload tool, use it according to the
environment's rules and return only the URL it gives you.
Otherwise, prepare `artifact.html` for the user's chosen hosting or storage.
It embeds the PDF and needs no application server. The script uploads nothing.
A successful local build does not mean successful publication.
Do not publish, commit, or push without authorization for those actions.

## Updates

Use the existing source-chapter directory and rebuild. Updating a published copy
requires its address and an available way to publish to it.

## Verification and recovery

Check that the PDF opens, characters in the selected language render correctly,
and diagrams and code are not clipped. Check font coverage and text shaping for
scripts requiring them. Programmatically compare the embedded PDF with the
separate file without printing base64 into the conversation.
If no PDF engine works, report the error and provide the completed HTML:
browser → Ctrl+P → “Save as PDF.” `build.py --check` reports dependencies.
The former 16 MiB limit concerned an older Claude delivery mechanism and does
not apply to local files. Check an external host's own size limits and JavaScript
support. If it blocks the download button, provide the separate PDF on that host.

**Base64 never passes through the model.** `build.py` reads the PDF from disk
and embeds it.
