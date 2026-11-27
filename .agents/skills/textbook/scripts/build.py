#!/usr/bin/env python3
"""
Build a textbook: Markdown -> self-contained HTML -> PDF -> artifact.html.

artifact.html is a portable page for local viewing or hosting. It contains the
whole textbook and a download button with the PDF embedded as base64. THIS
SCRIPT embeds base64, not the model: never pass megabytes of it through context.

Usage (English example; localize the title and all labels for other languages):
    python3 scripts/build.py --src docs/textbook/2026-08-13-connector \
        --lang en --title "Connector X: walkthrough" \
        --contents-label "Contents" --download-label "Download PDF" \
        --download-hint "Read here or download the PDF"
    python3 scripts/build.py --check

Expected --src layout:
    <src>/
      01-*.md, 02-*.md, ...   (or a single textbook.md)
      assets/*.svg            (diagrams, inlined into HTML)

Outputs (default: <src>/build/):
    textbook.html   — readable page for local viewing
    textbook.pdf    — separate PDF
    artifact.html   — the same page with the PDF embedded
"""

import argparse
import base64
import html
import re
import shutil
import subprocess
import sys
from pathlib import Path

# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------


def have(cmd):
    return shutil.which(cmd) is not None


def have_module(name):
    try:
        __import__(name)
        return True
    except ImportError:
        return False


def check_env(verbose=True):
    """Return a dictionary describing available build dependencies."""
    env = {
        "markdown": have_module("markdown"),
        "pygments": have_module("pygments"),
        "weasyprint": have_module("weasyprint"),
        "wkhtmltopdf": have("wkhtmltopdf"),
        "chromium": any(
            have(c) for c in ("chromium", "chromium-browser", "google-chrome", "chrome")
        ),
        "pandoc": have("pandoc"),
        "xelatex": have("xelatex"),
        "typst": have("typst"),
    }
    if verbose:
        print("Environment:")
        for k, v in env.items():
            print(f"  {'OK ' if v else '-- '} {k}")
        engines = [e for e in ("weasyprint", "wkhtmltopdf", "chromium") if env[e]]
        if env["pandoc"] and env["xelatex"]:
            engines.append("pandoc+xelatex")
        print(f"PDF engines: {', '.join(engines) if engines else 'NONE'}")
        if not env["markdown"]:
            print("  hint: pip install markdown pygments")
        if not engines:
            print("  hint: pip install weasyprint   (the simplest option)")
    return env


def ensure_markdown():
    """Markdown is required; try installing it if unavailable."""
    if have_module("markdown"):
        return True
    for args in (
        ["pip", "install", "--quiet", "markdown", "pygments"],
        [
            "pip",
            "install",
            "--quiet",
            "--break-system-packages",
            "markdown",
            "pygments",
        ],
        ["pip", "install", "--quiet", "--user", "markdown", "pygments"],
    ):
        try:
            subprocess.run(args, check=True, capture_output=True, timeout=180)
            if have_module("markdown"):
                return True
        except Exception:
            continue
    return False


# --------------------------------------------------------------------------
# Markdown -> HTML
# --------------------------------------------------------------------------

CSS = """
:root {
  --ink: #16181d;
  --muted: #5b6472;
  --rule: #d8dee8;
  --accent: #1a4d8f;
  --code-bg: #f5f7fa;
  --note-bg: #eef4fb;
  --warn-bg: #fff6e6;
}
* { box-sizing: border-box; }
body {
  font-family: "DejaVu Serif", "Liberation Serif", "Noto Serif", Georgia, serif;
  font-size: 11.5pt;
  line-height: 1.62;
  color: var(--ink);
  margin: 0 auto;
  padding: 2.2rem 1.4rem 5rem;
  max-width: 46rem;
  hyphens: auto;
}
h1, h2, h3, h4 {
  font-family: "DejaVu Sans", "Liberation Sans", "Noto Sans", Arial, sans-serif;
  line-height: 1.25;
  color: var(--ink);
  margin: 2.1em 0 0.6em;
}
h1 {
  font-size: 1.85em;
  border-bottom: 2px solid var(--accent);
  padding-bottom: 0.25em;
  break-before: page;
}
h1:first-of-type { break-before: avoid; }
h2 { font-size: 1.35em; color: var(--accent); }
h3 { font-size: 1.12em; }
h4 { font-size: 1em; color: var(--muted); }
p { margin: 0.75em 0; }
a { color: var(--accent); }
code, pre, kbd {
  font-family: "DejaVu Sans Mono", "Liberation Mono", "Noto Sans Mono", monospace;
}
code { font-size: 0.88em; background: var(--code-bg); padding: 0.1em 0.3em;
       border-radius: 3px; }
pre {
  background: var(--code-bg);
  border: 1px solid var(--rule);
  border-left: 3px solid var(--accent);
  border-radius: 4px;
  padding: 0.8em 1em;
  overflow-x: auto;
  font-size: 0.82em;
  line-height: 1.45;
  break-inside: avoid;
}
pre code { background: none; padding: 0; font-size: 1em; }
blockquote {
  margin: 1.1em 0; padding: 0.7em 1.1em;
  background: var(--note-bg); border-left: 3px solid var(--accent);
  border-radius: 0 4px 4px 0;
}
blockquote p:first-child { margin-top: 0; }
blockquote p:last-child { margin-bottom: 0; }
table {
  border-collapse: collapse; width: 100%; margin: 1.2em 0;
  font-size: 0.88em; break-inside: avoid;
}
th, td { border: 1px solid var(--rule); padding: 0.45em 0.6em; text-align: left;
         vertical-align: top; }
th { background: var(--code-bg); font-family: "DejaVu Sans", sans-serif; }
figure { margin: 1.6em 0; text-align: center; break-inside: avoid; }
figure svg { max-width: 100%; height: auto; }
figcaption {
  font-family: "DejaVu Sans", sans-serif; font-size: 0.82em;
  color: var(--muted); margin-top: 0.5em;
}
hr { border: none; border-top: 1px solid var(--rule); margin: 2.2em 0; }
ul, ol { padding-left: 1.4em; }
li { margin: 0.3em 0; }
.doc-header {
  border-bottom: 1px solid var(--rule); padding-bottom: 1.2em; margin-bottom: 1.6em;
}
.doc-header .title {
  font-family: "DejaVu Sans", sans-serif; font-size: 2em; font-weight: 700;
  line-height: 1.2; margin: 0 0 0.3em;
}
.doc-header .meta { color: var(--muted); font-size: 0.85em;
                    font-family: "DejaVu Sans", sans-serif; }
.toc {
  background: var(--code-bg); border: 1px solid var(--rule); border-radius: 5px;
  padding: 1em 1.4em; margin-bottom: 2em; font-size: 0.9em;
  font-family: "DejaVu Sans", sans-serif;
}
.toc > ul { padding-left: 1.1em; margin: 0.4em 0; }
.toc ul ul { font-size: 0.94em; color: var(--muted); }
.dl-bar {
  position: sticky; top: 0; z-index: 50;
  display: flex; gap: 0.8rem; align-items: center; justify-content: space-between;
  background: var(--accent); color: #fff; padding: 0.6rem 1rem;
  margin: -2.2rem -1.4rem 2rem; font-family: "DejaVu Sans", sans-serif;
  font-size: 0.9rem;
}
.dl-bar button {
  font: inherit; font-weight: 600; cursor: pointer;
  background: #fff; color: var(--accent); border: none;
  padding: 0.45em 1.1em; border-radius: 4px;
}
.dl-bar button:hover { background: #eaf0f8; }
.dl-bar .hint { opacity: 0.85; font-size: 0.85em; }
@media print {
  .dl-bar { display: none; }
  body { max-width: none; padding: 0; font-size: 10.5pt; }
  a { color: var(--ink); text-decoration: none; }
}
@page {
  size: A4;
  margin: 18mm 16mm 20mm;
  @bottom-center { content: counter(page); font-family: "DejaVu Sans", sans-serif;
                   font-size: 9pt; color: #5b6472; }
}
"""

PAGE = """<!DOCTYPE html>
<html lang="{lang}" dir="{direction}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>{css}</style>
</head>
<body>
{dlbar}
<div class="doc-header">
  <div class="title">{title}</div>
  <div class="meta">{meta}</div>
</div>
{toc}
{body}
</body>
</html>
"""

DL_BAR = """<div class="dl-bar">
  <span class="hint">{download_hint}</span>
  <button id="dl-pdf" type="button">{download_label}</button>
</div>
<script id="pdf-b64" type="text/plain">{b64}</script>
<script>
(function () {{
  var btn = document.getElementById('dl-pdf');
  var name = {fname};
  btn.addEventListener('click', function () {{
    var b64 = document.getElementById('pdf-b64').textContent.trim();
    var a = document.createElement('a');
    a.download = name;
    try {{
      var bin = atob(b64);
      var arr = new Uint8Array(bin.length);
      for (var i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
      a.href = URL.createObjectURL(new Blob([arr], {{type: 'application/pdf'}}));
    }} catch (e) {{
      a.href = 'data:application/pdf;base64,' + b64;
    }}
    document.body.appendChild(a);
    a.click();
    setTimeout(function () {{
      if (a.href.indexOf('blob:') === 0) URL.revokeObjectURL(a.href);
      a.remove();
    }}, 2000);
  }});
}})();
</script>
"""


def collect_markdown(src: Path) -> str:
    """Read textbook.md or the chapter Markdown files in filename order."""
    single = src / "textbook.md"
    if single.exists():
        return single.read_text(encoding="utf-8")
    parts = sorted(p for p in src.glob("*.md") if not p.name.startswith("_"))
    if not parts:
        sys.exit(f"ERROR: no textbook.md or chapter *.md files in {src}")
    return "\n\n".join(p.read_text(encoding="utf-8") for p in parts)


def inline_svgs(html_text: str, src: Path) -> str:
    """Inline local SVGs as markup and raster images as data URIs.

    A portable page must contain its images rather than depend on local files.
    """

    def repl(m):
        attrs, path = m.group(0), m.group(1)
        if path.startswith(("http://", "https://", "data:")):
            return attrs
        f = (src / path).resolve()
        if not f.exists():
            print(f"  WARNING: missing image {path}")
            return attrs
        alt_m = re.search(r'alt="([^"]*)"', attrs)
        alt = alt_m.group(1) if alt_m else ""
        cap = f"<figcaption>{html.escape(alt)}</figcaption>" if alt else ""
        if f.suffix.lower() == ".svg":
            svg = f.read_text(encoding="utf-8")
            svg = re.sub(r"<\?xml.*?\?>", "", svg, flags=re.S)
            svg = re.sub(r"<!DOCTYPE.*?>", "", svg, flags=re.S)
            return f"<figure>{svg.strip()}{cap}</figure>"
        mime = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".webp": "image/webp",
        }.get(f.suffix.lower())
        if not mime:
            return attrs
        b64 = base64.b64encode(f.read_bytes()).decode()
        return (
            f'<figure><img src="data:{mime};base64,{b64}" alt="{html.escape(alt)}">'
            f"{cap}</figure>"
        )

    return re.sub(r'<img[^>]*src="([^"]+)"[^>]*/?>', repl, html_text)


def md_to_html(md_text: str, src: Path, contents_label: str):
    import markdown as md

    exts = [
        "fenced_code",
        "tables",
        "toc",
        "attr_list",
        "sane_lists",
        "footnotes",
        "admonition",
        "md_in_html",
    ]
    if have_module("pygments"):
        exts.append("codehilite")
    conv = md.Markdown(
        extensions=exts,
        extension_configs={
            "toc": {"toc_depth": "1-3"},
            "codehilite": {"guess_lang": False, "noclasses": True},
        },
    )
    body = conv.convert(md_text)
    body = inline_svgs(body, src)
    toc = getattr(conv, "toc", "")
    return body, (
        f'<nav class="toc"><b>{html.escape(contents_label)}</b>{toc}</nav>'
        if toc
        else ""
    )


# --------------------------------------------------------------------------
# HTML -> PDF
# --------------------------------------------------------------------------


def build_pdf(
    html_path: Path,
    pdf_path: Path,
    md_text: str,
    src: Path,
    lang: str,
    contents_label: str,
    direction: str,
) -> str:
    """Try engines in order and return the name of the successful engine."""
    if have_module("weasyprint"):
        try:
            from weasyprint import HTML

            HTML(filename=str(html_path)).write_pdf(str(pdf_path))
            if pdf_path.exists() and pdf_path.stat().st_size > 0:
                return "weasyprint"
        except Exception as e:
            print(f"  weasyprint failed: {e}")

    for chrome in ("chromium", "chromium-browser", "google-chrome", "chrome"):
        if not have(chrome):
            continue
        try:
            subprocess.run(
                [
                    chrome,
                    "--headless",
                    "--disable-gpu",
                    "--no-sandbox",
                    "--print-to-pdf-no-header",
                    f"--print-to-pdf={pdf_path}",
                    html_path.as_uri(),
                ],
                check=True,
                capture_output=True,
                timeout=300,
            )
            if pdf_path.exists() and pdf_path.stat().st_size > 0:
                return chrome
        except Exception as e:
            print(f"  {chrome} failed: {e}")

    if have("wkhtmltopdf"):
        try:
            subprocess.run(
                [
                    "wkhtmltopdf",
                    "--enable-local-file-access",
                    "--quiet",
                    str(html_path),
                    str(pdf_path),
                ],
                check=True,
                capture_output=True,
                timeout=300,
            )
            if pdf_path.exists() and pdf_path.stat().st_size > 0:
                return "wkhtmltopdf"
        except Exception as e:
            print(f"  wkhtmltopdf failed: {e}")

    if have("pandoc") and have("xelatex"):
        try:
            tmp_md = pdf_path.parent / "_pandoc.md"
            tmp_md.write_text(md_text, encoding="utf-8")
            subprocess.run(
                [
                    "pandoc",
                    str(tmp_md),
                    "-o",
                    str(pdf_path),
                    "--pdf-engine=xelatex",
                    "--toc",
                    "--toc-depth=3",
                    "-V",
                    "mainfont=DejaVu Serif",
                    "-V",
                    "monofont=DejaVu Sans Mono",
                    "--metadata",
                    f"lang={lang}",
                    "--metadata",
                    f"dir={direction}",
                    "--metadata",
                    f"toc-title={contents_label}",
                    "-V",
                    "geometry:margin=2cm",
                    f"--resource-path={src}",
                ],
                check=True,
                capture_output=True,
                timeout=600,
            )
            tmp_md.unlink(missing_ok=True)
            if pdf_path.exists() and pdf_path.stat().st_size > 0:
                return "pandoc+xelatex"
        except subprocess.CalledProcessError as e:
            print(f"  pandoc failed: {e.stderr.decode()[-600:]}")
        except Exception as e:
            print(f"  pandoc failed: {e}")

    return ""


# --------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(
        description="Build HTML/PDF in the language chosen by the user or author."
    )
    ap.add_argument("--src", help="directory containing Markdown chapters and assets/")
    ap.add_argument("--out", help="output directory (default: <src>/build)")
    ap.add_argument("--title", help="title in the document's language")
    ap.add_argument("--meta", default="", help="subtitle: branch, date, commits")
    ap.add_argument("--lang", help="BCP 47 document language tag, e.g. en, fr, pl, ru")
    ap.add_argument(
        "--direction",
        choices=("ltr", "rtl"),
        default="ltr",
        help="document text direction (default: ltr)",
    )
    ap.add_argument(
        "--contents-label", help="contents heading in the document's language"
    )
    ap.add_argument(
        "--download-label", help="PDF download button text in the document's language"
    )
    ap.add_argument("--download-hint", help="download hint in the document's language")
    ap.add_argument("--check", action="store_true", help="only check the environment")
    args = ap.parse_args()

    if args.check:
        check_env()
        return
    for name in (
        "src",
        "title",
        "lang",
        "contents_label",
        "download_label",
        "download_hint",
    ):
        if not getattr(args, name):
            ap.error(f"--{name.replace('_', '-')} is required when building a textbook")
    if not re.fullmatch(r"[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*", args.lang):
        ap.error("--lang must be a language tag such as en, fr, pl, ru, or pt-BR")

    src = Path(args.src).resolve()
    out = Path(args.out).resolve() if args.out else src / "build"
    out.mkdir(parents=True, exist_ok=True)

    if not ensure_markdown():
        sys.exit(
            "ERROR: Markdown is unavailable and installation failed.\n"
            "  pip install markdown pygments"
        )

    md_text = collect_markdown(src)
    body, toc = md_to_html(md_text, src, args.contents_label)
    page_args = dict(
        title=html.escape(args.title),
        css=CSS,
        meta=html.escape(args.meta),
        toc=toc,
        body=body,
        lang=html.escape(args.lang),
        direction=args.direction,
    )

    # 1. Readable page without the embedded PDF.
    plain = PAGE.format(dlbar="", **page_args)
    html_path = out / "textbook.html"
    html_path.write_text(plain, encoding="utf-8")
    print(f"HTML   -> {html_path}  ({len(plain)/1024:.0f} KB)")

    # 2. Separate PDF.
    pdf_path = out / "textbook.pdf"
    engine = build_pdf(
        html_path,
        pdf_path,
        md_text,
        src,
        args.lang,
        args.contents_label,
        args.direction,
    )
    if not engine:
        print("\nERROR: no PDF engine could render the document.")
        print(
            "Install one of: pip install weasyprint | apt install chromium | "
            "apt install wkhtmltopdf | pandoc + xelatex"
        )
        print(
            f"HTML is available: {html_path}. Open it in a browser and "
            "print to PDF (Ctrl+P)."
        )
        sys.exit(2)
    print(f"PDF    -> {pdf_path}  ({pdf_path.stat().st_size/1024:.0f} KB, {engine})")

    # 3. Portable artifact.html with the PDF embedded.
    b64 = base64.b64encode(pdf_path.read_bytes()).decode()
    fname = pdf_path.name.replace('"', "")
    dlbar = DL_BAR.format(
        b64=b64,
        fname=f'"{fname}"',
        download_label=html.escape(args.download_label),
        download_hint=html.escape(args.download_hint),
    )
    art = PAGE.format(dlbar=dlbar, **page_args)
    art_path = out / "artifact.html"
    art_path.write_text(art, encoding="utf-8")
    size = art_path.stat().st_size
    print(f"ARTIFACT -> {art_path}  ({size/1024/1024:.2f} MB)")

    print(f"\nDone: open {art_path} in a browser")
    print(f"Separate PDF: {pdf_path}")
    print("Files were created locally; nothing was uploaded to a hosting service.")


if __name__ == "__main__":
    main()
