"""Capture review screenshots of pilot pages across theme x language x width.

Built for the Unification A2 visual check (two redesigned pages, home2.html
and macro.html, reviewed by the maintainer across every theme, language and
breakpoint before the redesign spreads further). Reusable for later batches
too -- pass different --pages.

For every combination of --pages x --themes x --langs x --widths, this
script:
  1. sets localStorage['belpulse-theme'] and localStorage['belpulse-lang']
     via a Playwright init script (the same keys assets/belpulse/shell.js and
     assets/i18n.js read -- see those files for why there is exactly one key
     of each, not per-page ones);
  2. navigates to the page, waiting for network idle and for
     document.fonts.ready;
  3. scrolls to the bottom and back, so lazy-rendered content has a chance to
     appear before the screenshot;
  4. takes a full-page PNG.

It also collects, per combination: browser console errors, uncaught page
errors, and horizontal overflow (document.documentElement.scrollWidth >
window.innerWidth). Those are written to report.md. An empty report says so
explicitly -- "no console errors, no horizontal overflow" -- rather than
being silently absent.

index.md lists every screenshot, grouped by page, as relative links.

Serves the repo root itself on a local, OS-assigned loopback port -- no
externally running server required. The only network access beyond that
local server is whatever the pages themselves already fetch (Google Fonts).

--out is required. This script never writes inside the repo by default
(screenshots are binary output, rule 12 -- no new binary artefacts without
asking).

Usage:
    python scripts/capture_review_screenshots.py --out /path/outside/repo
    python scripts/capture_review_screenshots.py --out /tmp/x \\
        --pages home2.html "macro.html#apercu" \\
        --themes light dark --langs fr en --widths 390 1122
"""

from __future__ import annotations

import argparse
import functools
import http.server
import re
import threading
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_PAGES = [
    "home2.html",
    "macro.html#apercu",
    "macro.html#croissance",
    "macro.html#europe",
]
DEFAULT_THEMES = ["light", "dark", "paper"]
DEFAULT_LANGS = ["fr", "nl", "en"]
DEFAULT_WIDTHS = [390, 768, 1122]

# The one theme key and the one language key the site reads everywhere --
# see assets/belpulse/shell.js (THEME_KEY) and assets/i18n.js
# (I18N.STORAGE_KEY). Duplicating the literals here would silently drift if
# either ever changes; they are small and stable enough that a plain
# duplicate is the honest tradeoff for a script with no import path into
# assets/*.js.
THEME_KEY = "belpulse-theme"
LANG_KEY = "belpulse-lang"

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify_page(page: str) -> str:
    """Turn a page spec like 'macro.html#apercu' into a filename-safe slug
    like 'macro-apercu'. Pure and deterministic: the same page spec always
    slugifies to the same string.

    The extension is stripped from the path part only, before any '?' or
    '#', so 'macro.html#apercu' slugifies from 'macro#apercu' rather than
    from the literal '.html' sitting in the middle of the string.
    """
    lowered = page.lower()
    match = re.search(r"[?#]", lowered)
    if match:
        path_part, rest = lowered[: match.start()], lowered[match.start() :]
    else:
        path_part, rest = lowered, ""
    if path_part.endswith(".html"):
        path_part = path_part[: -len(".html")]
    slug = _SLUG_RE.sub("-", f"{path_part}{rest}").strip("-")
    if not slug:
        raise ValueError(f"page spec {page!r} slugifies to an empty string")
    return slug


def build_filename(page: str, theme: str, lang: str, width: int) -> str:
    return f"{slugify_page(page)}_{theme}_{lang}_{width}.png"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture review screenshots across theme x language x width.",
    )
    parser.add_argument("--pages", nargs="+", default=list(DEFAULT_PAGES))
    parser.add_argument("--themes", nargs="+", default=list(DEFAULT_THEMES))
    parser.add_argument("--langs", nargs="+", default=list(DEFAULT_LANGS))
    parser.add_argument("--widths", nargs="+", type=int, default=list(DEFAULT_WIDTHS))
    parser.add_argument(
        "--out",
        required=True,
        type=Path,
        help="Output directory for screenshots, index.md and report.md. "
        "Required: this script never writes inside the repo on its own.",
    )
    return parser.parse_args(argv)


@dataclass(frozen=True)
class Finding:
    page: str
    theme: str
    lang: str
    width: int
    kind: str
    detail: str

    def as_line(self) -> str:
        return f"- {self.page} / {self.theme} / {self.lang} / {self.width}px -- {self.kind}: {self.detail}"


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler, minus the request log spam on stderr."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass


def start_server(root: Path) -> tuple[http.server.ThreadingHTTPServer, int]:
    """Serve `root` on an OS-assigned loopback port. Caller must call
    server.shutdown() + server.server_close() when done.
    """
    handler = functools.partial(_QuietHandler, directory=str(root))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port


def _capture_one(
    browser, port: int, page_spec: str, theme: str, lang: str, width: int, out_dir: Path
) -> list[Finding]:
    path_part, _, fragment = page_spec.partition("#")
    url = f"http://127.0.0.1:{port}/{path_part}"
    if fragment:
        url += f"#{fragment}"

    context = browser.new_context(viewport={"width": width, "height": 900})
    context.add_init_script(
        f"localStorage.setItem({THEME_KEY!r}, {theme!r}); "
        f"localStorage.setItem({LANG_KEY!r}, {lang!r});"
    )

    console_errors: list[str] = []
    page_errors: list[str] = []
    page = context.new_page()

    def on_console(msg: object) -> None:
        if getattr(msg, "type", None) == "error":
            console_errors.append(msg.text)

    page.on("console", on_console)
    page.on("pageerror", lambda exc: page_errors.append(str(exc)))

    findings: list[Finding] = []
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
        page.evaluate("document.fonts.ready.then(() => true)")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(300)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(300)

        overflow = page.evaluate("document.documentElement.scrollWidth > window.innerWidth")

        filename = build_filename(page_spec, theme, lang, width)
        page.screenshot(path=str(out_dir / filename), full_page=True)

        if overflow:
            findings.append(
                Finding(page_spec, theme, lang, width, "overflow", "horizontal overflow detected")
            )
        for text in console_errors:
            findings.append(Finding(page_spec, theme, lang, width, "console-error", text))
        for text in page_errors:
            findings.append(Finding(page_spec, theme, lang, width, "page-error", text))
    except Exception as exc:  # noqa: BLE001 - report and keep going, don't abort the run
        findings.append(Finding(page_spec, theme, lang, width, "capture-failed", str(exc)))
    finally:
        context.close()

    return findings


def _write_index(out_dir: Path, pages: list[str], manifest: dict[str, list[str]]) -> None:
    lines = ["# Review screenshots", ""]
    for page_spec in pages:
        lines.append(f"## {page_spec}")
        lines.append("")
        for filename in manifest.get(page_spec, []):
            lines.append(f"- [{filename}]({filename})")
        lines.append("")
    (out_dir / "index.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _write_report(out_dir: Path, findings: list[Finding]) -> None:
    lines = ["# Capture report", ""]
    if not findings:
        lines.append("no console errors, no horizontal overflow")
    else:
        lines.extend(f.as_line() for f in findings)
    (out_dir / "report.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def capture(args: argparse.Namespace) -> list[Finding]:
    from playwright.sync_api import sync_playwright

    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    server, port = start_server(REPO_ROOT)
    findings: list[Finding] = []
    manifest: dict[str, list[str]] = {page: [] for page in args.pages}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            try:
                for page_spec in args.pages:
                    for theme in args.themes:
                        for lang in args.langs:
                            for width in args.widths:
                                findings.extend(
                                    _capture_one(
                                        browser, port, page_spec, theme, lang, width, out_dir
                                    )
                                )
                                manifest[page_spec].append(
                                    build_filename(page_spec, theme, lang, width)
                                )
                                print(f"captured {page_spec} {theme}/{lang} @ {width}px")
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()

    _write_index(out_dir, args.pages, manifest)
    _write_report(out_dir, findings)
    return findings


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    findings = capture(args)
    print(f"wrote screenshots, index.md and report.md to {args.out}")
    if findings:
        print(f"{len(findings)} finding(s) -- see {args.out / 'report.md'}")
    else:
        print("no console errors, no horizontal overflow")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
