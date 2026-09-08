"""Capture screenshots of the REAL builder shell, driven in a headless browser.

Why this exists: the builder binds 127.0.0.1 and refuses any other Host (a
DNS-rebinding defence -- it writes files and holds a session token), so a
browser-based Codespace, which can only offer a *.app.github.dev forwarded
address, cannot open it at all. This script is how the maintainer sees the
thing without that control being loosened for convenience.

It drives the actual service and the actual shell -- same code path as
tests/builder/test_builder_shell_e2e.py -- so what you see here is what a
browser would show, not a mock.

PAGES_ROOT is redirected to a temp directory before the server starts, so
nothing here writes into config/pages/. Output is NOT committed
(docs/implementation/batches/*-screenshots/ is gitignored, rule 12); re-run to
regenerate.

Usage:  python scripts/capture_builder_screenshots.py
"""

from __future__ import annotations

import json
import secrets
import socket
import sys
import tempfile
import threading
from pathlib import Path

# Same as scripts/serve_builder.py: run from anywhere, import src/ from the repo.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

OUT = (
    Path(__file__).resolve().parents[1] / "docs/implementation/batches/batch-12-builder-screenshots"
)
VIEWPORT = {"width": 1440, "height": 1000}

# The six block types the registry ships, by their display names in the library.
BLOCK_NAMES = ["Hero", "Kpi Card", "Chart", "Comparison Table", "Map", "Rich Text"]

# A real indicator and a real commune, read from the published payloads by the
# same helpers the tests use -- never hand-typed (claude.md rule 36).
from tests.fixtures.pages import real_data  # noqa: E402

INDICATOR = real_data.an_additive_municipal_indicator_id()
NIS = real_data.a_municipal_nis_code()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _seed_bound_demo(pages_root: Path) -> None:
    """A small VALID page carrying one bound KPI, written straight to the temp
    pages root.

    Driving the whole form for this would bury the figure behind the other
    blocks' validation errors, which is what the demo page shows. This is a
    real document, saved in the real format, resolved by the real service --
    only the authoring is skipped.
    """
    from tests.fixtures.pages import builders

    doc = builders.minimal_valid_document()
    doc["page_id"] = "bound-demo"
    doc["seo"] = {"title": builders.label("A bound figure"), "description": builders.label("Demo")}
    props = builders.props_for("kpi_card")
    props["label"] = builders.label("Activation measures paid")
    doc["sections"][0]["blocks"].append(
        builders.make_block(
            "kpi_card",
            block_id="bound-kpi-1",
            props=props,
            binding=builders.municipal_binding(INDICATOR, nis=NIS),
            block_layout=builders.layout(
                desktop=(0, 2, 6, 3), tablet=(0, 2, 6, 3), mobile=(0, 2, 4, 3)
            ),
        )
    )
    page_dir = pages_root / "bound-demo"
    page_dir.mkdir(parents=True, exist_ok=True)
    (page_dir / "draft.json").write_text(
        json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="builder-shots-"))

    # Redirect BOTH bindings before importing the service: store.py imports
    # PAGES_ROOT by value, so patching only paths would not reach it -- the same
    # trap the test suite documents.
    from src.builder import paths as builder_paths
    from src.builder import store as builder_store

    builder_paths.PAGES_ROOT = tmp
    builder_store.PAGES_ROOT = tmp

    from playwright.sync_api import sync_playwright

    from src.builder.service import BuilderConfig, make_server

    token = secrets.token_urlsafe(32)
    config = BuilderConfig(host="127.0.0.1", port=_free_port(), token=token)
    server = make_server(config)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    _seed_bound_demo(tmp)
    pages_root_for_demo = tmp

    OUT.mkdir(parents=True, exist_ok=True)
    url = f"http://127.0.0.1:{config.port}/?token={token}"
    shots: list[tuple[str, str]] = []
    errors: list[str] = []

    def shot(page, slug: str, caption: str) -> None:
        path = OUT / f"{slug}.png"
        page.screenshot(path=str(path))
        shots.append((path.name, caption))
        print(f"  {path}  --  {caption}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            ctx = browser.new_context(viewport=VIEWPORT)
            page = ctx.new_page()
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.goto(url, wait_until="networkidle")

            shot(page, "01-empty-state", "No pages yet -- the honest empty state")

            page.click("text=+ New page")
            page.wait_for_timeout(200)
            shot(page, "02-new-page-form", "Creating a page: route, theme, trilingual SEO title")

            page.fill("#npf-page_id", "demo-page")
            page.fill("#npf-route", "/about.html")
            page.fill("#npf-theme", "light-institutional")
            title = page.locator("fieldset:has(legend:has-text('SEO title')) input")
            for i, lang in enumerate(("EN", "FR", "NL")):
                title.nth(i).fill(f"Demo page {lang}")
            page.wait_for_timeout(600)
            shot(page, "03-new-page-filled", "The form filled in, live-validated as you type")

            page.click("text=Create page")
            page.wait_for_timeout(800)

            for name in BLOCK_NAMES:
                page.locator(
                    f".bp-block-library-item:has(.bp-block-type-name:has-text('{name}')) "
                    "button:has-text('Add')"
                ).click()
                page.wait_for_timeout(150)
            page.wait_for_timeout(600)
            shot(
                page,
                "04-all-six-blocks",
                "All six block types placed: library, tree, grid, preview",
            )

            page.click("button:has-text('hero (hero-1)')")
            page.wait_for_timeout(300)
            shot(page, "05-inspector-content", "A block selected -- the inspector's content panel")

            page.click("#tab-layout")
            page.wait_for_timeout(300)
            shot(
                page,
                "06-inspector-layout",
                "The layout panel: per-breakpoint grid position, and Locked",
            )

            # Every block is added full-width and stacked, so a plain move
            # collides with its neighbour. Narrow it first -- that is a real
            # resize -- and only then is there room to move it.
            tile = page.locator('.bp-grid-tile[data-block-id="hero-1"]')
            tile.focus()
            page.keyboard.press("Shift+ArrowLeft")
            page.wait_for_timeout(400)
            shot(page, "07-resized", "Shift+arrow resizes: hero narrowed from 12 columns to 11")

            page.keyboard.press("ArrowRight")
            page.wait_for_timeout(400)
            shot(
                page,
                "08-moved",
                "Arrow moves: now that it is narrower, there is room to shift right",
            )

            page.keyboard.press("ArrowDown")
            page.wait_for_timeout(400)
            shot(
                page,
                "09-refusal-overlap",
                "A refused move names the block it would have overlapped",
            )

            page.keyboard.press("l")
            page.wait_for_timeout(400)
            shot(
                page, "10-locked-block", "Locked: hatched, labelled, and it refuses to move at all"
            )

            # Batch 14: bind a KPI to a real indicator and commune, so the
            # preview shows a published figure rather than a stand-in.
            page.click("button:has-text('kpi_card (kpi-card-1)')")
            page.wait_for_timeout(200)
            page.click("#tab-data")
            page.wait_for_timeout(300)
            shot(page, "11-data-panel", "The data panel before a binding is set")

            # THE POINT OF BATCH 14. Open the seeded page -- one hero, one
            # bound KPI, nothing else -- so the resolved figure is plainly
            # visible rather than buried under the demo page's other blocks.
            # That page has unsaved changes, so switching asks first; confirm.
            # By the page's own name, not by position: "bound-demo" sorts
            # before "demo-page", so .last picks the wrong one.
            open_bound = page.locator(
                ".bp-page-item:has(.bp-page-id:text-is('bound-demo')) "
                "button:has-text('Open draft')"
            )
            open_bound.click()
            page.wait_for_timeout(600)
            modal = page.locator(".bp-modal")
            if modal.count():
                modal.locator(".bp-modal-buttons button").first.click()
                page.wait_for_timeout(600)
                open_bound.click()
            page.wait_for_timeout(3000)

            # PROVE the caption before writing it. A screenshot captioned "a
            # real figure" that shows an error is exactly the class of defect
            # this project keeps finding: a label asserting behaviour the
            # system does not have. Checked against the published payload.
            published = json.loads(
                (Path("public/data/communes") / f"{NIS}.json").read_text(encoding="utf-8")
            )
            periods = published["indicators"][INDICATOR]["periods"]
            expected = f"{round(periods[sorted(periods)[-1]]['value']):,}"
            frame_text = page.frame_locator("#shell-preview").locator("body").inner_text()
            if expected not in frame_text:
                page.screenshot(path=str(OUT / "DEBUG-not-resolved.png"))
                raise SystemExit(
                    f"REFUSING to caption this as a real figure: expected {expected!r} "
                    f"in the preview.\nPreview said: {frame_text[:400]!r}"
                )

            shot(
                page,
                "12-bound-real-figure",
                f"Bound to {INDICATOR} for {NIS}: the preview shows {expected}, "
                "the same number the published payload holds",
            )

            # The preview frame on its own, because at full-window scale the
            # figure is legible to the guard above but not to a reader.
            # The block on a plain page, at a readable size. Screenshotting
            # an element across the iframe boundary does not land correctly
            # here, and the preview renders at 1280px inside a much narrower
            # column so most of it is scrolled out of view. This renders the
            # SAME document through the SAME renderer with the SAME resolver --
            # nothing is re-created for the picture.
            from src.builder.service import preview_data as _preview_data
            from src.pages import load_registry, render_document
            from src.pages.metadata import load_metadata as _load_md

            seeded = json.loads(
                (pages_root_for_demo / "bound-demo" / "draft.json").read_text(encoding="utf-8")
            )
            fragment = render_document(
                seeded,
                registry=load_registry(),
                lang="en",
                data=_preview_data(seeded, _load_md(), "en"),
            )
            css = "\n".join(
                (Path("assets/belpulse") / name).read_text(encoding="utf-8")
                for name in ("tokens.css", "components.css", "layout.css", "blocks.css")
            )
            block_page = ctx.new_page()
            block_page.set_content(
                f"<!doctype html><meta charset='utf-8'><style>{css}"
                "body{margin:0;padding:24px;background:#fff;width:760px}</style>"
                f"{fragment}"
            )
            block_page.wait_for_timeout(400)
            close_up = OUT / "12b-block-closeup.png"
            block_page.locator(".bp-kpi").first.screenshot(path=str(close_up))
            block_page.close()
            shots.append((close_up.name, f"The bound block itself: {expected}"))
            print(f"  {close_up}  --  the bound block itself: {expected}")

            page.click("text=Validate")
            page.wait_for_timeout(800)
            shot(page, "13-validation-errors", "Validate: findings as a list, each naming its path")

            for viewport in ("Tablet", "Mobile"):
                page.click(f"button:has-text('{viewport}')")
                page.wait_for_timeout(500)
                shot(
                    page,
                    f"14-viewport-{viewport.lower()}",
                    f"{viewport} viewport: the grid narrows, each breakpoint arranged separately",
                )

            browser.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    print(f"\n{len(shots)} screenshots in {OUT}/")
    if errors:
        print(f"\n{len(errors)} page error(s) -- these are real defects, not noise:")
        for e in errors:
            print(f"  {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
