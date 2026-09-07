"""Capture desktop + mobile screenshots of every core public page.

Used for Batch 0 of the website redesign (docs/features/page_builder.md) and again
before/after each later visual batch, as the raw material for visual-auditor's
before/after comparison. Requires the site served locally (e.g. `python -m
http.server 8912` from the repo root) and Playwright's Chromium installed.

Output is NOT committed -- see docs/implementation/batches/batch-0-baseline.md for
why (rule 12: no new binary artefacts without asking). Re-run this script to
regenerate the images; docs/implementation/batches/batch-0-screenshots/ is
gitignored.
"""

from playwright.sync_api import sync_playwright

PAGES = [
    ("index", "index.html"),
    ("communes", "communes.html"),
    ("map", "map.html"),
    ("all_data", "all_data.html"),
    ("about", "about.html"),
    ("dashboard", "dashboard.html"),
    ("local_search", "local.html"),
    ("local_antwerp", "local.html?nis=11002"),
    ("local_static_antwerp", "local/11002/"),
]
VIEWPORTS = {
    "desktop": {"width": 1440, "height": 1000},
    "mobile": {"width": 390, "height": 844},
}
OUT = "docs/implementation/batches/batch-0-screenshots"

errs = []
with sync_playwright() as p:
    b = p.chromium.launch()
    for vp_name, vp in VIEWPORTS.items():
        ctx = b.new_context(viewport=vp)
        page = ctx.new_page()
        page.on("pageerror", lambda e: errs.append(str(e)))
        for slug, path in PAGES:
            try:
                page.goto(f"http://localhost:8912/{path}", wait_until="networkidle", timeout=20000)
                page.wait_for_timeout(1200)
                page.screenshot(
                    path=f"{OUT}/{slug}_{vp_name}.png", full_page=(vp_name == "desktop")
                )
                print(f"captured {slug} @ {vp_name}")
            except Exception as e:
                print(f"FAILED {slug} @ {vp_name}: {e}")
        ctx.close()
    b.close()
print("console/page errors across all captures:", errs or "none")
