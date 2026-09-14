"""Sync the shared header/footer/bootstrap into the hand-edited pilot pages.

Batch A1.1 (docs/features/site_unification.md). Two pages -- home2.html and
macro.html -- are still hand-edited HTML, not generated from a page document,
so the shared header and footer `src/pages/shell.py` renders for every other
public page cannot simply be written over them once: a maintainer's next edit
to either page would drift the moment this script stops running.

Instead each page carries three delimited zones, written once by hand and
never touched again except by this script:

    <!-- bp-shell:bootstrap:start -->...<!-- bp-shell:bootstrap:end -->
    <!-- bp-shell:header:start page="..." -->...<!-- bp-shell:header:end -->
    <!-- bp-shell:footer:start -->...<!-- bp-shell:footer:end -->

This script finds each START/END pair and replaces ONLY the bytes between
them -- never the marker comments themselves, never anything outside a zone
-- with `SHELL_BOOTSTRAP`, `render_header(mode="client", ...)` and
`render_footer(mode="client", ...)` from src/pages/shell.py. A page missing a
marker pair, or carrying two, is refused rather than guessed at: a silent
partial sync is worse than a loud failure, because it ships looking correct.

Deterministic (rule 35): the same committed inputs produce the same bytes on
every run, so running this twice in a row changes nothing on the second run,
and `--check` can compare against a fresh render without ever writing.

Usage:  python scripts/sync_site_shell.py [--check]
        --check builds into memory and reports what WOULD change, as a
        unified diff, writing nothing. Exit 1 if anything would change.
"""

from __future__ import annotations

import argparse
import difflib
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.pages.shell import SHELL_BOOTSTRAP, ShellError, render_footer, render_header  # noqa: E402
from src.pages.strings import DEFAULT_LANG  # noqa: E402

#: (file, its own NAV entry) -- src.pages.shell.NAV. Both pilots live at the
#: repository root, so `asset_prefix` is always "" here; a page synced from a
#: subdirectory in a later batch would need one, same as a generated page's
#: does (scripts/export_page_documents.py's `asset_prefix_for`).
SHELL_PAGES = (
    ("home2.html", "home2.html"),
    ("macro.html", "macro.html"),
)

#: Every zone this script owns, and the fragment each one is filled with.
#: `header` and `footer` are functions of the page (its own `current`);
#: `bootstrap` is the same constant everywhere -- one more reason it is
#: `SHELL_BOOTSTRAP`, not a per-page render.
_ZONES = ("bootstrap", "header", "footer")


class SyncError(ShellError):
    """A page cannot be synced -- always fatal: writing a partial sync would
    leave a page whose header or footer silently stopped matching the shared
    shell, which is exactly the drift this script exists to prevent."""


#: The header start marker's own `page="..."` attribute -- the only zone
#: attribute this script reads back. Group 1 is None if the attribute is
#: missing entirely, which is refused just like a mismatched value (fixed
#: post-audit: the sync used to render the START marker's page attribute
#: without ever checking it agreed with SHELL_PAGES, so a copy-pasted or
#: hand-edited marker could silently render as the WRONG page's `current`
#: nav item -- wrong link marked `aria-current="page"`, nothing to say so).
_HEADER_START = re.compile(r'<!-- bp-shell:header:start(?:\s+page="([^"]*)")?\s*-->')


def _zone_pattern(zone: str) -> re.Pattern:
    """Matches a whole zone, START MARKER THROUGH END MARKER inclusive, so a
    replacement can put the markers straight back around the new content.
    `.*?` is non-greedy and `re.DOTALL` lets it span lines; the start marker
    may carry attributes (`page="..."`), matched but not captured -- the
    caller reads them separately when it needs one."""
    return re.compile(
        rf"(<!-- bp-shell:{zone}:start[^>]*-->)(.*?)(<!-- bp-shell:{zone}:end -->)",
        re.DOTALL,
    )


def _fragment_for(zone: str, *, page: str, current: str) -> str:
    if zone == "bootstrap":
        return f"\n{SHELL_BOOTSTRAP}\n"
    if zone == "header":
        return (
            "\n"
            + render_header(lang=DEFAULT_LANG, current=current, asset_prefix="", mode="client")
            + "\n"
        )
    if zone == "footer":
        return (
            "\n"
            + render_footer(lang=DEFAULT_LANG, asset_prefix="", mode="client", current=current)
            + "\n"
        )
    raise AssertionError(zone)  # pragma: no cover - _ZONES is closed


def synced(text: str, *, page: str, current: str) -> str:
    """`text` with every zone's content replaced. Refuses a page with zero or
    more than one instance of any zone -- see the module docstring."""
    for zone in _ZONES:
        # Counted on the raw markers first, not just on the paired regex
        # below: an unmatched start with no end (or vice versa) would make
        # the pair-matching regex find NOTHING, which is indistinguishable
        # from "no marker at all" unless the two halves are checked
        # separately -- and a lone stray marker is exactly the kind of
        # half-edited page this script must refuse rather than skip past.
        starts = len(re.findall(rf"<!-- bp-shell:{zone}:start", text))
        ends = len(re.findall(rf"<!-- bp-shell:{zone}:end -->", text))
        if starts == 0 or ends == 0:
            raise SyncError(f"{page}: no bp-shell:{zone} marker pair found")
        if starts > 1 or ends > 1:
            raise SyncError(
                f"{page}: {max(starts, ends)} bp-shell:{zone} marker(s) found, expected exactly 1"
            )
        pattern = _zone_pattern(zone)
        if not pattern.search(text):
            raise SyncError(f"{page}: bp-shell:{zone} start/end markers do not pair up")
        if zone == "header":
            declared = _HEADER_START.search(text)
            declared_page = declared.group(1) if declared else None
            if declared_page != page:
                raise SyncError(
                    f'{page}: bp-shell:header:start declares page="{declared_page}", '
                    f'expected "{page}" -- SHELL_PAGES and the marker have drifted, '
                    "refusing to guess which one is right"
                )
        fragment = _fragment_for(zone, page=page, current=current)
        # `frag=fragment` binds THIS iteration's value into the lambda's own
        # default-argument scope -- without it every lambda created across
        # the loop would share the loop variable and all resolve to
        # whichever zone was rendered LAST, since sub() calls the callback
        # immediately here but a closure over a loop variable is still the
        # bug ruff (B023) flags on principle.
        text = pattern.sub(lambda m, frag=fragment: m.group(1) + frag + m.group(3), text, count=1)
    return text


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="report changes, write nothing")
    args = parser.parse_args(argv)

    changed = []
    for page, current in SHELL_PAGES:
        path = REPO_ROOT / page
        before = path.read_text(encoding="utf-8")
        after = synced(before, page=page, current=current)
        if after != before:
            changed.append(page)
            if args.check:
                diff = difflib.unified_diff(
                    before.splitlines(keepends=True),
                    after.splitlines(keepends=True),
                    fromfile=f"a/{page}",
                    tofile=f"b/{page}",
                )
                sys.stdout.writelines(diff)
            else:
                # LF only (rule 35 depends on it -- Python's text-mode write
                # turns "\n" into "\r\n" on Windows unless told not to).
                path.write_text(after, encoding="utf-8", newline="\n")
                print(f"  synced {page}")
        elif not args.check:
            print(f"  {page} already up to date")

    if args.check:
        if changed:
            print(f"\n{len(changed)} page(s) would change: {', '.join(changed)}")
            return 1
        print("\nEvery shell zone is up to date.")
        return 0

    print(f"\n{len(changed)} page(s) synced, {len(SHELL_PAGES) - len(changed)} already current.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
