"""The site's interface strings, read once from `assets/i18n.js`.

ONE READER, NOT ONE PER EXPORTER. This function used to live inside
`scripts/export_local_pages.py`. It moved here when the page-document exporter
needed the same thing, because the alternative -- a second copy in
`src/pages/shell.py` -- is precisely the failure `assets/i18n.js` was created
to end. Before that file existed the licence notice was written out FIVE
times, and the two English copies were kept in step by a test comparing them
to each other, which cannot catch both drifting from the French and Dutch
versions.

That notice is a LICENCE CONDITION on every page publishing municipal data
(docs/data_catalog.md), and Statbel's 2015 licence terminates automatically on
non-compliance. This project has already had one attribution breach. So the
reader below refuses rather than degrades: no notice in some language means no
pages, not pages with a gap in them.

Read through node rather than regexed. The file is JavaScript, and parsing it
by pattern is exactly the guessing claude.md rule 13 forbids. node is already
a hard dependency of this repository's test suite (tests/test_i18n.py,
tests/test_local_ui_logic.py), so requiring it adds nothing a contributor did
not already need.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

#: The browser's own copy of these strings. The single source.
I18N_JS = REPO_ROOT / "assets" / "i18n.js"

#: Every language the site is generated in. Matches `I18N.LANGS`.
LANGS = ("en", "fr", "nl")

#: English keeps the bare URL: those routes are already indexed and linked, and
#: moving them under /en/ would break them for no gain (claude.md rule 31).
DEFAULT_LANG = "en"


def interface_strings(path: Path | None = None) -> dict[str, dict[str, str]]:
    """Every language's interface strings.

    `path` is a parameter rather than a hardcoded constant so a caller can
    keep its own patch point -- `scripts/export_local_pages.py` passes its
    module-level `I18N_JS`, which is what that script's licence-guard test
    substitutes to prove the refusal actually fires.

    Raises `SystemExit` -- not an exception a caller might swallow -- if node
    is unavailable, if a language is missing, or if any language has no
    licence notice.
    """
    path = path or I18N_JS
    result = subprocess.run(
        [
            "node",
            "-e",
            "const I=require(process.argv[1]);process.stdout.write(JSON.stringify(I.STRINGS))",
            str(path),
        ],
        capture_output=True,
        text=True,
        # EXPLICIT, because `text=True` alone decodes with the platform's
        # preferred encoding -- UTF-8 on the Linux runners, cp1252 on a
        # Belgian Windows machine. Every licence notice in this file contains
        # accented French and Dutch, so on Windows this call died with
        # UnicodeDecodeError and `interface_strings()` never returned: the
        # page exporters could not run at all, and the failure was inside the
        # one function that guards Statbel licence compliance.
        encoding="utf-8",
        timeout=30,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"could not read {path} through node: {result.stderr.strip()}\n"
            "node is required to build the static pages, because the interface "
            "strings (including the licence notice) live in a JavaScript module "
            "the browser also loads."
        )
    strings = json.loads(result.stdout)
    missing = [lang for lang in LANGS if lang not in strings]
    if missing:
        raise SystemExit(f"{path} has no strings for {missing}")
    for lang in LANGS:
        if not strings[lang].get("attribution"):
            raise SystemExit(
                f"{path} has no licence notice for {lang!r}. Refusing to generate "
                "pages that publish municipal data without one -- Statbel's 2015 "
                "licence terminates automatically on non-compliance."
            )
    return strings
