"""Guards for things that work on the Linux runners and break on Windows.

CI runs on ubuntu-latest. The maintainer's own machine is Windows. That gap is
invisible until someone opens the repository there, and then it is not subtle:
before this file existed, 119 tests failed on Windows on a commit CI called
green, and `make pages` could not run at all.

Each guard here exists because a real defect got through.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

#: Where to look. Skips the virtualenv and any vendored code.
SOURCE_DIRS = ("src", "scripts", "tests")

#: A `subprocess.run(...)` call, brace-matched shallowly -- enough for the call
#: shapes this repository actually uses.
_CALL = re.compile(r"subprocess\.run\((?:[^()]|\([^()]*\))*?\)", re.S)


def _python_files():
    """Every source file these guards scan -- except this one.

    Both assertions below quote the offending call shape in their own failure
    message, so a guard that scanned itself would report itself. Excluding it
    is the alternative to writing the message in a way that dodges the pattern,
    which would make the message worse to read for no gain.
    """
    here = Path(__file__).resolve()
    for directory in SOURCE_DIRS:
        for path in (REPO / directory).rglob("*.py"):
            if ".venv" not in path.parts and path.resolve() != here:
                yield path


def test_every_text_subprocess_declares_its_encoding():
    """`text=True` alone decodes with the PLATFORM's preferred encoding.

    That is UTF-8 on the runners and cp1252 on a Belgian Windows machine. Every
    licence notice in `assets/i18n.js` carries accented French and Dutch, so
    `src/pages/strings.py` -- the function that guards Statbel compliance --
    died with UnicodeDecodeError there and returned nothing. Statbel's licence
    terminates on non-compliance, so this is not a cosmetic portability nit.

    Asserted over the source rather than by running anything, because the
    failure only appears on a machine whose codepage is not UTF-8, and CI's
    is.
    """
    offenders = []
    for path in _python_files():
        source = path.read_text(encoding="utf-8")
        for call in _CALL.finditer(source):
            body = call.group(0)
            if "text=True" in body and "encoding=" not in body:
                line = source[: call.start()].count("\n") + 1
                offenders.append(f"{path.relative_to(REPO)}:{line}")
    assert not offenders, (
        "subprocess.run(..., text=True) without an explicit encoding decodes "
        "with the platform codepage, which is not UTF-8 on Windows:\n  " + "\n  ".join(offenders)
    )


def test_no_harness_is_passed_to_node_on_the_command_line():
    """Windows caps a whole command line at about 32 KB.

    `tests/test_map_ui_logic.py` concatenated `i18n.js` and `commune_map.js`
    into a single `node -e <harness>` argument -- comfortably over that -- and
    all ten tests in the file died with "[WinError 206] filename or extension
    too long". Linux allows a far larger argument list, which is why CI never
    saw it. A temp file has no such limit on either platform.
    """
    offenders = []
    for path in _python_files():
        source = path.read_text(encoding="utf-8")
        for call in _CALL.finditer(source):
            body = call.group(0)
            if '"-e"' not in body and "'-e'" not in body:
                continue
            # A short inline expression is fine; what is not fine is feeding a
            # variable that holds concatenated file contents.
            if re.search(r'"-e",\s*(harness|script|source|js|body)\b', body):
                line = source[: call.start()].count("\n") + 1
                offenders.append(f"{path.relative_to(REPO)}:{line}")
    assert not offenders, (
        "a whole script passed to `node -e` exceeds the Windows command-line "
        "limit; write it to a temp file instead:\n  " + "\n  ".join(offenders)
    )
