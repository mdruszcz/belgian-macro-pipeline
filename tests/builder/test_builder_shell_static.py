"""Batch 12b -- source-level assertions over builder/app/.

These do not need a running server or a browser: they scan the shell's own
source text for the things a reviewer reading for them would still miss one
of (the batch spec's own words). Three separate concerns, each with its own
section below:

* The security constraints that are P0 if broken (no `allow-same-origin` /
  `allow-scripts` anywhere they could reach the preview iframe, no browser
  storage, no external URL, no secret or local path -- `builder/app/` is
  publicly fetchable the moment it is committed, same as `history.js`
  already carries a matching test for in `test_builder_shell_service.py`).
* Invariant 3 / claude.md rule 36, mechanised: no numeral in any shell file
  reads as a plausible indicator value, commune figure or mockup number.
* The `SHELL_JS_FILES` / `SHELL_CSS_FILES` wiring in `src/builder/service.py`
  actually names every file this batch shipped, in the load order the files
  need (a module that reads `shell.dom` at its own top level must be loaded
  after `dom.js`, not merely "somewhere in the tuple").
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.builder.service import BUILDER_APP_DIR, SHELL_CSS_FILES, SHELL_JS_FILES

BATCH_12B_JS_FILES = (
    "dom.js",
    "model.js",
    "api.js",
    "canvas.js",
    "inspector.js",
    "sidebar.js",
    "topbar.js",
    "app.js",
)
BATCH_12B_CSS_FILES = ("shell.css",)

ALL_SHELL_FILES = [BUILDER_APP_DIR / name for name in SHELL_JS_FILES + SHELL_CSS_FILES]


def _text(name: str) -> str:
    return (BUILDER_APP_DIR / name).read_text(encoding="utf-8")


def _code_lines(text: str):
    """Lines with a `//` line comment or inside a `/* */` block stripped out,
    well enough for a numeral scan (not a real JS parser, but every shell
    file in this batch is plain, unminified, and written for this)."""
    without_block_comments = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    lines = []
    for line in without_block_comments.splitlines():
        code = line.split("//", 1)[0] if "//" in line else line
        lines.append(code)
    return lines


# ---------------------------------------------------------------------------
# wiring: every batch-12b file is actually in the tuples, in a safe order
# ---------------------------------------------------------------------------


def test_every_batch_12b_js_file_is_registered():
    for name in BATCH_12B_JS_FILES:
        assert (
            name in SHELL_JS_FILES
        ), f"{name} exists under builder/app/ but is not in SHELL_JS_FILES"
        assert (BUILDER_APP_DIR / name).is_file()


def test_every_batch_12b_css_file_is_registered():
    for name in BATCH_12B_CSS_FILES:
        assert name in SHELL_CSS_FILES
        assert (BUILDER_APP_DIR / name).is_file()


def test_history_js_is_still_first():
    assert SHELL_JS_FILES[0] == "history.js"


def test_load_order_respects_each_files_own_top_level_dependency():
    """Every file is concatenated into ONE closure (the seam contract), so a
    file that reads `shell.X` at its own top level -- not inside a function
    body, which only runs later -- needs X's file earlier in the tuple."""
    order = {name: i for i, name in enumerate(SHELL_JS_FILES)}
    # (file, the shell.* namespace it reads at top level, i.e. outside any
    # function/callback body)
    top_level_reads = {
        "canvas.js": ["dom"],
        "inspector.js": ["dom", "model"],
        "sidebar.js": ["dom", "model"],
        "topbar.js": ["dom", "canvas"],
        "layout.js": ["dom", "model"],
        "app.js": ["dom", "model", "api"],
    }
    providers = {"dom": "dom.js", "model": "model.js", "api": "api.js", "canvas": "canvas.js"}
    for consumer, namespaces in top_level_reads.items():
        for namespace in namespaces:
            provider = providers[namespace]
            assert order[provider] < order[consumer], (
                f"{consumer} reads shell.{namespace} at its own top level, so {provider} "
                f"must be concatenated before it"
            )


def test_no_shell_file_is_named_twice():
    assert len(SHELL_JS_FILES) == len(set(SHELL_JS_FILES))
    assert len(SHELL_CSS_FILES) == len(set(SHELL_CSS_FILES))


def test_every_js_file_under_builder_app_is_registered_somewhere():
    """A file sitting under builder/app/ unregistered would never ship and
    never be checked by anything -- dead, silently."""
    on_disk = {p.name for p in BUILDER_APP_DIR.glob("*.js")}
    assert on_disk == set(SHELL_JS_FILES)
    on_disk_css = {p.name for p in BUILDER_APP_DIR.glob("*.css")}
    assert on_disk_css == set(SHELL_CSS_FILES)


# ---------------------------------------------------------------------------
# security constraints, restated as source scans
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ALL_SHELL_FILES, ids=lambda p: p.name)
def test_no_file_relaxes_the_preview_sandbox(path: Path):
    """Comments are allowed to explain the constraint in full prose (as this
    very file's own docstring, and canvas.js's, both do) -- what must never
    appear is the token OUTSIDE a comment, where it could actually reach an
    iframe's `sandbox` attribute."""
    code = "\n".join(_code_lines(path.read_text(encoding="utf-8")))
    assert "allow-same" + "-origin" not in code, path.name
    assert "allow-" + "scripts" not in code, path.name


@pytest.mark.parametrize("path", ALL_SHELL_FILES, ids=lambda p: p.name)
def test_no_file_touches_browser_storage_or_a_cookie(path: Path):
    text = path.read_text(encoding="utf-8")
    for forbidden in ("localStorage", "sessionStorage", "document.cookie", "indexedDB"):
        assert forbidden not in text, f"{path.name} touches {forbidden}"


@pytest.mark.parametrize("path", ALL_SHELL_FILES, ids=lambda p: p.name)
def test_no_file_names_an_external_url_or_a_local_path(path: Path):
    text = path.read_text(encoding="utf-8")
    assert not re.search(r"https?://(?!$)", text), f"{path.name} names an external URL"
    assert not re.search(r"(?m)^\s*\S*/(home|workspaces|Users)/", text), path.name
    assert "X-BelPulse-Token" not in text, path.name


@pytest.mark.parametrize(
    "path", [p for p in ALL_SHELL_FILES if p.suffix == ".js"], ids=lambda p: p.name
)
def test_no_innerhtml_anywhere_in_the_shell(path: Path):
    """The one exception (the preview `srcdoc`) is a property assignment, not
    `innerHTML`, and lives in canvas.js -- this scan doesn't need to special
    case it. Comments may still mention the word in prose (dom.js's own
    header does, explaining the very constraint this test checks)."""
    code = "\n".join(_code_lines(path.read_text(encoding="utf-8")))
    assert "innerHTML" not in code, f"{path.name} uses innerHTML"


@pytest.mark.parametrize(
    "path", [p for p in ALL_SHELL_FILES if p.suffix == ".js"], ids=lambda p: p.name
)
def test_no_dynamically_injected_script_element(path: Path):
    text = path.read_text(encoding="utf-8")
    assert 'createElement("script"' not in text and "createElement('script'" not in text, path.name
    assert ".appendChild(script" not in text, path.name


def test_srcdoc_is_set_as_a_property_not_assembled_as_markup():
    text = _text("canvas.js")
    assert "previewFrame.srcdoc = " in text or ".srcdoc = buildSrcdoc" in text
    assert 'setAttribute("srcdoc"' not in text and "setAttribute('srcdoc'" not in text


def test_the_shell_never_commits_pushes_or_tags():
    """`RegExp.prototype.exec` is legitimate JS and must not trip this --
    the real concern (claude.md rule 34) is a shell command or a spawned
    process, neither of which a browser-side file could reach anyway."""
    for path in ALL_SHELL_FILES:
        text = path.read_text(encoding="utf-8")
        for forbidden in ("git commit", "git push", "git tag", "child_process"):
            assert forbidden not in text, f"{path.name} contains {forbidden!r}"


# ---------------------------------------------------------------------------
# invariant 3 / rule 36: no fabricated figure anywhere in the shell
# ---------------------------------------------------------------------------

#: Numbers that are legitimately part of the shell's OWN chrome: grid column
#: counts (12/8/4, matching the schema's own desktop/tablet/mobile grids),
#: the three reference viewport widths and the breakpoints they sit at
#: (matching assets/belpulse/blocks.css, read-only, never copied), HTTP-ish
#: bounds, small counters, coalescing/debounce windows, and version numbers.
ALLOWED_BARE_NUMBERS = {
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "8",
    "9",  # part of the `a-z0-9` regex character class, and HTTP 409
    "10",  # parseInt(..., 10): a radix, not a value
    "12",
    "63",
    "64",
    "100",
    "128",
    "200",  # HTTP status
    "390",
    "400",
    "401",  # HTTP status
    "403",  # HTTP status
    "404",  # HTTP status
    "405",  # HTTP status
    "413",  # HTTP status
    "422",  # HTTP status
    "500",
    "640",
    "641",
    "834",
    "1023",
    "1024",
    "1280",
    "1440",
    # Batch 13b, named constants in layout.js, listed so that adding a new
    # magic number to the drag code is a deliberate edit to this set rather
    # than something that slips through review.
    "28",  # ROW_HEIGHT_PX -- editor drawing height for one grid row
    "1200",  # AUTOSAVE_DEBOUNCE_MS
}


@pytest.mark.parametrize(
    "path", [p for p in ALL_SHELL_FILES if p.suffix == ".js"], ids=lambda p: p.name
)
def test_no_javascript_file_contains_a_number_that_reads_as_data(path: Path):
    """A CSS declaration (colours, px/rem sizes) is a separate file and a
    separate test below; this one is JS logic only, where a number has no
    business looking like an indicator value or a commune figure at all."""
    lines = _code_lines(path.read_text(encoding="utf-8"))
    code = "\n".join(lines)
    # String literals are masked out first: prose like "12-column grid" or an
    # error-message example ("e.g. UNEMPLOYMENT_RATE") is UI copy, not a
    # numeral in code, and the E2E/static split already covers copy content
    # elsewhere. What must never appear as a bare LITERAL is a plausible
    # figure used as a value.
    masked = re.sub(r'"(?:[^"\\]|\\.)*"', '""', code)
    masked = re.sub(r"'(?:[^'\\]|\\.)*'", "''", masked)
    numbers = set(re.findall(r"(?<![\w.])\d+(?:\.\d+)?", masked))
    unexpected = numbers - ALLOWED_BARE_NUMBERS
    assert not unexpected, f"{path.name} has unexpected numeric literals: {sorted(unexpected)}"


def test_no_block_library_entry_carries_a_fabricated_figure():
    """The library is name + icon + the registry's own text (sidebar.js) --
    never a thumbnail reading like '8.1%' or '€ 42.3k'."""
    text = _text("sidebar.js")
    assert not re.search(r"€|%\s*\d|\d\s*%", text)
    assert "42.3" not in text and "8.1" not in text


def test_no_placeholder_copy_reads_as_a_plausible_value():
    """Placeholder/help copy names the FIELD, never a plausible value (the
    batch's own wording). A hand-typed example number in a pattern hint is
    fine (e.g. a NIS code SHAPE); a hand-typed number that looks like an
    actual answer is not."""
    for path in ALL_SHELL_FILES:
        if path.suffix != ".js":
            continue
        text = path.read_text(encoding="utf-8")
        for forbidden_example in ("12345", "01000", "99999"):
            assert forbidden_example not in text, f"{path.name} contains {forbidden_example!r}"


# ---------------------------------------------------------------------------
# CSS: only structural / colour numbers
# ---------------------------------------------------------------------------


def test_shell_css_contains_only_structural_numbers():
    text = _text("shell.css")
    # Colours (#rrggbb, rgba(...)) are exempt; everything else must be a
    # small layout number.
    without_colors = re.sub(r"#[0-9a-fA-F]{3,8}", "", text)
    without_colors = re.sub(r"rgba?\([^)]*\)", "", without_colors)
    numbers = set(re.findall(r"(?<![\w.])\d+(?:\.\d+)?", without_colors))
    # Every one of these is a CSS length (rem/px/s), an opacity, a font-weight,
    # or a z-index -- exactly the vocabulary a stylesheet is allowed to carry.
    allowed = {
        "0",
        "0.04",
        "0.15",
        "0.2",
        "0.25",
        "0.3",
        "0.375",
        "0.4",
        "0.5",
        "0.6",
        "0.75",
        "0.8",
        "0.85",
        "0.9",
        "0.95",
        "1",
        "1.1",
        "1.25",
        "1.5",
        "2",
        "10",
        "12",
        "28",
        "30",
        "32",
        "36",
        "50",
        "100",
        "600",
        "700",
        # Batch 13b, the layout editor's own chrome.
        "3",
        "4",
        "6",
        "13",
        "45",  # a 45deg hatch on a locked tile
        "60",
        "90",  # a 90deg column-guide gradient
        "135",  # a 135deg resize-handle corner
        "0.6875",
        "0.8125",
    }
    unexpected = {n for n in numbers if n not in allowed}
    assert not unexpected, f"shell.css has unexpected numeric literals: {sorted(unexpected)}"
