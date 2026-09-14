"""Tests for assets/belpulse/tokens.css (Batch 1, docs/features/page_builder.md).

The values in this file were read off screenshots of four reference designs, not measured
from a source file (docs/design-references/README.md explains why). That makes these tests
more important than usual: nothing else catches a colour pair that reads fine on screen but
fails contrast, or a token that got typo'd and silently stopped being used.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
TOKENS_CSS = REPO / "assets" / "belpulse" / "tokens.css"


def _srgb_to_linear(c: float) -> float:
    return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4


def _relative_luminance(hex_color: str) -> float:
    hex_color = hex_color.lstrip("#")
    r, g, b = (int(hex_color[i : i + 2], 16) / 255 for i in (0, 2, 4))
    r, g, b = _srgb_to_linear(r), _srgb_to_linear(g), _srgb_to_linear(b)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast_ratio(fg: str, bg: str) -> float:
    """WCAG 2.x contrast ratio between two sRGB hex colours."""
    l1, l2 = _relative_luminance(fg), _relative_luminance(bg)
    l1, l2 = max(l1, l2), min(l1, l2)
    return (l1 + 0.05) / (l2 + 0.05)


@pytest.fixture(scope="module")
def css() -> str:
    return TOKENS_CSS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def tokens(css) -> dict:
    """Every --bp-* custom property defined in the FIRST :root block -- the
    light-mode defaults -- as {name: hex value}.

    Scoped to that one block deliberately. A naive scan of the whole file
    would pick up whichever definition appears LAST -- the dark-mode
    override -- for any token both blocks redefine, silently testing dark
    values against a "light background" label. Caught by this fixture's own
    first version: it graded --bp-accent-ink against #14131a (the dark
    background) while calling it "the light background."  Dark-mode
    contrast is checked on its own terms by
    test_dark_mode_editorial_surface_also_meets_contrast below.
    """
    root_block = re.search(r":root\{(.*?)\n\}", css, re.DOTALL).group(1)
    found = {}
    for match in re.finditer(r"--bp-([\w-]+):\s*(#[0-9a-fA-F]{6})\b", root_block):
        found[f"--bp-{match.group(1)}"] = match.group(2)
    return found


def test_contrast_ratio_helper_matches_a_known_value():
    """Pins the contrast maths itself against a textbook pair (black on white is
    exactly 21:1) before trusting it to grade every other pair below."""
    assert contrast_ratio("#000000", "#ffffff") == pytest.approx(21.0, abs=0.01)
    assert contrast_ratio("#ffffff", "#ffffff") == pytest.approx(1.0, abs=0.01)


# --- token existence and naming --------------------------------------------


def test_every_token_is_semantically_named(tokens):
    """No --bp-red-500, no --bp-blue-2. A rebrand or accessibility fix has to be
    a token EDIT here, not a find-and-replace across every block -- which only
    holds if nothing downstream ever names a raw colour."""
    assert tokens, "no tokens found -- the fixture's own regex may have drifted from the file"
    colour_word_pattern = re.compile(
        r"^--bp-(red|blue|green|navy-\d|grey|gray)-?\d*$", re.IGNORECASE
    )
    offenders = [name for name in tokens if colour_word_pattern.match(name)]
    # --bp-blue, --bp-green, --bp-navy-bg/-surface/-border/-text* are intentional,
    # named exceptions (this repo's own semantic names ARE colour words, because
    # "blue" IS what the link colour means here) -- the pattern above is a trap
    # for a NUMBERED scale creeping in (--bp-blue-500), not for these.
    numbered = [n for n in offenders if re.search(r"-\d+$", n)]
    assert not numbered, f"numbered colour-scale tokens found, should be semantic: {numbered}"


def test_the_measured_tokens_document_and_the_css_agree(tokens):
    """docs/design-references/tokens-measured.md is the record of what was
    observed; tokens.css is what ships. A value can be corrected in the CSS
    (as the green and text-muted tokens were, for contrast) without the two
    drifting apart, as long as the document still names every core token the
    CSS actually defines -- otherwise the doc silently stops being a useful
    record of where a value came from."""
    doc = (REPO / "docs" / "design-references" / "tokens-measured.md").read_text(encoding="utf-8")
    core_tokens = ["--bp-accent", "--bp-bg", "--bp-surface", "--bp-text", "--bp-navy-bg"]
    for name in core_tokens:
        assert name in tokens, f"{name} is documented but missing from tokens.css"
        assert name in doc, f"{name} is in tokens.css but not named in tokens-measured.md"


@pytest.fixture(scope="module")
def root_block(css) -> str:
    return re.search(r":root\{(.*?)\n\}", css, re.DOTALL).group(1)


@pytest.fixture(scope="module")
def dark_media_block(css) -> str:
    """The prefers-color-scheme mechanism's inner :root:not([data-theme="light"])
    block. Its closing brace is indented two spaces (nested inside the media
    query), unlike the other theme blocks below, which sit at the top level."""
    match = re.search(
        r'prefers-color-scheme:\s*dark\s*\)\s*\{\s*:root:not\(\[data-theme="light"\]\)\{(.*?)\n  \}',
        css,
        re.DOTALL,
    )
    assert match, "could not find the prefers-color-scheme dark block"
    return match.group(1)


@pytest.fixture(scope="module")
def dark_explicit_block(css) -> str:
    match = re.search(r':root\[data-theme="dark"\]\{(.*?)\n\}', css, re.DOTALL)
    assert match, 'could not find the :root[data-theme="dark"] block'
    return match.group(1)


@pytest.fixture(scope="module")
def paper_block(css) -> str:
    match = re.search(r':root\[data-theme="paper"\]\{(.*?)\n\}', css, re.DOTALL)
    assert match, 'could not find the :root[data-theme="paper"] block'
    return match.group(1)


@pytest.fixture(scope="module")
def paper_tokens(tokens, paper_block) -> dict:
    """Root tokens overridden by whatever paper's own block redefines --
    the same effective-value merge the browser itself performs when
    :root[data-theme="paper"] is more specific than :root for a shared name."""
    merged = dict(tokens)
    for match in re.finditer(r"--bp-([\w-]+):\s*(#[0-9a-fA-F]{6})\b", paper_block):
        merged[f"--bp-{match.group(1)}"] = match.group(2)
    return merged


# --- theme parity: light, dark (both mechanisms) and paper ------------------


@pytest.mark.parametrize(
    "label,block_fixture",
    [
        ("prefers-color-scheme dark", "dark_media_block"),
        ('data-theme="dark"', "dark_explicit_block"),
        ('data-theme="paper"', "paper_block"),
    ],
)
def test_every_theme_block_only_redefines_tokens_root_declares(request, root_block, label, block_fixture):
    """assets/commune_map.css's own pattern, now with a third theme:
    :root is the base, and every override block -- both dark mechanisms and
    paper -- must be a subset of what :root declares, or a reader in that
    theme silently loses (or gains, undefined elsewhere) a variable."""
    root_names = set(re.findall(r"(--bp-[\w-]+):", root_block))
    block = request.getfixturevalue(block_fixture)
    names = set(re.findall(r"(--bp-[\w-]+):", block))
    assert names, f"{label} defines no tokens at all"
    assert names <= root_names, f"{label}: tokens with no light default: {names - root_names}"


def test_paper_theme_defines_the_full_set_the_dark_theme_redefines(dark_explicit_block, paper_block):
    """Paper is a third full theme, not a partial patch on top of light or
    dark. Every token the dark theme bothers to override (surface, icon
    tints, accent-ink...) is one a page actually depends on differing per
    theme -- plus paper additionally redefines the navy-analytical and chart
    tokens neither dark block touches, so macro's shell and any multi-series
    chart don't fall through to light or dark colours under paper."""
    dark_names = set(re.findall(r"(--bp-[\w-]+):", dark_explicit_block))
    paper_names = set(re.findall(r"(--bp-[\w-]+):", paper_block))
    missing = dark_names - paper_names
    assert not missing, f"paper is missing tokens the dark theme redefines: {missing}"


def test_grid_line_token_exists_in_all_three_themes(root_block, dark_media_block, dark_explicit_block, paper_block):
    """--bp-grid-line (Batch A1.2) paints layout.css's background grid under
    Papier and nothing anywhere else -- but it still has to be an explicit,
    defined token in all three theme blocks, transparent or not, or a reader
    switching theme could hit an undefined custom property."""
    for label, block in (
        ("light (:root)", root_block),
        ("prefers-color-scheme dark", dark_media_block),
        ('data-theme="dark"', dark_explicit_block),
        ('data-theme="paper"', paper_block),
    ):
        assert "--bp-grid-line:" in block, f"{label} does not define --bp-grid-line"


# --- contrast ---------------------------------------------------------------

# (foreground, background, minimum ratio, what it's for)
# 4.5:1 is WCAG AA for normal text; 3:1 is AA for large text (18pt+/14pt+bold)
# or a UI component's own boundary, per the Batch 1 audit's own target.
CONTRAST_PAIRS = [
    ("--bp-text", "--bp-bg", 4.5, "body text on the light background"),
    ("--bp-text", "--bp-surface", 4.5, "body text on a card"),
    ("--bp-text-muted", "--bp-bg", 4.5, "muted/caption text on the light background"),
    ("--bp-accent-ink", "--bp-bg", 4.5, "accent-coloured text on the light background"),
    ("--bp-navy-text", "--bp-navy-bg", 4.5, "body text on the dark analytical surface"),
    ("--bp-navy-text-muted", "--bp-navy-bg", 4.5, "muted text on the dark analytical surface"),
    ("--bp-green", "--bp-bg", 3.0, "a favourable-delta arrow/icon (large-text threshold)"),
    (
        "--bp-accent",
        "--bp-bg",
        4.5,
        "the accent colour used AS text, e.g. an active nav underline label",
    ),
]


@pytest.mark.parametrize("fg_name,bg_name,minimum,purpose", CONTRAST_PAIRS)
def test_contrast_meets_its_wcag_floor(tokens, fg_name, bg_name, minimum, purpose):
    fg, bg = tokens[fg_name], tokens[bg_name]
    ratio = contrast_ratio(fg, bg)
    assert ratio >= minimum, (
        f"{fg_name} ({fg}) on {bg_name} ({bg}) = {ratio:.2f}:1, "
        f"below the {minimum}:1 floor for {purpose}"
    )


def test_white_text_on_the_accent_button_meets_contrast(tokens):
    """The primary CTA button (docs/design-references/*.md: filled red, white
    label) isn't a --bp-* pair -- white is the button's own text colour, not a
    token -- so it's checked directly rather than assumed from the token pass
    above."""
    ratio = contrast_ratio("#ffffff", tokens["--bp-accent"])
    assert ratio >= 4.5, f"white on --bp-accent = {ratio:.2f}:1, below AA for button text"


def test_every_chart_series_colour_is_visible_on_the_light_background(tokens):
    """A chart colour is a graphical object, not body text, so it's checked
    against the 3:1 non-text floor (WCAG 1.4.11) -- but it still needs to be
    checked. The gallery page's own visual check caught this for real: the
    amber series originally measured 1.99:1, a bar or line a reader could
    barely see against the page background."""
    chart_tokens = {
        name: hexval for name, hexval in tokens.items() if name.startswith("--bp-chart-")
    }
    assert len(chart_tokens) == 8, f"expected 8 chart series tokens, found {len(chart_tokens)}"
    bg = tokens["--bp-bg"]
    weak = []
    for name, hexval in chart_tokens.items():
        ratio = contrast_ratio(hexval, bg)
        if ratio < 3.0:
            weak.append(f"{name} ({hexval}) = {ratio:.2f}:1")
    assert not weak, f"chart colours too faint against the background: {weak}"


@pytest.mark.parametrize("fg_name,bg_name,minimum,purpose", CONTRAST_PAIRS)
def test_paper_theme_contrast_meets_its_wcag_floor(paper_tokens, fg_name, bg_name, minimum, purpose):
    """Same CONTRAST_PAIRS, same floors, graded against paper's effective
    values -- including --bp-accent and --bp-accent-soft-adjacent pairs that
    paper does NOT redefine and therefore inherit --bp-bg's new (darker,
    cream) background rather than light's near-white one; a pair that passed
    against #f6f8fb is not guaranteed to pass against #f4efe3."""
    fg, bg = paper_tokens[fg_name], paper_tokens[bg_name]
    ratio = contrast_ratio(fg, bg)
    assert ratio >= minimum, (
        f"paper: {fg_name} ({fg}) on {bg_name} ({bg}) = {ratio:.2f}:1, "
        f"below the {minimum}:1 floor for {purpose}"
    )


def test_every_chart_series_colour_is_visible_on_the_paper_background(paper_tokens):
    """Paper redefines all 8 chart tokens (unlike dark, which leaves them
    alone) -- checked the same way the light-mode chart colours are."""
    chart_tokens = {
        name: hexval for name, hexval in paper_tokens.items() if name.startswith("--bp-chart-")
    }
    assert len(chart_tokens) == 8, f"expected 8 chart series tokens, found {len(chart_tokens)}"
    bg = paper_tokens["--bp-bg"]
    weak = []
    for name, hexval in chart_tokens.items():
        ratio = contrast_ratio(hexval, bg)
        if ratio < 3.0:
            weak.append(f"{name} ({hexval}) = {ratio:.2f}:1")
    assert not weak, f"paper chart colours too faint against the background: {weak}"


def test_accent_ink_is_readable_under_paper(paper_block):
    """The dark-mode equivalent of this check (below) exists because the
    first pass of dark mode forgot to override --bp-accent-ink at all. Paper
    does override it (tokens.css) -- this pins that the override is actually
    present and actually passes, rather than assuming a designed value is
    automatically a legible one."""
    match = re.search(r"--bp-accent-ink:\s*(#[0-9a-fA-F]{6})", paper_block)
    assert match, "paper block does not override --bp-accent-ink"
    bg_match = re.search(r"--bp-bg:\s*(#[0-9a-fA-F]{6})", paper_block)
    assert bg_match, "paper block does not override --bp-bg"
    ratio = contrast_ratio(match.group(1), bg_match.group(1))
    assert ratio >= 4.5, f"paper: accent-ink on paper bg = {ratio:.2f}:1, below AA"


def test_dark_mode_editorial_surface_also_meets_contrast(css):
    """The prefers-color-scheme dark override for the EDITORIAL shell (not the
    deliberate navy-analytical one) needs its own contrast check -- it's a
    different colour pair from the light-mode one tested above."""
    dark_block = re.search(
        r'prefers-color-scheme:\s*dark\s*\)\s*\{\s*:root:not\(\[data-theme="light"\]\)\{(.*?)\n  \}',
        css,
        re.DOTALL,
    ).group(1)
    dark_bg = re.search(r"--bp-bg:\s*(#[0-9a-fA-F]{6})", dark_block).group(1)
    dark_text = re.search(r"--bp-text:\s*(#[0-9a-fA-F]{6})", dark_block).group(1)
    ratio = contrast_ratio(dark_text, dark_bg)
    assert ratio >= 4.5, f"dark-mode text on dark-mode bg = {ratio:.2f}:1, below AA"


def test_accent_ink_is_readable_in_dark_mode_too(css):
    """Found by actually rendering the token gallery in a real browser, not by
    reading the CSS: --bp-accent-ink is documented as "accent text on a light
    background," and the first version of this file had no dark-mode override
    for it at all -- so switching to dark mode kept the light value (a dark
    brownish-red) against a dark background, unreadable. Both dark blocks must
    redefine it, and the redefined value must actually pass."""
    dark_block = re.search(
        r'prefers-color-scheme:\s*dark\s*\)\s*\{\s*:root:not\(\[data-theme="light"\]\)\{(.*?)\n  \}',
        css,
        re.DOTALL,
    ).group(1)
    explicit_block = re.search(r':root\[data-theme="dark"\]\{(.*?)\n\}', css, re.DOTALL).group(1)
    for label, block in (
        ("prefers-color-scheme block", dark_block),
        ("data-theme=dark block", explicit_block),
    ):
        match = re.search(r"--bp-accent-ink:\s*(#[0-9a-fA-F]{6})", block)
        assert match, f"{label} does not override --bp-accent-ink"
        dark_bg = re.search(r"--bp-bg:\s*(#[0-9a-fA-F]{6})", block).group(1)
        ratio = contrast_ratio(match.group(1), dark_bg)
        assert ratio >= 4.5, f"{label}: accent-ink on dark bg = {ratio:.2f}:1, below AA"


# --- rule 36: no fabricated numbers in a design asset ------------------------


def test_the_token_file_names_no_indicator_and_no_commune(css):
    """claude.md rule 36: no indicator, commune or figure is ever hand-typed
    into a design asset. A token file should have zero reason to name either."""
    indicator_shape = re.compile(r"\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+){2,}\b")
    hits = indicator_shape.findall(css)
    assert not hits, f"token file names something indicator-shaped: {hits}"
