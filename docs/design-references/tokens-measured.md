# Tokens — what's measured, what's estimated, what's a recommendation

Every value below is read off a screenshot in a chat window, not a colour-picker on a source
file. Where that matters, it's marked. See `README.md` for how to tighten these later.

## Colour

Token names below are the actual names in `assets/belpulse/tokens.css` — this table was
initially drafted with placeholder names (`--bp-red`, `--bp-navy`) before the CSS was written,
and drifted from what shipped; a test now catches that
(`tests/pages/test_design_tokens.py::test_the_measured_tokens_document_and_the_css_agree`), and
this version is corrected. The "shipped value" column differs from "screenshot read" wherever a
contrast check moved it — see `tokens.css`'s own comments for the exact before/after numbers.

| Token | Screenshot read | Shipped value | Confidence | Used for |
|---|---|---|---|---|
| `--bp-accent` | `#c8402f` (a muted brick-red, not a pure/saturated red) | unchanged | estimated | primary CTA buttons, active nav underline, "this is the subject" highlight in a comparison |
| `--bp-navy-bg` | `#0e1c3a` | unchanged | estimated | dark-theme background (macro page), sidebar background, footer background |
| `--bp-navy-surface` | `#152647` | unchanged | estimated | card background on the dark theme (one step lighter than the page background) |
| `--bp-blue` | `#3a67e0` | unchanged | estimated | links, chart series, KPI icon backgrounds |
| `--bp-green` | `#2ea562` | **`#278a53`** (darkened) | estimated | a favourable delta, per the indicator's own `preferred_direction` — screenshot value measured 2.94:1 against `--bp-bg`, short of the 3:1 floor |
| `--bp-bg` | `#f8f7f4` | unchanged | estimated | light-theme page background (warm off-white, not pure white) |
| `--bp-surface` | `#ffffff` | unchanged | confident | card backgrounds on the light theme |
| `--bp-text` | `#1a1f2e` | unchanged | estimated | body text on light backgrounds |
| `--bp-text-muted` | `#6b7280` | **`#5b6270`** (darkened) | estimated | subtitles, captions, secondary labels — screenshot value measured 4.51:1, a hair over the 4.5:1 floor with no real margin |
| `--bp-accent-ink` | `#7a2116` | unchanged (light mode); **`#e8836f`** in dark mode | estimated | accent-coloured text. The dark-mode value didn't exist at all in the first pass — rendering the token gallery caught that accent-coloured text on a dark background kept the light-mode value and was unreadable |
| `--bp-border` | `#e5e2da` | unchanged | estimated | card borders, dividers |

**Chart palette — not measured at all, chosen deliberately.** The macro page's
international-comparison chart has 6 bars, each a distinct shade, but a 6–8 colour categorical
palette can't be lifted from a screenshot swatch by swatch — it needs choosing against WCAG
contrast and colour-blind-safe criteria, which a screenshot can't verify. `--bp-chart-1`
through `--bp-chart-8` in `tokens.css` are that deliberate choice. Three of the eight failed
their first contrast check and were darkened with margin: `--bp-chart-4` (amber) measured
1.99:1 against the background — a chart series a reader could barely see — `--bp-chart-6`
(teal) measured 2.68:1, and `--bp-chart-8` (grey) measured 3.03:1, right on the 3:1 floor with
no margin.

## Typography

**CORRECTED 2026-09-07 (Batch 3).** A high-fidelity homepage design supplied after this file was
written shows the H1, every section heading and the dark CTA band set in a **transitional serif**.
The paragraph below — written from a low-resolution screenshot — concluded the opposite. The
redesign therefore does *not* drop this repo's existing Spectral headings; it keeps them, and
`--bp-font-display` in `tokens.css` points at Spectral for exactly that reason. Treat the rest of
this section as the body-text finding it accurately is, not as a claim about headings.

The reference designs use a clean, geometric sans-serif for body and UI text — the original
reading was "no serif anywhere", which is **this repo's existing pages (`local.html`,
`communes.html`) use "Spectral" (a serif) for headings and "IBM Plex Sans/Mono" for everything
else.** The new designs don't use that pairing at all. This is expected — `design_system.md`
already said the new tokens sit alongside the old system until pages are actually converted —
but it means Batch 15 (page conversion) inherits a real type-system change, not just a colour
one.

Cannot reliably identify the exact typeface from a screenshot. Recommending, not claiming:

- **UI and body text**: `Inter` — wide language coverage (handles French/Dutch accented
  characters correctly, which IBM Plex Sans already does too), the de facto standard for this
  visual style, free via Google Fonts.
- **A small decorative/handwritten face** for the pull-quotes only (`"Des données locales pour
  comprendre l'économie réelle."` and similar) — `Caveat` or `Kalam` both match the casual,
  slightly-tilted look shown; either is a reasonable pick, neither is a confident match to a
  specific screenshot.

Scale (estimated from relative proportions in the screenshots, not measured in px):

| Token | Approx. | Used for |
|---|---|---|
| `--bp-text-h1` | `2rem` / bold | page titles ("Microéconomie belge") |
| `--bp-text-h2` | `1.25rem` / semibold | card/section titles |
| `--bp-text-kpi` | `1.75rem` / bold | KPI card figures |
| `--bp-text-body` | `0.9375rem` / regular | paragraphs |
| `--bp-text-small` | `0.8125rem` / regular | captions, deltas, unit labels |
| `--bp-text-eyebrow` | `0.75rem` / semibold / letter-spaced / uppercase | "DONNÉES · TERRITOIRES · PROSPÉRITÉ" |

## Spacing and shape

- Card corner radius: moderate, roughly `8px`–`12px` — soft but not pill-shaped.
- Buttons: roughly `6px`–`8px` radius, or fully pill-shaped for small tags/chips.
- Card padding: generous, roughly `20px`–`24px`.
- Grid gaps: roughly `16px`–`20px` between cards in a row.

## Breakpoints (not shown in any mockup — all four are desktop-only screenshots)

**No tablet or mobile view was supplied.** Every one of the four images is a wide desktop
layout. Batch 1's tablet/mobile breakpoints, and the analytical shell's expected "collapse the
sidebar into a drawer" behaviour on narrow viewports, are **not informed by these designs at
all** — they follow this repo's own existing convention (`assets/commune_map.css`,
`local.html`'s existing responsive rules) until/unless mobile mockups are supplied. Recorded as
an open item, not guessed at.

## Dark-mode handling

The existing shared components (`assets/commune_map.css`) already have a working pattern for
this: light values on `:root`, dark values under `prefers-color-scheme: dark` plus a
`data-theme="dark"` override for an explicit user choice. The new token file follows the exact
same structure rather than inventing a second theming mechanism — see `tokens.css`.

## Re-measured 2026-09-10, against a new reference

The maintainer supplied `homepage.png` — a new reference covering the homepage and, in a
matching image, the commune profile. **The palette had moved**, and the first block-built profile
was built against the old one, which is why it read as almost-but-not-the-design:

| token | Batch 1 (warm) | now (cool) |
|---|---|---|
| `--bp-bg` | `#f8f7f4` | `#f6f8fb` |
| `--bp-surface-alt` | `#f1efe9` | `#eef2f8` |
| `--bp-border` | `#e5e2da` | `#e2e8f0` |
| `--bp-text` | `#1a1f2e` | `#14213d` |
| `--bp-text-muted` | `#5b6270` | `#55617a` |
| `--bp-accent` | `#c8402f` | `#cc2b38` |

**Sampled, not eyeballed.** `homepage.png` was drawn onto a canvas in the same headless Chromium
the screenshot tests already use and read back with `getImageData` — the most common colours by
area (the ground, the card face, the top bar) plus named points (the accent button, the hero
navy). "Looks about right" is how a page ends up almost matching its design, which reads worse
than not matching at all.

Also added: `--bp-icon-*-bg` / `--bp-icon-*-ink`, four tint pairs. The reference gives every
figure a tinted square and **not all the same tint** — population blue, money green, work violet,
housing amber, safety red. Which tint an icon gets is decided in `assets/belpulse/blocks.css` from
the icon's own name, so a block author picks a picture and never a colour.

Every contrast pair in `tests/pages/test_design_tokens.py` still passes on the new values, and the
two deliberate Batch 1 darkenings (`--bp-text-muted`, `--bp-green`) are preserved for the same
reason they were made.

## Papier — designed, not measured (2026-09-14)

Unlike every value above, no screenshot or reference file exists to sample for the Papier theme.
`docs/design-references/maquettes-unification-v1/09-themes.png` (third column) is an AI-generated
mockup, per `docs/features/site_unification.md`: it supplies a mood (cream ground, faint square
grid, pastel accents, dark ink, crisp warm strokes), not pixel values worth reading with
`getImageData` the way `homepage.png` was above. Every value below was therefore chosen directly
against the WCAG floors in `tests/pages/test_design_tokens.py` and tuned until it cleared them with
real margin — the same discipline this file applies whenever a *measured* value fails contrast
(`--bp-green`, `--bp-chart-4`, above), just starting from a design goal instead of a sample.
`--bp-grid-line` is background-only: it never carries text, so it isn't a contrast pair at all and
is transparent in the other two themes so this rule paints nothing there (`assets/belpulse/
layout.css`'s Batch A1.2 block).

| Token | Shipped value | Contrast pair checked | Confidence |
|---|---|---|---|
| `--bp-bg` | `#f4efe3` | -- (background reference for the pairs below) | designed |
| `--bp-surface` | `#fbf8f1` | -- (card background; not itself a text colour) | designed |
| `--bp-surface-alt` | `#ece2cb` | -- (decorative row/card-within-card tint) | designed |
| `--bp-border` | `#d6c7a1` | -- (not text; a crisp warm hairline) | designed |
| `--bp-text` | `#2b2418` | on `--bp-bg` = 13.38:1, on `--bp-surface` = 14.47:1 | designed |
| `--bp-text-muted` | `#5c5240` | on `--bp-bg` = 6.69:1, on `--bp-surface` = 7.24:1 | designed |
| `--bp-text-faint` | `#a89878` | below AA on purpose, same convention as light/dark | designed |
| `--bp-grid-line` | `rgba(139,115,68,.16)` | background-only, not a contrast pair | designed |
| `--bp-accent-ink` | `#9c1f29` | on `--bp-bg` = 6.93:1 | designed (equals the light theme's value) |
| `--bp-accent-soft` | `#f6dfdd` | -- (a filled chip's background, not text) | designed |
| `--bp-icon-*-bg` / `-ink` | see `tokens.css` | not contrast-tested (no CONTRAST_PAIRS entry, same as light/dark) | designed |
| `--bp-navy-bg` | `#ece0c4` | -- (macro's shell, reflavoured paper-toned rather than navy) | designed |
| `--bp-navy-surface` | `#f2e8d2` | -- | designed |
| `--bp-navy-border` | `#d8c69e` | -- | designed |
| `--bp-navy-text` | `#2b2418` | on `--bp-navy-bg` = 11.71:1 | designed |
| `--bp-navy-text-muted` | `#6b5c44` | on `--bp-navy-bg` = 4.95:1 | designed |
| `--bp-chart-1` (blue) | `#3d6fd1` | on `--bp-bg` = 4.17:1 | designed |
| `--bp-chart-2` (coral) | `#c1512f` | on `--bp-bg` = 4.08:1 | designed |
| `--bp-chart-3` (green) | `#3f8a58` | on `--bp-bg` = 3.67:1 | designed |
| `--bp-chart-4` (amber) | `#a8761f` | on `--bp-bg` = 3.47:1 — the tightest of the eight, still with margin over the 3:1 floor | designed |
| `--bp-chart-5` (violet) | `#7156c4` | on `--bp-bg` = 4.78:1 | designed |
| `--bp-chart-6` (teal) | `#227478` | on `--bp-bg` = 4.77:1 | designed |
| `--bp-chart-7` (rose) | `#b45a80` | on `--bp-bg` = 3.88:1 | designed |
| `--bp-chart-8` (grey) | `#6d6353` | on `--bp-bg` = 5.14:1 | designed |

The choropleth ramp (`assets/commune_map.css`, `:root[data-theme="paper"]`) follows the same
approach: a pastel blue seven-step sequential ramp, same step count and "dark = more" convention
as the default light ramp, distinguishable from it at a glance rather than lifted from the
mockup swatch by swatch.
