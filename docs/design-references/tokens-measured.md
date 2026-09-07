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

The reference designs use a clean, geometric sans-serif throughout — no serif anywhere, which is
a real mismatch worth naming plainly: **this repo's existing pages (`local.html`,
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
