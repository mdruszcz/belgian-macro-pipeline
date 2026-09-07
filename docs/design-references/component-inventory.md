# Component inventory — de-duplicated across all four designs

What Batch 2 (`docs/features/page_builder.md`) actually builds. Cross-referenced against the
block list already sketched in that spec's Batch 10 section; nothing here invents a block type
outside that list, it just confirms which of them are real (seen in a design) versus
speculative, and adds detail the original list didn't have.

## Shell / chrome

- **Top bar** — two variants: analytical (light or dark, no account button except macro's "Mon
  espace") and editorial (light only, breadcrumb below it).
- **Left sidebar (analytical shell)** — icon nav list, active-item highlight, a status card
  (last-updated + auto-update indicator), an editorial pull-quote card, a mini footer.
- **Right sidebar (editorial shell, municipality profile only)** — peer comparison picker,
  ranking list, neighbours list, methodology blurb, utility links.
- **Breadcrumb** — editorial shell only.
- **Section tabs** — municipality profile only (Vue d'ensemble / Population / Économie / …).
- **Dark CTA band** — full-width, appears once near the bottom of both editorial pages. Title +
  description + two buttons + a row of 3 stat callouts.
- **Footer** — two variants (light, on the analytical-micro page only; dark, everywhere else).

## Data components

- **KPI card** — icon (coloured circular/rounded-square background), label, value, delta with
  direction-aware colouring (reads `preferred_direction`, doesn't hardcode up=green), optional
  sparkline. Seen in three size variants: full (macro/micro rows), compact (homepage), mini
  (municipality profile's "chiffres clés" row, which adds a small chart under the value).
- **KPI grid** — a row of KPI cards, 4–6 wide, responsive.
- **Stat tile** — simpler than a KPI card: icon, label, value, delta, no sparkline. Used in 2×2
  or 2×3 grids (municipality profile hero, homepage finance band).
- **List panel** ("Indicateurs clés") — icon + label + value + unit caption, stacked rows, no
  chart. A simpler sibling of the KPI grid for indicators that don't need a trend.
- **Chart container** — wraps a chart with a title, an optional subtitle, an optional period or
  indicator dropdown, and a consistent card frame. The dropdown itself is a real interaction
  question for Batch 10 — see "Open question" in `micro.md`.
- **Line chart**, **bar chart** (single and grouped/multi-series), **donut chart**, **horizontal
  bar / ranking chart** (with one item visually highlighted as "the subject," e.g. Belgique
  among provinces, or Namur among neighbours) — four chart shapes, all seen more than once.
- **Choropleth map** — the existing shared map component (`assets/commune_map.js`), restyled
  into a card frame with a dropdown and legend to match. Not a new implementation — see rule 29.
- **Comparison table** — indicator rows, this-geography column, one or more reference columns
  (regional/national average), a delta column per reference. Maps onto the existing `comparison`
  payload shape directly.
- **Ranking list** — "N / 581" style rows. Real for a single indicator's percentile (this
  pipeline already computes this); **not real** for the named composite rankings shown in the
  municipality-profile sidebar ("Dynamisme économique," etc.) — those need Peer Model / Fiscal
  Score (Blocks M/P), not built yet. Build the component to accept either; don't fabricate the
  composite scores to fill it early.
- **News/article card** — thumbnail, date, headline, excerpt. No content source exists in this
  pipeline for this yet (no CMS, no article store) — this is editorial content, not pipeline
  data, and is out of scope for the binding layer entirely. Static/manually-authored for now.
- **Simulated live counter** — the "Finances publiques (compteurs en temps réel, simulés)"
  component. Explicitly labelled as simulated in its own design; keep that label. See `macro.md`
  for the full note — this is the one component that is not, and should never silently become,
  a real-time data feed.
- **Editorial pull-quote** — a short italic/handwritten-style line + a small decorative flourish
  (Belgian flag stripe, town-silhouette line art). Purely decorative, appears on every page in a
  slightly different spot; worth being one component with a position prop rather than four
  one-offs.
- **Commune preview card** — photo + stat grid + link-through, seen on the homepage. Needs the
  photo/tagline question resolved (see `municipality-profile.md`).
- **Feature/benefit tile** — icon + title + one-line description, no data (the homepage's
  four-icon band, the municipality profile's "Focus thématiques" six-card grid).
- **Chip/tag list** — small rounded link buttons (homepage's commune-name chips).
- **Freshness badge** — this pipeline already has exactly this concept live (the provenance
  badge, `docs/features/provenance.md`) and none of the four mockups show a per-figure grade
  chip the way `/local` already does — worth deciding whether the redesign keeps that visible
  detail or moves it behind a tooltip/hover, since it doesn't appear explicitly in any of the
  four designs but the pipeline already promises it on every figure.

## Not yet buildable against real data (checked, not assumed)

| Component | Needs |
|---|---|
| International GDP comparison (macro page) | `EUROSTAT_GDP_Q_MEUR_{DE,EA,ES,FR,NL}` configs exist but have zero loaded observations (pre-existing fetch issue); Poland isn't configured at all |
| Municipal finance stats/chart (profile page) | Block O (Walloon municipal finance) — not built |
| Named composite rankings (profile page sidebar) | Peer Model / Fiscal Score (Blocks M/P) — not built |
| Sector-level population map (profile page) | Statistical-sector boundary geometry — this pipeline only has commune-level boundaries |
| Age breakdown at 4 bands (profile page donut) | This pipeline stores 3 bands (0–14, 15–64, 65+), not the mockup's 4 (0–17, 18–34, 35–64, 65+) |
| News/article cards (micro page, homepage) | No article/CMS content source exists |
| Commune hero photo + tagline (profile page) | No photo library or per-commune editorial copy exists |

None of these block Batch 1 or Batch 2 (tokens and components can be built and tested with
placeholder/fixture data, same as every other block already is). They block the *specific*
instances of Batches 4 and 6 that need them — worth sequencing those sub-parts last within their
batches, or shipping the page with an honest "coming soon" for that one panel, exactly the
pattern this pipeline already uses for Mobility and Finances on the current `/local` page.
