# Feature: site clarity — fewer figures in front, every one of them true

Status: draft — awaiting maintainer approval
Issue: #312
Branch: `docs/312-site-clarity-spec` for this document; one `feat/312-<slug>` branch per batch,
named in each batch below.

Audit record: `docs/reviews/2026-10-04-site-clarity-audit.md`.
Source-side fixes this programme deliberately does **not** make: `docs/decisions/0017-ameco-forecast-periods.md`
and `docs/decisions/0018-stopped-inflation-series.md`, both PROPOSED.

## Problem

A visitor cannot find a figure, and two of the figures they do find are mislabelled.

**Nothing is hidden where it matters.** `commune.html?nis=11002` is 19,289 px tall — 21 desktop
screens, 47.7 phone screens — carries 81 fact tiles and puts **0** figures inside a closed panel.
`macro.html`, the same design, hides 93.6 % of its figures in 16 closed `<details>`.

**Half the figures restate the other half.** 43 of 94 commune indicators are one fact stated twice
(a count beside its own rate, a total beside its parts, a count beside its share). 51 of 94 tiles
are counts or euro totals carrying a rank that only restates how big the commune is.

**The front door costs 27.5 seconds.** On a first visit to `home2.html` the brand film occupies
828 of 900 px at 1440x900 and 11,287,965 bytes download before the first Belgian figure appears at
28,377 ms. LCP is `VIDEO#filmVideo`; CLS is 0.3163, of which 0.1788 fires at 27,952 ms as the film
band collapses. The reduced-motion run proves the alternative: no video requested, first figure at
527 ms, 12 figures above the fold. The commune search below it is inert unless the visitor types the
French name *plus* its NIS code, and at 1366x768 it sits below the fold.

**No figure carries a reading.** 194 of 194 indicator configs have a `definition`; **0** have a
"why it matters" field. A tile's only explanation is a hover tooltip — 81 of 81 tiles, 0 info
buttons — which does not exist on a phone.

**Two published figures are wrong about themselves.** `public/data/national.json` publishes
`LABOUR_COST_BE` 2026 = 142.26 and 2027 = 145.02 with `"status": "final"`. Both are European
Commission AMECO forecasts; 2027 had not begun. `macro.html` shows `145.0 · 2027` as a headline tile
with no status word. And `HICP` stops at `2025-12` = 2.17715, shown on `macro.html`,
`home2.html` and `explorer.html` as current Belgian inflation. NBB and Eurostat both publish 4.2 %
for August 2026 and a 4.6 % flash estimate for September: our page is 2.4 points low on the number a
reader is most likely to check against the news. The Europe panel's inflation is frozen at `2025-12`
for all 36 geographies for the same reason. Nothing failed — both sources moved to a new dataflow
and the old one still answers HTTP 200 with the same 192 rows.

Who this hurts: the paying buyer is a communal finance officer or a consultant. A wrong inflation
figure is the one a journalist checks first; a forecast printed as a measurement is the one that ends
a sales conversation.

## Goal

When this programme is done, all of the following are true and checkable:

1. No page shows a period that ends after the latest period its own payload reports as measured, in
   a headline or current-value slot.
2. Every figure in front shows its own reference date, not the page's build date.
3. A series its source has stopped says so, with the date of its last publication, and is not in a
   "current" slot. It is not deleted, blanked, or turned into a missing value (rule 26).
4. No `unavailable` reason names a series the payload actually carries.
5. The homepage's first screen contains the commune search and two real national figures; the first
   Belgian figure renders in under 1 s on a first visit with no video requested.
6. The brand film is reachable from the About page and plays on a click. It never downloads unless
   the visitor asks for it.
7. Each commune chapter shows about five figures in front; the rest sit behind one control whose
   label says how many figures and how many are withheld.
8. On a phone each commune chapter opens closed, showing its title, its key figure and a one-line
   reading.
9. Every indicator in front carries one sentence of why it matters, in en/fr/nl, containing no digit,
   `%` or `€` — enforced at config load, not at review.
10. Every URL and every `#anchor` valid before this programme is still valid after it (rule 31).
11. `make all` still rebuilds byte-identical output from identical inputs (rule 35).

## Non-goals

- **No source adapter, geography resolution or analytical formula is altered.** The two mislabelled
  series are fixed on the pages in batch 1. The source-side fix waits for ADR 0017/0018 approval
  (rule 19) and is not part of any batch below.
- **No new indicator and no new data source is loaded.** The one proposed row in
  `docs/data_catalog.md` is marked PROPOSED and is not acted on (rule 8).
- **No database schema change** (rule 18).
- **Nothing is deleted.** No figure, no URL, no anchor, no download leaves the site. "Hidden" means
  a closed panel on the same page.
- **No framework.** The public pages stay static HTML, JS and the shared asset modules (rule 17,
  rule 30).
- **Not in this programme:** a ten-figure key row, a comparator picker, a commune-versus-commune
  view, a composed sentence under the commune name (needs `tests/golden/`, which does not exist),
  re-cutting the film, a tooltip gloss layer, serving the homepage at the root, retiring `micro.html`.

## Proposed approach

Six batches in the maintainer's order: wrong numbers, the homepage, the explanatory texts, folding
(two steps), one yardstick. One pull request each. Each batch is a single branch owned end to end by
one `builder`. A "mockup folder" is a folder the maintainer opens himself — never screenshots.

### Dependency to settle before batch 1 opens

PR 2 of issue #309 (branch `feat/309-public-finance-live-ui`, worktree
`C:/Users/marcd/.vscode/belpulse/bp-pf-ui`) is **in progress and unpushed**. Its working tree
already modifies `assets/i18n.js`, `config/national_sections.yaml`, `home2.html`, `macro.html`,
`public/data/metadata/national_sections.json`, `scripts/export_site_payloads.py` and
`tests/test_macro.py`. Specifically it already:

- rewrites `kpis_note` in all three languages;
- **removes** the `public_finance` entry from `unavailable:` in `config/national_sections.yaml`;
- rewrites `homeFinanceUnavailable` and `homeFinanceWhy` and adds six further `homeFinance*` /
  `finLive*` keys;
- replaces the single `public-finance` placeholder card with four real blocks.

Batch 1 therefore **does not** touch `kpis_note`, the `public_finance` unavailable reason, or the
`homeFinance*` keys. Those three findings from the audit are already being fixed. Batch 1 branches
from `#309` PR 2 once it merges, or rebases onto it; it never runs concurrently in the same working
tree. If #309 PR 2 stalls, batch 1 still ships — its own scope (the AMECO years, the HICP date, the
PLCD label) touches none of those four lines.

### Batch 1 — Wrong numbers off the pages

Branch `feat/312-honest-periods`.

**Objective.** Every national figure shows a period its source has actually measured, labelled with
its own date. Two specific wrong figures stop being shown as current.

**Pages and files.** `macro.html` (the KPI row at `:375-383`, `:453-457`, `:651-657`, `:726-734`,
the Prices chapter, the Europe panel), `home2.html` (hero card 2 and national card 3 at `:647-655`,
`:778-784`, `:830-831`, `:885-886` — **not** the commune call at `:1035`), `micro.html` (`:549-557`),
`explorer.html` (`:952-958`, national scope only), `assets/commune_map.js` (one new `MapUI` helper,
the module all four pages already load), `assets/belpulse/europe_map.js` (`:1558`, `:2609`),
`config/indicators/HICP.yaml` and `config/indicators/LABOUR_COST_BE.yaml` (display text only),
`config/national_sections.yaml` (the Prices chapter label, `:180-182`),
`scripts/export_site_payloads.py` (two new deterministic keys), `assets/i18n.js` (new strings only).

**What changes.**

1. **One helper, four pages.** A single function in `assets/commune_map.js` decides whether a period
   may appear in a current-value slot. The rule is narrow on purpose: a `final` whole-year period
   that ends after the series' own `updated` date is not shown as current. Measured against the real
   payloads, that rule hides exactly the two AMECO years and nothing else. The unrestricted version
   ("any period ending after `updated`") also hits `UNEMPLOYMENT_RATE_INSURED` 2026 (a real
   provisional part-year, verified in `national.json` as 5.05), `CONSUMER_CONFIDENCE` and
   `EC_CONS_CONF_BE` 2026-09 (real September surveys, and `EC_CONS_CONF_BE` is a headline KPI that
   is always one month "ahead" of `updated`) — 4 of 45 national series instead of 1. The helper is
   never applied to commune payloads.
2. **Each figure shows its own date.** The tile prints the reference period of the value it displays.
   Today `macro.html:49-52` and `:797-803` show the build date as "Last data update" beside figures
   from twelve different months. `home2.html:830-831` prints a raw `2025-12` with no source date at
   all.
3. **A stopped series says it stopped.** `HICP`'s December 2025 value is real, final and correct —
   it is not missing (rule 26). It keeps its tile, its chart, its `explorer.html` row and its
   download, labelled "last published: December 2025; the source has stopped this series", and it
   leaves the KPI row and the home hero. `scripts/export_site_payloads.py` gains two keys computed
   from config, not from the clock: `stale_after` (period end plus the indicator's staleness
   allowance — 2026-04-03 for HICP) and `superseded_by`. The page only compares dates, so output
   stays byte-identical on an unchanged input (rules 28, 35).
4. **Two labels corrected.** AMECO `PLCD` is "Nominal unit labour costs (ratio of compensation per
   employee to real GDP per person employed)". `LABOUR_COST_BE.yaml:20,26` calls it "Labour Cost
   Index (LCI) — Nominal hourly costs" and "Nominal compensation per employee". Neither is what the
   series measures. `HICP.yaml:34` has `sdmx_code: HICP_INDEX` on a growth-rate series. Display text
   and the code comment only; no indicator id changes (it is part of the `observations` primary key).

**What is reused.** `MapUI` and its `unitLabel`; the existing provenance line; the existing
`estimate` / `provisional` wording on `macro.html:449,569` and `explorer.html:468`; the staleness
allowances already declared in `src/validation/rules.py` (`_STALENESS_DEFAULT_DAYS`).

**Tests.** New: Node tests for the helper, following the pattern of `tests/test_map_ui_logic.py`; a
browser test that no national tile shows a period ending after that series' own `updated`; a payload
test that `stale_after` is derived from config and is stable across two export runs. Changed guards,
reason in the docstring of each: `tests/test_macro_panels.py:434` (one table row per payload period —
it fails for the `conjoncture` chapter under the unrestricted rule and must assert the narrow rule);
`tests/test_macro_panels.py:380` and `tests/test_charts_tooltip.py:142` (last drawn point = last
payload period; charts keep every period, only current-value slots are filtered, so these must say
so explicitly rather than be relaxed); `tests/test_explorer.py:109` (explorer must not show fewer
rows than its own download). Unchanged and must stay green: `tests/test_macro.py:67`,
`tests/test_explorer.py:68`, `tests/test_home2.py:128` (no indicator ids in page code — rule 24).

**Effort** S–M. **Risk** high: this is the data-integrity batch, and the helper is one function that
four pages trust. **Mockup** no — label and date text, not a redesign. **Ceremony** lead → builder →
auditor (data integrity, rules 6, 26, 28, 35) → lead's final decision.

### Batch 2 — The front door

Branch `feat/312-home-data-hero`.

**Objective.** The homepage opens on data. The film moves to the About page and plays on a click.

**Pages and files.** `home2.html`, `assets/i18n.js`, `config/national_sections.yaml` (the `hero`
block), `config/pages/about/published.json`, `assets/belpulse/blocks/registry.json`,
`src/pages/render.py`, `src/pages/schema.py` (if the props schema is enforced there),
`tests/test_home_film_hero.py`, `tests/test_home_film_hero_browser.py`,
`assets/belpulse/home-film/` (no new file).

**What changes on the homepage.** A one-line headline; a one-sentence lead; the commune search with
a visible trilingual label and a submit control, matching French **and** Dutch commune names in all
three languages, with Enter falling back to the filtered directory; a row of two real national
figures (`GOV_DEBT_PCT_GDP_BE` 107.9 % of GDP, 2025, final; `GOV_BALANCE_PCT_GDP_BE` -5.2 %, 2025,
provisional — both already in `national.json`, verified); one quiet "see an indicator on the map"
link. The map card, featured commune, finance strip and indicator grid move one screen down,
unchanged. Same batch: convert the two decorative PNGs (4,923 KB) and serve one poster, not two
(`poster.jpg` 243,384 B + `poster.webp` 238,284 B).

**What changes on the About page.** `about.html` is generated from
`config/pages/about/published.json` through typed blocks; it uses `hero` and `rich_text` today.
The film needs a **new typed block**, not a change to an existing one. Add `video`, version 1, to
`assets/belpulse/blocks/registry.json`, mirroring the existing `photo` type: `src` and `poster` as
`$defs/relative_url` (site-relative only — rule 23 forbids a remote URL in a page definition);
trilingual `alt`, `caption` and `overlay_title`; `additionalProperties: false`. The renderer in
`src/pages/render.py` emits `<video controls preload="none" poster=...>` with **no** `autoplay` and
**no** script in the page document (rule 22): the click-to-play behaviour is the browser's own
controls, so nothing executable enters `published.json`. The registry's own
`forward_compatibility` note allows adding a block type and forbids changing or removing one, which
is exactly what this does. The film files are reused in place
(`assets/belpulse/home-film/film-1080.mp4` 11,287,965 B, `film-720.mp4` 5,147,261 B, `poster.jpg`);
no re-encode, no new binary (rule 12).

**Tests.** `tests/test_home_film_hero.py:78,:101,:111` and the browser test change in this PR, each
with the reason in its docstring — they currently pin the film *to the homepage*, which is the
design being reversed. New: a registry test that `video` v1 exists, that `src` rejects an absolute
URL, and that the rendered About page contains no `<script>` and no `autoplay`; a browser test that
a first visit to `home2.html` requests no video and renders a Belgian figure above the fold; a test
that the search matches a Dutch name in French and a French name in Dutch.

**Effort** S–M. **Risk** medium: reverses a design approved on 2026-09-28 (PR #300/#301), and adding
a block type is a registry change. **Mockup** yes — a folder with the new homepage and the new About
page, opened by the maintainer himself, before the PR. **Ceremony** lead → builder → auditor (the
registry addition and rules 22/23/30) → builder fixes.

### Batch 3 — Why it matters, wave 1

Branch `feat/312-why-it-matters`.

**Objective.** Every figure in front gets one sentence saying why it matters, with no number in it.

**Files.** `docs/features/indicator_config.schema.json` **and**
`docs/features/derived_indicator_config.schema.json` (both — the finance ratios are derived;
170 source + 24 derived = 194 configs), the configs that get wave-1 text,
`src/exporters/metadata.py`, `scripts/export_site_payloads.py` (add `definition` to
`national.json`), `config/local_sections.yaml` and `config/national_sections.yaml` (chapter leads),
`assets/i18n.js`, the commune/macro/micro chapter lead cards.

**What changes.** Two optional keys per indicator, `why_it_matters` and `how_to_read`, each
`{en,fr,nl}`, one or two sentences, 45 words maximum, declared in both schemas with a pattern that
forbids digits, `%` and `€`. A typed figure then fails at config load, not at review (rule 36). The
exporter writes each key only when present, so every other row of every payload stays
byte-identical. One sentence appears in each chapter's lead card, **replacing** the current
69–141-word method paragraph, which moves to a closed "About these figures" block at the chapter's
end; one under the headline cards on home and macro; one line under the map picker. Nothing goes
inside the six key-figure cells. Free alongside: `definition` into `national.json` — 2 of 16
national chart cards have any explanation today, and 13 indicators carry a trilingual hint no page
shows.

**Guards.** A lint that forbids a verdict word where `preferred_direction` is `neutral` or
`contextual` — 62 of 107 municipal indicators, plus 24 with no direction — reusing the map's
existing "context, not a score" sentence. The same lint runs over the 30 existing chapter blurbs,
21 of which contain digits, including one French blurb reading "6,28 % en février 2026 à 4,41 % en
avril" which its own payload already contradicts (3.63 at 2026-07).

**How the text is written.** Drafted only from each indicator's own `definition` and the
maintainer's existing English `description` (186 files, median 56 words, holding the caveats a
reader needs — "per TAX RETURN, not per inhabitant"). Every clause that goes beyond those two
sources is listed for him; he rewrites it in his own words ([H] step). `description` is never
published as it stands: 163 of 186 are English-only (rule 7). One proofreading sheet per language
for a native reader ([H] step). The first text written is the ONEM insured-rate caveat.

**Tests.** Schema tests: a digit, a `%` and a `€` each rejected in each of the three languages, in
both schemas. A lint test with a verdict word on a `contextual` indicator. A byte-identical
re-export test on the configs that have no new key. Hand-checked: the ten wave-1 sentences render in
the right chapter in all three languages.

**Effort** M. **Risk** medium: the schema pattern and the lint are what keep every later text safe,
so a weak pattern is a lasting hole. **Mockup** no. **Ceremony** lead → builder → auditor (the two
schemas and the lint) → builder fixes.

### Batch 4 — Five figures in front per chapter (folding, step 1)

Branch `feat/312-chapter-detail-tier`.

**Objective.** 33 of 94 commune figures in front; the rest behind one control per chapter whose
label says how many.

**Files.** `config/local_sections.yaml` (a new per-section `detail:` list),
`scripts/export_site_payloads.py` (`_check_sections` at `:522`), `commune.html`,
`assets/belpulse/*` chapter CSS/JS, `docs/features/commune_portrait.md`, its "Structure" section
(amended **first**, in the same PR).

**What changes.** Per chapter, about five figures in front and the rest in a `detail:` list:
demography 5 of 21, origins 2 of 5, households 2 of 7, income 2 of 5, social 2 of 4, housing 5 of 20,
business 3 of 3, employment 3 of 10, safety 4 of 4, finances 5 of 15 — 33 of 94 in front. Per
redundancy cluster the comparable form stays (rate, share, total, ratio) and the redundant form
folds. Each chapter keeps its chart, its map and its neighbours box. `#chapter-finances` opens its
panel. The six key figures come first on a phone.

`config/local_sections.yaml` has **eleven** sections and ten of them carry figures (verified by
loading the file: demography 21, origins 5, households 7, income 5, social 4, housing 20, business 3,
employment 10, safety 4, finances 15 = 94; `mobility` carries only its trilingual `unavailable` note
and no figures). `age_structure` is a composition block above the `sections:` list, not a chapter.
So the sticky menu shows ten chapters plus the mobility note, and nothing in this batch changes that
count.

**The label carries the withheld count.** 405 latest-period cells are suppressed or not-applicable
across 287 of 565 communes, concentrated on exactly the indicators this batch folds. A control
reading "Show 16 more" when 7 of them are withheld hides a gap behind a number (rule 26), so the
label says both.

**The validator must be extended, and this is the real risk.** `_check_sections` validates
`headline`, `indicators`, `headlines` and composition `parts`/`whole` — verified by reading it. A
new `detail:` key would not be validated, so a typo renders empty boxes on 565 pages with no test
failing. Extending it is part of this batch, not a follow-up.

**Honest effect.** Desktop 23 → about 18 screens; phone 54 → about 38. The audit's "7–8 screens" is
wrong: only folding whole chapters gets under ten, which is batch 5.

**Tests.** `_check_sections` rejects an unknown id in `detail:`. Every indicator in front or in
detail appears exactly once. The panel label's counts match the payload for a commune with
withheld cells. Every `#anchor` valid before the change resolves after it (rule 31). A phone-width
browser test that the six key figures precede chapter one.

**Effort** M. **Risk** medium. **Mockup** yes — a folder, two or three real communes including one
with withheld cells. **Ceremony** lead → builder → auditor (rules 26 and 31) → builder fixes.

### Batch 5 — Closed chapters on phones (folding, step 2)

Branch `feat/312-phone-closed-chapters`.

**Objective.** On a phone each commune chapter opens closed: title, key figure, one-line reading.
About six screens.

**Files.** `commune.html`, the chapter CSS/JS, `config/local_sections.yaml` (which figure and which
reading a closed chapter shows — config, not page logic, rule 24).

**What changes.** Phone width only; desktop is untouched. A closed chapter shows its title, one key
figure and one line of reading. Opening is a click; deep links keep working — a URL with a
`#chapter-*` anchor opens that chapter and scrolls to it (rule 31). No figure is removed from the
DOM in a way that breaks an in-page anchor or the browser's own find.

**Tests.** At 390 px: all ten figure-carrying chapters closed, each showing exactly one figure, and
the `mobility` note unchanged (it has no figure to show). A `#chapter-income`
URL opens income. Every anchor still resolves. Desktop output unchanged — a before/after DOM
comparison at 1440 px.

**Effort** M. **Risk** medium: an anchor that stops working is a broken public URL. **Mockup** yes.
**Ceremony** lead → builder → auditor (rule 31) → builder fixes.

### Batch 6 — One yardstick

Branch `feat/312-one-yardstick`.

**Objective.** Every figure in front is compared to the same thing, said the same way, with the
scope named.

**Files.** `commune.html`, `config/local_sections.yaml`, `assets/belpulse/similar.js`,
`comparables.html`, `assets/i18n.js`.

**What changes.** The peer median and the commune's position beside every front figure, with the
scope named in the label. The rank moves to the detail tier. One "Comparable communes" explanation
instead of seven.

**Why the peer median and not a province line.** Of ten candidate headline indicators only three
have a province/region/Belgium aggregate; all ten have a peer median in `public/data/peers/*.json`.
72 of 107 indicators have any aggregate at all, so a province line's fallback is the design, not an
edge case. And the scopes disagree: Namur's burglaries are 7th nationally and 11th regionally;
`MUN_DEBT_TO_REVENUE` has no national row at all (withheld, `few_peers`) and is +38.4 % regionally.
A comparison whose scope is not printed is a number a reader cannot check.

**Tests.** Every front figure has a comparator or is not a front figure. The scope in the label
equals the scope in the payload for a sample of communes across all three regions, including one
with `peer_deviation: none` and one with `few_peers`. No ratio is averaged across geographies in the
browser (rule 27).

**Effort** M. **Risk** medium: a comparison against the wrong universe is a wrong number.
**Mockup** no. **Ceremony** lead → builder → auditor (scope correctness, rule 27) → builder fixes.

### Queued behind batch 6, order not yet set

- **No absurd numbers, and plain words.** `comparables.html` honours the same `peer_deviation` flag
  `assets/belpulse/similar.js:90` already honours; no percentage or rank on an additive row; two
  `i18n` grammar slips ("of the the province"); the duplicate micro link; the per-language About URL
  via `route_for()`; About rewritten honestly with a contact address. S. builder only.
- **Defaults.** `map_default:` as its own key — `map.html:1243` takes the map's opening indicator
  from `sections.headlines`, so re-ordering the commune hero row silently re-orders the map. A
  Headline group first in both pickers. "Macro"/"Micro" renamed in three languages with the six nav
  links unchanged. `explorer.html` on the shared shell with a footer link — it is linked from no live
  page today, which is why nothing in this programme moves a figure there. S–M. builder only.
  **Open:** whether debt and the balance also belong in the `macro.html` KPI row. #309 PR 2 puts them
  in the Finances publiques chapter; that may be enough.
- **Walloon finance ratios in front.** Blocked on the maintainer's own two validation steps, and
  requires saying plainly on the 305 Flemish and Brussels profiles that these figures cover Wallonia
  only (rule 26). S. builder + auditor.

## Data / schema changes

No change to `observations`, `indicators`, `geo`, `forecasts` or `fetch_log`. No migration.

Configuration changes, all additive:

- `docs/features/indicator_config.schema.json` and
  `docs/features/derived_indicator_config.schema.json`: two optional keys, `why_it_matters` and
  `how_to_read`, trilingual objects with a pattern forbidding digits, `%` and `€` (batch 3). Both
  schemas are `additionalProperties: false`, so this declaration is required before any config can
  carry the keys.
- `config/local_sections.yaml`: a per-section `detail:` list, validated by `_check_sections`
  (batch 4); the closed-chapter key figure and reading (batch 5); `map_default:` (queued).
- `config/indicators/HICP.yaml`, `config/indicators/LABOUR_COST_BE.yaml`: display text and a code
  comment only (batch 1). No `id`, `unit`, `frequency` or `fetch` change.
- `assets/belpulse/blocks/registry.json`: a new block type `video`, version 1 (batch 2). An
  addition, which the file's own `forward_compatibility` rule permits; no existing type, version or
  prop schema is renamed, narrowed or removed.
- `public/data/national.json` and the explorer index gain `definition` (batch 3) and the two
  deterministic keys `stale_after` and `superseded_by` (batch 1). Both computed from config, never
  from the clock, so an unchanged input still rebuilds byte-identical (rule 35).

**Source-side changes, not made here.** `docs/decisions/0017-ameco-forecast-periods.md` (forecast
periods stored as `final`) and `docs/decisions/0018-stopped-inflation-series.md` (the NBB dataflow
that stopped) are both PROPOSED. Until the maintainer approves them, `src/fetchers/dbnomics.py`,
`src/fetchers/nbb.py`, `config/indicators/HICP.yaml`'s `fetch.query`,
`config/indicators/HICP_ANNUAL_RATE_EUROPE.yaml`'s `fetch.dataset` and
`scripts/sync_to_canonical.py` are untouched (rule 19). The stored rows and
`data/belgian_macro_export.csv` keep saying `final`; batch 1 fixes what a reader sees, not what is
stored, and says so in the PR body.

## New data sources

None loaded. ADR 0018's recommended fix would need one Eurostat dataset, `prc_hicp_minr`. Its row is
in `docs/data_catalog.md` under **"PROPOSED — awaiting maintainer approval"** and is explicitly not
approved. Per rule 8 nothing fetches it until the maintainer approves that row.

## Tests

Per batch, listed above. Programme-wide, every batch must leave these green:

- `tests/test_macro.py:67`, `tests/test_explorer.py:68`, `tests/test_home2.py:128` — no indicator id
  in page code (rule 24).
- `tests/test_international_stays_off_belgian_pages.py:28,76-111` — the multi-country row stays off
  Belgian pages. This guard followed a real leak of 755 duplicate rows; no batch here relaxes it.
- `tests/test_primary_navigation.py` — the six-destination top bar.
- The `make all` byte-identical rebuild tests (rule 35).
- `ruff`, `black`, and the full `pytest` suite, with the actual output in each PR body (rule 9).

Three existing guards are *expected* to change, each in the batch that changes the behaviour they
pin, with the reason in the test's own docstring: `tests/test_macro_panels.py:434`,
`tests/test_home_film_hero.py:78,:101,:111`, and `_check_sections`'s own test in
`tests/test_compositions.py`.

## Assumptions and open questions

**For the maintainer, in the order they block work:**

1. **Which unemployment rate leads on a commune page?** The audit recommends the ONEM insured rate:
   it is the only one covering all 565 communes and the only one with a peer median. It needs one
   sentence saying a fall can mean a benefit-rule change rather than more people working. Blocks the
   first wave-1 text in batch 3.
2. **ADR 0017 and ADR 0018.** Until they are approved, the stored status stays wrong and inflation
   stays frozen at December 2025 in the store. Batch 1 makes the pages honest about both; it cannot
   make them current.
3. **Flanders and Brussels municipal finances.** 305 of 565 communes have no debt, revenue or
   expenditure figure, so a Flemish finance director finds a tax rate and five land-registry totals.
   The recommendation is to say so plainly on those profiles now and decide the ABB/IBSA source
   separately — the biggest product gap, not a layout question.
4. **The two Walloon finance-ratio validation steps** (each definition against real budget documents;
   three communes against their published budgets). His own, and until they are done
   `MUN_DEBT_TO_REVENUE` cannot go in front — the figure the paying buyer opens the page for.

**Assumptions this spec makes, any of which he can overturn:** that "about five figures per chapter"
means the per-chapter counts in batch 4, not exactly five everywhere; that the film's existing cut
and files are reused as they are; that the About page is the right home for the film rather than a
page of its own; that the two national figures on the new homepage are debt and the balance.

**Not established.** Whether the simulated live counters from #309 PR 2 should also appear on the
new homepage hero — that depends on a design the maintainer has not seen yet, so batch 2 leaves the
finance strip where #309 puts it and does not move it into the first screen.

## Rollout / risks

**The riskiest batch is the first one.** One helper function in `assets/commune_map.js` decides what
four pages treat as a current value. Too broad and it hides four real series (verified: 4 of 45
national series under the unrestricted rule, including a headline KPI); too narrow and the AMECO
years stay visible. This is why the rule is written as "a `final` whole-year period past its own
`updated` date" and why the helper gets its own tests rather than being trusted because the page
looks right.

**Reversing an approved design.** Batch 2 undoes the film hero the maintainer approved on
2026-09-28 and shipped in PR #300/#301. The mockup folder exists so he sees the replacement before
the PR, and the film is not deleted — it moves.

**A broken public URL.** Batches 4 and 5 hide figures inside panels. Rule 31 says every existing URL
and anchor stays valid, so an anchor test is a hard gate in both, not a nice-to-have.

**Determinism.** Batches 1 and 3 add keys to published payloads. Both derive them from config, never
from the clock, and both carry a byte-identical re-export test. A key computed from "today" would
make every daily build differ from the last one and break rule 35 silently.

**What happens if a batch is wrong.** Each batch is one PR against `develop`, merged by the
maintainer, and nothing here writes to the database or publishes automatically. The worst case is a
page that reads badly for one day and a revert.

**Why no reader caught the two wrong figures, which is the risk worth fixing beyond this
programme.** Staleness is a `WARN` by design, and `src/validation/rules.py:51-52` names HICP in its
own comment as "stuck on 2025-12". Warnings reach the Actions summary and the daily PR body, which
auto-merges — PR #308 merged in 12 minutes. `orchestration/checks.py:123-124` ignores warnings, so
the page prints "Validation passed". No issue and no `known-risks.md` row tracked it. The adapter
could not have noticed: the dead NBB dataflow still answers HTTP 200 with 192 rows. Making a named,
known-stale series a build failure rather than a warning is a separate change, and it belongs in the
validation layer, not here.
