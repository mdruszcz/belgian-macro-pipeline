# Review: "too much information, people are lost"

Date: 2026-10-04
Scope: the whole public site — `home2.html`, `commune.html`, `macro.html`, `micro.html`,
`map.html`, `explorer.html`, `comparables.html`, `about.html` — plus the indicator configs and the
payloads they read. Issue #312. Nothing was implemented; the resulting plan is
`docs/features/site_clarity.md`.

> "A visitor lands on the page and cannot find anything. There is too much information."
> — the maintainer, 2026-10-03

Method: six lenses, each measured against a read-only copy of `origin/develop` at commit
`d274d5a0c` (`C:/Users/marcd/.vscode/belpulse/bp-site-audit`), served locally and driven with
Playwright/Chromium at 1440x900 and 390x844, in en/fr/nl — plus the live GitHub Pages site where a
lens needed to confirm the local copy renders the same. Then critic passes over the lenses' own
numbers, then this synthesis. Nothing was accepted because a lens asserted it: every number carried
forward below is either traced to the file and line that produces it, or marked as reported only.

| Lens | Question | Report |
|---|---|---|
| `inv` | What figures exist, and how many say the same thing twice? | `inv.md` |
| `ux` | What does a first-time visitor actually do? | `ux.md` |
| `home` | What does the homepage cost before it shows a figure? | `home.md` |
| `why` | Does any figure explain itself? | `why.md` |
| `val` | Which indicators are worth a reader's attention? | `val.md` |
| `bench` | How do comparable sites avoid the same overload? | `bench.md` |

Three critic passes were run over the lenses. Two left written reports, `critic_product.md`
(independent Playwright runs) and `critic_rules.md` (a read-only pass for rule violations and
feasibility). The third left no file in the audit folder, so nothing in this record rests on it
alone. All reports are under
`C:/Users/marcd/.vscode/belpulse/screenshots-review/site-audit-2026-10-04/`.

## Diagnosis — five causes, with their evidence

**D1 — nothing is hidden on the page where it matters.** `commune.html?nis=11002` renders
19,289 px: 21 desktop screens, 47.7 phone screens. 81 fact tiles. **0** figures inside a closed
panel. `macro.html`, built from the same Portrait design, hides 93.6 % of its figures inside 16
closed `<details>`. The commune page is not a design that failed to fold; it is a design where
folding was never switched on.

**D2 — half the figures restate the other half.** 43 of 94 commune indicators are one fact stated
twice: a count beside its own rate, a total beside its own parts, a count beside its share. 51 of
94 tiles are counts or euro totals carrying a rank that only restates how big the commune is. The
fix is not deletion: per redundancy cluster the comparable form (a rate, a share, a ratio) stays in
front and the other form folds.

**D3 — the front door costs 27.5 seconds.** On `home2.html` at 1440x900, first visit: the film band
occupies 828 of 900 px; 11,287,965 bytes of video download; the first Belgian figure paints at
28,377 ms. LCP is `VIDEO#filmVideo`. CLS is 0.3163, of which 0.1788 fires at 27,952 ms as the band
collapses. Per desktop visit, 16,944 KB. The 1080p variant is served even on a throttled 4G link,
because `isSlowConnection()` catches only `saveData` and `slow-2g`/`2g`/`3g`. The
`prefers-reduced-motion` run is the control case and proves the alternative: no video requested,
first figure at 527 ms, 12 figures above the fold. Separately, the commune search is inert unless
the visitor types the French name *plus* its NIS code, and at 1366x768 it sits below the fold.

**D4 — no figure carries a reading.** 194 of 194 indicator configs (170 source + 24 derived) have a
`definition`; **0** have any "why it matters" field. A tile's only explanation is a hover tooltip —
81 of 81 tiles, 0 info buttons — which does not exist on a phone. One profile line ranks a commune
against four different universes (565 / 581 / 552 / 388) without saying which.

**D5 — the site contradicts itself in public.** Four separate instances:

- `config/national_sections.yaml` and `assets/i18n.js:252/1069/1825` state in three languages that
  public debt and the fiscal balance have no national series, while `public/data/national.json`
  carries `GOV_DEBT_PCT_GDP_BE` = 107.9 (2025, `final`) and `GOV_BALANCE_PCT_GDP_BE` = -5.2 (2025,
  `provisional`).
- `LABOUR_COST_BE` publishes 2026 = 142.26 and 2027 = 145.02, both `"status": "final"`. Both are
  European Commission AMECO forecasts. 2027 had not begun. `macro.html` prints
  `145.0 · 2027` as a headline tile with no status word.
- `HICP` stops at `2025-12` = 2.17715 and is shown as current Belgian inflation on three pages,
  while both NBB and Eurostat publish 4.2 % for 2026-08 and a 4.6 % flash estimate for 2026-09.
  The Europe panel's inflation is frozen at `2025-12` for all 36 geographies.
- Nine empty cards, and two grammar faults in published French and Dutch ("of the the province").

## Measured evidence I re-checked myself

Re-run in `C:/Users/marcd/.vscode/belpulse/bp-site-audit` and
`C:/Users/marcd/.vscode/belpulse/bp-clarity-docs` while writing this record. **Verified, not
audited** — I checked these; no independent reviewer has.

| Claim | How I checked it | Result |
|---|---|---|
| `LABOUR_COST_BE` publishes forecast years as `final` | Read `public/data/national.json` | 20 periods; 2025 = 138.98, 2026 = 142.26, 2027 = 145.02, all `final` ✓ |
| `HICP` is frozen at 2025-12 | Same payload | 192 periods, last `2025-12` = 2.17715 `final`, `updated` 2026-09-05, grade A ✓ |
| Debt and balance really are loaded | Same payload | `GOV_DEBT_PCT_GDP_BE` 2025 = 107.9 `final`; `GOV_BALANCE_PCT_GDP_BE` 2025 = -5.2 `provisional`, `updated` 2026-10-03 ✓ |
| …while three languages say they are not | `grep homeFinanceUnavailable assets/i18n.js` | lines 252, 1069, 1825 ✓ |
| Every AMECO row is stamped `final` at fetch time, so config cannot fix it | Read `src/fetchers/dbnomics.py:40-63` | `results.append({... "obs_status": "A"})` on every row, unconditionally ✓ |
| No indicator config has a "why it matters" field | `grep -rl why_it_matters how_to_read config/indicators/` | 0 files; 194 have `definition` (170 + 24 derived) ✓ |
| A `detail:` list would not be validated | Read `scripts/export_site_payloads.py:522-556` | validates `headline`, `indicators`, `headlines`, composition `parts`/`whole` — and nothing else ✓ |
| `prc_hicp_manr` is still the configured Europe dataset | Read `config/indicators/HICP_ANNUAL_RATE_EUROPE.yaml:18-24` | `dataset: prc_hicp_manr`, `filters: {coicop: CP00}` ✓ |
| The HICP query still names the frozen dataflow and base year | Read `config/indicators/HICP.yaml:19` | `,DF_HICP,1.0/M.BE.000000.2015.HCP.GROWTH_RATE?startPeriod=2010-01` ✓ |
| The validation layer already knows HICP is stuck | Read `src/validation/rules.py:45-55` | its own comment names "HICP and EC_CONS_CONF_BE stuck on 2025-12" — and `staleness` is a `WARN` ✓ |
| The film's weight | `ls -la assets/belpulse/home-film/` | `film-1080.mp4` 11,287,965 B, `film-720.mp4` 5,147,261 B, `poster.jpg` 243,384 B, `poster.webp` 238,284 B — two posters, no WebM video ✓ |
| The About page is built from typed blocks | Read `config/pages/about/published.json` and the registry | `hero` + `rich_text`; 16 block types exist; no video type ✓ |
| Hiding "any period past `updated`" is too broad | Compared each of the 45 national series' last period against its own `updated` | 4 series hit, not 1: `LABOUR_COST_BE` 2026/2027 (wanted), `UNEMPLOYMENT_RATE_INSURED` 2026 = 5.05 `provisional`, `CONSUMER_CONFIDENCE` and `EC_CONS_CONF_BE` 2026-09 (real surveys; `EC_CONS_CONF_BE` is a headline KPI) ✓ |
| #309 PR 2 already fixes part of D5 | `git status` and `git diff` in the `bp-pf-ui` worktree | uncommitted work already rewrites `kpis_note`, removes `public_finance` from `unavailable:`, and rewrites `homeFinanceUnavailable`/`homeFinanceWhy` ✓ |

That last check changed the plan: the first batch was scoped to rewrite three sets of strings that
another branch is already rewriting. It now leaves them alone.

## Reported by a lens, not re-checked by me

Taken on the lens's own evidence. Each is a measurement a lens says it made with a script or a
Playwright run; none is load-bearing for a *correctness* claim, and no batch in
`docs/features/site_clarity.md` depends on one being exact:

19,289 px and 47.7 phone screens; 81 tiles and 0 closed panels on the commune page; 93.6 % hidden on
macro; 43 of 94 redundant and 51 of 94 size-restating; 28,377 ms to the first figure, CLS 0.3163 and
its 0.1788 component, 16,944 KB per desktop visit, 527 ms in the reduced-motion control; the 405
suppressed or not-applicable latest-period cells across 287 of 565 communes; 72 of 107 indicators
having any aggregate and all ten candidate headlines having a peer median; Namur's burglaries 7th
nationally and 11th regionally; `MUN_DEBT_TO_REVENUE` +38.4 % regionally with no national row;
the 30 chapter blurbs of which 21 contain digits; 186 configs with a `description`, median 56 words,
163 of them English-only; 305 of 565 communes with no municipal finance figure; the 4,923 KB of
decorative PNGs.

## Dropped or refuted

**"The film is 95 % black, so nobody is losing anything."** Refuted. Resampling the frames gives a
mean luma of 0.073 with peaks of 0.83–0.99, and the frame at 1.7 s is dark but legible — a wireframe
map of Europe with a "GDP GROWTH" caption. The case against the film hero is the 28 seconds and the
11.3 MB, not the picture. Dropping this mattered: it is the argument that would have justified
*deleting* the film rather than moving it.

**"Move surplus figures to `explorer.html`."** Dropped. `explorer.html` is linked from no live page,
so moving a figure there removes it from the site in practice. Nothing is EXPLORER-ONLY until that
page is on the shared shell with a footer link. This is why the plan has two tiers, not three.

**"Fold the commune page down to 7–8 screens."** Wrong as stated. Folding figures inside chapters
gets desktop from 23 to about 18 screens and phone from 54 to about 38, because each chapter keeps
its chart, its map and its neighbours box. Only closing whole chapters on a phone gets under ten —
which is why that is a separate, second step the maintainer sees as its own mockup.

**"No comparison → demote to detail," as written.** The real count is 29 of 94, not the lens's
figure, and it sweeps in `MUN_IPP_ADDITIONAL_RATE` (the rate the commune sets itself, 565/565
coverage) and `UNEMPLOYMENT_RATE_INSURED` — two of the most useful figures on the page. Two further
demotions it would have caused rest on a deliberate `peer_deviation: none`. The rule was replaced by
a per-cluster judgement.

**A rule-19 gate on the rank-universe wording.** Dropped: the payload already publishes each
universe's period and scope, so naming the scope in the label is a page change, not a formula
change.

**The homepage live-counter finding.** Dropped: no page reads `live_counters.json` yet (#309 PR 1
publishes it, PR 2 builds the UI), so a lens's complaint about the counter strip was about something
not on the site.

**A "recently viewed communes" row.** Dropped: the directory already offers six starter communes.

**Every external benchmark figure** in `bench.md` (ONS, INSEE, Census Reporter, Statbel) is a
reference, not a measurement. None was reproduced, and no decision in the plan rests on one.

## Findings this audit hands to other work

- **Two source-side defects, both ADR-gated (rule 19).** `src/fetchers/dbnomics.py:57` stamps every
  AMECO row `obs_status: "A"`, which `src/fetchers/sdmx_status.py:25` maps to `final`, so ten
  Commission forecast rows are stored as measurements — a rule 6 problem that config cannot reach.
  And the NBB dataflow `DF_HICP` froze at 2025-12 while NBB continued the series in `DF_HICP_2025`;
  Eurostat did the same, retiring `prc_hicp_manr` in favour of `prc_hicp_minr`. Written up as
  `docs/decisions/0017-ameco-forecast-periods.md` and
  `docs/decisions/0018-stopped-inflation-series.md`, both PROPOSED. Nothing is changed until he
  approves.
- **Two adapters silently skip unparseable rows**, against rule 13: `src/fetchers/dbnomics.py:58-59`
  and `src/fetchers/nbb.py:46-51`. Not fixed here; recorded in `docs/implementation/known-risks.md`.
- **Two label faults.** AMECO `PLCD` is "Nominal unit labour costs (ratio of compensation per
  employee to real GDP per person employed)"; `config/indicators/LABOUR_COST_BE.yaml:20,26` calls it
  "Labour Cost Index (LCI) — Nominal hourly costs" and "Nominal compensation per employee".
  `config/indicators/HICP.yaml:34` carries `sdmx_code: HICP_INDEX` on a growth-rate series. Display
  text only; fixed in batch 1.
- **Why nobody noticed the wrong figures**, which is the finding with the longest reach.
  `staleness` is a `WARN` by design and names HICP in its own comment. Warnings reach the Actions
  summary and the daily PR body, which auto-merges — PR #308 merged in 12 minutes.
  `orchestration/checks.py:123-124` ignores warnings, so `macro.html` prints "Validation passed"
  over a frozen series. Nothing tracked it: no issue, no `known-risks.md` row. And the adapter could
  not have noticed, because the dead dataflow still answers HTTP 200 with the same 192 rows. Turning
  a named, known-stale series into a build failure belongs in the validation layer and is not part of
  issue #312.

## Two corrections to the brief this audit was given

- No page reads `data/belgian_forecasts.csv`. `dashboard.html` is a redirect stub to `macro.html`,
  and `all_data.html` is one to `explorer.html`. The FPB consensus goes to the `forecasts` table and
  never to `observations` — that path is correct today.
- Provenance grade D ("Forecast") is defined but never assigned:
  `src/exporters/provenance.py:230,238`, and `tests/test_provenance.py:235` asserts it is empty. So
  the AMECO forecast years are not merely mislabelled in `status` — nothing anywhere marks them as
  forecasts.

## Residual gap

No batch in `docs/features/site_clarity.md` is audited. Three of the six batches carry a planned
`auditor` pass; none has run. The counts in "Reported by a lens, not re-checked by me" above are a
single lens's measurement each, and the two mockup folders (batches 2 and 4/5) are the only
independent check the maintainer gets on the layout decisions before they ship.
