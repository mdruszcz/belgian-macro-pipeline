# Batch 5 (completion) — card view and mobile filter drawer on `communes.html`

```
Batch:                 5 (completion) -- municipality explorer redesign (docs/steps L562)
Base commit:           c9cd9a96 (origin/develop)
Final commit:          see branch feat/batch-5-commune-explorer
Files changed:         communes.html, assets/i18n.js, tests/test_communes_card_view.py (new),
                       docs/steps
Requirements completed: card view (Table/Cards toggle), mobile filter drawer, headline
                       figures from metadata/sections.json, sorted-indicator fallback,
                       lazy "all indicators" expander, five-state distinction preserved
Deferred requirements: none from this batch's own scope (see "Out of scope" below --
                       unchanged by design, not deferred)
Data-contract impact:  none. No payload, schema or exporter touched.
Commands executed:     see "Verification"
Tests passed:          21 new; full numbers below
Screenshots produced:  1400 / 820 / 390, light and dark, both views (not committed -- rule 12)
Performance results:   render() now also builds up to 565 cards per call; expander contents
                       (69 rows x 565 cards) are NOT built until a reader opens one
Reviewer findings:     pending
Known limitations:     below
Rollback procedure:    revert the communes.html and assets/i18n.js hunks in this batch's
                       commit(s) and delete tests/test_communes_card_view.py; nothing else
                       in the repo references belpulse-communes-view or the new i18n keys
Next batch:            per docs/steps
```

## What this batch does

Adds exactly two things to `communes.html`: a card-view alternative to its table, and a
mobile filter drawer. Everything else about the page -- the table, the map, search, the
region and year filters, sorting, the attribution block, the footer -- is unchanged.

This completes Batch 5, whose first half (`profiles.html`, the commune-profiles directory
and the Belfius typology work) shipped 2026-09-12 and is untouched here.

## The roadmap line was wrong about one thing

The roadmap's "How" line for this batch says to preserve, among other things, communes.html's
"URL state." **communes.html has no URL state.** There is no `URLSearchParams`, no
`history.pushState`, no hash handling anywhere in the file; the only use of `location` is the
map's `window.location.href = 'local/' + nis + '/'` on a shape click, which is navigation, not
state. "Preserve URL state" is therefore satisfied by adding none — and this batch does not add
any: the new view choice (Table vs Cards) lives in `localStorage` under
`belpulse-communes-view`, deliberately not a query parameter, because rule 31 would then oblige
the project to keep that URL valid forever for what is a per-viewer display preference, not
shareable state.

## Decisions and why

- **The page's own visual language, not `--bp-*` tokens.** This is not a migration of
  communes.html to the design system; a half-migrated page next to an untouched table would
  look broken and put the table's own styling at risk. The view toggle reuses the existing
  `.theme-toggle` look (a new `.view-toggle` class carrying the identical ruleset, kept
  separate so it is not affected by the theme switcher's own mobile-only positioning rule).
- **Headline figures are config, not hardcoded (rules 2, 24).** Fetched from
  `public/data/metadata/sections.json` — the same file, the same field (`headlines`), and the
  same defensive filter (`.filter(code => INDICATOR_CODES.includes(code))`) that
  `profiles.html` already uses for its own directory cards. A failed fetch leaves the headline
  list empty; the card still shows identity, whichever column the table is sorted by, and the
  full expander. Nothing stands in for a real headline list — no "first few codes."
- **The sorted indicator never vanishes.** If the table is sorted by a column that is not in
  the headline set, `cardFigureCodes()` prepends it. Verified for real (not eyeballed) in
  `tests/test_communes_card_view.py::test_the_sorted_indicator_is_never_dropped_from_the_card`,
  which runs the actual function under Node for five scenarios including the two fallback
  cases (no headlines loaded, with and without an indicator sort active).
- **The five states stay distinct on a card.** `formatValue()` and `statusPill()` are reused
  completely unchanged; a new `cardCellHtml()` makes the same three-way dispatch (absent /
  suppressed-with-no-value / a real value) the table's `render()` already makes, in one place
  shared by a card's headline figures and its expander. The table's own per-cell code in
  `render()` was not touched, so this dispatch is written in exactly two places in the whole
  file (the table's and the card path's), not three.
- **The expander is genuinely lazy.** Each card's `<details class="cc-expand">` carries only
  its `<summary>` at render time; its 69-row body is built on first `toggle` via a single
  listener delegated onto `#cardsWrap` (registered for the capture phase, since `toggle` does
  not bubble in every browser), guarded by `dataset.built` so reopening does not rebuild it.
  565 cards therefore start at a few hundred nodes, not 39,000.
- **The mobile filter drawer uses the page's own existing 720px breakpoint** (the one the
  light/auto/dark switch already collapses at), because no mobile mockup exists for this page.
  `docs/design-references/tokens-measured.md`'s "Breakpoints" section records exactly this:
  "No tablet or mobile view was supplied... follow this repo's own existing convention... until
  mobile mockups are supplied." That is the rationale used here, not an invented one. The
  drawer is a plain collapsible panel (`display:contents` above the breakpoint, so nothing
  about desktop layout changes at all) — no focus trap, no scroll lock — carrying
  `aria-expanded`/`aria-controls` exactly as `#mapToggle` already does, closing on Escape.
  `#status` (the "N communes" count) sits outside it and stays visible whether the drawer is
  open or closed.
- **Cards are the default view below the drawer breakpoint when no choice has been made yet;
  an explicit choice always wins and is remembered.** Above that breakpoint the table stays the
  default, unchanged from today.

## Tests

`tests/test_communes_card_view.py`, 21 tests, following `test_communes_map_panel.py`'s
module-scoped `page` fixture plus regex assertions on the source, with two pieces of real
branching logic (`cardFigureCodes`, `cardCellHtml`) additionally run for real under Node
(`test_map_ui_logic.py`'s technique) rather than only eyeballed:

- the card view exists and is togglable; the toggle is not a URL parameter (no
  `URLSearchParams`/`pushState` anywhere in the page, confirming the finding above);
- headline codes come from `metadata/sections.json`, filtered against what the table has;
- no indicator code is hardcoded, checked against the real
  `public/data/metadata/indicators.json` list (mirrors
  `test_map_ui_logic.py::test_no_page_names_an_indicator`);
- `cardFigureCodes()` run under Node for five scenarios (sorted by a headline, sorted by a
  non-headline, not sorted by an indicator at all, and both fallback cases with no headlines
  loaded);
- `cardCellHtml()` run under Node for absent / suppressed / real-value cells, confirming a
  suppressed cell renders the Suppressed pill and never collapses into the generic n/a pill;
- the expander is not built eagerly (`cardHTML()`'s own output carries no per-indicator rows)
  and is filled in only by the delegated `toggle` listener, guarded by `dataset.built`;
- the drawer button's `aria-expanded`/`aria-controls` and the id it names both exist; `#status`
  sits outside the drawer's markup; the drawer closes on Escape; it touches no scroll lock;
- `#mapToggle`'s own id, `aria-expanded` and `aria-controls` are unchanged by moving inside the
  drawer wrapper;
- all five new i18n keys exist in en/fr/nl and are actually referenced by the page; the reused
  `pfSeeProfile` key (the card's link to `local/{nis}/`, borrowed from `profiles.html` rather
  than duplicated) still exists;
- the table itself (`#tableScroll`, `#tbody`, `#headRow`) and the licence attribution block are
  still present.

## Verification

- `ruff check` and `black --check` on the new test file: clean.
- `.venv/Scripts/python.exe -m pytest tests/ -q -m "not browser and not generated_site and not slow" -p no:cacheprovider`:
  **1598 passed, 5 failed, 19 skipped, 2046 deselected in 151.31s.** All 5 failures are
  pre-existing and outside this batch's files (`src/builder/**`, `tests/builder/**`,
  `tests/security/**`) — four are exactly the ones the handoff named (the oversized-payload
  test, the builder-paths symlink test, the atomic-write crash test, and the
  service-hardening symlink test — the last two are Windows privilege/socket issues:
  `WinError 1314` for `os.symlink` without the Windows "create symlink" privilege, and the
  atomic-write test's simulated-crash assertion). The fifth,
  `test_builder_api.py::test_post_with_wrong_origin_is_forbidden`, was **not** on the handoff's
  list; it passed on its own when re-run in isolation (`1 passed`), so it is a pre-existing
  flake under full-suite load, not a regression — nothing in this batch touches
  `src/builder/` or `tests/builder/`.
- Full suite, `.venv/Scripts/python.exe -m pytest tests/ -q -p no:cacheprovider` (browser and
  slow tests included, no marker filter): **6 failed, 3642 passed, 20 skipped in 653.46s
  (10:53).** The 6 failures are EXACTLY the six the handoff named, no more and no fewer:
  `test_builder_api.py::test_state_payload_too_large`, the two browser-marked twins in
  `test_builder_shell_e2e.py` (`test_413_payload_too_large_is_stated_plainly`,
  `test_server_unreachable_is_stated_plainly_not_as_a_blank_failure`),
  `test_builder_paths.py`'s and `test_builder_service_hardening.py`'s symlink tests, and
  `test_builder_transaction.py`'s atomic-write crash test. None is in a file this batch
  touched. (The `test_post_with_wrong_origin_is_forbidden` flake noted above did not recur
  here.)
- `tests/test_communes_card_view.py` alone: 21 passed.
- `tests/test_communes_map_panel.py`, `tests/test_map_ui_logic.py`, `tests/test_i18n.py`
  (the three existing suites this page's behaviour is covered by): 49 passed, unchanged.
- Chromium, driven directly against `python -m http.server` on the repo root (real origin, so
  `fetch()` behaves as it does on GitHub Pages), at 1400 / 820 / 390 px, light and dark, both
  views: **zero console errors at every one of the 6 width/theme combinations**, zero
  horizontal overflow (`document.scrollingElement.scrollWidth - innerWidth === 0`) in both the
  table and the card view at every size, the card count equal to the table row count (565 =
  565) under the same (empty) filters every time, the "all indicators" expander unbuilt before
  first open and holding all 69 rows immediately after, the map still drawing all 565 shapes in
  both views, and — narrow width only — the Filters button appearing only there, the drawer
  starting collapsed, opening on click with `aria-expanded` flipping to `true`, `#status`
  staying visible throughout, and closing on Escape.
- Search and the region filter, exercised for real (not just read from source) in that same
  Chromium session: typing "namur" cut the table to 1 row and the status text to "1 communes
  sur 565"; switching to Cards under that same filter showed exactly 1 card, named "Namur".
  Clearing the search and picking the first real region from `#regionFilter` cut both the
  table and the cards to the same 19 rows/cards, with the same status text. Table and cards
  never disagreed on count under any filter tried.
- Hand-check: commune **Antwerp, NIS 11002, year 2026**. Three indicators —
  `POPULATION_BY_COMMUNE`, `POPULATION_CHANGE_5Y`, `UNEMPLOYMENT_RATE_INSURED` — read
  **565 615 (Final)**, **6,84 % (Derived)** and **6,55 % (Provisional)** in the table, and the
  identical text and pills in the card's headline figures after switching views. (The browser's
  default locale rendered these in French during the check — "Évolution de la population sur 5
  ans", "Taux de chômage" — which is the page's own existing language-detection behaviour, not
  something this batch changed; the values and pills are what was compared.)

## Known limitations

1. **Opening a card's expander does not survive the next `render()`.** Search, region, year and
   sort changes — and a language switch — all rebuild `#cardsWrap` from scratch, so an open
   expander collapses back to unbuilt on any of those. This matches how the table has always
   behaved (`#tbody` is also fully rebuilt on every `render()`, with no attempt to preserve
   scroll position or other transient UI state); it is a pre-existing characteristic of this
   file's render loop, not a new inconsistency introduced here.
2. **No hover history tooltip on card figures.** The table's cells carry a full year-by-year
   history in their `title` attribute; cards do not reproduce this, since it was not asked for
   and the full history is one click away either way (the expander shows the current year for
   all 69 indicators; the linked profile page carries full history).
3. **Card markup interpolates commune names and indicator labels the same way the table's own
   cells already do** — directly, without HTML-escaping. This matches the table's existing
   practice throughout the file (e.g. its own `name_fr` interpolation) rather than introducing
   a second, inconsistent convention; a proper fix would be a whole-file change outside this
   batch's scope.
4. **The page is unchanged in every other respect.** No `--bp-*` migration, no virtualization
   (Batch 8, for `all_data.html`), no photos on cards, no URL parameters — all as scoped.
