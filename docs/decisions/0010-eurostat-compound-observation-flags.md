# 10. Eurostat adapter: read compound observation flags

Status: Accepted 2026-09-14 (maintainer approved the fix). The `u` -> `estimate`
mapping introduced here is an ASSUMPTION, pending maintainer confirmation
(see "Decision" below) -- it is deliberately a one-entry table change so it
is trivial to flip if the maintainer disagrees.

## Context

`EurostatSource._parse` (`src/fetchers/eurostat.py`) reads Eurostat's
JSON-stat 2.0 `status` map, one OBS_FLAG string per cell, and translates it
to this pipeline's canonical `obs_status` through a `FLAG_STATUS` table. Per
CLAUDE.md rule 13 (fail loudly, never silently coerce), any flag letter not
in that table raises `FetchError` rather than being guessed at. Until now
`FLAG_STATUS` only had single-letter (or empty) keys, so a cell carrying more
than one letter concatenated together -- a compound OBS_FLAG, which Eurostat
uses when more than one qualifier applies to the same cell -- always failed
that lookup and refused the whole dataset.

This surfaced concretely while scoping a European NUTS 2 map
(`docs/features/europe_nuts2.md` on branch `feat/europe-nuts2-spec`, PR #166,
section "The adapter gap: compound OBS_FLAG values"). Two of the three
candidate NUTS 2 datasets could not be loaded at all because every cell that
needed more than one flag letter tripped the "unrecognized flag" refusal.
The real flag censuses from that scoping work:

- `lfst_r_lfu3rt` (unemployment rate, NUTS 2): `b` 2008 cells, `u` 361,
  `d` 226, `bu` 117, `bd` 106, `du` 6, `bdu` 4. Even the single-letter `u`
  ("low reliability") was unmapped before this change.
- `demo_r_pjanaggr3` (population, NUTS 2): `be` 13, `e` 1196, `b` 1370,
  `bep` 3, `ep` 199, `p` 631. The single letters already mapped; only the
  compounds failed.

This is not a NUTS-2-specific problem. Any national Eurostat indicator this
pipeline already loads breaks the same way the moment Eurostat compounds a
flag on it -- the adapter has been refusing valid data by design, not by
bug. The maintainer approved fixing it as its own PR, ahead of and
independent from any NUTS 2 pipeline work.

## Decision

1. Add `"u": "estimate"` to `FLAG_STATUS`. Eurostat's OBS_FLAG codelist
   defines `u` as "low reliability" -- a confidence caveat on an otherwise
   settled number. `estimate` is the closest canonical status: the figure is
   not confirmed to the same standard as an unflagged one, but "provisional"
   specifically implies Eurostat expects to revise it, which `u` does not
   assert. **This is a stated assumption, not a confirmed mapping** --
   flagged here and in the PR for the maintainer's confirmation. Kept to a
   single dict entry precisely so it is a one-line change to reverse.

2. Split a compound OBS_FLAG (two or more letters, e.g. `bu`, `bdu`) into its
   individual letters. Every letter must already be a key of `FLAG_STATUS`;
   if any single letter within the compound is unrecognized, the whole cell
   is refused with the same loud `FetchError` as an unrecognized single-letter
   flag always has been. There is no partial acceptance of the known letters
   in a compound that also carries an unknown one (CLAUDE.md rule 13).

3. A compound's canonical status is the **most cautious** of its letters'
   individual statuses, using one named, documented precedence constant
   (`STATUS_PRECEDENCE` in `src/fetchers/eurostat.py`):

   ```
   suppressed > na > estimate > provisional > revised > final
   ```

   All eight compounds actually observed in the two datasets above resolve
   correctly under this rule, checked by hand against each letter's own
   `FLAG_STATUS` entry (not derived from the precedence constant, per
   CLAUDE.md rule 5):

   | flag | letters -> statuses | resolved |
   |---|---|---|
   | `u` | u->estimate | estimate |
   | `bu` | b->final, u->estimate | estimate |
   | `bd` | b->final, d->final | final |
   | `du` | d->final, u->estimate | estimate |
   | `bdu` | b->final, d->final, u->estimate | estimate |
   | `be` | b->final, e->estimate | estimate |
   | `bep` | b->final, e->estimate, p->provisional | estimate |
   | `ep` | e->estimate, p->provisional | estimate |

4. The existing rules that already govern a single flag's interaction with
   the cell's value are unchanged and apply identically to a compound once it
   has resolved to one canonical status: only `suppressed`/`na` may pair with
   `value=None` (`NULLABLE_STATUSES`); any other resolved status with no
   value is refused, exactly as for a single letter today. No new nullability
   logic was needed -- the compound resolver produces one canonical status
   and every downstream rule already keys off that.

5. `sdmx_for_status()` in `src/fetchers/sdmx_status.py` (canonical status ->
   SDMX letter, used when a `EurostatSource`-derived row lands in
   `legacy_observations.obs_status`) needs no change. A compound flag always
   resolves to exactly one canonical status before it reaches that function;
   `sdmx_for_status()` never sees the original flag string, compound or not,
   only the resolved canonical word it already handles.

## Alternatives rejected

- **Take only the first letter of a compound and ignore the rest.** Silently
  discards information Eurostat put there on purpose (e.g. `bu` is not the
  same claim as `b`), and the "first" letter has no defined meaning in
  Eurostat's codelist -- letter order is not guaranteed. Violates rule 13's
  spirit of not guessing.
- **Ignore letters not in `FLAG_STATUS` within an otherwise-known compound**
  (e.g. accept `b` out of `bx` and drop `x`). Silently drops a real qualifier
  Eurostat attached; if `x` ever means something serious (e.g. a stronger
  suppression), this would publish a number that should have been withheld.
  Rejected in favor of refusing the whole cell, matching how an unrecognized
  single-letter flag already behaves.
- **Hardcode a canonical status per exact compound string.** Does not
  generalize -- every new compound Eurostat introduces (and the two
  datasets above already show six of them, with a NUTS 2 rollout likely to
  surface more) would need its own hand-added entry, defeating the reuse
  that a letter-level table already gives every other adapter change. The
  precedence-over-individual-letters approach handles a compound never seen
  before as long as its individual letters are known.

## Consequences

- `lfst_r_lfu3rt` and `demo_r_pjanaggr3` (and any other dataset with a
  compound or `u` flag) can now be loaded once the `u` mapping is confirmed;
  today's change alone does not load them, it removes the flag-table reason
  they would fail.
- Any existing indicator that later starts carrying a compound flag (a
  dataset Eurostat did not previously compound, but might) will keep working
  without an adapter change, as long as each individual letter is already
  known.
- The `u` -> `estimate` mapping affects every dataset this adapter loads,
  not just the two NUTS 2 candidates -- if the maintainer wants a different
  mapping, it is a single dict entry to change, and every already-loaded
  dataset was checked (see the PR) to confirm no currently stored row's
  status would change, because no stored dataset has ever produced a `u` or
  compound flag (they were never loaded, being exactly the datasets this fix
  targets).
- `STATUS_PRECEDENCE` is now a second place (alongside `FLAG_STATUS` itself)
  that encodes how cautious the pipeline considers each canonical status;
  any future new canonical status must be added to both, in the right
  position, or a compound involving it would either be refused (if absent
  from `FLAG_STATUS`, unrelated to this ordering) or resolve to the wrong
  precedence silently. Currently exhaustive: every value `FLAG_STATUS`
  produces (`final`, `provisional`, `estimate`, `suppressed`, `na`) appears
  in `STATUS_PRECEDENCE`, plus `revised` for completeness even though no
  flag maps to it today.
