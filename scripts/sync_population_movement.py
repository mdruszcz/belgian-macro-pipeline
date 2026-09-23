"""Load Statbel's population-movement workbook -- BIRTHS, DEATHS,
INTERNAL_MIGRATION_NET, INTERNATIONAL_MIGRATION_NET, Block NS3.

NOT PART OF THE DAILY AUTOMATIC FETCH SINCE 2026-09-23. Used to be
discovered live off the theme page
(https://statbel.fgov.be/fr/themes/population/mouvement-de-la-population),
the same discipline scripts/sync_bankruptcies.py uses for its own landing
page: the href is read off the page's HTML, never hard-coded, never a
cached fallback (CLAUDE.md rule 13). That discovery-and-download logic is
unchanged and still runs when no `--from-file` is given.

WHY NOT DAILY ANY MORE. Diagnosed 2026-09-23 on the scheduled run
(2026-09-23T18:09): statbel.fgov.be answers EVERY request from a GitHub
Actions runner with a CAPTCHA challenge page (HTTP 200, text/html, ~46 KB,
"This question is for testing whether you are a human visitor... What code
is in the image?", carrying a support ID) instead of the theme page or the
workbook. A probe run confirmed this is runner-specific: the same URLs
still return the real page/file from a maintainer's own machine. This
pipeline does not solve or evade CAPTCHAs. So orchestration/commands.py's
`population_movement_observations` Command carries no `workflow_step` and
is absent from TRACKED / the daily fetch_sources job; the committed store
(config/stores.yaml `population_movement`, mode: in_db) is untouched and
keeps flowing into every export.

HOW TO REFRESH. Two ways, both from a machine that still passes the CAPTCHA
(this one, as of 2026-09-23):
  1. Live, unattended:  python scripts/sync_population_movement.py --db data/belgian_macro.db
  2. From a hand-downloaded file, when even this machine gets challenged:
     open the theme page (THEME_PAGE_URL below) in a browser (which passes
     the CAPTCHA interactively), follow its workbook link, save the XLSX,
     then:
       python scripts/sync_population_movement.py --db data/belgian_macro.db \
           --from-file mouvement-de-la-population.xlsx
     `--from-file` runs the exact same parse/resolve/transition-exclusion
     path as the live fetch (PopulationMovementSource._parse, then the same
     per-row resolve_geo below) -- only the transport differs.
The store's `max_age_days` / the `staleness` validation rule is what flags
when a refresh is actually due -- this source's own fetch_window_days no
longer applies since it is not in the daily gate, but staleness still
checks the DATA's own age regardless of how it arrived.

GEOGRAPHY IS PER-ROW, NOT PINNED -- the opposite of sync_bankruptcies.py /
sync_police.py / sync_realestate.py, and the reason this script exists
separately rather than reusing their loop. Every (indicator, nis, sheet
year) cell resolves via resolve_geo(conn, nis, sheet_year) -- the row's OWN
sheet year, the ordinary rule (CLAUDE.md rule 3), NOT sheet_year + 1. An
earlier version of this script resolved at sheet_year + 1 on the theory
that the workbook's own "as of following 1 January" population column
described the whole sheet's commune grid; that reading corrupted the
coverage denominator downstream (scripts/export_aggregates_csv.py's
`_universe_resolver` compares each indicator's per-period geo_id set
against the calendar map, and sheet_year+1 resolution put post-merger codes
onto their pre-merger sheet year, making every one of these four indicators
look "pinned" to a union universe that never existed -- see the PR body and
docs/features/population_movement.md for the measured numbers). Resolving
at the sheet's own year keeps this indicator on the ordinary calendar map.

TRANSITION-SHEET EXCLUSION. Two sheets carry codes that do not exist yet at
their own sheet year's 1 January -- Statbel's transition-year convention,
not a data error (see src/fetchers/population_movement.py's module
docstring for the measurement). Sheet "2018" carries the eighteen codes the
2019-01-01 merger created; sheet "2024" carries 82039 (valid_from
2024-12-02) and the twelve other 2025-01-01 merger codes. Resolved at their
own sheet year, none of these thirty-one rows resolves to anything --
their predecessor codes are simply absent from the same sheet, so there is
no fallback row to write instead. These rows are DROPPED, never written,
but LOUDLY: counted per sheet, the codes printed in the run's report, and
checked against `_EXPECTED_TRANSITION_EXCLUSIONS` below -- an excluded code
outside that declared set, in any sheet, fails the whole run (CLAUDE.md
rule 13; nothing is ever dropped silently).

Every other unresolved (nis, sheet_year) -- one outside the declared
transition set -- is collected and never causes a partial load: every row
in the file is checked before the run is either fully committed or fully
refused (sync_police.py's own pattern, CLAUDE.md rule 13). A code that
resolves to a NON-municipality level (province, arrondissement, region,
country -- Belgium itself is a row, "01000") is silently skipped, expected
and by design: the row-filtering trap in the handoff is exactly this -- a
naive "5-digit code not ending in 000" filter would also catch 20001/20002
(the two Brabant provinces), which resolve to level='province' here and are
dropped, never counted as unresolved or as a transition exclusion.

STALE VINTAGE RETIREMENT, one-time consequence of the audit fix. A run of
this script from before the sheet_year+1 -> sheet_year resolution fix wrote
is_latest=1 rows for the transition codes above, under the WRONG (post-
merger) geo_id for their pre-merger sheet year. upsert_observation() only
ever compares the exact (indicator_id, geo_id, period) key it is about to
write, so a key this run correctly stops writing (an excluded transition
code) is never revisited and its stale is_latest=1 row would otherwise
survive forever. `sync()` retires those specific keys explicitly --
is_latest flipped to 0, nothing ever deleted (observations stay append-only,
scripts/offload_stores.py's own invariant) -- the same "retire before
rewrite" pattern scripts/port_existing_indicators.py already uses for its
own full-indicator resyncs. This runs every time, not just once, but is a
no-op after the first correct run (there is nothing stale left to retire).

FOUR INDICATORS, ALL ADDITIVE COUNTS (is_additive=1, aggregation_method=
'sum'), summed the ordinary way to province/region/Belgium over the
geographies that existed in that period -- the existing aggregation engine
does this; nothing here touches it. NAISSANCES and DECES are always
non-negative; the two migration SOLDE columns can be negative (net outflow)
-- see config/indicators/INTERNAL_MIGRATION_NET.yaml and
INTERNATIONAL_MIGRATION_NET.yaml's own description for how this batch
resolves the conflict with the `counts_non_negative` validation rule
(src/validation/rules.py), which currently rejects any negative value where
unit='count'. This script does NOT weaken that rule; see the PR body.

RATES ARE OUT OF SCOPE (per-1,000 birth/death/migration rates would need
src/analytics/derived.py, gated behind an ADR per CLAUDE.md rule 19) -- this
script writes only the four raw counts.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import ssl
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.fetchers.population_movement import (  # noqa: E402
    ALL_INDICATORS,
    PopulationMovementSource,
    discover_xlsx_url,
)
from src.geography.resolve import UnknownGeographyError, resolve_geo  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

THEME_PAGE_URL = "https://statbel.fgov.be/fr/themes/population/mouvement-de-la-population"

_YEAR = re.compile(r"^(19|20)\d{2}$")

#: Non-municipality levels expected in the file (Belgium, regions, provinces,
#: arrondissements) -- rows at these levels are dropped, not treated as
#: unresolved. Any level string outside this set alongside 'municipality' is
#: still accepted implicitly (the check below is "not municipality -> skip"),
#: this set exists only for the log message.
_EXPECTED_NON_MUNICIPAL_LEVELS = {"country", "region", "province", "arrondissement"}

#: The exact (sheet_year, nis) pairs expected to fail resolution against
#: THEIR OWN sheet year, because the sheet forward-maps a merger's new codes
#: ahead of that merger's actual date (module docstring). Measured directly
#: against the geographies table, 2026-09-23: sheet "2018" carries the
#: eighteen codes the 2019-01-01 merger created; sheet "2024" carries 82039
#: (Bastogne+Bertogne, valid_from 2024-12-02) plus the twelve other
#: 2025-01-01 merger codes. Any excluded code NOT in this set -- in any
#: sheet -- fails the run (CLAUDE.md rule 13): a new, undeclared exclusion
#: is exactly the kind of silent gap this guard exists to catch.
_EXPECTED_TRANSITION_EXCLUSIONS: dict[str, set[str]] = {
    "2018": {
        "12041",
        "44083",
        "44084",
        "44085",
        "45068",
        "51067",
        "51068",
        "51069",
        "55085",
        "55086",
        "57096",
        "57097",
        "58001",
        "58002",
        "58003",
        "58004",
        "72042",
        "72043",
    },
    "2024": {
        "82039",
        "23106",
        "37021",
        "37022",
        "44086",
        "44087",
        "44088",
        "46029",
        "46030",
        "71071",
        "71072",
        "73110",
        "73111",
    },
}


def _fetch_theme_page_html() -> str:
    """A plain GET for the theme page HTML -- not through DataSource.fetch(),
    which caches raw bytes under the ADAPTER's own source_id/raw_extension,
    and this page is not the xlsx. This machine's TLS interception means a
    live GET here needs an unverified SSL context, exactly like every other
    live network call this pipeline makes from this environment.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(THEME_PAGE_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    # INSERT OR IGNORE, same as scripts/sync_bankruptcies.py's own
    # _ensure_reference_rows: whichever store's reference-rows script runs
    # first (config/stores.yaml's declaration order) creates this row, every
    # later one is a harmless no-op against the same source_id.
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('statbel', 'Statbel Bestat API', 'Statbel', 'statbel',
                'https://bestat.statbel.fgov.be/bestat/api', ?,
                'docs/data_catalog.md -- Population movement indicators', 'annual (automatic)', 1)
        """,
        (
            'See docs/data_catalog.md "Statbel licence" -- two Statbel documents, both '
            "confirmed to grant commercial reuse",
        ),
    )
    for indicator_id in ALL_INDICATORS:
        ind = indicator_configs[indicator_id]
        conn.execute(
            """
            INSERT INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'statbel', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'sum', 1, 0, ?, 1)
            ON CONFLICT(indicator_id) DO UPDATE SET
                name_nl = excluded.name_nl,
                name_fr = excluded.name_fr,
                name_en = excluded.name_en,
                description_en = excluded.description_en,
                unit = excluded.unit,
                preferred_direction = excluded.preferred_direction
            """,
            (
                indicator_id,
                ind["name"]["nl"],
                ind["name"]["fr"],
                ind["name"]["en"],
                ind.get("description", {}).get("en", ""),
                ind["frequency"],
                ind["unit"],
                ind["preferred_direction"],
                f"config/indicators/{indicator_id}.yaml",
            ),
        )
    conn.commit()


def sync(
    db_path: Path,
    reference_rows_only: bool = False,
    xlsx_bytes: bytes | None = None,
) -> tuple[int, int]:
    """Returns (rows_read, rows_written).

    `xlsx_bytes`, when given, replaces the theme-page discovery and live
    download entirely -- what tests use to run the real parse/resolve/write
    logic against a fixture with no network. Production (main()) always
    discovers and downloads live.
    """
    indicator_configs, _sources = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, indicator_configs)
    if reference_rows_only:
        conn.close()
        return 0, 0

    source = PopulationMovementSource()
    now = datetime.now(timezone.utc).isoformat()

    if xlsx_bytes is not None:
        rows = source._parse(xlsx_bytes)
        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            ("statbel", "population_movement", now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    else:
        html = _fetch_theme_page_html()
        xlsx_url = discover_xlsx_url(html)
        rows = source.fetch(xlsx_url, cache_key="population_movement", conn=conn)
        fetch_run_id = conn.execute(
            "SELECT fetch_run_id FROM fetch_runs WHERE source_id = 'statbel' AND "
            "adapter = 'population_movement' ORDER BY fetch_run_id DESC LIMIT 1"
        ).fetchone()[0]

    if not rows:
        raise ValueError(
            "Statbel's population-movement workbook produced zero rows after parsing -- "
            "refusing to load an empty series (CLAUDE.md rule 13)."
        )

    # PASS 1: resolve every (indicator, nis, sheet year) cell against the
    # commune grid as of 1 January of its OWN sheet year (module docstring;
    # this is the ordinary resolve_geo rule, CLAUDE.md rule 3). Collect every
    # genuinely-unresolved failure and refuse the whole run at the end rather
    # than loading a partial series. A code that resolves to a
    # NON-municipality level is dropped silently -- expected (Belgium,
    # regions, provinces, arrondissements all appear in the file). A code
    # that fails to resolve AND is declared in
    # _EXPECTED_TRANSITION_EXCLUSIONS for this sheet is dropped loudly --
    # logged and counted, never written, never treated as unresolved.
    seen: set[tuple[str, str, str]] = set()  # (indicator_id, nis, sheet_year)
    unresolved: list[tuple[str, str, str]] = []
    excluded: dict[str, list[str]] = {}  # sheet_year -> [nis, ...] (transition exclusions)
    to_write: list[tuple[str, str, str, float, str]] = (
        []
    )  # (indicator_id, geo_id, period, value, status)
    rows_read = 0

    for row in rows:
        rows_read += 1
        indicator_id = row["indicator_id"]
        nis = row["geo_id"]
        sheet_year = row["period"]
        if not _YEAR.match(sheet_year):
            raise ValueError(
                f"Unexpected period {sheet_year!r} from the adapter -- expected a bare "
                "4-digit year. Refusing to guess (CLAUDE.md rule 13)."
            )

        key = (indicator_id, nis, sheet_year)
        if key in seen:
            raise ValueError(
                f"Duplicate row: indicator {indicator_id!r}, NIS {nis!r} appears twice for "
                f"sheet {sheet_year!r}. Refusing to silently keep the last one (CLAUDE.md "
                "rule 13, mirrors src/fetchers/walstat.py's own duplicate guard)."
            )
        seen.add(key)

        # Classify by NIS code alone, not by the row valid at this sheet
        # year -- a code's level (municipality vs. Belgium/region/province/
        # arrondissement) never changes across time (measured: exactly one
        # exception in the whole table, an unrelated NULL nis_code row for
        # EU/NUTS2 aggregates), but an arrondissement CODE can be entirely
        # NEW as of a later merger (e.g. 58000, created 2019-01-01 alongside
        # the same reorganisation that created the 18 transition-sheet
        # municipality codes) and so has no row at all as of an earlier
        # sheet year. Resolving that "as of sheet year" would wrongly treat
        # it as an unknown code needing the transition-exclusion guard,
        # instead of the ordinary non-municipal row-filtering trap it
        # actually is.
        level_row = conn.execute(
            "SELECT level FROM geographies WHERE nis_code = ? LIMIT 1", (nis,)
        ).fetchone()

        if level_row is not None and level_row[0] != "municipality":
            # Belgium, a region, a province or an arrondissement row -- the
            # row-filtering trap the handoff calls out explicitly. Not an
            # error, not counted as unresolved, not a transition exclusion.
            continue

        try:
            geo_id = resolve_geo(conn, nis, sheet_year)
        except UnknownGeographyError:
            if nis in _EXPECTED_TRANSITION_EXCLUSIONS.get(sheet_year, ()):
                excluded.setdefault(sheet_year, []).append(nis)
                continue
            unresolved.append((indicator_id, sheet_year, nis))
            continue

        to_write.append((indicator_id, geo_id, sheet_year, row["value"], row["status"]))

    if unresolved:
        raise SystemExit(
            f"::error::{len(unresolved)} (indicator, sheet year, NIS) triple(s) resolved to a "
            f"municipality-level code with no covering geography row (or no geography row at "
            f"all) as of 1 January of that SAME sheet year, and are not in "
            f"_EXPECTED_TRANSITION_EXCLUSIONS, e.g. {unresolved[:5]}. Refusing to load a "
            "partial series."
        )

    for sheet_year, codes in sorted(excluded.items()):
        expected = _EXPECTED_TRANSITION_EXCLUSIONS.get(sheet_year, set())
        got = set(codes)
        # One row per indicator per code, so len(codes) is a multiple of 4;
        # de-duplicate to the distinct NIS codes for the expected-set check
        # and the report.
        if got != expected:
            missing = expected - got
            extra = got - expected
            raise SystemExit(
                f"::error::Sheet {sheet_year!r} transition exclusions do not match "
                f"_EXPECTED_TRANSITION_EXCLUSIONS. Missing (declared but not actually "
                f"excluded): {sorted(missing)}. Extra (excluded but not declared): "
                f"{sorted(extra)}. Refusing to load a series whose excluded set drifted "
                "from what was measured (CLAUDE.md rule 13)."
            )
        print(
            f"Sheet {sheet_year!r}: excluded {len(expected)} transition code(s), matching "
            f"the declared expected set exactly: {sorted(expected)}."
        )

    # RETIRE STALE VINTAGES from the pre-fix sync (audit fix). An earlier
    # version of this script resolved every row at sheet_year + 1, so a
    # transition code EXCLUDED under this version's own-period resolution
    # may already have an is_latest=1 row committed under the wrong geo_id
    # -- e.g. BIRTHS/be:mun:12041/2018, written when this sync still
    # resolved 12041 against the 2019 map. upsert_observation() only ever
    # compares the SAME (indicator_id, geo_id, period) key it is about to
    # write; it has no way to discover that a key it is NOT writing this
    # run should no longer be current. Observations are append-only
    # (src/db/vintages.py, scripts/offload_stores.py's own check 2), so
    # this never deletes a row -- it only flips is_latest to 0, the same
    # "retire before rewrite" pattern scripts/port_existing_indicators.py
    # already uses for its own full-indicator resyncs. A period this run
    # never even touches (no excluded codes for that sheet) is left alone.
    retired = 0
    for sheet_year, codes in excluded.items():
        for nis in set(codes):
            geo_id = f"be:mun:{nis}"
            for indicator_id in ALL_INDICATORS:
                cur = conn.execute(
                    "UPDATE observations SET is_latest = 0 WHERE indicator_id = ? "
                    "AND geo_id = ? AND period = ? AND is_latest = 1",
                    (indicator_id, geo_id, sheet_year),
                )
                retired += cur.rowcount
    if retired:
        print(
            f"Retired {retired} stale is_latest vintage(s) written by an earlier, "
            "incorrectly-resolved run of this sync, for the transition codes excluded "
            "above (never deleted -- is_latest flipped to 0, history intact)."
        )

    rows_written = 0
    for indicator_id, geo_id, period, value, status in sorted(
        to_write, key=lambda t: (t[0], t[1], t[2])
    ):
        period_start, period_end = derive_period_bounds(period, "A")
        rows_written += upsert_observation(
            conn,
            indicator_id=indicator_id,
            geo_id=geo_id,
            period=period,
            period_start=period_start,
            period_end=period_end,
            value=value,
            status=status,
            vintage=now,
            fetch_run_id=fetch_run_id,
        )

    conn.execute(
        "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
        "WHERE fetch_run_id = ?",
        (datetime.now(timezone.utc).isoformat(), rows_read, rows_written, fetch_run_id),
    )
    conn.commit()
    conn.close()

    total_excluded_rows = sum(len(codes) for codes in excluded.values())
    print(
        f"Read {rows_read} (indicator, commune, year) cells from the workbook; wrote "
        f"{rows_written} new vintage(s) across {len({g for _, g, _, _, _ in to_write})} "
        f"communes; excluded {total_excluded_rows} transition-sheet cell(s) across "
        f"{sum(len(s) for s in _EXPECTED_TRANSITION_EXCLUSIONS.values() if s)} declared "
        "codes (see per-sheet lines above)."
    )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Load Statbel's population-movement workbook (not part of the daily automatic "
            "fetch since 2026-09-23 -- see module docstring)"
        )
    )
    ap.add_argument("--db", required=True)
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no network",
    )
    ap.add_argument(
        "--from-file",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Load a hand-downloaded population-movement workbook instead of discovering and "
            "fetching live. Same parse/resolve/transition-exclusion path as the live fetch; "
            "only the transport differs. No network."
        ),
    )
    args = ap.parse_args()
    if args.from_file:
        read, written = sync(
            Path(args.db), args.reference_rows_only, xlsx_bytes=args.from_file.read_bytes()
        )
    else:
        read, written = sync(Path(args.db), args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for: {', '.join(ALL_INDICATORS)}")
    else:
        print(f"Wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
