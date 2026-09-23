"""Load SPF Finances' communal additional personal-income-tax rate --
MUN_IPP_ADDITIONAL_RATE, docs/features/ipp_rate.md.

DAILY, AUTOMATIC. One XLSX per tax year at a fixed URL pattern:
`https://fin.belgium.be/sites/default/files/media/documents/
taux-taxe-communale-{year}.xlsx`. There is no landing page listing these
files (checked 2026-09-23 -- unlike Statbel's bankruptcies/population-
movement pages, SPF Finances has no page that links them by href), so this
script ITERATES tax years from 2024 up to (current year + 1) at the known
pattern, exactly as the handoff specifies: a 404 on a year beyond what has
been published yet is expected (not yet released) and simply stops the
iteration; a 404 on a year already loaded in a PREVIOUS successful run, or a
response whose sheet/header has drifted from the documented shape, fails the
WHOLE run (CLAUDE.md rule 13) -- never a silent partial load, never a
fallback to a cached or hard-coded file.

NO NIS CODES IN THE SOURCE FILE -- the key is a commune NAME. Unlike
bankruptcies.py/population_movement.py (which resolve a raw NIS code),
src/fetchers/spf_finances.py hands back the raw name string in `geo_id`; THIS
script does two things per row: (1) resolve name -> NIS against the commune
map valid at f"{tax_year}-01-01" (name_nl/name_fr, after accent/case/
punctuation normalisation -- `_normalize_name` below), then (2) NIS ->
geo_id via resolve_geo(conn, nis, tax_year), the ordinary rule (CLAUDE.md
rule 3). Measured 2026-09-23 (see PR body): every row resolves to exactly
one commune this way across all three years EXCEPT "Saint-Nicolas", which is
genuinely ambiguous -- it is both the French name of Sint-Niklaas (NIS
46021) and the name of a distinct commune in Liège province (NIS 62093). The
file lists Sint-Niklaas separately under its Dutch name, so "Saint-Nicolas"
in this file is always the Liège commune -- one explicit, verified override,
`NAME_OVERRIDES` below, the same shape as src/fetchers/statbel.py's own
NAME_OVERRIDES (no fuzzy fallback, ever). Any OTHER unmatched or ambiguous
name -- one the override does not cover -- fails the entire run; collected
across every row and refused at the end, never partial (sync_bankruptcies.py's
`unresolved` shape).

STATUS IS ALWAYS 'final' -- the file publishes the year's settled rate, no
provisional marker. A rate of exactly 0 (Knokke-Heist, tax year 2026) is a
real measured zero and is written as such (CLAUDE.md rule 26).

is_additive=0, aggregation_method='not_applicable' -- a rate has no
defensible aggregate (CLAUDE.md's Definitions section): it is neither an
additive total nor a ratio recomputable from an underlying sum this pipeline
holds. There is no `is_additive` field in the indicator YAML schema (the
handoff's second blocker) -- these two values are written directly into the
`indicators` reference row here, the same scripts/sync_walstat.py pattern
(walstat's own per-inhabitant euro figures are not_applicable for the same
reason).

Usage:  python scripts/sync_ipp_rate.py --db data/belgian_macro.db
        python scripts/sync_ipp_rate.py --db X --reference-rows-only
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import ssl
import sys
import unicodedata
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.fetchers.spf_finances import INDICATOR_ID, URL_PATTERN, IppRateSource  # noqa: E402
from src.geography.resolve import UnknownGeographyError, resolve_geo  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

#: First tax year this source is in scope for (handoff: 2018-2023 are
#: PDF-only, out of scope). Never lowered without re-verifying the earlier
#: years are still PDF-only.
FIRST_TAX_YEAR = 2024

#: One explicit, verified name->geo_id override for the one genuinely
#: ambiguous name in the file, measured 2026-09-23 (module docstring; see
#: the PR body for the verification against `geographies`). Keyed on the
#: NORMALIZED name (see `_normalize_name`) so it matches regardless of the
#: exact punctuation/accenting a given year's file uses. No fuzzy fallback --
#: precedent src/fetchers/statbel.py:NAME_OVERRIDES.
NAME_OVERRIDES: dict[str, str] = {
    "saint nicolas": "62093",  # Liège province, NOT Sint-Niklaas (46021) --
    # the file lists Sint-Niklaas separately under its own Dutch name.
}


def _normalize_name(name: str) -> str:
    """Accent/case/punctuation-insensitive key for matching a source file's
    commune name against geographies.name_nl / geographies.name_fr.

    NFKD-decompose, drop combining marks (accents), lowercase, collapse
    every run of non-alphanumeric characters (hyphens, apostrophes, spaces)
    to a single space, and strip. "Saint-Nicolas" and "Saint Nicolas" and
    "SAINT-NICOLAS" all normalize to "saint nicolas"; "Sint-Niklaas" to
    "sint niklaas".
    """
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    lowered = stripped.lower()
    return re.sub(r"[^a-z0-9]+", " ", lowered).strip()


def _fetch_year_bytes(year: int) -> bytes | None:
    """The raw XLSX bytes for one tax year, or None on a 404 (not yet
    published). Any other HTTP error, or a connection failure, propagates --
    this machine's TLS interception means a live GET needs an unverified SSL
    context, exactly like every other live network call this pipeline makes
    from this environment.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    url = URL_PATTERN.format(year=year)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def _build_name_map(conn: sqlite3.Connection, tax_year: str) -> dict[str, set[str]]:
    """{normalized_name: {nis_code, ...}} for every commune valid at
    f"{tax_year}-01-01" -- both name_nl and name_fr feed the same map, so a
    name matching either resolves. A name matching more than one NIS code
    (only "Saint-Nicolas", per the module docstring) surfaces as a set with
    more than one member; the caller decides ambiguous vs. override there.
    """
    as_of = f"{tax_year}-01-01"
    rows = conn.execute(
        "SELECT nis_code, name_nl, name_fr FROM geographies WHERE level = 'municipality' "
        "AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)",
        (as_of, as_of),
    ).fetchall()
    name_map: dict[str, set[str]] = {}
    for nis, name_nl, name_fr in rows:
        for raw_name in (name_nl, name_fr):
            name_map.setdefault(_normalize_name(raw_name), set()).add(nis)
    return name_map


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict, source: dict) -> None:
    # INSERT OR IGNORE for the source row (first sync to run creates it,
    # matching every other store's own _ensure_reference_rows); UPSERT the
    # indicator row so name/unit corrections in config always propagate.
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES (?, ?, ?, 'spf_finances', ?, ?, ?, ?, 1)
        """,
        (
            source["source_id"],
            source["name"],
            source["agency"],
            source.get("base_url"),
            source.get("licence"),
            source.get("catalog_ref"),
            source.get("cadence"),
        ),
    )
    ind = indicator_configs[INDICATOR_ID]
    conn.execute(
        """
        INSERT INTO indicators
            (indicator_id, source_id, name_nl, name_fr, name_en,
             description_nl, description_fr, description_en,
             frequency, unit, preferred_direction, aggregation_method,
             is_additive, decimals, config_path, is_active)
        VALUES (?, 'spf_finances', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'not_applicable', 0, 1, ?, 1)
        ON CONFLICT(indicator_id) DO UPDATE SET
            name_nl = excluded.name_nl,
            name_fr = excluded.name_fr,
            name_en = excluded.name_en,
            description_nl = excluded.description_nl,
            description_fr = excluded.description_fr,
            description_en = excluded.description_en,
            unit = excluded.unit,
            preferred_direction = excluded.preferred_direction
        """,
        (
            INDICATOR_ID,
            ind["name"]["nl"],
            ind["name"]["fr"],
            ind["name"]["en"],
            ind.get("description", {}).get("nl", ""),
            ind.get("description", {}).get("fr", ""),
            ind.get("description", {}).get("en", ""),
            ind["frequency"],
            ind["unit"],
            ind["preferred_direction"],
            f"config/indicators/{INDICATOR_ID}.yaml",
        ),
    )
    conn.commit()


def sync(
    db_path: Path,
    reference_rows_only: bool = False,
    year_bytes: dict[int, bytes] | None = None,
) -> tuple[int, int]:
    """Returns (rows_read, rows_written).

    `year_bytes`, when given, is {tax_year: xlsx_bytes} and replaces live
    discovery/download entirely -- what tests use to run the real
    parse/resolve/write logic against fixtures with no network. Production
    (main()) always discovers and downloads live, iterating years.
    """
    indicator_configs, sources = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, indicator_configs, sources["spf_finances"])
    if reference_rows_only:
        conn.close()
        return 0, 0

    source = IppRateSource()
    now = datetime.now(timezone.utc).isoformat()

    if year_bytes is not None:
        years_loaded = sorted(year_bytes)
        all_rows: list[dict] = []
        for year in years_loaded:
            all_rows.extend(source._parse(year_bytes[year], tax_year=str(year)))
        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            ("spf_finances", "spf_finances", now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    else:
        current_year = datetime.now(timezone.utc).year
        years_loaded = []
        all_rows = []
        year = FIRST_TAX_YEAR
        while year <= current_year + 1:
            raw = _fetch_year_bytes(year)
            if raw is None:
                # Not yet published. Expected for a future year; stop
                # iterating rather than probing further ahead.
                break
            all_rows.extend(
                source.fetch(
                    URL_PATTERN.format(year=year),
                    cache_key=str(year),
                    conn=conn,
                    tax_year=str(year),
                )
            )
            years_loaded.append(year)
            year += 1
        fetch_run_id = conn.execute(
            "SELECT fetch_run_id FROM fetch_runs WHERE source_id = 'spf_finances' AND "
            "adapter = 'spf_finances' ORDER BY fetch_run_id DESC LIMIT 1"
        ).fetchone()[0]

    if not all_rows:
        raise ValueError(
            "SPF Finances' IPP additional-rate files produced zero rows after parsing -- "
            "refusing to load an empty series (CLAUDE.md rule 13)."
        )

    # PASS 1: resolve every (name, tax_year) row to a geo_id. Collect every
    # failure -- unmatched, ambiguous-without-override, or resolve_geo
    # failure -- and refuse the whole run at the end rather than loading a
    # partial series (sync_bankruptcies.py's own `unresolved` pattern).
    unresolved: list[tuple[str, str, str]] = []  # (tax_year, name, reason)
    to_write: list[tuple[str, str, float, str]] = []  # (geo_id, period, value, status)
    name_maps: dict[str, dict[str, set[str]]] = {}

    for row in all_rows:
        name = row["geo_id"]
        tax_year = row["period"]
        if tax_year not in name_maps:
            name_maps[tax_year] = _build_name_map(conn, tax_year)
        name_map = name_maps[tax_year]

        normalized = _normalize_name(name)
        if normalized in NAME_OVERRIDES:
            nis = NAME_OVERRIDES[normalized]
        else:
            matches = name_map.get(normalized, set())
            if len(matches) == 1:
                nis = next(iter(matches))
            elif len(matches) == 0:
                unresolved.append((tax_year, name, "no commune matches this name"))
                continue
            else:
                unresolved.append(
                    (
                        tax_year,
                        name,
                        f"ambiguous -- matches {sorted(matches)} and no override is "
                        "declared for this name",
                    )
                )
                continue

        try:
            geo_id = resolve_geo(conn, nis, tax_year)
        except UnknownGeographyError as exc:
            unresolved.append((tax_year, name, f"resolve_geo failed for NIS {nis!r}: {exc}"))
            continue

        to_write.append((geo_id, tax_year, row["value"], row["status"]))

    if unresolved:
        raise SystemExit(
            f"::error::{len(unresolved)} (tax year, name) row(s) failed to resolve to a "
            f"commune, e.g. {unresolved[:5]}. Refusing to load a partial series "
            "(CLAUDE.md rule 13)."
        )

    rows_read = len(all_rows)
    rows_written = 0
    for geo_id, period, value, status in sorted(to_write, key=lambda t: (t[1], t[0])):
        period_start, period_end = derive_period_bounds(period, "A")
        rows_written += upsert_observation(
            conn,
            indicator_id=INDICATOR_ID,
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

    print(
        f"Loaded tax year(s) {years_loaded}: read {rows_read} (commune, tax year) rows, "
        f"wrote {rows_written} new vintage(s) across {len({g for g, *_ in to_write})} communes."
    )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load SPF Finances' communal additional IPP rate (daily, automatic)"
    )
    ap.add_argument("--db", required=True)
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no network",
    )
    args = ap.parse_args()
    read, written = sync(Path(args.db), args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for: {INDICATOR_ID}")
    else:
        print(f"Wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
