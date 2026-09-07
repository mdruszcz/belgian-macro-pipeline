"""
Split the bulk CSV exports into per-entity JSON payloads small enough for a
browser to fetch on demand -- Block K's future `/local` interface needs one
commune's worth of data, not all 565. See docs/features/site_payloads.md.

Every payload here is a RESHAPE of an existing bulk export, never a new
computation: `communes/{nis}.json` slices `communes_history.csv`,
`indicators/{id}.json` slices `communes_export.csv`, `national.json` slices
`belgian_macro_export.csv`. Nothing here writes to `observations`, and
nothing here recomputes a derived indicator -- CONTROL G stays satisfied.

Bulk CSV/JSON exports are left completely untouched; this only reads them.
"""

import argparse
import csv
import json
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

# EVERY letter export_communes_csv.py's STATUS_TO_LETTER can emit. It was
# mapping two of the six, and _status_word passed the rest through, so a
# revised or estimate value would have reached 565 published pages as a bare
# "R" or "E". communes.html shipped exactly this bug once already -- see the
# comment above its statusPill(). Nothing in the store hits R/E/N today, which
# is precisely why the gap was invisible.
STATUS_LETTER_TO_WORD = {
    "A": "final",
    "P": "provisional",
    "R": "revised",
    "E": "estimate",
    "S": "suppressed",
    "N": "na",
}

# Written as a word, not a letter, by export_communes_history_csv.py, because a
# derived figure has no source status to letter-code. Allowed through as-is.
STATUS_WORDS_PASSED_THROUGH = {"derived"}


def _status_word(status: str) -> str:
    """A single status letter as the word the pages publish.

    RAISES on anything unrecognised rather than passing it through. Rule 13:
    a new status letter arriving from a source is a schema change, and it has
    to stop the build rather than render itself onto a commune page as an
    unexplained capital letter that a reader cannot look up.
    """
    if not status:
        return None
    if status in STATUS_WORDS_PASSED_THROUGH:
        return status
    try:
        return STATUS_LETTER_TO_WORD[status]
    except KeyError:
        raise ValueError(
            f"unknown observation status {status!r}. The permitted statuses are "
            f"{sorted(STATUS_LETTER_TO_WORD)} (letters) and "
            f"{sorted(STATUS_WORDS_PASSED_THROUGH)} (words). If a source has "
            "started emitting a new one, map it here deliberately -- do not let "
            "it reach a published page unexplained."
        ) from None


# Statuses that explain a NULL value. The schema permits a null only for these
# (migrations/001_core_schema.sql: CHECK (value IS NOT NULL OR status IN
# ('suppressed','na'))), and they are the whole reason this pipeline can tell
# "the source has this figure and will not publish it" apart from "we have no
# reading at all".
STATUSES_EXPLAINING_A_NULL = {"suppressed", "na"}


def _cell(row: dict) -> dict | None:
    """One observation as the payload publishes it, or None to skip the row.

    A WITHHELD CELL IS PUBLISHED, NOT DROPPED. ONEM masks any count below 10
    for privacy; those arrive with an empty value and status `S`. Both readers
    here used to `continue` on the empty value before recording anything, so
    1,044 such cells never reached public/data at all -- 203 (commune,
    indicator) pairs across 188 of the 565 communes, 36 of which vanished
    entirely. On a commune page a figure the source deliberately withheld then
    looked exactly like one that was never collected.

    That also made local.html's attribution block untrue. It states, in all
    three languages, that figures ONEM withholds "are shown as suppressed,
    never as zero" -- and the page had no rendering path for it. That sentence
    is part of a licence notice.

    Publishing `{"value": null, "status": "suppressed"}` is the ONE documented
    exception to this format's absent-means-no-data rule (see
    docs/features/site_payloads.md and docs/features/provenance.md). Absence
    means "we have no reading"; a null with a status means "the source has a
    reading and will not publish it". Collapsing the two destroys a
    distinction the source deliberately created.

    A blank with no status to explain it is still nothing, and is skipped.
    """
    status = _status_word(row["status"])
    if row["value"] == "":
        if status not in STATUSES_EXPLAINING_A_NULL:
            return None
        return {"value": None, "status": status}
    return {"value": float(row["value"]), "status": status}


def _note_updated(indicator: dict, fetched_at: str) -> None:
    """Record the latest retrieval date seen for an indicator, as a date.

    This is a LICENCE CONDITION, not a nicety: Statbel's 2015 open-data
    licence requires the attribution to carry "de datum van de laatste
    bijwerking" (the date of last update) and terminates automatically if it
    does not -- see docs/data_catalog.md, "What this obliges us to build".
    The pages cannot show a date the payload does not carry.

    A DERIVED indicator has no retrieval date of its own (its `fetched_at`
    is empty in the source CSV, correctly -- it was computed, not fetched),
    so the key is simply absent for it rather than carrying an invented or
    inherited date. Absence is already this format's "no data" signal
    (docs/features/site_payloads.md); a wrong date here would be a licence
    breach of its own, since the 2015 licence also forbids misleading a
    reader about the update date.
    """
    if not fetched_at:
        return
    day = fetched_at[:10]
    if day > indicator.get("updated", ""):
        indicator["updated"] = day


def _read_communes_history(csv_path: Path) -> dict[str, dict]:
    """One entry per commune, keyed by nis_code, holding every indicator that
    commune has any value for across every period -- the shape
    `communes/{nis}.json` publishes directly."""
    communes: dict[str, dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            nis = row["nis_code"]
            commune = communes.setdefault(
                nis,
                {
                    "nis_code": nis,
                    "geo_id": row["geo_id"],
                    "name": {"en": row["name_en"], "fr": row["name_fr"], "nl": row["name_nl"]},
                    "region": row["region"] or None,
                    "province": row["province"] or None,
                    "arrondissement": row["arrondissement"] or None,
                    "indicators": {},
                },
            )
            indicator = commune["indicators"].setdefault(
                row["indicator_code"],
                {"name": row["indicator_name"], "unit": row["unit"], "periods": {}},
            )
            cell = _cell(row)
            if cell is None:
                continue
            indicator["periods"][row["period"]] = cell
            # Gated on a real value, so `updated` keeps meaning "when the
            # number you are looking at was retrieved" rather than "when we
            # last read a file that declined to give us one".
            if cell["value"] is not None:
                _note_updated(indicator, row["fetched_at"])
    return communes


def _read_communes_latest(csv_path: Path) -> dict[str, dict]:
    """One entry per indicator, holding every current commune's latest value
    -- the cross-sectional shape `indicators/{id}.json` publishes directly.
    `communes_export.csv` already restricts to current communes and the most
    recent period per (commune, indicator), so no filtering happens here."""
    indicators: dict[str, dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            indicator = indicators.setdefault(
                row["indicator_code"],
                {"name": row["indicator_name"], "unit": row["unit"], "communes": {}},
            )
            cell = _cell(row)
            if cell is None:
                continue
            indicator["communes"][row["nis_code"]] = {**cell, "period": row["period"]}
            if cell["value"] is not None:
                _note_updated(indicator, row["fetched_at"])
    return indicators


def _read_national(csv_path: Path) -> dict[str, dict]:
    """be:country's full history across every indicator in
    `belgian_macro_export.csv`, reshaped the same way as a commune payload."""
    indicators: dict[str, dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            indicator = indicators.setdefault(
                row["indicator_code"],
                {
                    "name": row["name"],
                    "unit": row["unit"],
                    "source_agency": row["source_agency"],
                    "periods": {},
                },
            )
            # Same rule as the commune readers, via the same helper. Nothing
            # national is suppressed today; keeping one rule means a national
            # source that starts masking cells does not need this remembered.
            cell = _cell({**row, "status": row["obs_status"]})
            if cell is None:
                continue
            if cell["value"] is not None:
                _note_updated(indicator, row["fetched_at"])
            indicator["periods"][row["period"]] = cell
    return indicators


def _build_geographies(db_path: Path) -> list[dict]:
    """Every currently-valid geography with its trilingual name and its
    region/province/arrondissement ancestry, for Block K's search and Block
    L's comparison picker."""
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute("""
        SELECT geo_id, nis_code, level, name_en, name_fr, name_nl, parent_geo_id
        FROM geographies WHERE valid_to IS NULL ORDER BY level, nis_code
        """).fetchall()
    conn.close()

    out = []
    for geo_id, nis_code, level, name_en, name_fr, name_nl, parent_geo_id in rows:
        out.append(
            {
                "geo_id": geo_id,
                "nis_code": nis_code,
                "level": level,
                "name": {"en": name_en, "fr": name_fr, "nl": name_nl},
                "parent_geo_id": parent_geo_id,
            }
        )
    return out


def _ancestry(db_path: Path) -> dict[str, list[str]]:
    """geo_id -> its ancestor geo_ids, nearest first.

    Walked from parent_geo_id over ALL geographies (including historical
    ones), tolerating a missing level and stopping on a cycle -- the same
    defence the exporters' own _ancestor_names() applies.
    """
    conn = sqlite3.connect(str(db_path))
    # Canonicalised, matching export_aggregates_csv.py: the geographies table
    # holds versioned ancestors (be:prov:10000@1977-01-01) whose figures the
    # aggregate export now folds into the current geography. If the two walks
    # disagreed, a comparison would silently fail to match and simply not
    # appear. No CURRENT commune has a versioned parent today -- 34 historical
    # ones do -- so this is agreement insurance, not a live fix.
    parents = {
        geo_id.split("@", 1)[0]: (parent.split("@", 1)[0] if parent else None)
        for geo_id, parent in conn.execute("SELECT geo_id, parent_geo_id FROM geographies")
    }
    conn.close()

    chains: dict[str, list[str]] = {}
    for geo_id in parents:
        chain: list[str] = []
        seen = {geo_id}
        current = parents.get(geo_id)
        while current and current not in seen:
            seen.add(current)
            chain.append(current)
            current = parents.get(current)
        chains[geo_id] = chain
    return chains


def _read_percentiles(csv_path: Path) -> dict[tuple[str, str], dict]:
    """Peer positions keyed by (nis_code, indicator).

    From scripts/export_percentiles_csv.py, latest period per indicator. An
    empty `percentile` column means the peer set was below the floor and the
    percentile was deliberately withheld -- the rank is still there, and the
    page says "4th of 19" instead. That distinction has to survive into the
    payload, so a withheld percentile becomes None rather than 0.
    """
    out: dict[tuple[str, str], dict] = {}
    if not csv_path.is_file():
        return out
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            entry = out.setdefault((row["nis_code"], row["indicator_code"]), {})
            entry[row["scope"]] = {
                "scope_name": row["scope_name"],
                "pct": float(row["percentile"]) if row["percentile"] != "" else None,
                "rank": int(row["rank"]),
                "peers": int(row["peers"]),
                "period": row["period"],
            }
    return out


def _attach_percentiles(communes: dict[str, dict], percentiles: dict) -> int:
    attached = 0
    for nis, commune in communes.items():
        for indicator_id, entry in commune["indicators"].items():
            pos = percentiles.get((nis, indicator_id))
            if pos:
                entry["percentile"] = pos
                attached += 1
    return attached


def _indicator_names(db_path: Path, derived_dir: Path | None = None) -> dict[str, dict]:
    """indicator_id -> {en, fr, nl}.

    The bulk CSVs carry only the English name, because they are flat tables
    with one name column. The payloads are what a multilingual page reads, so
    they carry all three -- every indicator already has them, in the
    `indicators` table for fetched ones and in the YAML for derived ones
    (CLAUDE.md rule 7: preserve multilingual labels on every user-facing
    string). Without this the French page would silently fall back to English
    indicator names, which is the kind of half-translation that reads worse
    than no translation at all.
    """
    names: dict[str, dict] = {}
    conn = sqlite3.connect(str(db_path))
    try:
        for indicator_id, en, fr, nl in conn.execute(
            "SELECT indicator_id, name_en, name_fr, name_nl FROM indicators"
        ):
            names[indicator_id] = {"en": en, "fr": fr, "nl": nl}
    finally:
        conn.close()

    derived_dir = derived_dir or (
        Path(__file__).resolve().parents[1] / "config" / "indicators" / "derived"
    )
    if derived_dir.is_dir():
        import yaml

        for path in sorted(derived_dir.glob("*.yaml")):
            cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
            names[cfg["id"]] = {k: cfg["name"][k] for k in ("en", "fr", "nl")}
    return names


SECTIONS_CONFIG = Path(__file__).resolve().parents[1] / "config" / "local_sections.yaml"


def _sections(path: Path = SECTIONS_CONFIG) -> dict:
    """The /local page layout, so the page holds no indicator ids.

    The 50% gate checks for "zero indicator-specific frontend logic", and
    Block B's premise is that adding an indicator to a page is config rather
    than a code change. local.html renders whatever this describes.

    Every indicator named here is checked against the payloads before being
    emitted: a section pointing at an indicator nothing provides would render
    an empty box on 565 pages, which looks like a bug and is invisible to
    every other test.
    """
    if not path.is_file():
        return {"headlines": [], "sections": []}
    import yaml

    layout = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        "headlines": layout.get("headlines") or [],
        "sections": layout.get("sections") or [],
    }


def _check_sections(layout: dict, known: set[str]) -> None:
    """Every indicator the /local layout names must exist in the payloads.

    Covers the hero row as well as the sections: HEADLINE_SPEC used to be a
    hardcoded array in local.html and so escaped this check entirely, which
    is exactly the "zero indicator-specific frontend logic" property the 50%
    gate tests for.
    """
    unknown = sorted(
        {
            indicator_id
            for section in layout["sections"]
            for indicator_id in [
                *([section["headline"]] if section.get("headline") else []),
                *(section.get("indicators") or []),
            ]
            if indicator_id not in known
        }
        | {i for i in layout["headlines"] if i not in known}
    )
    if unknown:
        raise ValueError(
            f"config/local_sections.yaml names indicator(s) no commune payload carries: "
            f"{unknown}. They would render as empty boxes on every commune page. Remove "
            "them from the layout, or load the data they need."
        )


def _read_aggregates(csv_path: Path) -> dict[tuple[str, str, str], dict]:
    """Aggregate values keyed by (geo_id, indicator, period).

    Produced by scripts/export_aggregates_csv.py -- Block L. Read rather than
    recomputed here, for the same reason every other payload is a reshape of
    an existing export: one computation, one set of numbers, and no chance of
    the page disagreeing with the CSV a researcher downloaded.
    """
    out: dict[tuple[str, str, str], dict] = {}
    if not csv_path.is_file():
        return out
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            out[(row["geo_id"], row["indicator_code"], row["period"])] = {
                "geo_id": row["geo_id"],
                "level": row["level"],
                "name": {"en": row["name_en"], "fr": row["name_fr"], "nl": row["name_nl"]},
                "value": float(row["value"]),
                "period": row["period"],
                "coverage": {
                    "n": int(row["coverage_n"]),
                    "of": int(row["coverage_of"]),
                    "pct": float(row["coverage_pct"]),
                },
            }
    return out


# Levels shown in the comparison column, nearest first. Arrondissement is
# computed but deliberately NOT shown -- see docs/features/comparison.md,
# "The comparison set". A Brussels commune has no province and simply gets
# two entries instead of three; nothing here assumes a fixed depth.
COMPARISON_LEVELS = ("province", "region", "country")


def _additive_indicators(db_path: Path) -> set[str]:
    """Indicators that are counts or totals rather than ratios.

    Needed by the page, not just the maths: a commune is PART of its province,
    so "Antwerp population is -70.7% of Antwerp province" is arithmetically
    true and useless, while "Antwerp holds 29.3% of its province" is the
    figure a reader wants. A ratio is the opposite -- it belongs on the same
    scale as its reference, so the difference is what means something.

    Read from the indicators table rather than inferred from the unit: a
    total and an average can share the unit `eur`, so the unit cannot carry
    this. Derived indicators are absent from that table and correctly default
    to non-additive.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        return {
            row[0]
            for row in conn.execute("SELECT indicator_id FROM indicators WHERE is_additive = 1")
        }
    finally:
        conn.close()


def _latest_valued_period(periods: dict) -> str | None:
    """The newest period that actually carries a number, or None.

    NOT simply the newest period. 158 (commune, indicator) pairs have a
    SUPPRESSED latest period -- ONEM withheld the most recent year -- and
    `sorted(periods)[-1]` picks it. Every caller here goes on to use the value
    at that period, so taking the newest blindly would compare a null against
    a real province aggregate and print a dash beside a figure of 4,120, or
    plot a chart point with a hole in it.

    One helper rather than a check at each call site, because there are
    several and the failure is silent at every one of them.
    """
    for period in sorted(periods, reverse=True):
        if periods[period].get("value") is not None:
            return period
    return None


def _attach_comparisons(
    communes: dict[str, dict],
    aggregates: dict[tuple[str, str, str], dict],
    ancestry: dict[str, list[str]],
) -> int:
    """Attach province/region/Belgium values to each commune's indicators.

    Matched at the commune's OWN latest period for that indicator, never at
    the aggregate's latest: comparing a commune's 2023 income against
    Belgium's 2026 would be a different kind of wrong number, and the
    mismatch would be invisible on screen. If the aggregate does not exist
    for that exact period, the entry is absent -- the payload format's
    absent-not-null rule.
    """
    attached = 0
    for commune in communes.values():
        for indicator_id, entry in commune["indicators"].items():
            period = _latest_valued_period(entry["periods"])
            if period is None:
                continue
            comparison = {}
            for ancestor in ancestry.get(commune["geo_id"], []):
                agg = aggregates.get((ancestor, indicator_id, period))
                if agg and agg["level"] in COMPARISON_LEVELS:
                    comparison[agg["level"]] = agg
            if comparison:
                entry["comparison"] = comparison
                attached += 1
    return attached


def _backfill_cross_sections(indicators: dict[str, dict], communes: dict[str, dict]) -> int:
    """Give the DERIVED indicators a cross-section payload too.

    `indicators/{id}.json` is sliced from communes_export.csv, which reads the
    observations table directly and therefore holds only STORED indicators.
    The derived ones -- average income, unemployment rate, dependency ratio,
    the share indicators -- are computed by the Block G engine on the way into
    communes_history.csv, so until now they existed in every commune's own
    payload but had no cross-commune file at all.

    That was invisible while the only consumer was the commune profile page,
    which reads one commune. A map reads one INDICATOR across every commune,
    and the thirteen indicators missing here are precisely the ones worth
    mapping: a choropleth of a rate says something about a commune, while a
    choropleth of a headcount mostly redraws the population.

    Built from the same history the commune payloads use, taking each commune's
    most recent period -- the same "latest per (commune, indicator)" rule
    communes_export.csv applies, so the two agree on what "latest" means.
    Stored indicators are left exactly as the CSV produced them; this only
    fills gaps, so nothing already published changes shape.
    """
    # Worked out UP FRONT, before anything is added. Testing `indicator_id in
    # indicators` inside the loop would be true again as soon as the first
    # commune contributed, leaving every indicator with exactly one commune.
    stored = set(indicators)

    added = 0
    for nis, commune in communes.items():
        for indicator_id, entry in commune["indicators"].items():
            if indicator_id in stored:
                continue
            periods = entry["periods"]
            if not periods:
                continue
            latest = max(periods)
            payload = indicators.setdefault(
                indicator_id,
                {"name": entry["name"], "unit": entry["unit"], "communes": {}},
            )
            payload["communes"][nis] = {
                "value": periods[latest]["value"],
                "period": latest,
                "status": periods[latest]["status"],
            }
            if "updated" in entry:
                payload["updated"] = max(payload.get("updated", ""), entry["updated"])
            added += 1
    return added


def _indicator_index(
    indicators: dict[str, dict],
    names: dict[str, dict],
    additive: set[str],
    db_path: Path,
) -> list[dict]:
    """One row per municipal indicator, for a page that must not name any.

    THE MAP NEEDS THIS TO EXIST. `indicators/{id}.json` is one file per
    indicator, and a browser cannot list a directory, so without an index the
    only way for a page to know what it may draw is to hardcode the ids --
    exactly what the 50% gate's "zero indicator-specific frontend logic" check
    forbids, and what config/local_sections.yaml was created to avoid.

    `direction` travels because a choropleth has to choose which end of the
    colour ramp is which. Guessing from the name ("unemployment sounds bad")
    would be the page inventing meaning; preferred_direction is the source of
    truth the indicators table already holds.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        directions = dict(
            conn.execute("SELECT indicator_id, preferred_direction FROM indicators").fetchall()
        )
        decimals = dict(conn.execute("SELECT indicator_id, decimals FROM indicators").fetchall())
    finally:
        conn.close()

    index = []
    for indicator_id, payload in sorted(indicators.items()):
        index.append(
            {
                "indicator_code": indicator_id,
                "names": names.get(indicator_id, {"en": payload["name"]}),
                "unit": payload["unit"],
                "additive": indicator_id in additive,
                "direction": directions.get(indicator_id),
                "decimals": decimals.get(indicator_id),
                # How many communes actually carry a NUMBER. A choropleth over
                # an indicator covering 40 communes is a map of the gaps, so
                # the page shows this before drawing rather than after.
                #
                # Counts values, not keys: withheld cells are now published
                # (with a null value and status "suppressed"), and counting
                # them as coverage would tell a reader the map has data it
                # cannot draw. They are reported separately instead, because
                # "the source masked 13 communes" and "13 communes were never
                # measured" are different facts about an indicator.
                "coverage": sum(
                    1 for cell in payload["communes"].values() if cell["value"] is not None
                ),
                "suppressed": sum(
                    1
                    for cell in payload["communes"].values()
                    if cell["value"] is None and cell.get("status") == "suppressed"
                ),
            }
        )
    return index


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def _git_commit(repo_root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo_root, text=True
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def export_site_payloads(
    db_path: Path,
    communes_history_csv: Path,
    communes_latest_csv: Path,
    national_csv: Path,
    out_dir: Path,
    build_id: str,
    validation_status: str,
    aggregates_csv: Path | None = None,
    percentiles_csv: Path | None = None,
    sections_config: Path | None = SECTIONS_CONFIG,
) -> dict[str, int]:
    communes = _read_communes_history(communes_history_csv)
    indicators = _read_communes_latest(communes_latest_csv)
    national = _read_national(national_csv)
    geographies = _build_geographies(db_path)

    _backfill_cross_sections(indicators, communes)

    names = _indicator_names(db_path)
    for commune in communes.values():
        for indicator_id, entry in commune["indicators"].items():
            # `name` stays the English string the CSV supplied, so nothing
            # already reading it breaks; `names` adds the other two.
            entry["names"] = names.get(indicator_id, {"en": entry.get("name")})

    additive = _additive_indicators(db_path)
    for commune in communes.values():
        for indicator_id, entry in commune["indicators"].items():
            entry["additive"] = indicator_id in additive

    compared = 0
    if aggregates_csv is not None:
        compared = _attach_comparisons(
            communes, _read_aggregates(aggregates_csv), _ancestry(db_path)
        )

    ranked = 0
    if percentiles_csv is not None:
        ranked = _attach_percentiles(communes, _read_percentiles(percentiles_csv))

    _write_json(out_dir / "national.json", {"geo_id": "be:country", "indicators": national})

    for nis, payload in communes.items():
        _write_json(out_dir / "communes" / f"{nis}.json", payload)

    for indicator_id, payload in indicators.items():
        _write_json(
            out_dir / "indicators" / f"{indicator_id}.json",
            {"indicator_code": indicator_id, **payload},
        )

    _write_json(out_dir / "metadata" / "geographies.json", {"geographies": geographies})

    _write_json(
        out_dir / "metadata" / "indicators.json",
        {"indicators": _indicator_index(indicators, names, additive, db_path)},
    )

    # None skips the layout entirely: a caller exercising the payload RESHAPE
    # with a fixture-scale set of indicators is not testing the page layout,
    # and the cross-check below would rightly reject every real indicator as
    # missing from a two-row fixture.
    layout = _sections(sections_config) if sections_config else {"headlines": [], "sections": []}
    if layout["sections"] or layout["headlines"]:
        _check_sections(
            layout,
            {i for commune in communes.values() for i in commune["indicators"]},
        )
        _write_json(
            out_dir / "metadata" / "sections.json",
            {"headlines": layout["headlines"], "sections": layout["sections"]},
        )

    manifest = {
        "build_id": build_id,
        "git_commit": _git_commit(db_path.resolve().parents[0]),
        "build_date": datetime.now(timezone.utc).isoformat(),
        "datasets": {
            "national": {"indicators": len(national)},
            "comparisons": {"indicator_cells": compared},
            "percentiles": {"indicator_cells": ranked},
            "communes": {"count": len(communes), "indicators": len(indicators)},
            "geographies": {"count": len(geographies)},
        },
        "validation_status": validation_status,
    }
    _write_json(out_dir / "manifest.json", manifest)

    return {
        "comparisons": compared,
        "percentiles": ranked,
        "national_indicators": len(national),
        "communes": len(communes),
        "indicator_files": len(indicators),
        "geographies": len(geographies),
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export per-entity site payloads for the /local interface"
    )
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file (for geographies)")
    ap.add_argument("--communes-history", default="data/communes_history.csv")
    ap.add_argument("--communes-latest", default="data/communes_export.csv")
    ap.add_argument("--national", default="data/belgian_macro_export.csv")
    ap.add_argument(
        "--aggregates",
        default="data/aggregates.csv",
        help="Aggregate CSV from export_aggregates_csv.py; comparisons are omitted if absent",
    )
    ap.add_argument(
        "--percentiles",
        default="data/percentiles.csv",
        help="Peer positions from export_percentiles_csv.py; omitted if absent",
    )
    ap.add_argument("--out-dir", default="public/data")
    ap.add_argument("--build-id", default="local")
    ap.add_argument("--validation-status", default="unknown")
    args = ap.parse_args()

    counts = export_site_payloads(
        Path(args.db),
        Path(args.communes_history),
        Path(args.communes_latest),
        Path(args.national),
        Path(args.out_dir),
        args.build_id,
        args.validation_status,
        Path(args.aggregates) if args.aggregates else None,
        Path(args.percentiles) if args.percentiles else None,
    )
    print(
        f"Exported {counts['communes']} commune payloads, {counts['indicator_files']} "
        f"indicator payloads, {counts['national_indicators']} national indicators, "
        f"{counts['comparisons']} comparison cells, {counts['percentiles']} ranked cells, "
        f"{counts['geographies']} geographies to {args.out_dir}"
    )


if __name__ == "__main__":
    main()
