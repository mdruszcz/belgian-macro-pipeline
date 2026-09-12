"""
WalStat (IWEPS) adapter -- Block O, docs/features/walstat_adapter.md.

The fourth `MunicipalTimeSeriesSource`, and the first municipal source that
publishes a NIS code on every row, so geography goes through `resolve_geo`
exactly as CLAUDE.md rule 3 says it must -- no name matching, no overrides.

THE API RETURNS A BARE JSON ARRAY AND NOTHING ELSE. One request per series,
one object per (commune, year):

    {"ins": "92094", "type_entite": "Commune", "entite": "Namur",
     "periode": "année 2024", "valeur": "2319.7"}

No metadata, no unit, no status, no flag for a masked or missing cell. So
every guarantee this adapter offers is one it checks itself, and every check
refuses rather than coerces (rule 13):

* the response must be a list of objects with exactly those five keys;
* every row must be a commune -- the same series ids answer `provinces` and
  `arr` selections with other entity types, and a province row loaded as a
  commune would be a wrong number under a real NIS code;
* the period must read "année YYYY" (the site varies the capital), and the
  value must be a number -- with ONE documented exception, learned from the
  first live run: WalStat writes the literal string "non disponible" where a
  commune's account for a year is not yet available (row 407 of the 2024
  revenue series, Chièvres). That is a MISSING reading -- not zero, not a
  suppression (nothing is withheld; the account has not been filed) -- so
  the row is skipped, counted, and never written: absence is how this
  pipeline says "we have no reading" (CLAUDE.md rule 26). Any other
  non-numeric value is still refused;
* the NIS code must resolve for that year -- with the source's grid
  convention handled explicitly, because WALSTAT PUBLISHES EVERY YEAR ON ITS
  CURRENT COMMUNE GRID (learned from the first live run, 2026-09-11):
    - the eleven Hainaut communes RE-CODED on 2019-01-01 (Enghien 55010 ->
      51067, Mouscron 54007 -> 57096, ...) appear under their new code for
      2013-2018 too. Same commune, same name, one predecessor: the value is
      attributed to the code valid in that year, found by walking the
      geography table's own `successor_geo_id` back ONE step, and only when
      that step is 1:1 and the names agree. A code with two predecessors is a
      merger, and a merger is never substituted;
    - Bastogne+Bertogne (82039, merged 2024-12-02) appears for every year
      back to 2013, with a backcast per-inhabitant figure, BESIDE its two
      parts. For a year in which the merged commune did not exist and its
      parts are in the response, the backcast row is SKIPPED AND COUNTED --
      never loaded as a third commune, never silently dropped: the count is
      reported through `rows_read`, and the rule is written here;
* and, per period, the communes returned are checked against the Walloon
  communes that existed on that period's first day. A commune the source
  did not list for a year (it happens: 53068 is absent from the 2017
  revenue series) is a MISSING reading -- recorded in `missing`, reported
  by the sync, never invented. An UNEXPECTED commune is refused: a Flemish
  code in a Walloon series means the request or the API changed. And a
  year covering fewer than 90% of the communes is refused as a partial
  response -- the same 90% floor this pipeline already applies to
  aggregates (CLAUDE.md, Definitions), not a new threshold.

Status is `final` on every row: WalStat publishes closed municipal accounts
(the source is the SPW's Département des Finances locales, accounts "à
l'exercice global"), and it publishes no provisional marker to carry. Stated
here rather than assumed elsewhere.
"""

from __future__ import annotations

import json
import re
import sqlite3

from src.fetchers.base import MunicipalTimeSeriesSource
from src.geography.resolve import UnknownGeographyError, period_to_date, resolve_geo

#: The keys IWEPS's data API writes on every row, exactly. A new key would
#: mean the API changed shape; a missing one means it broke.
EXPECTED_KEYS = frozenset({"ins", "type_entite", "entite", "periode", "valeur"})

#: "année 2024", "Année 2023" -- the site is not consistent about the capital.
_PERIOD = re.compile(r"^ann[ée]e\s+(\d{4})$", re.IGNORECASE)

#: The Walloon Region's geo_id, the root every commune here must chain up to.
WALLONIA = "be:reg:03000"

#: WalStat's own marker for an account not yet filed. Exact string, seen on the
#: live API 2026-09-11; anything else non-numeric is a broken row.
NOT_AVAILABLE = "non disponible"

#: Below this share of a year's communes the response is a partial year, not a
#: year with gaps. The pipeline's existing coverage floor for aggregates.
COVERAGE_FLOOR = 0.9


class WalStatSchemaError(ValueError):
    """The response is not the shape docs/features/walstat_adapter.md documents."""


class WalStatCoverageError(ValueError):
    """The communes in the response are not the Walloon communes of that year."""


class WalStatGeographyError(ValueError):
    """A code the period-aware lookup refuses, and no 1:1 re-coding explains it."""


def resolve_on_source_grid(conn: sqlite3.Connection, nis: str, period: str) -> str | None:
    """The geo_id for a WalStat row, honouring the source's current-grid habit.

    Returns the period-valid geo_id; or the geo_id of the ONE predecessor of a
    re-coded commune when the code is only valid later; or None when the code
    is a merged entity that did not yet exist in that period (the caller skips
    and counts the row). Raises for anything else -- an unknown code, or a
    code valid later with no predecessor at all.

    Deliberately not a change to `resolve_geo` (CLAUDE.md rule 19): this is
    one adapter's documented reading of one source's convention, kept beside
    the adapter, tested against the real 2019 re-coding list.
    """
    try:
        return resolve_geo(conn, nis, period)
    except UnknownGeographyError:
        pass
    as_of = period_to_date(period)
    later = conn.execute(
        "SELECT geo_id, name_fr, valid_from FROM geographies "
        "WHERE nis_code = ? AND valid_from > ? ORDER BY valid_from",
        (nis, as_of),
    ).fetchone()
    if later is None:
        raise WalStatGeographyError(
            f"NIS {nis} does not resolve for {period} and is not a later code either; "
            "refusing to guess which commune this row belongs to"
        )
    geo_id, name, valid_from = later
    predecessors = conn.execute(
        "SELECT geo_id, name_fr, valid_from, valid_to FROM geographies WHERE successor_geo_id = ?",
        (geo_id,),
    ).fetchall()
    if len(predecessors) >= 2:
        # A merger. The source backcasts a figure for an entity that did not
        # exist; its parts carry the real accounts for that year.
        return None
    if len(predecessors) == 1:
        p_geo, p_name, p_from, p_to = predecessors[0]
        if p_name == name and p_from <= as_of and (p_to is None or p_to > as_of):
            return p_geo
        raise WalStatGeographyError(
            f"NIS {nis} ({name}, valid from {valid_from}) has predecessor {p_geo} ({p_name}) "
            f"that does not cover {as_of} under the same name; not a plain re-coding, refusing"
        )
    raise WalStatGeographyError(
        f"NIS {nis} ({name}) is only valid from {valid_from} and has no predecessor in the "
        f"geography table, so its {period} value has no commune to belong to"
    )


def walloon_communes_on(conn: sqlite3.Connection, period: str) -> dict[str, str]:
    """`{nis_code: geo_id}` for every commune inside Wallonia that existed on
    the first day of `period`, walked up the geography table's parent chain.

    The reference set for coverage. Derived from `geographies` rather than
    hardcoded to 261 or 262: the answer depends on the year (262 in 2024, 261
    from the Bastogne-Bertogne merger of 2024-12-02 onward), and a number in
    the code would have been wrong by the next merger.
    """
    as_of = period_to_date(period)
    rows = conn.execute(
        """
        WITH RECURSIVE up(start_geo, start_nis, node) AS (
            SELECT geo_id, nis_code, parent_geo_id FROM geographies
             WHERE level = 'municipality'
               AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
            UNION ALL
            SELECT up.start_geo, up.start_nis, g.parent_geo_id
              FROM geographies g JOIN up ON g.geo_id = up.node
        )
        SELECT DISTINCT start_nis, start_geo FROM up WHERE node = ?
        """,
        (as_of, as_of, WALLONIA),
    ).fetchall()
    return dict(rows)


class WalStatSource(MunicipalTimeSeriesSource):
    source_id = "walstat"
    adapter = "walstat"
    raw_extension = "json"

    def __init__(self) -> None:
        #: (nis, period) pairs the source marked "non disponible" in the last
        #: parse. Exposed so a sync can report them; never written as rows.
        self.unavailable: list[tuple[str, str]] = []
        #: (nis, period) pairs the source backcast for a merged commune that
        #: did not yet exist in that period. Skipped, counted, reported.
        self.backcast: list[tuple[str, str]] = []
        #: (nis, period, geo_id) rows attributed to a re-coded commune's
        #: earlier code. Loaded under the period-valid geo_id.
        self.recoded: list[tuple[str, str, str]] = []
        #: period -> geo_ids of communes the source listed in no row at all
        #: for that year. Missing readings, reported, never invented.
        self.missing: dict[str, list[str]] = {}

    def _rows_read_hint(self, rows: list[dict]) -> int | None:
        # rows_read counts every row the source sent, including the
        # "non disponible" cells and the backcast rows; rows_written does not.
        # The gap is the number of readings that were not there to load.
        return len(rows) + len(self.unavailable) + len(self.backcast)

    def _parse(self, raw: bytes, **kwargs) -> list[dict]:
        """`{geo_id, period, value, status}` per row, after every check above.

        `geo_conn` is required: without the geography table there is no way
        to resolve a NIS code for a year, and guessing `be:mun:{ins}` would
        attribute a merged commune's figure to a code that no longer exists.
        """
        conn: sqlite3.Connection | None = kwargs.get("geo_conn")
        if conn is None:
            raise ValueError("WalStatSource._parse needs geo_conn= (a connection with geographies)")
        reconcile: bool = kwargs.get("reconcile", True)

        try:
            data = json.loads(raw)
        except ValueError as exc:
            raise WalStatSchemaError(f"WalStat response is not JSON: {exc}") from exc
        if not isinstance(data, list):
            raise WalStatSchemaError(
                f"WalStat response is a {type(data).__name__}, not the documented bare array"
            )

        results: list[dict] = []
        seen: set[tuple[str, str]] = set()
        resolved: dict[tuple[str, str], str] = {}
        by_period: dict[str, set[str]] = {}
        self.unavailable = []
        self.backcast = []
        self.recoded = []
        self.missing = {}
        for index, row in enumerate(data):
            if not isinstance(row, dict) or set(row) != EXPECTED_KEYS:
                got = sorted(row) if isinstance(row, dict) else type(row).__name__
                raise WalStatSchemaError(
                    f"row {index}: keys {got} are not the documented {sorted(EXPECTED_KEYS)}"
                )
            if row["type_entite"] != "Commune":
                raise WalStatSchemaError(
                    f"row {index}: type_entite {row['type_entite']!r} is not 'Commune' -- this "
                    "series was requested for communes and something else came back"
                )
            match = _PERIOD.match(str(row["periode"]).strip())
            if not match:
                raise WalStatSchemaError(
                    f"row {index}: periode {row['periode']!r} does not read 'année YYYY'"
                )
            period = match.group(1)
            nis = str(row["ins"]).strip()
            if (nis, period) in seen:
                raise WalStatSchemaError(f"INS {nis} appears twice for {period}")
            seen.add((nis, period))
            geo_id = resolve_on_source_grid(conn, nis, period)
            if geo_id is None:
                self.backcast.append((nis, period))
                continue
            # The re-coding rule can map a second code onto a commune another
            # row already supplied (55010 and 51067 both for 2013, say). Two
            # rows for one commune-year would become two vintages in one run,
            # the last one winning silently -- refused like any duplicate.
            if (geo_id, period) in resolved:
                raise WalStatSchemaError(
                    f"INS {nis} for {period} resolves to {geo_id}, which INS "
                    f"{resolved[(geo_id, period)]} already supplied for {period}"
                )
            resolved[(geo_id, period)] = nis
            if not geo_id.endswith(nis):
                self.recoded.append((nis, period, geo_id))
            raw_value = str(row["valeur"]).strip()
            if raw_value.lower() == NOT_AVAILABLE:
                # Present in the response -- so it counts toward coverage --
                # but carrying no reading.
                by_period.setdefault(period, set()).add(geo_id)
                self.unavailable.append((nis, period))
                continue
            try:
                value = float(raw_value)
            except ValueError as exc:
                # Not zero, and not a suppression: WalStat documents no masking
                # rule, so an unreadable value is a broken row, not a fact.
                raise WalStatSchemaError(
                    f"row {index}: valeur {row['valeur']!r} for INS {nis} in {period} is not a number"
                ) from exc
            by_period.setdefault(period, set()).add(geo_id)
            results.append({"geo_id": geo_id, "period": period, "value": value, "status": "final"})

        if reconcile:
            for period, returned in sorted(by_period.items()):
                # Compared on RESOLVED geo_ids, so a re-coded commune counts
                # once under the code valid that year.
                expected = set(walloon_communes_on(conn, period).values())
                extra = sorted(returned - expected)
                if extra:
                    raise WalStatCoverageError(
                        f"{period}: {len(extra)} commune(s) in the response are not Walloon "
                        f"communes of that year: {extra[:10]}{'...' if len(extra) > 10 else ''}. "
                        "The request or the API changed; refusing to load."
                    )
                missing = sorted(expected - returned)
                if len(returned) < COVERAGE_FLOOR * len(expected):
                    raise WalStatCoverageError(
                        f"{period}: the response covers {len(returned)} of the {len(expected)} "
                        f"Walloon communes on {period_to_date(period)}, under the 90% floor. "
                        f"Missing: {missing[:10]}{'...' if len(missing) > 10 else ''}. "
                        "Refusing to load a partial year as if it were complete."
                    )
                if missing:
                    self.missing[period] = missing
        return results
