"""Commune-to-commune property-buyer flows -- SPF Finances AGDP 52.01.21
("origin of buyers, natural persons", uuid b90b50be-9dfc-11f0-99e9-00be432db085).
docs/features/commune_flows.md (spec), docs/decisions/0013-commune-flow-shares-and-buckets.md
(ADR, the formulas below implement it exactly -- read that file before changing anything here).

PR 1 SCOPE (this module): the reader (OriginTable.csv + the Municipality wide CSV,
ParcelNature=TOTAL rows only) and the per-destination computation -- denominator, top 8 named
origins with the deterministic tie-break, the six buckets, coverage, and the zero-purchase
state. NOT in scope: the public payload, the block, owners' origin (52.01.16), any year but
the latest (maintainer's decision, 2026-09-24).

REUSE, NOT A NEW READER. `parse_atom_feed(..., frequency="AY")` and `open_municipality_csv()`
from src.fetchers.spf_agdp are the same ATOM/ranged-zip machinery every other AGDP dataset
uses -- this module only adds what is genuinely new: the OriginTable.csv (latin-1, not the
utf-8-sig every other AGDP CSV uses -- measured live 2026-09-24, country names are mojibake
under utf-8) and the wide-matrix parse (813 origin columns, not spf_agdp.py's one-row `select`
shape).

DECIMAL THROUGHOUT, PER ADR DECISION 6. Every cell value is parsed as `Decimal`, every sum is a
Decimal sum, and rounding to one decimal place happens exactly once, at the end, half-up. A
float would not reproduce the ADR's own worked values byte-for-byte (413.995833337 has more
significant digits than a float safely round-trips through repr), and rule 35 (byte-identical
rebuilds) depends on this.

GEOGRAPHY, BOTH AXES, PER ADR DECISION 4 (rules 3/25). The destination's own geo_id AND every
non-zero origin NIS resolve through resolve_geo(conn, nis, period) at the row's own period --
never a hardcoded province/region list, never the destination's geography reused for the
origin. An origin NIS that fails to resolve raises (rule 13); it is never silently bucketed as
unknown -- only 000/999 go to origin_unknown (ADR decision 4, sub-bullet 6).

ADR DECISION 9 (latent for this release): the latest-year-only scope means every 2025 origin
and destination NIS resolves on the post-2025-merger grid with no predecessor-summing needed
(verified live: 565/565 destinations resolve). This module does NOT sum predecessors into a
successor -- that is explicitly out of scope until a second year is added (ADR decision 9), and
doing it here now would be an unreviewed formula change.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal

from src.geography.resolve import UnknownGeographyError, resolve_geo

#: The two origin codes the OriginTable.csv/TechSpec disagree about (ADR "One place the
#: TechSpec contradicts the origin lookup") -- both are unknown-origin states regardless of
#: which document is read, so both go to one bucket, never split and never folded into abroad.
_UNKNOWN_ORIGIN_CODES = frozenset({"000", "999"})

#: How many named (non-bucket) origins the store keeps per destination (ADR decision 5).
TOP_N = 8

#: Required columns in the Municipality wide CSV -- CLAUDE.md rule 13: a header drift refuses
#: loudly rather than silently reading a re-indexed row.
_REQUIRED_MUNICIPALITY_COLUMNS = ("NISCode", "ParcelNature", "ParcelsNumber")

#: Required columns in OriginTable.csv.
_REQUIRED_ORIGIN_TABLE_COLUMNS = ("BuyerFrom", "NISCode", "NameFre", "NameDut", "ISOCode")

#: The row we read per destination -- ADR decision 2, "the row's own summed origin cells at
#: ParcelNature=TOTAL".
_PARCEL_NATURE_SELECT = "TOTAL"

_ONE_DECIMAL = Decimal("0.1")

#: The six bucket ids, ADR decision 4/7 -- fixed, exhaustive, disjoint. Never a Python set
#: ordering: the store writes them in this order for a stable, deterministic file (rule 35).
BUCKET_IDS = (
    "same_commune",
    "rest_of_arrondissement",
    "rest_of_region",
    "other_regions",
    "abroad",
    "origin_unknown",
)

#: The zero-purchase state (ADR decision 7): a destination with D(d) = 0. No share of any kind
#: is published for it -- not 0% for six buckets, which would assert a distribution that does
#: not exist.
NO_PURCHASES_RECORDED = "no_purchases_recorded"


class FlowSchemaError(ValueError):
    """The OriginTable.csv or the Municipality wide CSV is not the documented shape, or an
    origin/destination NIS code does not resolve (CLAUDE.md rule 13)."""


@dataclass(frozen=True)
class OriginEntry:
    """A named origin's parsed identity (row of OriginTable.csv) -- resolved lazily per
    non-zero cell only, never for all 813 columns up front (most are zero for a given
    destination and cost nothing to skip)."""

    code: str  # the raw BuyerFromXXXXXX suffix, e.g. "11004" or "101" or "000"
    nis_code: str | None  # None for a non-NIS (country/unknown) code
    name_fr: str
    name_dut: str
    iso_code: str | None


@dataclass(frozen=True)
class NamedOrigin:
    """One of the top-8 named origins for a destination, per ADR decision 5 -- Belgian
    communes only (buckets 1-4's members), abroad and origin_unknown never appear here."""

    nis: str
    value: Decimal
    share: Decimal  # rounded to one decimal place, ADR decision 6


@dataclass(frozen=True)
class DestinationFlows:
    """One destination commune's computed result for one period -- exactly what the store
    persists (plus provenance the caller adds)."""

    dest_geo_id: str
    dest_nis: str
    period: str
    parcels_number: int
    denominator: Decimal  # D(d), unrounded -- ADR decision 6 keeps the unrounded sum
    coverage: Decimal  # D(d) / ParcelsNumber, rounded to one decimal place (a %, e.g. 97.9)
    state: str  # "final" (has a D(d) > 0) or NO_PURCHASES_RECORDED
    top_origins: tuple[NamedOrigin, ...] = field(default_factory=tuple)
    bucket_values: dict[str, Decimal] = field(default_factory=dict)  # bucket_id -> unrounded sum
    bucket_shares: dict[str, Decimal] = field(default_factory=dict)  # bucket_id -> rounded %


def _round1(value: Decimal) -> Decimal:
    return value.quantize(_ONE_DECIMAL, rounding=ROUND_HALF_UP)


def parse_origin_table(raw: bytes) -> dict[str, OriginEntry]:
    """OriginTable.csv -> {BuyerFromXXXXXX suffix: OriginEntry}.

    latin-1, semicolon-delimited (ADR "Consequences") -- NOT utf-8-sig, the encoding every
    other AGDP Municipality CSV in this pipeline uses. Decoding this file as UTF-8 does not
    raise (every byte here is valid Latin-1, hence valid as a UTF-8 continuation byte in some
    cases and simply wrong in others) -- it silently mojibakes every accented character, which
    is exactly the kind of silent corruption CLAUDE.md rule 13 exists to prevent, so this
    function is the ONLY place that encoding is chosen and it is never reused elsewhere.
    """
    text = raw.decode("latin-1")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    if reader.fieldnames is None:
        raise FlowSchemaError("OriginTable.csv has no header row.")
    missing = [c for c in _REQUIRED_ORIGIN_TABLE_COLUMNS if c not in reader.fieldnames]
    if missing:
        raise FlowSchemaError(
            f"OriginTable.csv is missing required column(s) {missing}. Header has "
            f"{reader.fieldnames}. Refusing to guess a replacement (CLAUDE.md rule 13)."
        )

    entries: dict[str, OriginEntry] = {}
    for row in reader:
        buyer_from = row["BuyerFrom"].strip()
        if not buyer_from.startswith("BuyerFrom"):
            raise FlowSchemaError(
                f"OriginTable.csv row {row!r}: BuyerFrom column does not start with "
                "'BuyerFrom'. Refusing to guess the code (CLAUDE.md rule 13)."
            )
        code = buyer_from[len("BuyerFrom") :]
        nis = row["NISCode"].strip()
        iso = row["ISOCode"].strip()
        entries[code] = OriginEntry(
            code=code,
            nis_code=nis if nis == code and nis.isdigit() and len(nis) == 5 else None,
            name_fr=row["NameFre"].strip(),
            name_dut=row["NameDut"].strip(),
            iso_code=iso or None,
        )
    if not entries:
        raise FlowSchemaError(
            "OriginTable.csv has zero data rows. Refusing to load an empty origin lookup "
            "(CLAUDE.md rule 13)."
        )
    return entries


def _parse_decimal(raw: str, *, context: str) -> Decimal:
    raw = raw.strip()
    if raw == "":
        raise FlowSchemaError(
            f"{context}: a BuyerFrom cell is blank. The ADR/spec measured zero blank cells "
            "across the whole matrix -- a blank cell is a schema surprise (CLAUDE.md rule 13)."
        )
    try:
        return Decimal(raw)
    except Exception as exc:  # noqa: BLE001 -- re-raised as a typed schema error below
        raise FlowSchemaError(f"{context}: cell {raw!r} is not a decimal number.") from exc


def parse_municipality_rows(raw: bytes, *, origin_table: dict[str, OriginEntry]) -> list[dict]:
    """Municipality wide CSV -> one dict per ParcelNature=TOTAL destination row:
    {"nis": str, "parcels_number": int, "cells": {origin_code: Decimal}} -- only non-zero
    cells are kept in `cells` (813 columns per row, the overwhelming majority zero for any
    one destination; keeping only non-zero values is what makes the trimmed store small).

    utf-8-sig, semicolon-delimited -- same shape every other AGDP Municipality CSV uses
    (src/fetchers/spf_agdp.py's module docstring), UNLIKE OriginTable.csv above.
    """
    text = raw.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    if reader.fieldnames is None:
        raise FlowSchemaError("Municipality wide CSV has no header row.")
    missing = [c for c in _REQUIRED_MUNICIPALITY_COLUMNS if c not in reader.fieldnames]
    if missing:
        raise FlowSchemaError(
            f"Municipality wide CSV is missing required column(s) {missing}. Header has "
            f"{reader.fieldnames[:20]}... Refusing to guess a replacement (CLAUDE.md rule 13)."
        )
    origin_columns = [c for c in reader.fieldnames if c.startswith("BuyerFrom")]
    if not origin_columns:
        raise FlowSchemaError(
            "Municipality wide CSV has zero BuyerFrom* columns. Refusing to guess "
            "(CLAUDE.md rule 13)."
        )
    # Every origin column's suffix must be a code OriginTable.csv knows about -- a column the
    # lookup does not cover cannot be bucketed or named, which is a schema surprise, not a
    # silently-droppable extra.
    unknown_columns = [c for c in origin_columns if c[len("BuyerFrom") :] not in origin_table]
    if unknown_columns:
        raise FlowSchemaError(
            f"Municipality wide CSV has {len(unknown_columns)} BuyerFrom* column(s) with no "
            f"matching OriginTable.csv row, e.g. {unknown_columns[:5]}. Refusing to guess "
            "(CLAUDE.md rule 13)."
        )

    destinations: list[dict] = []
    seen_nis: set[str] = set()
    for row in reader:
        if row.get("ParcelNature") != _PARCEL_NATURE_SELECT:
            continue
        nis = row["NISCode"].strip()
        if not nis:
            raise FlowSchemaError("Municipality wide CSV: a TOTAL row has a blank NISCode.")
        if nis in seen_nis:
            raise FlowSchemaError(
                f"Municipality wide CSV: NIS {nis!r} has more than one ParcelNature=TOTAL row. "
                "Refusing to guess which one is authoritative (CLAUDE.md rule 13)."
            )
        seen_nis.add(nis)

        parcels_raw = row["ParcelsNumber"].strip()
        if parcels_raw == "" or not parcels_raw.lstrip("-").isdigit():
            raise FlowSchemaError(
                f"Municipality wide CSV, NIS {nis}: ParcelsNumber {parcels_raw!r} is not an "
                "integer. Refusing to guess (CLAUDE.md rule 13)."
            )
        parcels_number = int(parcels_raw)

        cells: dict[str, Decimal] = {}
        for col in origin_columns:
            code = col[len("BuyerFrom") :]
            value = _parse_decimal(row[col], context=f"NIS {nis}, column {col}")
            if value != 0:
                cells[code] = value

        destinations.append({"nis": nis, "parcels_number": parcels_number, "cells": cells})

    if not destinations:
        raise FlowSchemaError(
            f"Municipality wide CSV: zero rows matched ParcelNature={_PARCEL_NATURE_SELECT!r}. "
            "Refusing to load an empty series (CLAUDE.md rule 13)."
        )
    return destinations


def _bucket_for_origin(
    *,
    origin_geo_id: str | None,
    origin_is_unknown: bool,
    origin_is_abroad: bool,
    dest_geo_id: str,
    dest_arr_id: str,
    dest_reg_id: str,
    origin_arr_id: str | None,
    origin_reg_id: str | None,
) -> str:
    """ADR decision 4's six-way exhaustive, disjoint classification, evaluated on the ORIGIN's
    own geography at the row's period (never the destination's geography reused for the
    origin, and never a hardcoded province/region list)."""
    if origin_is_unknown:
        return "origin_unknown"
    if origin_is_abroad:
        return "abroad"
    if origin_geo_id == dest_geo_id:
        return "same_commune"
    if origin_arr_id == dest_arr_id:
        return "rest_of_arrondissement"
    if origin_reg_id == dest_reg_id:
        return "rest_of_region"
    return "other_regions"


def _ancestors(conn, geo_id: str) -> tuple[str | None, str | None]:
    """(arrondissement_geo_id, region_geo_id) for a municipality geo_id, walking
    parent_geo_id up the chain: municipality -> arrondissement -> province -> region (the
    real hierarchy depth on this schema, checked live 2026-09-24 against `geographies` --
    a municipality's parent is its arrondissement, but the region is the arrondissement's
    GRANDPARENT via the province, never its direct parent). Walks by LEVEL, not a fixed hop
    count, so a future schema change that drops or adds a level fails loudly here rather than
    silently misclassifying a bucket."""
    row = conn.execute(
        "SELECT level, parent_geo_id FROM geographies WHERE geo_id = ?", (geo_id,)
    ).fetchone()
    if row is None or row[0] != "municipality" or row[1] is None:
        raise FlowSchemaError(f"geo_id {geo_id!r} is not a municipality with a parent.")
    arr_id = row[1]

    arr_row = conn.execute(
        "SELECT level, parent_geo_id FROM geographies WHERE geo_id = ?", (arr_id,)
    ).fetchone()
    if arr_row is None or arr_row[0] != "arrondissement" or arr_row[1] is None:
        raise FlowSchemaError(
            f"geo_id {geo_id!r}'s parent {arr_id!r} is not an arrondissement with a parent -- "
            "geography hierarchy does not match what this module assumes."
        )
    current_id = arr_row[1]
    # Walk up from the arrondissement's parent until a 'region' row is reached, tolerating
    # either a province in between (the real Belgian hierarchy) or a region as the direct
    # parent, so a schema simplification does not silently break this.
    seen = {geo_id, arr_id}
    while True:
        node = conn.execute(
            "SELECT level, parent_geo_id FROM geographies WHERE geo_id = ?", (current_id,)
        ).fetchone()
        if node is None:
            raise FlowSchemaError(
                f"geo_id {geo_id!r}: ancestor {current_id!r} not found in geographies while "
                "walking up to its region."
            )
        level, parent_id = node
        if level == "region":
            return arr_id, current_id
        if current_id in seen or parent_id is None:
            raise FlowSchemaError(
                f"geo_id {geo_id!r}: walked up to {current_id!r} (level {level!r}) without "
                "reaching a 'region' row -- geography hierarchy does not match what this "
                "module assumes."
            )
        seen.add(current_id)
        current_id = parent_id


def compute_destination_flows(
    conn,
    *,
    dest_row: dict,
    origin_table: dict[str, OriginEntry],
    period: str,
) -> DestinationFlows:
    """The ADR's full per-destination computation: resolve both axes, sum D(d), the six
    buckets, coverage, the top-8 named origins with their tie-break, and the zero-purchase
    state. Raises FlowSchemaError (wrapping UnknownGeographyError) if the destination or any
    non-zero origin NIS fails to resolve -- never silently skipped or bucketed as unknown
    (ADR decision 4's own text: "An origin NIS that fails to resolve raises... it is never
    bucketed as unknown")."""
    dest_nis = dest_row["nis"]
    try:
        dest_geo_id = resolve_geo(conn, dest_nis, period)
    except UnknownGeographyError as exc:
        raise FlowSchemaError(f"Destination NIS {dest_nis!r}: {exc}") from exc
    dest_arr_id, dest_reg_id = _ancestors(conn, dest_geo_id)

    cells: dict[str, Decimal] = dest_row["cells"]
    parcels_number = dest_row["parcels_number"]
    denominator = sum(cells.values(), start=Decimal(0))

    if denominator == 0:
        return DestinationFlows(
            dest_geo_id=dest_geo_id,
            dest_nis=dest_nis,
            period=period,
            parcels_number=parcels_number,
            denominator=Decimal(0),
            coverage=Decimal("0.0") if parcels_number == 0 else _round1(Decimal(0)),
            state=NO_PURCHASES_RECORDED,
        )

    bucket_values: dict[str, Decimal] = {b: Decimal(0) for b in BUCKET_IDS}
    named_candidates: list[tuple[str, Decimal]] = []  # (origin_nis, value) -- buckets 1-4 only

    for code, value in cells.items():
        entry = origin_table[code]
        is_unknown = code in _UNKNOWN_ORIGIN_CODES
        is_abroad = (not is_unknown) and entry.nis_code is None

        if is_unknown or is_abroad:
            bucket = _bucket_for_origin(
                origin_geo_id=None,
                origin_is_unknown=is_unknown,
                origin_is_abroad=is_abroad,
                dest_geo_id=dest_geo_id,
                dest_arr_id=dest_arr_id,
                dest_reg_id=dest_reg_id,
                origin_arr_id=None,
                origin_reg_id=None,
            )
            bucket_values[bucket] += value
            continue

        # A named Belgian origin -- resolve it at THIS row's own period (ADR decision 4/9).
        origin_nis = entry.nis_code
        try:
            origin_geo_id = resolve_geo(conn, origin_nis, period)
        except UnknownGeographyError as exc:
            raise FlowSchemaError(
                f"Destination NIS {dest_nis!r}, origin NIS {origin_nis!r} (column "
                f"BuyerFrom{code}): {exc}. An origin NIS that fails to resolve is a schema "
                "surprise -- never silently bucketed as unknown (ADR 0013 decision 4)."
            ) from exc
        origin_arr_id, origin_reg_id = _ancestors(conn, origin_geo_id)
        bucket = _bucket_for_origin(
            origin_geo_id=origin_geo_id,
            origin_is_unknown=False,
            origin_is_abroad=False,
            dest_geo_id=dest_geo_id,
            dest_arr_id=dest_arr_id,
            dest_reg_id=dest_reg_id,
            origin_arr_id=origin_arr_id,
            origin_reg_id=origin_reg_id,
        )
        bucket_values[bucket] += value
        named_candidates.append((origin_nis, value))

    bucket_sum = sum(bucket_values.values(), start=Decimal(0))
    if bucket_sum != denominator:
        raise FlowSchemaError(
            f"Destination NIS {dest_nis!r}: bucket sum {bucket_sum} does not equal the "
            f"denominator {denominator}. The six buckets must be exhaustive and disjoint "
            "(ADR 0013 decision 4) -- this is a bug, not a data surprise."
        )

    # ADR decision 5: rank by value descending, tie-break by NIS ascending (zero-padded
    # string, not int, and on the exact Decimal value, never a rounded/float one).
    named_candidates.sort(key=lambda t: (-t[1], t[0]))
    top = named_candidates[:TOP_N]
    top_origins = tuple(
        NamedOrigin(nis=nis, value=value, share=_round1(value / denominator * 100))
        for nis, value in top
    )

    bucket_shares = {b: _round1(v / denominator * 100) for b, v in bucket_values.items()}

    coverage = _round1(denominator / Decimal(parcels_number) * 100) if parcels_number else None
    if coverage is None:
        raise FlowSchemaError(
            f"Destination NIS {dest_nis!r}: ParcelsNumber is 0 but D(d) is {denominator} "
            "(non-zero) -- a denominator built entirely from cells the SPF itself reports "
            "against a zero-parcel row is a schema surprise, not something to divide by zero "
            "and hide."
        )

    return DestinationFlows(
        dest_geo_id=dest_geo_id,
        dest_nis=dest_nis,
        period=period,
        parcels_number=parcels_number,
        denominator=denominator,
        coverage=coverage,
        state="final",
        top_origins=top_origins,
        bucket_values=bucket_values,
        bucket_shares=bucket_shares,
    )


def compute_all_destinations(
    conn, *, destinations: list[dict], origin_table: dict[str, OriginEntry], period: str
) -> list[DestinationFlows]:
    """Every destination row's DestinationFlows, sorted by dest_geo_id for a deterministic,
    byte-identical store (CLAUDE.md rule 35)."""
    results = [
        compute_destination_flows(conn, dest_row=row, origin_table=origin_table, period=period)
        for row in destinations
    ]
    results.sort(key=lambda d: d.dest_geo_id)
    return results
