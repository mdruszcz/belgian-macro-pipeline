"""
EurostatSource -- fetches Eurostat's own dissemination API directly
(JSON-stat 2.0), one request per dataset returning every country Eurostat
publishes for it. Replaces DBnomics as the transport for Eurostat data
(international pilot PR 1): same agency, same figures, no DBnomics ODbL
share-alike obligation on top of Eurostat's own licence (docs/data_catalog.md,
docs/features/international.md).

WHY A NEW ADAPTER, NOT A LOOP OVER DBnomicsSource. DBnomicsSource (and the
old EurostatSource it used to be) requests one series and keeps
`series.docs[0]` only -- pointed at a multi-country query it would silently
keep one country's rows and drop the rest. This adapter requests the whole
dataset, walks every (geo, time) cell in the JSON-stat cube explicitly, and
refuses (FetchError) rather than guess when a dimension it did not expect to
vary turns out to carry more than one code.

JSON-STAT 2.0 SHAPE. A dataset response carries:
  id        -- dimension names, in cube order, e.g. ["freq","unit","s_adj","na_item","geo","time"]
  size      -- each dimension's cardinality, same order as `id`
  dimension -- {dim_name: {"category": {"index": {code: position}}}}
  value     -- {"<linear offset>": number}, sparse (absent = no observation)
  status    -- {"<linear offset>": "<flag letters>"}, sparse, present only
               where Eurostat qualifies a value

`num_found`/pagination is a DBnomics concept and does not apply here: a
dataset response is the whole cube in one reply, verified against a live
namq_10_gdp/gov_10dd_edpt1/prc_hicp_manr/une_rt_m/ei_bssi_m_r2 fetch
(2026-09-13; up to 44 countries, no pagination header of any kind).

The linear offset is standard row-major: for dimensions d_0..d_n-1 (in `id`
order) with sizes s_0..s_n-1, offset(i_0,...,i_n-1) = sum_k(i_k * stride_k),
stride_k = product of s_j for j > k. Verified against namq_10_gdp's real BE
Q1-2023 cell (2026-09-13): geo index 6 (BE), time index 0 (2023-Q1), computed
offset 84 matched the brute-force position of value 109617.1 in the raw
response.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from src.fetchers.base import FetchError, MultiGeoTimeSeriesSource

#: Eurostat's own OBS_FLAG codelist (SDMX 2.1, ESTAT:OBS_FLAG -- the full,
#: authoritative list fetched and re-checked 2026-09-14 from
#: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/codelist/ESTAT/OBS_FLAG/latest?format=TSV,
#: not just the letters seen on one scan). That list has NO "revised" entry
#: and no bare "r" code at all -- Eurostat's compound codes are built from
#: b(reak)/d(efinition differs)/e(stimated)/f(orecast)/i(mputed)/m(issing)/
#: n(ot significant)/p(rovisional)/u(nreliable), never an "r". So "revised"
#: is never produced by this table; see LEAD DECISION below for why "b"/"d"
#: do not use it either.
#:
#: LEAD DECISION (audit SHOULD-FIX 4), correcting this table's first version:
#:   - "b" (break in time series) and "d" (definition differs) map to
#:     "final", not "revised". They are comparability caveats -- Eurostat
#:     still treats the figure as its own settled number -- but the
#:     canonical status enum has no "break" or "definition changed" state,
#:     and "revised" specifically implies a later vintage superseded an
#:     earlier one (src/geography/resolve.py and the vintage-comparison
#:     rules treat "revised" as exactly that kind of comparable supersession,
#:     which a mere comparability flag is not). "final" is the closest real
#:     meaning: Eurostat is not withholding or estimating this number.
#:   - "f" (forecast) is deliberately NOT mapped: a forecast appearing where
#:     this pipeline expects settled history is a surprise worth failing
#:     loudly on (CLAUDE.md rule 13), not silently downgrading to "estimate".
#:   - "c" and "z" are not in Eurostat's real OBS_FLAG list either (there is
#:     no confidentiality/not-applicable letter in it) but are kept mapped
#:     to suppressed/na as a defensive no-op: if Eurostat ever attaches
#:     either through some path this scan has not seen, a genuinely
#:     suppressed or not-applicable observation still resolves sensibly
#:     rather than crashing, and neither can fire today.
#:
#: A flag not listed here at all is deliberately unrecognized (CLAUDE.md rule
#: 13): a genuinely new one must be verified against the codelist above
#: before a value is silently reinterpreted, not guessed at as it arrives.
FLAG_STATUS = {
    "": "final",
    "p": "provisional",
    "e": "estimate",
    "b": "final",
    "d": "final",
    "c": "suppressed",
    "z": "na",
    # CONFIRMED BY THE MAINTAINER (2026-09-14, docs/decisions/0010-eurostat-
    # compound-observation-flags.md): "u" (low reliability) -> "estimate". Not
    # "provisional" -- a low-reliability figure is a confidence caveat on a
    # settled number, closer to "estimate" than to "this will be revised".
    "u": "estimate",
}

#: Precedence for resolving a COMPOUND OBS_FLAG (two or more letters, e.g.
#: "bu", "bdu") to one canonical status: the canonical status of a compound
#: cell is the MOST CAUTIOUS of its letters' individual statuses, read
#: left-to-right here from most to least cautious. Documented once, used only
#: by `_status_for_flag`. "revised" is included for completeness even though
#: FLAG_STATUS never currently produces it (see the module docstring: no
#: Eurostat OBS_FLAG letter maps to "revised" today).
STATUS_PRECEDENCE = ("suppressed", "na", "estimate", "provisional", "revised", "final")

#: Canonical statuses value=None is legal for (CLAUDE.md: missing/suppressed/
#: na/zero are five distinct states, never collapsed). A flagged position
#: with no value and any other status is malformed data, not a fifth state
#: this adapter should invent -- refused explicitly (audit SHOULD-FIX 8)
#: rather than left to fail later as a bare sqlite CHECK-constraint error
#: with no context.
NULLABLE_STATUSES = frozenset({"suppressed", "na"})


def _status_for_flag(flag: str, dataset: str, geo_code: str, period: str) -> str:
    """Resolve one cell's OBS_FLAG string (possibly a compound of several
    letters, e.g. "bu") to one canonical status.

    Split into individual letters (order irrelevant, repeats collapse: "bu",
    "ub" and "bbu" all resolve the same way); every letter must already be a
    key of FLAG_STATUS or the whole cell is refused (no partial acceptance --
    CLAUDE.md rule 13), same loud error as an unrecognized single-letter flag
    always has been. Among the (possibly several) canonical statuses the
    letters map to, return the most cautious one per STATUS_PRECEDENCE.
    """
    if flag == "":
        return FLAG_STATUS[""]
    letters = set(flag)
    unknown = sorted(letters - FLAG_STATUS.keys())
    if unknown:
        bad = unknown[0]
        detail = f" (within compound flag {flag!r})" if len(flag) > 1 else ""
        raise FetchError(
            f"{dataset!r}: unrecognized Eurostat OBS_FLAG {bad!r}{detail} at "
            f"geo={geo_code} period={period}. Refusing to guess (CLAUDE.md "
            "rule 13); verify it against https://ec.europa.eu/eurostat/api/"
            "dissemination/sdmx/2.1/codelist/ESTAT/OBS_FLAG and add it to "
            "FLAG_STATUS."
        )
    statuses = {FLAG_STATUS[letter] for letter in letters}
    for status in STATUS_PRECEDENCE:
        if status in statuses:
            return status
    raise AssertionError(  # pragma: no cover -- STATUS_PRECEDENCE is exhaustive over FLAG_STATUS's values
        f"{dataset!r}: no entry in STATUS_PRECEDENCE matched {statuses!r} for flag {flag!r}"
    )


def _strides(dims: list[str], sizes: list[int]) -> dict[str, int]:
    strides: dict[str, int] = {}
    acc = 1
    for dim, size in zip(reversed(dims), reversed(sizes), strict=True):
        strides[dim] = acc
        acc *= size
    return strides


class EurostatSource(MultiGeoTimeSeriesSource):
    adapter = "eurostat"
    source_id = "eurostat"
    raw_extension = "json"

    @staticmethod
    def build_url(base_url: str, dataset: str, filters: dict[str, str], since: str) -> str:
        """`base_url` already ends at `.../statistics/1.0/data` (config/sources/eurostat.yaml);
        this appends the dataset and the query string. `filters` pins down every
        dimension the caller wants fixed to one code (e.g. unit, s_adj, na_item,
        and geo for a national single-country fetch); an unfiltered dimension
        returns every one of its codes, which is exactly what the multi-geo
        pilot path wants for `geo`."""
        params = "&".join(f"{k}={v}" for k, v in filters.items())
        query = f"format=JSON&lang=EN&{params}" if params else "format=JSON&lang=EN"
        return f"{base_url}/{dataset}?{query}&sinceTimePeriod={since}"

    def _parse(
        self,
        raw: bytes,
        *,
        dataset: str = "",
        geo_filter: Callable[[str], bool] | None = None,
        **kwargs,
    ) -> list[dict]:
        """`geo_filter`, when given, decides WHICH geo codes this parse even
        looks at -- a code it rejects is skipped entirely, for every period,
        before its OBS_FLAG/value are ever read. This is a narrowing filter
        for the caller's OWN geography shape (e.g. "is this 4-character and
        NUTS-2-shaped", scripts/sync_nuts2.py's is_nuts2_code), never a
        licence or allowlist decision -- those still happen exactly as
        before, one layer up, once this method has returned its rows (a
        code that passes `geo_filter` but is not actually recognized by the
        caller's own allowlist still fails loudly there, unchanged).

        WHY THIS EXISTS (Europe NUTS 2 batch B2 audit, 2026-09-14): these
        regional datasets mix every NUTS level (0-3) into one `geo`
        dimension, and a handful of cells for levels a caller does not even
        want (e.g. demo_r_pjanaggr3's PL912, a NUTS 3 code) carry an
        OBS_FLAG with no value at all -- which used to refuse the WHOLE
        response outright, before a NUTS 2 caller ever got the chance to
        say it never wanted a NUTS 3 row in the first place. Filtering
        before validation, not after, means a cell nobody asked for cannot
        block a fetch nobody asked it not to. A cell for a WANTED geography
        is validated exactly as before, including refusing loudly on a flag
        with no value -- this narrows what gets checked, it does not weaken
        the check itself. `geo_filter=None` (every existing caller: the
        five country-level pilot indicators, the eight single-country ones)
        preserves the exact previous behaviour -- every geo code is
        validated, nothing is skipped.
        """
        try:
            data = json.loads(raw)
            dims: list[str] = data["id"]
            sizes: list[int] = data["size"]
            dimension: dict = data["dimension"]
            values: dict = data.get("value") or {}
            status: dict = data.get("status") or {}
        except (KeyError, ValueError, TypeError) as e:
            raise FetchError(f"Unexpected Eurostat JSON-stat structure for {dataset!r}: {e}") from e

        if "error" in data:
            raise FetchError(f"Eurostat API error for {dataset!r}: {data['error']}")
        if "geo" not in dims or "time" not in dims:
            raise FetchError(f"{dataset!r}: response has no geo/time dimension (dims={dims})")

        # Every dimension other than geo/time must already be pinned to
        # exactly one code by the config's `filters` -- otherwise rows from
        # two different concepts/units would be silently mixed into one
        # series under the same indicator_id.
        for dim in dims:
            if dim in ("geo", "time"):
                continue
            codes = dimension[dim]["category"]["index"]
            if len(codes) != 1:
                raise FetchError(
                    f"{dataset!r}: dimension {dim!r} has {len(codes)} code(s), expected "
                    f"exactly 1 ({sorted(codes)}). Add a filter that pins it down."
                )

        geo_index: dict[str, int] = dimension["geo"]["category"]["index"]
        time_index: dict[str, int] = dimension["time"]["category"]["index"]
        strides = _strides(dims, sizes)
        geo_stride = strides["geo"]
        time_stride = strides["time"]

        rows: list[dict] = []
        for geo_code, geo_pos in sorted(geo_index.items(), key=lambda kv: kv[1]):
            if geo_filter is not None and not geo_filter(geo_code):
                continue  # not a geography this caller wants -- never validated, never a row
            for period, time_pos in sorted(time_index.items(), key=lambda kv: kv[1]):
                offset = str(geo_pos * geo_stride + time_pos * time_stride)
                raw_value = values.get(offset)
                flag = status.get(offset, "")
                if raw_value is None and flag == "":
                    continue  # position absent from the cube -- no row, not a state
                obs_status = _status_for_flag(flag, dataset, geo_code, period)
                if raw_value is None and "u" in set(flag):
                    # MAINTAINER DECISION (2026-09-14, docs/decisions/0010-
                    # eurostat-compound-observation-flags.md, amendment): an
                    # EMPTY cell (no value published at all) whose flag's
                    # letters include "u" ("low reliability") is
                    # `suppressed`, not the "estimate" a VALUED `u` cell
                    # maps to (FLAG_STATUS["u"], unchanged) -- the source
                    # has a reading but withholds it, the same meaning
                    # `suppressed` already carries for a confidentiality
                    # flag. Deliberately narrow: this overrides the
                    # resolved status ONLY when there is no value at all;
                    # an empty cell whose flag does NOT contain "u" (e.g. a
                    # bare "b" or "e") is still refused by the check right
                    # below, exactly as before (CLAUDE.md rule 13) -- this
                    # override names "u" specifically and no other letter.
                    obs_status = "suppressed"
                if raw_value is None and obs_status not in NULLABLE_STATUSES:
                    raise FetchError(
                        f"{dataset!r}: geo={geo_code} period={period} has flag {flag!r} "
                        f"(-> status {obs_status!r}) but no value. Only "
                        f"{sorted(NULLABLE_STATUSES)} may have value=None; refusing to "
                        "write a row that would fail the observations table's own "
                        "CHECK constraint downstream."
                    )
                value = None if raw_value is None else float(raw_value)
                rows.append(
                    {
                        "geo": geo_code,
                        "period": str(period),
                        "value": value,
                        "obs_status": obs_status,
                    }
                )
        return rows


def singleton_geo(rows: list[dict]) -> list[dict]:
    """The national-fetch path: exactly one geography's rows, reshaped to the
    plain TimeSeriesSource contract ({period, value, obs_status}).

    Used by belgian_macro_db.fetch_all and scripts/port_existing_indicators.py
    for the eight migrated indicators, whose config pins `geo` to one code in
    `fetch.filters` -- so the response SHOULD already carry one geography.
    Raises FetchError rather than silently keeping the first one if it does
    not (a config that forgot to pin `geo`, or a code Eurostat renamed).
    """
    geos = {r["geo"] for r in rows}
    if len(geos) != 1:
        raise FetchError(f"expected exactly one geography in a national fetch, got {sorted(geos)}")
    return [
        {"period": r["period"], "value": r["value"], "obs_status": r["obs_status"]} for r in rows
    ]
