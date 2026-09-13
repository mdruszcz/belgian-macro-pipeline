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

from src.fetchers.base import FetchError, MultiGeoTimeSeriesSource

#: Eurostat's own OBS_FLAG codelist (SDMX 2.1, ESTAT:OBS_FLAG), restricted to
#: the letters actually observed on the five pilot datasets' full history
#: since 2008, every allowlisted country (fetched and inspected 2026-09-13):
#: gov_10dd_edpt1 carried none; namq_10_gdp carried b/e/p; prc_hicp_manr
#: carried d; une_rt_m carried b/d. A flag never seen in that scan is
#: deliberately NOT in this table (CLAUDE.md rule 13): a genuinely new one
#: must be verified against
#: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/codelist/ESTAT/OBS_FLAG
#: before a value is silently reinterpreted, not guessed at as it arrives.
#:
#: "b" (break in time series) and "d" (definition differs) are both
#: comparability caveats, not confidence flags -- Eurostat still treats the
#: figure as its own settled number, just methodologically discontinuous with
#: the adjoining period. The canonical six-state model has no dedicated
#: bucket for that. Mapped to "revised" rather than invented a seventh state,
#: the same read scripts/port_existing_indicators.py's OBS_STATUS_MAP already
#: gives SDMX's own "break in series" code ("weakest mapping here -- re-verify
#: if seen" -- this is that re-verification, on a different codelist for the
#: same underlying concept). Flagged as a deviation from the plan's draft
#: table in the batch report; not a guess made silently.
FLAG_STATUS = {
    "": "final",
    "p": "provisional",
    "e": "estimate",
    "f": "estimate",
    "b": "revised",
    "d": "revised",
    "c": "suppressed",
    "z": "na",
}


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

    def _parse(self, raw: bytes, *, dataset: str = "", **kwargs) -> list[dict]:
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
            for period, time_pos in sorted(time_index.items(), key=lambda kv: kv[1]):
                offset = str(geo_pos * geo_stride + time_pos * time_stride)
                raw_value = values.get(offset)
                flag = status.get(offset, "")
                if raw_value is None and flag == "":
                    continue  # position absent from the cube -- no row, not a state
                if flag not in FLAG_STATUS:
                    raise FetchError(
                        f"{dataset!r}: unrecognized Eurostat OBS_FLAG {flag!r} at "
                        f"geo={geo_code} period={period}. Refusing to guess (CLAUDE.md "
                        "rule 13); verify it against https://ec.europa.eu/eurostat/api/"
                        "dissemination/sdmx/2.1/codelist/ESTAT/OBS_FLAG and add it to "
                        "FLAG_STATUS."
                    )
                obs_status = FLAG_STATUS[flag]
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
