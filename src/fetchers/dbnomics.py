"""
DBnomicsSource -- fetches DBnomics JSON time series. Renamed from
EurostatSource (src/fetchers/eurostat.py used to hold this class): DBnomics is
being retired as the transport for Eurostat data (PR 1 of the international
pilot moves those eight series to src/fetchers/eurostat.py's new, direct
Eurostat adapter) but stays in place for AMECO until PR 2 builds a direct
AMECO adapter -- both go through DBnomics today, sharing this exact adapter,
config/sources/*.yaml giving them the same `adapter: dbnomics` and different
source_ids.

_parse is DBnomicsFetcher.fetch's original body, unchanged: the HTTP GET and
raise_for_status() live in DataSource._get_with_retry, but the DBnomics JSON
navigation, the < "2008" filter and the index_2010 rebasing are identical to
before this refactor -- verified by tests/test_dbnomics_source.py against a
fixture response. Only the rebasing's own body moved out, to
src/fetchers/rebase.py::rebase_to_2010, so the same function serves this
class and the new direct Eurostat national path (belgian_macro_db.py,
scripts/port_existing_indicators.py) without either importing the other's
adapter class.

source_id is an __init__ parameter, not a class attribute: unlike NBBSource
(one source_id for the whole adapter), the same DBnomicsSource class serves
two distinct sources.source_id values ("dbnomics_ameco" today; "dbnomics_eurostat"
until this pilot moved it), so it cannot be fixed at class-definition time.
"""

import json

from src.fetchers.base import TimeSeriesSource
from src.fetchers.rebase import rebase_to_2010


class DBnomicsSource(TimeSeriesSource):
    adapter = "dbnomics"
    raw_extension = "json"

    def __init__(self, source_id: str):
        self.source_id = source_id

    def _parse(self, raw: bytes, *, unit: str = "", **kwargs) -> list[dict]:
        try:
            data = json.loads(raw)
            series = data["series"]["docs"][0]
            periods = series["period"]
            values = series["value"]
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"Unexpected DBnomics JSON structure: {e}") from e

        results = []
        for p, v in zip(periods, values, strict=False):
            if str(p) < "2008":
                continue
            if v is None or v == "NA":
                continue
            try:
                val = float(v)
                results.append({"period": str(p), "value": val, "obs_status": "A"})
            except ValueError:
                continue

        if unit == "index_2010":
            results = rebase_to_2010(results)
        return results
