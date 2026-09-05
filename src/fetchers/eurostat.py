"""
EurostatSource -- fetches DBnomics JSON time series (Eurostat and AMECO both
go through DBnomics, sharing the exact same adapter -- config/sources/*.yaml
gives them the same `adapter: dbnomics` and different `source_id`s).

_parse and _rebase_to_2010 are DBnomicsFetcher.fetch's original body,
unchanged: the HTTP GET and raise_for_status() now live in
DataSource._get_with_retry, but the DBnomics JSON navigation, the < "2008"
filter, and the index_2010 rebasing are identical to before this refactor --
verified by tests/test_eurostat_source.py against a fixture response.

source_id is an __init__ parameter, not a class attribute: unlike NBBSource
(one source_id for the whole adapter), the same EurostatSource class serves
two distinct sources.source_id values ("dbnomics_eurostat", "dbnomics_ameco"),
so it cannot be fixed at class-definition time.
"""

import json

from src.fetchers.base import TimeSeriesSource


class EurostatSource(TimeSeriesSource):
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
            results = self._rebase_to_2010(results)
        return results

    @staticmethod
    def _rebase_to_2010(results: list[dict]) -> list[dict]:
        """Rescale values so the 2010 average equals 100 (index_2010 unit)."""
        q2010 = [r["value"] for r in results if str(r["period"]).startswith("2010")]
        if not q2010:
            return results
        avg_2010 = sum(q2010) / len(q2010)
        if avg_2010 == 0:
            return results
        return [{**r, "value": round((r["value"] / avg_2010) * 100, 2)} for r in results]
