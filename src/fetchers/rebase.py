"""
rebase_to_2010 -- rescale a time series so its 2010 average equals 100.

Extracted verbatim from EurostatFetcher._rebase_to_2010 (now
DBnomicsSource._rebase_to_2010's former body) so it can be called from both
the national DBnomics path (src/fetchers/dbnomics.py) and the national
Eurostat path (belgian_macro_db.py's fetch_all, scripts/port_existing_indicators.py)
without either importing the other's adapter class. Triggered the same way it
always was: the caller checks `unit == "index_2010"` (src/exporters/provenance.py
keys its grade-B logic on that exact string) and calls this function itself --
it is no longer invoked automatically inside a `_parse` method.
"""


def rebase_to_2010(results: list[dict]) -> list[dict]:
    """Rescale values so the 2010 average equals 100 (index_2010 unit)."""
    q2010 = [r["value"] for r in results if str(r["period"]).startswith("2010")]
    if not q2010:
        return results
    avg_2010 = sum(q2010) / len(q2010)
    if avg_2010 == 0:
        return results
    return [{**r, "value": round((r["value"] / avg_2010) * 100, 2)} for r in results]
