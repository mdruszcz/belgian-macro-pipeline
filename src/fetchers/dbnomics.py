"""
DBnomicsSource -- fetches DBnomics JSON time series. Renamed from
EurostatSource (src/fetchers/eurostat.py used to hold this class): DBnomics is
being retired as the transport for Eurostat data (PR 1 of the international
pilot moves those eight series to src/fetchers/eurostat.py's new, direct
Eurostat adapter) but stays in place for AMECO until PR 2 builds a direct
AMECO adapter -- both go through DBnomics today, sharing this exact adapter,
config/sources/*.yaml giving them the same `adapter: dbnomics` and different
source_ids.

_parse is DBnomicsFetcher.fetch's original body, with one addition: ADR 0017
(docs/decisions/0017-ameco-forecast-periods.md, ACCEPTED 2026-10-04) requires
a European Commission forecast year to never enter `observations` as if it
were a measurement. Everything else -- the DBnomics JSON navigation, the
< "2008" filter and the index_2010 rebasing -- is identical to before this
refactor -- verified by tests/test_dbnomics_source.py against a fixture
response. Only the rebasing's own body moved out, to
src/fetchers/rebase.py::rebase_to_2010, so the same function serves this
class and the new direct Eurostat national path (belgian_macro_db.py,
scripts/port_existing_indicators.py) without either importing the other's
adapter class.

source_id is an __init__ parameter, not a class attribute: unlike NBBSource
(one source_id for the whole adapter), the same DBnomicsSource class serves
two distinct sources.source_id values ("dbnomics_ameco" today; "dbnomics_eurostat"
until this pilot moved it), so it cannot be fixed at class-definition time.

ADR 0017 -- forecast years excluded, never silently. AMECO's own Reference
Metadata (26 November 2025) states the rule: "The most recent available two
years (Spring forecast) or three years (Autumn forecast) in the AMECO
database are forecasts", updated "usually ... in mid-May and mid-November,
not in between". Neither DBnomics nor AMECO's own bulk file flags a forecast
year -- there is no last-actual marker in either -- so the split has to come
from the release date, `indexed_at`, the one date either response carries
(verified against the live API: no `observations_attributes` on any of the
five AMECO series DBnomics exposes). `_last_outturn_year` below reproduces
AMECO's rule from that date; _parse then drops (never silently -- rule 13)
any period after it, counting and logging what it left out. Routing those
years into the `forecasts` table instead is its own, later decision (ADR
0017, Recommendation #3) -- not this adapter's job.
"""

import json
import logging
from datetime import datetime

from src.fetchers.base import TimeSeriesSource
from src.fetchers.rebase import rebase_to_2010

log = logging.getLogger("fetchers.dbnomics")


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

        last_outturn_year = self._last_outturn_year(series)

        results = []
        excluded: list[str] = []
        for p, v in zip(periods, values, strict=False):
            period = str(p)
            if period < "2008":
                continue
            if v is None or v == "NA":
                continue
            try:
                val = float(v)
            except ValueError:
                continue
            if int(period[:4]) > last_outturn_year:
                excluded.append(period)
                continue
            results.append({"period": period, "value": val, "obs_status": "A"})

        if excluded:
            # CLAUDE.md rule 13: counted and logged, never silently dropped.
            # tests/test_dbnomics_source.py asserts this count directly, so a
            # year where AMECO's own rule changes shows up as a test failure
            # rather than as rows quietly appearing or disappearing.
            log.info(
                "%s: excluded %d forecast period(s) after the last outturn year "
                "%d (ADR 0017): %s",
                self.source_id,
                len(excluded),
                last_outturn_year,
                ", ".join(sorted(excluded)),
            )

        if unit == "index_2010":
            results = rebase_to_2010(results)
        return results

    def _last_outturn_year(self, series: dict) -> int:
        """ADR 0017: the last calendar year AMECO has actually measured,
        derived from the release date (`indexed_at`), never from today's
        date -- between two releases `indexed_at` does not move, which is
        what keeps a just-ended year correctly a forecast until AMECO's own
        next release says otherwise (the risk the ADR names: anchoring on
        the fetch date instead would flip the just-ended year to `final`
        the moment the calendar turns, weeks before AMECO agrees).

        AMECO never has outturn data for the year a release happens in --
        that year is not over yet -- so the last FULLY MEASURED year is
        always the calendar year before the release's own year, for a
        Spring release (mid-May) exactly as for an Autumn one (mid-November):
        both leave the same year as the last outturn; only how many forecast
        years follow it differs (2, then 3 from the Autumn release on), and
        that is left to fall out of whichever periods the series actually
        carries beyond this cutoff -- never hardcoded here, so a change to
        AMECO's own 2-or-3 rule cannot silently slip past this adapter.

        A fetch between 1 January and that year's own mid-May release has an
        `indexed_at` that still reflects the PRIOR November's release (AMECO
        has not published anything new yet), so it reads as that November's
        year, not the calendar year the fetch happens to run in.
        """
        indexed_at = series.get("indexed_at")
        if not indexed_at:
            name = series.get("series_code") or series.get("dataset_code") or self.source_id
            raise ValueError(
                f"{name!r}: DBnomics response has no indexed_at, so this adapter cannot "
                "derive AMECO's last outturn year from the release date (ADR 0017). "
                'Refusing to default to "everything is final" (CLAUDE.md rule 13).'
            )
        released = datetime.fromisoformat(str(indexed_at).replace("Z", "+00:00"))
        release_year = released.year if released.month >= 5 else released.year - 1
        return release_year - 1
