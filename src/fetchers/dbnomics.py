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

ADR 0017 -- forecast years excluded, never silently, AMECO ONLY. AMECO's own
Reference Metadata (26 November 2025) states the rule: "The most recent
available two years (Spring forecast) or three years (Autumn forecast) in the
AMECO database are forecasts", updated "usually ... in mid-May and
mid-November, not in between". Neither DBnomics nor AMECO's own bulk file
flags a forecast year -- there is no last-actual marker in either -- so the
split has to come from the release date, `indexed_at`, the one date either
response carries (verified against the live API: no `observations_attributes`
on any of the five AMECO series DBnomics exposes). `_last_outturn_year` below
reproduces AMECO's rule from that date; `_parse` then drops (never silently
-- rule 13) any period after it, counting and logging what it left out.
Routing those years into the `forecasts` table instead is its own, later
decision (ADR 0017, Recommendation #3) -- not this adapter's job.

This is an AMECO rule, not a DBnomics one, and this very docstring already
says this one class serves (or served) more than one source_id. `_parse`
gates the whole ADR 0017 behaviour -- the exclusion AND the indexed_at
requirement -- on `self.source_id == AMECO_SOURCE_ID`, so a future, non-AMECO
DBnomics source is never required to carry indexed_at and never has its own
latest periods silently dropped by a rule that was never about it. See
test_a_non_ameco_source_id_is_never_forecast_filtered.
"""

import json
import logging
from datetime import datetime

from src.fetchers.base import TimeSeriesSource
from src.fetchers.rebase import rebase_to_2010

log = logging.getLogger("fetchers.dbnomics")

#: source_id_for("AMECO/EC") (belgian_macro_db.py) -- the one source_id
#: config/sources/dbnomics_ameco.yaml's `agency: AMECO/EC` ever produces, and
#: the one every existing LABOUR_COST_* config and test uses. ADR 0017's
#: forecast-year exclusion is an AMECO rule; a DBnomics source under any
#: other source_id (the module docstring's own former "dbnomics_eurostat",
#: or any future one) passes through unfiltered, exactly as this adapter
#: always did before ADR 0017 -- and is never required to carry indexed_at
#: either, since that requirement is AMECO-specific too.
AMECO_SOURCE_ID = "ameco_ec"


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

        # ADR 0017 is an AMECO rule: for any other source_id, skip it
        # entirely (no indexed_at requirement, no exclusion) rather than
        # silently filtering periods a non-AMECO series never asked for.
        last_outturn_year = (
            self._last_outturn_year(series) if self.source_id == AMECO_SOURCE_ID else None
        )

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
            if last_outturn_year is not None and int(period[:4]) > last_outturn_year:
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
        """ADR 0017, AMECO only (callers gate this on AMECO_SOURCE_ID): the
        last calendar year AMECO has actually measured, derived from the
        release date (`indexed_at`), never from today's date -- between two
        releases `indexed_at` does not move, which is what keeps a
        just-ended year correctly a forecast until AMECO's own next release
        says otherwise (the risk the ADR names: anchoring on the fetch date
        instead would flip the just-ended year to `final` the moment the
        calendar turns, weeks before AMECO agrees).

        AMECO never has outturn data for the year a release happens in --
        that year is not over yet -- so the last FULLY MEASURED year is
        always the calendar year before the release's own year, for a
        Spring release (mid-May) exactly as for an Autumn one (mid-November):
        both leave the same year as the last outturn; only how many forecast
        years follow it differs (2, then 3 from the Autumn release on), and
        that is left to fall out of whichever periods the series actually
        carries beyond this cutoff -- never hardcoded here, so a change to
        AMECO's own 2-or-3 rule cannot silently slip past this adapter.

        THE CUTOFF IS MID-MAY -- A DAY WITHIN THE MONTH, NOT "month >= 5".
        AMECO's real release is dated 21 May 2026, and DBnomics' own
        indexed_at for it is 2026-05-22. An indexed_at early in May (e.g. a
        re-index on 2026-05-02, nothing new actually published yet) must
        still read as the PRIOR November's release, not this year's Spring
        one -- otherwise the just-ended year would flip to outturn roughly
        two weeks before AMECO itself has published anything, the exact
        anchoring risk the ADR warns against, only moved from the month
        boundary to the day one. Day 15 is the cut, matching AMECO's own
        "usually ... mid-May" wording: on or after the 15th of May counts as
        this year's Spring release having happened; any earlier day in May,
        like any day in January-April, still reflects the prior November.
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
        this_years_spring_release_has_happened = released.month > 5 or (
            released.month == 5 and released.day >= 15
        )
        release_year = (
            released.year if this_years_spring_release_has_happened else released.year - 1
        )
        return release_year - 1
