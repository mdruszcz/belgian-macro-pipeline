"""sources_api: the fetches CI runs every day, plus sync_to_canonical, which
runs right after the macro fetch -- before Statbel, ONEM and WalStat -- as in
daily_fetch.yml.

Each source depends only on the working database (and canonical_observations
on the macro fetch), so under the in-process executor they run one after the
other, never two writing the database at once, in an order Dagster picks.
None reads what another wrote, so that order does not matter.
"""

from dagster import AssetExecutionContext, MaterializeResult, asset

from orchestration import run
from orchestration.commands import COMMANDS
from orchestration.paths import PipelinePaths
from orchestration.policies import fetch_windows, freshness_policy

_WINDOWS = fetch_windows()

_DESCRIPTIONS = {
    "macro_legacy_fetch": "NBB, Eurostat/DBnomics, AMECO and FPB via belgian_macro_db.py (one script today).",
    "statbel_local_units": "Statbel Bestat local business units (scripts/sync_statbel.py).",
    "onem_observations": "ONEM/RVA commune unemployment (scripts/sync_onem.py).",
    "onem_rates_observations": "ONEM/RVA published unemployment rate (scripts/sync_onem_rates.py).",
    "walstat_observations": "WalStat (IWEPS) municipal finance (scripts/sync_walstat.py).",
    "bankruptcies_observations": "Statbel monthly bankruptcies by NACE (scripts/sync_bankruptcies.py).",
    "market_data": "BEL 20, EUR/USD and spreads to data/stocks.json (fetch_stocks.py). No database.",
    "canonical_observations": "Legacy macro tables synced into the canonical schema (scripts/sync_to_canonical.py).",
    "international_observations": "International pilot: five direct-Eurostat, every-country indicators (scripts/sync_international.py).",
}


def _source_asset(name: str, *, group: str, deps: list):
    @asset(
        name=name,
        group_name=group,
        deps=deps,
        description=_DESCRIPTIONS[name],
        freshness_policy=freshness_policy(name, _WINDOWS),
        kinds={"python"},
    )
    def _asset(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
        run.run_script(context, paths, name)
        metadata = run.output_metadata(paths, name)
        metadata.update(
            run.source_snapshot(paths.resolve(paths.working_db), COMMANDS[name].source_ids)
        )
        return MaterializeResult(metadata=metadata)

    return _asset


macro_legacy_fetch = _source_asset("macro_legacy_fetch", group="sources_api", deps=["staging_db"])
canonical_observations = _source_asset(
    "canonical_observations", group="canonical", deps=["staging_db", "macro_legacy_fetch"]
)
statbel_local_units = _source_asset("statbel_local_units", group="sources_api", deps=["staging_db"])
onem_observations = _source_asset("onem_observations", group="sources_api", deps=["staging_db"])
onem_rates_observations = _source_asset(
    "onem_rates_observations", group="sources_api", deps=["staging_db"]
)
walstat_observations = _source_asset(
    "walstat_observations", group="sources_api", deps=["staging_db"]
)
bankruptcies_observations = _source_asset(
    "bankruptcies_observations", group="sources_api", deps=["staging_db"]
)
international_observations = _source_asset(
    "international_observations", group="sources_api", deps=["staging_db"]
)
market_data = _source_asset("market_data", group="sources_api", deps=[])

ASSETS = [
    macro_legacy_fetch,
    canonical_observations,
    statbel_local_units,
    onem_observations,
    onem_rates_observations,
    walstat_observations,
    bankruptcies_observations,
    international_observations,
    market_data,
]
