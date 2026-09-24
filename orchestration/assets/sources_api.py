"""sources_api: the fetches CI runs every day, plus sync_to_canonical, which
runs right after the macro fetch -- before Statbel, ONEM and WalStat -- as in
daily_fetch.yml.

Each source depends only on the working database (and canonical_observations
on the macro fetch), so under the in-process executor they run one after the
other, never two writing the database at once, in an order Dagster picks.
None reads what another wrote, so that order does not matter.

THREE OF THESE ASSETS ARE NOT PART OF THE DAILY RUN ANY MORE (2026-09-23):
bankruptcies_observations, population_movement_observations and
ipp_rate_observations still exist here -- so a maintainer can materialize one
by hand from a machine that passes the CAPTCHA -- but their Command in
orchestration/commands.py carries no `workflow_step`, so they are absent from
TRACKED and therefore from the `fetch_sources` job's AssetSelection
(orchestration/definitions.py: `AssetSelection.assets(*TRACKED)`). See
commands.py's own comment on those three entries for why.
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
    "population_movement_observations": "Statbel population movement: births, deaths, internal/international migration (scripts/sync_population_movement.py).",
    "ipp_rate_observations": "SPF Finances communal additional personal-income-tax rate (scripts/sync_ipp_rate.py).",
    "spf_agdp_observations": "SPF Finances AGDP real-estate leases/transactions, owner occupants/property dynamics, and land use/building condition/tax exemptions (scripts/sync_spf_agdp.py).",
    "commune_flows_buyer_origin": "SPF Finances buyer-origin flows, latest year only, written to the separate committed flows store rather than observations (scripts/sync_commune_flows.py).",
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
population_movement_observations = _source_asset(
    "population_movement_observations", group="sources_api", deps=["staging_db"]
)
ipp_rate_observations = _source_asset(
    "ipp_rate_observations", group="sources_api", deps=["staging_db"]
)
spf_agdp_observations = _source_asset(
    "spf_agdp_observations", group="sources_api", deps=["staging_db"]
)
commune_flows_buyer_origin = _source_asset(
    "commune_flows_buyer_origin", group="sources_api", deps=["staging_db"]
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
    population_movement_observations,
    ipp_rate_observations,
    spf_agdp_observations,
    commune_flows_buyer_origin,
    international_observations,
    market_data,
]
