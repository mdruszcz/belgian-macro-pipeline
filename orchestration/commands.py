"""The one table of what every asset runs: the exact command line the Makefile
or daily_fetch.yml already runs, with its paths as placeholders.

tests/test_orchestration.py renders each entry the way the Makefile and the
workflow spell it and requires the result to appear there verbatim, so this
table cannot drift from the pipeline it wraps.

Placeholders (orchestration/paths.py): {db} {stores} {committed_db} {data}
{public_data} {local} {build_id} {validation_status}, plus {today} for the
revisions report.

Two routes. An entry without `function` runs its command line as a
subprocess (run.run_script). An entry with `function` is called in this
process instead (run.call_function): the script's own function, given the
arguments its command line names. The command line stays the reference either
way: the drift tests pin it to the Makefile, and tests/test_orchestration_parity.py
runs it beside the asset and compares the files byte for byte.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Command:
    argv: tuple[str, ...]
    # Files or directories the command writes, for the asset's metadata.
    outputs: tuple[str, ...] = ()
    # The step id in daily_fetch.yml whose outcome decides auto-merge today.
    # Exactly these eight are tracked in the coordinator's source manifest.
    workflow_step: str | None = None
    # fetch_runs source ids; the freshness window comes from their
    # `fetch_window_days` (config/sources/*.yaml).
    source_ids: tuple[str, ...] = ()
    # The script has no output-path option and always writes into the
    # repository, so it refuses to run when PipelinePaths.out_root points elsewhere.
    writes_repo_only: bool = False
    # "module:callable" in scripts/. The asset calls that function in process
    # (run.call_function) instead of running argv.
    function: str | None = None
    # The script's own exception class for a designed refusal. call_function
    # turns exactly that class into a red asset carrying the script's message;
    # anything else propagates with its traceback. None: nothing is caught.
    refusal: str | None = None


COMMANDS: dict[str, Command] = {
    # ── reference_data ─────────────────────────────────────────────────────
    "staging_db": Command(
        ("scripts/build_staging_db.py", "--working-db", "{db}"),
        outputs=("{db}",),
    ),
    "commune_adjacency": Command(
        ("scripts/export_commune_adjacency.py",),
        outputs=("public/data/metadata/adjacency.json",),
        writes_repo_only=True,
    ),
    "commune_typology": Command(
        ("scripts/export_commune_typology.py",),
        outputs=("public/data/metadata/typology.json",),
        writes_repo_only=True,
    ),
    # ── sources_api ────────────────────────────────────────────────────────
    "macro_legacy_fetch": Command(
        ("belgian_macro_db.py", "--db", "{db}", "--fetch", "--latest", "--export", "csv"),
        outputs=("data/belgian_macro_export.csv", "data/belgian_forecasts.csv"),
        workflow_step="fetch_macro",
        source_ids=("nbb", "eurostat", "ameco_ec", "fpb"),
        writes_repo_only=True,
    ),
    "statbel_local_units": Command(
        ("scripts/sync_statbel.py", "--db", "{db}"),
        workflow_step="sync_statbel",
        source_ids=("statbel",),
    ),
    "onem_observations": Command(
        ("scripts/sync_onem.py", "--db", "{db}"),
        workflow_step="sync_onem",
        source_ids=("onem",),
    ),
    "onem_rates_observations": Command(
        ("scripts/sync_onem_rates.py", "--db", "{db}"),
        workflow_step="sync_onem_rates",
        source_ids=("onem",),
    ),
    "walstat_observations": Command(
        ("scripts/sync_walstat.py", "--db", "{db}"),
        workflow_step="sync_walstat",
        source_ids=("walstat",),
    ),
    "bankruptcies_observations": Command(
        ("scripts/sync_bankruptcies.py", "--db", "{db}"),
        workflow_step="sync_bankruptcies",
        source_ids=("statbel",),
    ),
    "population_movement_observations": Command(
        ("scripts/sync_population_movement.py", "--db", "{db}"),
        workflow_step="sync_population_movement",
        source_ids=("statbel",),
    ),
    "ipp_rate_observations": Command(
        ("scripts/sync_ipp_rate.py", "--db", "{db}"),
        workflow_step="sync_ipp_rate",
        source_ids=("spf_finances",),
    ),
    "spf_agdp_observations": Command(
        ("scripts/sync_spf_agdp.py", "--db", "{db}"),
        workflow_step="sync_spf_agdp",
        source_ids=("spf_finances",),
    ),
    "international_observations": Command(
        ("scripts/sync_international.py", "--db", "{db}"),
        workflow_step="sync_international",
        source_ids=("eurostat",),
    ),
    "market_data": Command(
        ("fetch_stocks.py",),
        outputs=("data/stocks.json",),
        workflow_step="fetch_stocks",
        writes_repo_only=True,
    ),
    # ── canonical ──────────────────────────────────────────────────────────
    "canonical_observations": Command(
        ("scripts/sync_to_canonical.py", "--db", "{db}"),
        workflow_step="sync_canonical",
    ),
    "revisions_report": Command(
        ("scripts/revisions_report.py", "--db", "{db}", "--since", "{today}"),
        function="revisions_report:report_revisions",
    ),
    # The offload, `make offload`'s line. Its outputs are the committed database
    # and every in_db CSV config/stores.yaml names; only the database is listed
    # here, so the registry stays the one list of store files.
    "committed_stores": Command(
        (
            "scripts/offload_stores.py",
            "--working-db",
            "{db}",
            "--committed-db",
            "{committed_db}",
            "--stores",
            "{stores}",
        ),
        outputs=("{committed_db}",),
        function="offload_stores:offload",
        refusal="OffloadError",
    ),
    # ── derived ────────────────────────────────────────────────────────────
    "communes_table_json": Command(
        (
            "scripts/export_communes_table_json.py",
            "--communes-history",
            "{data}/communes_history_full.csv",
            "--out",
            "{data}/communes_table.json",
            "--db",
            "{db}",
        ),
        outputs=("{data}/communes_table.json",),
        function="export_communes_table_json:export_communes_table_json",
    ),
    "aggregates_csv": Command(
        (
            "scripts/export_aggregates_csv.py",
            "--db",
            "{db}",
            "--out",
            "{data}/aggregates.csv",
            "--stores",
            "{stores}",
        ),
        outputs=("{data}/aggregates.csv",),
        function="export_aggregates_csv:export_aggregates_csv",
    ),
    "percentiles_csv": Command(
        (
            "scripts/export_percentiles_csv.py",
            "--db",
            "{db}",
            "--out",
            "{data}/percentiles.csv",
            "--stores",
            "{stores}",
        ),
        outputs=("{data}/percentiles.csv",),
        function="export_percentiles_csv:export_percentiles_csv",
    ),
    # ── website ────────────────────────────────────────────────────────────
    "national_csv": Command(
        (
            "scripts/export_canonical_csv.py",
            "--db",
            "{db}",
            "--out",
            "{data}/belgian_macro_export.csv",
        ),
        outputs=("{data}/belgian_macro_export.csv",),
        function="export_canonical_csv:export_canonical_csv",
    ),
    "communes_csv": Command(
        (
            "scripts/export_communes_csv.py",
            "--db",
            "{db}",
            "--out",
            "{data}/communes_export.csv",
            "--stores",
            "{stores}",
        ),
        outputs=("{data}/communes_export.csv",),
        function="export_communes_csv:export_communes_csv",
    ),
    "communes_history_full_csv": Command(
        (
            "scripts/export_communes_history_csv.py",
            "--db",
            "{db}",
            "--out",
            "{data}/communes_history_full.csv",
            "--all-periods",
            "--stores",
            "{stores}",
        ),
        outputs=("{data}/communes_history_full.csv",),
        function="export_communes_history_csv:export_communes_history_csv",
    ),
    "communes_history_csv": Command(
        (
            "scripts/export_communes_history_csv.py",
            "--db",
            "{db}",
            "--out",
            "{data}/communes_history.csv",
            "--stores",
            "{stores}",
        ),
        outputs=("{data}/communes_history.csv",),
        function="export_communes_history_csv:export_communes_history_csv",
    ),
    "indicator_metadata_json": Command(
        ("-m", "src.exporters.metadata", "--out", "{data}/metadata/indicators.json"),
        outputs=("{data}/metadata/indicators.json",),
    ),
    "site_payloads": Command(
        (
            "scripts/export_site_payloads.py",
            "--db",
            "{db}",
            "--communes-history",
            "{data}/communes_history_full.csv",
            "--communes-latest",
            "{data}/communes_export.csv",
            "--national",
            "{data}/belgian_macro_export.csv",
            "--aggregates",
            "{data}/aggregates.csv",
            "--percentiles",
            "{data}/percentiles.csv",
            "--out-dir",
            "{public_data}",
            "--build-id",
            "{build_id}",
            "--validation-status",
            "{validation_status}",
        ),
        outputs=("{public_data}",),
    ),
    "explorer_payloads": Command(
        ("scripts/export_explorer_payloads.py",),
        outputs=("public/data/explorer",),
        writes_repo_only=True,
    ),
    "europe_countries_payloads": Command(
        ("scripts/export_europe_countries.py",),
        outputs=("public/data/europe/countries",),
        writes_repo_only=True,
    ),
    "local_pages": Command(
        (
            "scripts/export_local_pages.py",
            "--db",
            "{db}",
            "--payload-dir",
            "{public_data}",
            "--out-dir",
            "{local}",
            "--build-id",
            "{build_id}",
        ),
        outputs=("{local}",),
    ),
    "page_documents": Command(
        ("scripts/export_page_documents.py",),
        writes_repo_only=True,
    ),
    "site_index": Command(
        ("scripts/export_site_index.py",),
        writes_repo_only=True,
    ),
}

# The eight outcomes daily_fetch.yml's "Check every source succeeded" step
# gates auto-merge on, in the workflow's order.
TRACKED: tuple[str, ...] = tuple(name for name, c in COMMANDS.items() if c.workflow_step)
