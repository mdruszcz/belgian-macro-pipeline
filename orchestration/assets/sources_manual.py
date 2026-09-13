"""sources_manual: the hand-loaded stores, shown as external assets.

One AssetSpec per `extra_csv` store in config/stores.yaml, generated from the
registry so a new store appears without editing this file. They are never
materialised by Dagster -- a person downloads the file and commits the CSV --
only observed, by the observe_manual_sources job: row count, latest period,
checksum and file date. That is the automated-versus-manual distinction in
the UI.
"""

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path

from dagster import AssetObservation, AssetSpec, OpExecutionContext, job, op

from orchestration.paths import PipelinePaths
from src.stores import DEFAULT_STORES_PATH, extra_csv_stores, load_stores


def manual_source_specs(stores_path: Path = DEFAULT_STORES_PATH) -> list[AssetSpec]:
    return [
        AssetSpec(
            key=store.name,
            group_name="sources_manual",
            kinds={"csv"},
            description=(
                f"Hand-loaded store {store.raw_path} (source {store.source_id}). "
                "Not fetched by CI: updated by committing a new CSV."
            ),
            metadata={"path": store.raw_path, "source_id": store.source_id},
        )
        for store in extra_csv_stores(load_stores(stores_path))
    ]


def describe_store_csv(path: Path) -> dict:
    """Metadata for one committed store CSV. The latest period is the one with
    the latest period_end, since periods of different frequencies do not sort
    as strings."""
    rows = 0
    latest_end, latest_period = "", "none"
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            rows += 1
            if row["period_end"] > latest_end:
                latest_end, latest_period = row["period_end"], row["period"]
    return {
        "rows": rows,
        "latest period": latest_period,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "file modified": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
    }


@op(description="Record one observation per hand-loaded store.")
def observe_stores(context: OpExecutionContext, paths: PipelinePaths) -> None:
    for store in extra_csv_stores(load_stores(paths.resolve(paths.stores))):
        context.log_event(
            AssetObservation(asset_key=store.name, metadata=describe_store_csv(store.path))
        )


@job(description="Observe the hand-loaded stores. Reads files only; writes nothing.")
def observe_manual_sources():
    observe_stores()


SPECS = manual_source_specs()
