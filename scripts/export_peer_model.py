"""Peer model v1 exporter -- Block M, docs/features/peer_model.md, ADR 0015.

Reads the committed history CSVs (data/communes_history.csv plus every
data/communes_history/*.csv shard -- export_communes_history_csv.py's split,
see that script's own module docstring) and config/geography/commune_area_km2.csv
(scripts/derive_commune_area.py), builds the eleven-variable feature matrix,
standardises it, computes national and region peer lists for every one of
today's 565 communes, and writes:

  public/data/metadata/peers.json   the full model payload (model_version,
                                     variant, per-variable metadata, per
                                     commune features/z-scores/national/region
                                     top-10)
  data/peers.csv                    the same national/region pairs, flattened,
                                     one row per (commune, list, rank)

`--pca` additionally writes the PCA variant to public/data/metadata/peers_pca.json
(never the default path -- the default job writes the plain "standardised"
variant only) and prints the mean Jaccard overlap between the plain and PCA
national top-10 lists across all 565 communes.

Deterministic: sorted keys, compact JSON, ensure_ascii=False, no embedded
clock, `\n` line endings on the CSV -- CLAUDE.md rule 35, byte-identical
rebuild from the same committed inputs.

NEVER computes a ratio here without first checking whether the derived
figure already exists, reconciled onto today's territory, in
data/communes_history.csv (AVG_NET_TAXABLE_INCOME, AVERAGE_HOUSEHOLD_SIZE,
SHARE_FOREIGN_NATIONALS, POPULATION_CHANGE_5Y, LOCAL_UNITS_BY_COMMUNE are all
read verbatim from there, never recomputed -- CONTROL G stays satisfied, and
the merger back-aggregation machinery that already reconciled them is not
re-implemented here). Only three ratios are computed fresh in this script,
directly from raw counts published for today's communes (all "native to the
current territory" per the spec, no reconstruction needed): share aged 65+,
share aged 0-14, population density, and property tax base per resident.

Usage:  python scripts/export_peer_model.py
            [--history-dir data/communes_history]
            [--history-csv data/communes_history.csv]
            [--areas config/geography/commune_area_km2.csv]
            [--geographies config/geography/geographies.csv]
            [--out-json public/data/metadata/peers.json]
            [--out-csv data/peers.csv]
            [--pca]
            [--pca-out public/data/metadata/peers_pca.json]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analytics.peers import (  # noqa: E402
    PEER_MODEL_VERSION,
    VARIABLES,
    K,
    build_feature_matrix,
    nearest,
    pairwise_distances,
    pca_reduce,
    similarity_score,
    standardise,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY_DIR = REPO_ROOT / "data" / "communes_history"
DEFAULT_HISTORY_CSV = REPO_ROOT / "data" / "communes_history.csv"
DEFAULT_AREAS = REPO_ROOT / "config" / "geography" / "commune_area_km2.csv"
DEFAULT_GEOGRAPHIES = REPO_ROOT / "config" / "geography" / "geographies.csv"
DEFAULT_OUT_JSON = REPO_ROOT / "public" / "data" / "metadata" / "peers.json"
DEFAULT_OUT_CSV = REPO_ROOT / "data" / "peers.csv"
DEFAULT_PCA_OUT = REPO_ROOT / "public" / "data" / "metadata" / "peers_pca.json"

REGION_LABEL_TO_CODE = {
    "Flanders": "VLA",
    "Wallonia": "WAL",
    "Brussels-Capital Region": "BXL",
}

# Statuses whose value is a usable measurement for this model -- everything
# the history CSVs actually emit for the ten buildable variables at their
# exact periods (verified against the committed data 2026-09-26). "S"
# (suppressed) and "N"/"na" carry no usable value and are excluded, same as
# every other reader in this pipeline (rule 26: a withheld figure is never
# treated as present).
USABLE_STATUSES = {"A", "P", "derived", "reconstructed"}


class ExportError(ValueError):
    """The committed data does not support building the peer model as the
    spec requires -- raised rather than silently coercing (CLAUDE.md rule 13)."""


def _read_all_history_rows(history_dir: Path, history_csv: Path) -> list[dict]:
    """Every row from the core history CSV plus every shard under
    history_dir, in one flat list -- the raw source indicators
    (POPULATION_BY_COMMUNE, POPULATION_AGE_65_PLUS, POPULATION_AGE_0_14,
    UNEMPLOYMENT_RATE_INSURED, MUN_CADASTRAL_INCOME_TOTAL) live in the
    shards; the derived indicators this model reuses verbatim
    (AVG_NET_TAXABLE_INCOME, AVERAGE_HOUSEHOLD_SIZE, SHARE_FOREIGN_NATIONALS,
    POPULATION_CHANGE_5Y, LOCAL_UNITS_BY_COMMUNE) live in the core file --
    export_communes_history_csv.py's "THE SPLIT" note explains why.
    """
    rows: list[dict] = []
    paths = [history_csv, *sorted(history_dir.glob("*.csv"))]
    seen_any = False
    for path in paths:
        if not path.exists():
            continue
        seen_any = True
        with path.open(encoding="utf-8", newline="") as fh:
            rows.extend(csv.DictReader(fh))
    if not seen_any:
        raise ExportError(f"no history CSV found under {history_csv} or {history_dir}")
    return rows


def _index_by_indicator_period(
    rows: list[dict],
) -> dict[tuple[str, str], dict[str, tuple[float | None, str]]]:
    """(indicator_code, period) -> {nis_code: (value_or_None, status)}.

    Only rows whose status is in USABLE_STATUSES contribute a value; other
    statuses (suppressed, na) are recorded as present-but-unusable so a
    caller can tell "no row at all" apart from "a row exists but is
    withheld" if it ever needs to (this model refuses on either, per the
    spec's "no imputation" rule, but the distinction stays visible).
    """
    index: dict[tuple[str, str], dict[str, tuple[float | None, str]]] = {}
    for row in rows:
        key = (row["indicator_code"], row["period"])
        bucket = index.setdefault(key, {})
        status = row["status"]
        if status in USABLE_STATUSES and row["value"] not in ("", None):
            bucket[row["nis_code"]] = (float(row["value"]), status)
        else:
            bucket.setdefault(row["nis_code"], (None, status))
    return index


def _series(
    index: dict[tuple[str, str], dict[str, tuple[float | None, str]]],
    indicator: str,
    period: str,
    communes: set[str],
) -> pd.Series:
    """A float Series indexed by NIS for one (indicator, period), refusing
    (rule 13) if any of `communes` is missing or has no usable value."""
    bucket = index.get((indicator, period), {})
    missing = [nis for nis in communes if nis not in bucket or bucket[nis][0] is None]
    if missing:
        raise ExportError(
            f"{indicator} at {period}: no usable value for {len(missing)} commune(s): "
            f"{sorted(missing)[:10]}{'...' if len(missing) > 10 else ''}"
        )
    return pd.Series({nis: bucket[nis][0] for nis in communes}, dtype="float64").sort_index()


def _commune_metadata(rows: list[dict], communes: set[str]) -> dict[str, dict]:
    """nis -> {name_fr, name_nl, region_code}, read verbatim from the first
    row seen for that commune (every row for a commune carries the same
    name/region fields -- verified 2026-09-26)."""
    meta: dict[str, dict] = {}
    for row in rows:
        nis = row["nis_code"]
        if nis not in communes or nis in meta:
            continue
        region_label = row["region"]
        if region_label not in REGION_LABEL_TO_CODE:
            raise ExportError(f"commune {nis}: unknown region label {region_label!r}")
        meta[nis] = {
            "name_fr": row["name_fr"],
            "name_nl": row["name_nl"],
            "region": REGION_LABEL_TO_CODE[region_label],
        }
    missing = communes - set(meta)
    if missing:
        raise ExportError(f"no metadata row found for commune(s): {sorted(missing)}")
    return meta


def _current_municipality_nis(geographies_path: Path) -> set[str]:
    codes: set[str] = set()
    with geographies_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["level"] == "municipality" and not row["valid_to"]:
                codes.add(row["nis_code"])
    return codes


def _read_areas(areas_path: Path, communes: set[str]) -> pd.Series:
    areas: dict[str, float] = {}
    with areas_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            areas[row["nis"]] = float(row["area_km2"])
    missing = communes - set(areas)
    if missing:
        raise ExportError(f"commune_area_km2.csv missing area for: {sorted(missing)}")
    return pd.Series({nis: areas[nis] for nis in communes}, dtype="float64").sort_index()


def build_raw_values(
    history_dir: Path, history_csv: Path, areas_path: Path, communes: set[str]
) -> pd.DataFrame:
    """One float column per Variable.id (src.analytics.peers.VARIABLES),
    indexed by NIS -- the raw ratio/count values build_feature_matrix will
    standardise and (where the spec says) log-transform. Ratios computed
    here are the three "native, computed fresh" ones the module docstring
    names; everything else is read verbatim from an already-reconciled
    derived indicator.
    """
    rows = _read_all_history_rows(history_dir, history_csv)
    index = _index_by_indicator_period(rows)

    population_2026 = _series(index, "POPULATION_BY_COMMUNE", "2026", communes)
    age_65_2026 = _series(index, "POPULATION_AGE_65_PLUS", "2026", communes)
    age_0_14_2026 = _series(index, "POPULATION_AGE_0_14", "2026", communes)
    unemployment_2026 = _series(index, "UNEMPLOYMENT_RATE_INSURED", "2026", communes)
    cadastral_income_2026 = _series(index, "MUN_CADASTRAL_INCOME_TOTAL", "2026", communes)
    population_change_5y = _series(index, "POPULATION_CHANGE_5Y", "2026", communes)
    avg_income_2023 = _series(index, "AVG_NET_TAXABLE_INCOME", "2023", communes)
    share_foreign_2021 = _series(index, "SHARE_FOREIGN_NATIONALS", "2021", communes)
    household_size_2021 = _series(index, "AVERAGE_HOUSEHOLD_SIZE", "2021", communes)
    local_units_2023q4 = _series(index, "LOCAL_UNITS_BY_COMMUNE", "2023-Q4", communes)
    # Spec variable 10's own denominator is POPULATION_BY_COMMUNE 2023 (matching
    # the 2023-Q4 enterprise snapshot's own year), NOT the 2026 population used
    # everywhere else -- src/analytics/peers.py's Variable("enterprise_density", ...)
    # description says this explicitly. Audit finding B1 (2026-09-26): this was
    # wrongly using population_2026, moving the national top-10 of 101/565 communes.
    population_2023 = _series(index, "POPULATION_BY_COMMUNE", "2023", communes)

    areas = _read_areas(areas_path, communes)

    values = pd.DataFrame(index=sorted(communes))
    values["population"] = population_2026
    values["population_density"] = population_2026 / areas
    values["share_65_plus"] = (age_65_2026 / population_2026) * 100.0
    values["share_0_14"] = (age_0_14_2026 / population_2026) * 100.0
    values["population_change_5y"] = population_change_5y
    values["avg_net_taxable_income"] = avg_income_2023
    values["unemployment_rate_insured"] = unemployment_2026
    values["share_foreign_nationals"] = share_foreign_2021
    values["average_household_size"] = household_size_2021
    values["enterprise_density"] = (local_units_2023q4 / population_2023) * 1000.0
    values["property_tax_base_per_resident"] = cadastral_income_2026 / population_2026

    return values


def _jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(a), set(b)
    union = sa | sb
    if not union:
        return 1.0
    return len(sa & sb) / len(union)


def _round(distance: float) -> float:
    """4 decimal places, output only -- full precision is used for every
    computation (ranking, similarity score); rounding happens only when a
    number is about to be written."""
    return round(distance, 4)


def build_model(
    values: pd.DataFrame,
    meta: dict[str, dict],
    variant: str = "standardised",
    pca_min_variance: float = 0.90,
) -> dict:
    X = build_feature_matrix(values)
    Z, means, stds = standardise(X)

    if variant == "pca":
        scores, _explained = pca_reduce(Z, min_variance=pca_min_variance)
        distance_space = scores
    elif variant == "standardised":
        distance_space = Z
    else:
        raise ExportError(f"unknown variant {variant!r}")

    D = pairwise_distances(distance_space)
    nis_index = list(Z.index)

    national_peers = nearest(D, nis_index, k=K, pool=None)

    region_pools: dict[str, list[str]] = {}
    for nis in nis_index:
        region_code = meta[nis]["region"]
        region_pools[nis] = [n for n in nis_index if meta[n]["region"] == region_code]
    region_peers = nearest(D, nis_index, k=K, pool=region_pools)

    def _entries(peer_list) -> list[dict]:
        # d_max is PER LIST -- the largest of the ten distances actually
        # returned in THIS list (national or region), never a value borrowed
        # from the other list. Audit finding S2 (2026-09-26, lead decision):
        # passing the national d_max into the region list produced negative
        # "similarity" scores whenever a region peer was farther than the
        # farthest national peer (461 cases, e.g. Antwerpen's region list
        # down to -11.97) -- outside the spec's own 0-100 display range.
        d_max = max(d for _peer, d in peer_list)
        entries = []
        for rank, (peer_nis, distance) in enumerate(peer_list, start=1):
            entries.append(
                {
                    "nis": peer_nis,
                    "distance": _round(distance),
                    "rank": rank,
                    "similarity": _round(similarity_score(distance, d_max)),
                }
            )
        return entries

    communes_payload: dict[str, dict] = {}
    for nis in nis_index:
        communes_payload[nis] = {
            "features": {v.id: float(X.loc[nis, v.id]) for v in VARIABLES},
            "z": {v.id: float(Z.loc[nis, v.id]) for v in VARIABLES},
            "national": _entries(national_peers[nis]),
            "region": _entries(region_peers[nis]),
        }

    variables_payload = {
        v.id: {
            "period": v.period,
            "transform": v.transform,
            "mean": float(means[v.id]),
            "std": float(stds[v.id]),
        }
        for v in VARIABLES
    }

    return {
        "model_version": PEER_MODEL_VERSION,
        "variant": variant,
        "variables": variables_payload,
        "communes": communes_payload,
    }


def write_json(payload: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    out_path.write_text(text + "\n", encoding="utf-8", newline="\n")


def write_csv(model: dict, meta: dict[str, dict], out_path: Path) -> None:
    rows = []
    for nis, commune in model["communes"].items():
        for list_name in ("national", "region"):
            for entry in commune[list_name]:
                peer_nis = entry["nis"]
                rows.append(
                    (
                        model["model_version"],
                        model["variant"],
                        list_name,
                        nis,
                        meta[nis]["name_fr"],
                        meta[nis]["name_nl"],
                        entry["rank"],
                        peer_nis,
                        meta[peer_nis]["name_fr"],
                        meta[peer_nis]["name_nl"],
                        f"{entry['distance']:.4f}",
                        f"{entry['similarity']:.4f}",
                    )
                )
    rows.sort(key=lambda r: (r[2], r[3], r[6]))  # list, nis, rank

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(
            [
                "model_version",
                "variant",
                "list",
                "nis",
                "name_fr",
                "name_nl",
                "rank",
                "peer_nis",
                "peer_name_fr",
                "peer_name_nl",
                "distance",
                "similarity",
            ]
        )
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-dir", type=Path, default=DEFAULT_HISTORY_DIR)
    parser.add_argument("--history-csv", type=Path, default=DEFAULT_HISTORY_CSV)
    parser.add_argument("--areas", type=Path, default=DEFAULT_AREAS)
    parser.add_argument("--geographies", type=Path, default=DEFAULT_GEOGRAPHIES)
    parser.add_argument("--out-json", type=Path, default=DEFAULT_OUT_JSON)
    parser.add_argument("--out-csv", type=Path, default=DEFAULT_OUT_CSV)
    parser.add_argument("--pca", action="store_true")
    parser.add_argument("--pca-out", type=Path, default=DEFAULT_PCA_OUT)
    args = parser.parse_args()

    communes = _current_municipality_nis(args.geographies)
    if len(communes) != 565:
        raise SystemExit(f"expected 565 current municipalities, found {len(communes)}")

    rows = _read_all_history_rows(args.history_dir, args.history_csv)
    meta = _commune_metadata(rows, communes)
    values = build_raw_values(args.history_dir, args.history_csv, args.areas, communes)

    model = build_model(values, meta, variant="standardised")
    write_json(model, args.out_json)
    write_csv(model, meta, args.out_csv)
    print(f"wrote {args.out_json} ({args.out_json.stat().st_size / 1e6:.3f} MB)")
    print(f"wrote {args.out_csv} ({args.out_csv.stat().st_size / 1e6:.3f} MB)")

    if args.pca:
        pca_model = build_model(values, meta, variant="pca")
        write_json(pca_model, args.pca_out)
        print(f"wrote {args.pca_out} ({args.pca_out.stat().st_size / 1e6:.3f} MB)")

        overlaps = []
        for nis in model["communes"]:
            plain_top10 = [e["nis"] for e in model["communes"][nis]["national"]]
            pca_top10 = [e["nis"] for e in pca_model["communes"][nis]["national"]]
            overlaps.append(_jaccard(plain_top10, pca_top10))
        mean_overlap = sum(overlaps) / len(overlaps)
        print(
            f"plain vs PCA national top-10 mean Jaccard overlap: {mean_overlap:.4f} "
            f"(over {len(overlaps)} communes)"
        )


if __name__ == "__main__":
    main()
