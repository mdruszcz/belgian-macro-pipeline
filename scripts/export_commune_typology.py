"""The Belfius socio-economic typology, resolved onto today's 565 communes.

Belfius Banque & Assurances groups every Belgian commune into one of 37
clusters (16 Walloon, 16 Flemish, 5 in Brussels), each cluster in a family
such as "rural", "residential" or "urbanised". BelPulse publishes that
classification so a reader can browse communes by type. It is a CLASSIFICATION,
never a figure: nothing here is computed from data, and nothing here is
invented -- every assignment traces to a printed name in one of the three
Belfius publications under docs/Communes caracterisation/.

Inputs, all committed:
  config/geography/belfius_clusters.csv   the composition, name for name as
                                          printed (region, cluster, subgroup,
                                          name_printed)
  config/geography/belfius_clusters.yaml  cluster and family labels, en/fr/nl;
                                          the reference date; the few printed
                                          names that need a spelled-out alias
  config/geography/geographies.csv        every municipality that ever
                                          existed here, with validity dates
  config/geography/municipality_crosswalk.csv  which old commune became which

THE COMPOSITION PREDATES THE 2019 AND 2025 MERGERS, so a printed name is
resolved against the communes VALID ON THE REFERENCE DATE (Puurs and
Sint-Amands, not Puurs-Sint-Amands), then carried to today's commune through
the crosswalk. A commune of today whose constituent parts sat in DIFFERENT
clusters is published as `mixed`, with the parts named, and is assigned to
none of them: Belfius never classified the merged commune, and picking one
part's cluster for it would be an invention (CLAUDE.md, rule 26 and the
Definitions' "refuse, do not invent"). A commune of today that no printed
name reaches is `unclassified`, and says so.

Refuses to write (rule 13) when a printed name resolves to no commune or to
more than one, when a commune is printed in two clusters, when a resolved
commune's region disagrees with the publication it came from, or when a label
lacks one of the three languages (rule 7). Deterministic: sorted keys, no
clock (rule 35).

Usage:  python scripts/export_commune_typology.py
            [--clusters config/geography/belfius_clusters.csv]
            [--labels config/geography/belfius_clusters.yaml]
            [--geographies config/geography/geographies.csv]
            [--crosswalk config/geography/municipality_crosswalk.csv]
            [--out public/data/metadata/typology.json]
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GEO_DIR = REPO_ROOT / "config" / "geography"
DEFAULT_CLUSTERS = GEO_DIR / "belfius_clusters.csv"
DEFAULT_LABELS = GEO_DIR / "belfius_clusters.yaml"
DEFAULT_GEOGRAPHIES = GEO_DIR / "geographies.csv"
DEFAULT_CROSSWALK = GEO_DIR / "municipality_crosswalk.csv"
DEFAULT_OUT = REPO_ROOT / "public" / "data" / "metadata" / "typology.json"

LANGS = ("en", "fr", "nl")

#: The region a publication covers, by the region geo_id of the communes in it.
REGION_BY_GEO_ID = {"be:reg:02000": "VLA", "be:reg:03000": "WAL", "be:reg:04000": "BXL"}

#: How many successor hops a crosswalk chain may take before it is a loop.
MAX_SUCCESSOR_HOPS = 5


class TypologyError(ValueError):
    """The inputs do not describe one classification per commune."""


def normalise(name: str) -> str:
    """One spelling for a name however a publication prints it.

    Case, accents, hyphens, apostrophes and spacing all vary between the
    Belfius print and the geography file ("FONTAINE L'ÉVEQUE" against
    "Fontaine-l'Évêque", "SCHERPENHEUVEL - ZICHEM" against
    "Scherpenheuvel-Zichem"); none of them distinguishes two communes. The
    ligature is expanded first because NFKD does not decompose it.
    """
    s = name.replace("Œ", "OE").replace("œ", "oe").replace("Æ", "AE").replace("æ", "ae")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[^A-Za-z0-9]+", " ", s).strip().upper()


def _read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_labels(path: Path) -> dict:
    import yaml

    labels = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for key in ("source", "families", "clusters"):
        if key not in labels:
            raise TypologyError(f"{path}: missing top-level `{key}`")
    if not labels["source"].get("reference_date"):
        raise TypologyError(f"{path}: source.reference_date is required")

    def _trilingual(what: str, label: object) -> None:
        if not isinstance(label, dict) or any(not label.get(lang) for lang in LANGS):
            raise TypologyError(f"{path}: {what} needs a label in all of {LANGS} (rule 7)")

    for fid, family in labels["families"].items():
        _trilingual(f"family {fid}", family.get("label"))
        if family.get("region") not in REGION_BY_GEO_ID.values():
            raise TypologyError(f"{path}: family {fid} has no region")
    for code, cluster in labels["clusters"].items():
        _trilingual(f"cluster {code}", cluster.get("label"))
        if cluster.get("family") not in labels["families"]:
            raise TypologyError(
                f"{path}: cluster {code} names unknown family {cluster.get('family')!r}"
            )
        for sid, sub in (cluster.get("subgroups") or {}).items():
            _trilingual(f"subgroup {code}/{sid}", sub)
    return labels


def municipalities_valid_on(rows: list[dict], date: str) -> dict[str, dict]:
    """Every municipality row that existed on `date`, keyed by NIS."""
    valid = {}
    for row in rows:
        if row["level"] != "municipality":
            continue
        if row["valid_from"] and row["valid_from"] > date:
            continue
        if row["valid_to"] and row["valid_to"] <= date:
            continue
        valid[row["nis_code"]] = row
    return valid


def region_of(row: dict, by_geo_id: dict[str, dict]) -> str:
    """WAL / VLA / BXL for a municipality row, walking up its parents."""
    current = row
    for _ in range(6):
        if current["geo_id"] in REGION_BY_GEO_ID:
            return REGION_BY_GEO_ID[current["geo_id"]]
        current = by_geo_id.get(current.get("parent_geo_id") or "")
        if current is None:
            break
    raise TypologyError(f"{row['geo_id']} has no region above it")


def successors(crosswalk_rows: list[dict]) -> dict[str, str]:
    return {row["old_nis"]: row["new_nis"] for row in crosswalk_rows if row["new_nis"]}


def resolve_current(nis: str, successor_of: dict[str, str], current: set[str]) -> str:
    """Follow the crosswalk from a commune of the reference date to today's."""
    seen = [nis]
    for _ in range(MAX_SUCCESSOR_HOPS):
        if nis in current:
            return nis
        nis = successor_of.get(nis)
        if nis is None:
            break
        seen.append(nis)
    raise TypologyError(f"no current commune at the end of the crosswalk chain {' -> '.join(seen)}")


def _names(row: dict) -> dict[str, str]:
    return {"nl": row["name_nl"], "fr": row["name_fr"], "en": row["name_en"]}


def build(
    clusters_rows: list[dict],
    labels: dict,
    geography_rows: list[dict],
    crosswalk_rows: list[dict],
) -> dict:
    """The typology payload. Raises TypologyError rather than guessing."""
    reference_date = str(labels["source"]["reference_date"])
    by_geo_id = {row["geo_id"]: row for row in geography_rows}
    at_reference = municipalities_valid_on(geography_rows, reference_date)
    today = municipalities_valid_on(geography_rows, "9999-12-31")
    successor_of = successors(crosswalk_rows)

    index: dict[str, set[str]] = defaultdict(set)
    for nis, row in at_reference.items():
        for name in _names(row).values():
            if name:
                index[normalise(name)].add(nis)

    aliases = {
        normalise(printed): normalise(str(spec["use"]) if isinstance(spec, dict) else str(spec))
        for printed, spec in (labels["source"].get("printed_name_aliases") or {}).items()
    }

    # printed name -> commune of the reference date, one cluster each
    cluster_of_reference: dict[str, tuple[str, str, str]] = {}
    problems: list[str] = []
    for row in clusters_rows:
        code = row["cluster"]
        if code not in labels["clusters"]:
            problems.append(f"{row['name_printed']}: cluster {code} is not in the labels file")
            continue
        key = normalise(row["name_printed"])
        key = aliases.get(key, key)
        matches = sorted(index.get(key, ()))
        if len(matches) > 1:
            # One name, two communes: "Saint-Nicolas" is 62093 in Liège AND the
            # French name of Sint-Niklaas (46021). Each publication covers one
            # region, so the region it was printed in is the tie-break -- a
            # fact about the print, not a guess about the commune.
            in_region = [
                nis for nis in matches if region_of(at_reference[nis], by_geo_id) == row["region"]
            ]
            if len(in_region) == 1:
                matches = in_region
        if len(matches) != 1:
            problems.append(
                f"{row['name_printed']} ({code}): resolves to {matches or 'no commune'} "
                f"among the communes valid on {reference_date}"
            )
            continue
        nis = matches[0]
        if nis in cluster_of_reference:
            problems.append(
                f"{row['name_printed']} ({code}): {nis} is already printed in cluster "
                f"{cluster_of_reference[nis][0]}"
            )
            continue
        region = region_of(at_reference[nis], by_geo_id)
        if region != row["region"]:
            problems.append(
                f"{row['name_printed']} ({code}): resolved to {nis}, which is in {region}, "
                f"but was printed in the {row['region']} publication"
            )
            continue
        cluster_of_reference[nis] = (code, row.get("subgroup") or "", row["name_printed"])
    if problems:
        raise TypologyError("the composition does not resolve cleanly:\n  " + "\n  ".join(problems))

    # commune of today -> its constituent parts of the reference date
    parts_of_today: dict[str, list[dict]] = defaultdict(list)
    for nis, (code, subgroup, _printed) in sorted(cluster_of_reference.items()):
        current = resolve_current(nis, successor_of, set(today))
        part = {
            "nis": nis,
            "name": _names(at_reference[nis]),
            "cluster": code,
            "family": labels["clusters"][code]["family"],
        }
        if subgroup:
            part["subgroup"] = subgroup
        parts_of_today[current].append(part)

    communes: dict[str, dict] = {}
    members: dict[str, list[str]] = defaultdict(list)
    mixed_parts: dict[str, list[str]] = defaultdict(list)
    counts = {"classified": 0, "mixed": 0, "unclassified": 0}
    for nis in sorted(today):
        parts = parts_of_today.get(nis, [])
        clusters = sorted({p["cluster"] for p in parts})
        if not parts:
            communes[nis] = {"status": "unclassified"}
            counts["unclassified"] += 1
        elif len(clusters) == 1:
            code = clusters[0]
            entry = {
                "status": "classified",
                "cluster": code,
                "family": labels["clusters"][code]["family"],
            }
            subgroups = sorted({p.get("subgroup", "") for p in parts} - {""})
            if len(subgroups) == 1:
                entry["subgroup"] = subgroups[0]
            if len(parts) > 1 or parts[0]["nis"] != nis:
                entry["parts"] = parts
            communes[nis] = entry
            members[code].append(nis)
            counts["classified"] += 1
        else:
            communes[nis] = {"status": "mixed", "parts": parts}
            for code in clusters:
                mixed_parts[code].append(nis)
            counts["mixed"] += 1

    clusters_out = {}
    for code, spec in labels["clusters"].items():
        out = {
            "family": spec["family"],
            "region": labels["families"][spec["family"]]["region"],
            "label": {lang: spec["label"][lang] for lang in LANGS},
            "members": members.get(code, []),
            "mixed_parts": mixed_parts.get(code, []),
        }
        if spec.get("printed_as"):
            out["printed_as"] = spec["printed_as"]
        if spec.get("subgroups"):
            out["subgroups"] = {
                sid: {lang: sub[lang] for lang in LANGS} for sid, sub in spec["subgroups"].items()
            }
        clusters_out[code] = out

    families_out = {
        fid: {
            "region": spec["region"],
            "label": {lang: spec["label"][lang] for lang in LANGS},
            "clusters": [code for code, c in labels["clusters"].items() if c["family"] == fid],
        }
        for fid, spec in labels["families"].items()
    }

    source = labels["source"]
    return {
        "_note": (
            "Belfius Banque & Assurances' socio-economic typology of Belgian municipalities, "
            "resolved onto today's communes by scripts/export_commune_typology.py from "
            "config/geography/belfius_clusters.csv (the composition, name for name as printed) "
            "and belfius_clusters.yaml (labels). A classification, not a figure. A commune "
            "merged from parts that sat in different clusters is `mixed` and assigned to none."
        ),
        "source": {
            "publisher": source["publisher"],
            "title": {lang: source["title"][lang] for lang in LANGS},
            "reference_date": reference_date,
            "documents": list(source.get("documents") or []),
            "translations_note": source.get("translations_note", ""),
        },
        "families": families_out,
        "clusters": clusters_out,
        "communes": communes,
        "counts": counts,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish the Belfius commune typology as metadata/typology.json"
    )
    parser.add_argument("--clusters", type=Path, default=DEFAULT_CLUSTERS)
    parser.add_argument("--labels", type=Path, default=DEFAULT_LABELS)
    parser.add_argument("--geographies", type=Path, default=DEFAULT_GEOGRAPHIES)
    parser.add_argument("--crosswalk", type=Path, default=DEFAULT_CROSSWALK)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    for path in (args.clusters, args.labels, args.geographies, args.crosswalk):
        if not path.is_file():
            print(f"missing input: {path}", file=sys.stderr)
            return 1
    try:
        payload = build(
            _read_csv(args.clusters),
            load_labels(args.labels),
            _read_csv(args.geographies),
            _read_csv(args.crosswalk),
        )
    except TypologyError as exc:
        print(f"refusing to publish the typology: {exc}", file=sys.stderr)
        return 1

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    counts = payload["counts"]
    print(
        f"{args.out}: {len(payload['communes'])} communes -- {counts['classified']} classified, "
        f"{counts['mixed']} mixed (merged from different clusters), "
        f"{counts['unclassified']} unclassified; {len(payload['clusters'])} clusters"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
