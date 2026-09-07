"""Where every published figure came from, and how it was made.

One module computes this, and three exporters import it, so the badge on a
commune page, the badge on the map and the badge on the static page cannot
disagree about the same indicator.

THREE ORTHOGONAL AXES, deliberately not collapsed (docs/features/provenance.md):

    grade      how was this number MADE?          per indicator (lineage)
    status     how confident is the SOURCE?       per observation
    freshness  when did we RETRIEVE it?           per indicator + per observation

A provisional official figure and a final derived one differ in kind, and a
reader needs both facts. Folding any two of these axes into one letter is what
would make the badge lie.

Nothing here is stored: grade and source are computed at export from lineage
that already exists, exactly as `additive`, `updated`, `comparison` and
`percentile` are (CLAUDE.md rule 6).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
SOURCES_DIR = REPO / "config" / "sources"
INDICATORS_DIR = REPO / "config" / "indicators"
DERIVED_DIR = INDICATORS_DIR / "derived"

# The grade vocabulary. Trilingual because it is user-facing (rule 7), and in
# one place because two copies of these four words would drift into meaning
# slightly different things on two pages.
GRADES = {
    "A": {
        "en": "Official",
        "fr": "Officiel",
        "nl": "Officieel",
        "definition": {
            "en": "The agency's own published figure, re-keyed to internal geography and "
            "corrected across commune mergers, but otherwise unchanged.",
            "fr": "Le chiffre publié par l'agence elle-même, réindexé sur la géographie "
            "interne et corrigé des fusions de communes, mais par ailleurs inchangé.",
            "nl": "Het door de instelling zelf gepubliceerde cijfer, opnieuw gekoppeld aan de "
            "interne geografie en gecorrigeerd voor gemeentefusies, maar verder onveranderd.",
        },
    },
    "B": {
        "en": "Restated",
        "fr": "Retraité",
        "nl": "Herberekend",
        "definition": {
            "en": "One source series, but this pipeline changed the number itself — a rebasing "
            "or a unit conversion.",
            "fr": "Une seule série source, mais ce pipeline a modifié le chiffre lui-même — "
            "changement de base ou conversion d'unité.",
            "nl": "Één bronreeks, maar deze pipeline heeft het cijfer zelf gewijzigd — een "
            "herbasering of een eenheidsomrekening.",
        },
    },
    "C": {
        "en": "Derived",
        "fr": "Calculé",
        "nl": "Berekend",
        "definition": {
            "en": "Computed by us from two or more published series, or across time.",
            "fr": "Calculé par nous à partir de deux séries publiées ou plus, ou dans le temps.",
            "nl": "Door ons berekend uit twee of meer gepubliceerde reeksen, of over de tijd.",
        },
    },
    "D": {
        "en": "Forecast",
        "fr": "Prévision",
        "nl": "Prognose",
        "definition": {
            "en": "A value for a period the source has not yet measured.",
            "fr": "Une valeur pour une période que la source n'a pas encore mesurée.",
            "nl": "Een waarde voor een periode die de bron nog niet heeft gemeten.",
        },
    },
}

# THE CONFIG AND THE DATABASE DISAGREE ABOUT TWO SOURCE IDENTIFIERS, and the
# join here would silently find nothing for them. Verified rather than guessed:
#
#   config/indicators/EUROSTAT_GDP_Q_MEUR.yaml declares source_id
#   `dbnomics_eurostat`; the indicators table row for that same indicator says
#   `eurostat`. Same source (Eurostat via DBnomics), two ids.
#
#   config/indicators/LABOUR_COST_BE.yaml declares `dbnomics_ameco`; its
#   indicators row says `ameco_ec`. Same source (AMECO via DBnomics).
#
# Aliased here rather than renamed either side, because renaming a source_id
# touches the observations that reference it and is a migration, not an export
# change. resolve() below REFUSES on an unresolvable id, so a third mismatch
# cannot hide the way these two did.
DB_TO_CONFIG_SOURCE_ID = {
    "eurostat": "dbnomics_eurostat",
    "ameco_ec": "dbnomics_ameco",
}

# The unit EurostatFetcher.fetch rebases on, by that exact string
# (src/fetchers/eurostat.py:51). Any indicator carrying it has had its numbers
# changed by us and is grade B; one that carries it WITHOUT declaring the
# transform is a config that would grade itself as the agency's own figure, so
# the export refuses rather than publish that.
REBASED_UNIT = "index_2010"


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def source_registry(sources_dir: Path = SOURCES_DIR) -> dict[str, dict]:
    """source_id -> the publishable record of that source.

    Built from `config/sources/*.yaml`, NOT the `sources` table. The configs
    are the declared truth; three rows in that table were written by
    scripts/port_existing_indicators.py with the agency copied into the name
    and `catalog_ref = "docs/data_catalog.md (pending)"`, so reading the
    database would publish the weaker copy of a licence notice.

    Keyed by the id the `indicators` table uses, since that is what the
    lineage joins on -- see DB_TO_CONFIG_SOURCE_ID.
    """
    by_config_id = {}
    for path in sorted(sources_dir.glob("*.yaml")):
        cfg = _load_yaml(path)
        by_config_id[cfg["source_id"]] = cfg

    config_id_for = {v: k for k, v in DB_TO_CONFIG_SOURCE_ID.items()}
    registry = {}
    for config_id, cfg in by_config_id.items():
        source_id = config_id_for.get(config_id, config_id)
        label = cfg.get("label") or {}
        registry[source_id] = {
            "source_id": source_id,
            # Falls back to the agency rather than inventing a short label, so
            # a source without a translated one is still nameable on a page.
            "label": label or dict.fromkeys(("en", "fr", "nl"), cfg["agency"]),
            "agency": cfg["agency"],
            "name": cfg["name"],
            "homepage": cfg.get("homepage"),
            "licence_note": cfg.get("licence_note"),
            "cadence": cfg.get("cadence"),
            "catalog_ref": cfg.get("catalog_ref"),
        }
    return registry


def _stored_indicators(db_path: Path) -> dict[str, str | None]:
    conn = sqlite3.connect(str(db_path))
    try:
        return dict(conn.execute("SELECT indicator_id, source_id FROM indicators").fetchall())
    finally:
        conn.close()


def _indicator_configs(indicators_dir: Path) -> dict[str, dict]:
    return {
        path.stem: _load_yaml(path)
        for path in sorted(indicators_dir.glob("*.yaml"))
        if path.is_file()
    }


def _derived_configs(derived_dir: Path) -> dict[str, dict]:
    if not derived_dir.is_dir():
        return {}
    return {path.stem: _load_yaml(path) for path in sorted(derived_dir.glob("*.yaml"))}


def indicator_lineage(
    db_path: Path,
    indicators_dir: Path = INDICATORS_DIR,
    derived_dir: Path = DERIVED_DIR,
    sources_dir: Path = SOURCES_DIR,
) -> dict[str, dict]:
    """indicator_id -> {grade, source, derived_from, input_sources, transform}.

    `source` is None for a derived indicator: it has no single source, and
    naming one of its inputs' sources as its own would attribute a computed
    figure to an agency that never published it. What it has instead is
    `derived_from` (the inputs) and `input_sources` (their sources, resolved
    transitively), which is what lets a page say "computed from Statbel
    figures" without claiming Statbel published this number.
    """
    stored = _stored_indicators(db_path)
    configs = _indicator_configs(indicators_dir)
    derived = _derived_configs(derived_dir)
    registry = source_registry(sources_dir)

    lineage: dict[str, dict] = {}

    for indicator_id, source_id in stored.items():
        cfg = configs.get(indicator_id, {})
        transform = cfg.get("transform")

        # A config carrying the rebased unit without declaring the transform
        # would grade itself A -- "the agency's own published figure" -- for a
        # number this pipeline rescaled. Refuse rather than publish that.
        if cfg.get("unit") == REBASED_UNIT and not transform:
            raise ValueError(
                f"{indicator_id} has unit {REBASED_UNIT!r}, which the fetcher rebases "
                "(src/fetchers/eurostat.py), but declares no `transform:` block. It would "
                "be graded A as the agency's own figure. Declare the transform."
            )

        if source_id is not None and source_id not in registry:
            raise ValueError(
                f"{indicator_id} names source {source_id!r}, which no config in "
                f"{sources_dir} describes (known: {sorted(registry)}). Either add the "
                "source config or extend DB_TO_CONFIG_SOURCE_ID -- do not let a figure "
                "publish with an unattributable source."
            )

        lineage[indicator_id] = {
            "grade": "B" if transform else "A",
            "source": source_id,
            "transform": transform.get("method") if transform else None,
        }

    for indicator_id, cfg in derived.items():
        inputs = list((cfg.get("derived") or {}).get("inputs") or [])
        lineage[indicator_id] = {
            "grade": "C",
            "source": None,
            "transform": None,
            "derived_from": inputs,
        }

    # Input sources resolved AFTER every indicator is placed, so a derived
    # indicator whose input is itself derived resolves to the fetched sources
    # underneath. The graph is acyclic -- src/analytics/engine.py's
    # resolve_order() already proves that on the same configs -- but the walk
    # guards against a cycle anyway rather than recursing forever.
    for indicator_id, entry in lineage.items():
        if entry["grade"] != "C":
            continue
        entry["input_sources"] = sorted(_walk_sources(indicator_id, lineage, set()))

    return lineage


def _walk_sources(indicator_id: str, lineage: dict[str, dict], seen: set[str]) -> set[str]:
    if indicator_id in seen:
        return set()
    seen.add(indicator_id)
    entry = lineage.get(indicator_id)
    if entry is None:
        return set()
    if entry.get("source"):
        return {entry["source"]}
    found: set[str] = set()
    for parent in entry.get("derived_from") or []:
        found |= _walk_sources(parent, lineage, seen)
    return found
