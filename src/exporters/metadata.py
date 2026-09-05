"""
Reads config/indicators/*.yaml + config/sources/*.yaml, validates them
(re-validates rather than trusting CI already ran -- daily_fetch.yml never
runs the CI validator step), and emits one compact JSON file the frontend
fetches instead of dashboard.html's old hardcoded M/SECTIONS objects.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.validation.config_schema import load_and_validate_all  # noqa: E402

# Mirrors dashboard.html's old hardcoded SECTIONS object. A whole config
# directory for 8 rows that rarely change would be over-engineering; this
# constant is the SECTIONS replacement, emitted into the same JSON as the
# indicators so the frontend needs exactly one fetch.
CATEGORY_ORDER = [
    {
        "id": "gdp",
        "label": {
            "en": "Gross Domestic Product",
            "fr": "Produit Intérieur Brut",
            "nl": "Bruto Binnenlands Product",
        },
    },
    {
        "id": "confidence",
        "label": {
            "en": "Confidence & Sentiment",
            "fr": "Confiance et Sentiment",
            "nl": "Vertrouwen & Sentiment",
        },
    },
    {
        "id": "prices",
        "label": {
            "en": "Production & Prices",
            "fr": "Production et Prix",
            "nl": "Productie & Prijzen",
        },
    },
    {
        "id": "labour",
        "label": {"en": "Labour Costs", "fr": "Coûts de la Main-d'œuvre", "nl": "Arbeidskosten"},
    },
    {"id": "employment", "label": {"en": "Employment", "fr": "Emploi", "nl": "Werkgelegenheid"}},
    {"id": "unemployment", "label": {"en": "Unemployment", "fr": "Chômage", "nl": "Werkloosheid"}},
    {
        "id": "business",
        "label": {
            "en": "Business Environment",
            "fr": "Environnement des Affaires",
            "nl": "Bedrijfsklimaat",
        },
    },
    {
        "id": "fiscal",
        "label": {
            "en": "Government Finances",
            "fr": "Finances Publiques",
            "nl": "Overheidsfinanciën",
        },
    },
]


def export_metadata(indicators_dir: Path, sources_dir: Path, out_path: Path) -> int:
    indicators, sources = load_and_validate_all(indicators_dir, sources_dir)

    out_indicators = {}
    for code, ind in indicators.items():
        source = sources[ind["source_id"]]
        out_indicators[code] = {
            "name": ind["name"],
            "unit": ind["unit"],
            "frequency": ind["frequency"],
            "source_agency": source["agency"],
            "geo_levels": ind["geo_levels"],
            "display": ind["display"],
        }

    payload = {"categories": CATEGORY_ORDER, "indicators": out_indicators}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return len(out_indicators)


def main() -> None:
    ap = argparse.ArgumentParser(description="Export indicator metadata for the dashboard")
    ap.add_argument("--indicators-dir", default="config/indicators")
    ap.add_argument("--sources-dir", default="config/sources")
    ap.add_argument("--out", default="data/metadata/indicators.json")
    args = ap.parse_args()
    n = export_metadata(Path(args.indicators_dir), Path(args.sources_dir), Path(args.out))
    print(f"Exported {n} indicators to {args.out}")


if __name__ == "__main__":
    main()
