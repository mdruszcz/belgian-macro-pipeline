"""
Review deliverable for Block B: loads every config/indicators/*.yaml and
reports any missing en/fr/nl on name/display.title/display.description as a
Markdown table. A point-in-time check, run and read, not a committed
artifact -- matches how docs/data_catalog.md's "stub" status is handled.
"""

import argparse
from pathlib import Path

import yaml

LANGS = ("en", "fr", "nl")


def _missing(d: dict | None) -> list[str]:
    if d is None:
        return []
    return [lang for lang in LANGS if not (d.get(lang) or "").strip()]


def report(indicators_dir: Path) -> str:
    rows = []
    for path in sorted(indicators_dir.glob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        code = data["id"]
        missing_name = _missing(data.get("name"))
        display = data.get("display")
        missing_title = _missing(display.get("title")) if display else []
        missing_desc = (
            _missing(display.get("description")) if display and display.get("description") else []
        )
        has_description = "description" in data
        if not (missing_name or missing_title or missing_desc) and has_description:
            continue
        rows.append(
            (
                code,
                ",".join(missing_name) or "-",
                ",".join(missing_title) or "-",
                ",".join(missing_desc) or "-",
                "yes" if has_description else "no",
            )
        )

    lines = [
        "| id | missing name | missing display.title | missing display.description | has description |",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Report indicator config completeness gaps")
    ap.add_argument("--indicators-dir", default="config/indicators")
    args = ap.parse_args()
    print(report(Path(args.indicators_dir)))


if __name__ == "__main__":
    main()
