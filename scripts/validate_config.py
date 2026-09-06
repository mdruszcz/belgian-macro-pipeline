"""CLI entry point for CI: validates config/indicators/*.yaml and
config/sources/*.yaml, printing every violation named by file and field
rather than a generic "invalid config" for the whole batch."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.validation.config_schema import (  # noqa: E402
    ConfigValidationError,
    load_and_validate_all,
    load_and_validate_derived,
)


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate indicator/source config")
    ap.add_argument("--indicators-dir", default="config/indicators")
    ap.add_argument("--sources-dir", default="config/sources")
    ap.add_argument("--derived-dir", default="config/indicators/derived")
    args = ap.parse_args()

    try:
        indicators, sources = load_and_validate_all(
            Path(args.indicators_dir), Path(args.sources_dir)
        )
        derived = load_and_validate_derived(Path(args.derived_dir), set(indicators))
    except ConfigValidationError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    print(
        f"OK: {len(indicators)} indicators, {len(sources)} sources, "
        f"{len(derived)} derived validated"
    )


if __name__ == "__main__":
    main()
