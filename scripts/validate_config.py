"""CLI entry point for CI: validates config/indicators/*.yaml and
config/sources/*.yaml, printing every violation named by file and field
rather than a generic "invalid config" for the whole batch."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.validation.config_schema import ConfigValidationError, load_and_validate_all  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate indicator/source config")
    ap.add_argument("--indicators-dir", default="config/indicators")
    ap.add_argument("--sources-dir", default="config/sources")
    args = ap.parse_args()

    try:
        indicators, sources = load_and_validate_all(
            Path(args.indicators_dir), Path(args.sources_dir)
        )
    except ConfigValidationError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    print(f"OK: {len(indicators)} indicators, {len(sources)} sources validated")


if __name__ == "__main__":
    main()
