"""Where the pipeline reads and writes: the one resource every asset takes.

The defaults are the repository's own layout, the paths the Makefile and
daily_fetch.yml use. Tests point `out_root`, `working_db` and `runs_dir` at a
temporary directory instead.
"""

from pathlib import Path

from dagster import ConfigurableResource

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKING_DB = "data/local/working.db"
DEFAULT_STORES = "config/stores.yaml"
DEFAULT_RUNS_DIR = "data/local/dagster_runs"


class PipelinePaths(ConfigurableResource):
    """Every script runs with its working directory at `repo_root`, so a
    relative path here means the same thing it means in the Makefile."""

    repo_root: str = str(REPO_ROOT)
    # Empty means "the repository itself". Anything else redirects the exports
    # that accept an output path; the scripts that can only write into the
    # repository refuse to run (see Command.writes_repo_only).
    out_root: str = ""
    working_db: str = DEFAULT_WORKING_DB
    # Empty means build_staging_db.py's own default: the committed database.
    source_db: str = ""
    stores: str = DEFAULT_STORES
    build_id: str = "local"
    # Where the validation checks append validate_data.py's markdown summary
    # (its --summary-file). Empty: no summary. The runner points it at the
    # job summary page.
    validation_summary: str = ""
    runs_dir: str = DEFAULT_RUNS_DIR

    @property
    def root(self) -> Path:
        return Path(self.repo_root)

    def resolve(self, value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else self.root / path

    @property
    def writes_into_repo(self) -> bool:
        return not self.out_root or Path(self.out_root).resolve() == self.root.resolve()

    def placeholders(self) -> dict[str, str]:
        if self.writes_into_repo:
            data, public_data, local = "data", "public/data", "local"
        else:
            out = Path(self.out_root)
            data, public_data, local = (
                str(out / "data"),
                str(out / "public" / "data"),
                str(out / "local"),
            )
        return {
            "db": self.working_db,
            "stores": self.stores,
            "data": data,
            "public_data": public_data,
            "local": local,
            "build_id": self.build_id,
            # What `make exports` passes. site_payloads replaces it with the
            # outcome of the checks in its own run (checks.validation_status).
            "validation_status": "unknown",
        }

    def render(self, tokens: tuple[str, ...], **extra: str) -> list[str]:
        values = {**self.placeholders(), **extra}
        return [token.format(**values) for token in tokens]
