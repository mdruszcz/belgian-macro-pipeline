"""The ONEM/WalStat cutover is only safe if four files agree on it -- asserted
here on their text, since there is no cheap way to run Actions locally.

daily_fetch.yml, manual_sources.yml, ci.yml and the Makefile must all read the
assembled working database, never the committed one on its own: since the
cutover (docs/decisions/0006-stores-split-by-volume.md) the committed file
holds 1,939 of ~86,000 observations. One file left behind means either
published pages with ONEM and WalStat silently missing, or a CI run that
fails every PR -- and, through the open-PR check, a daily run that then stops
for good.
"""

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO / ".github" / "workflows"

SYNC_IDS = [
    "fetch_macro",
    "sync_canonical",
    "sync_statbel",
    "sync_onem",
    "sync_onem_rates",
    "sync_walstat",
    "fetch_stocks",
]


def _steps(name: str) -> list[dict]:
    doc = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    (job,) = doc["jobs"].values()
    return job["steps"]


def _index(steps: list[dict], *, id: str | None = None, contains: str | None = None) -> int:
    for i, step in enumerate(steps):
        if id is not None and step.get("id") == id:
            return i
        if contains is not None and contains in step.get("run", ""):
            return i
    raise AssertionError(f"no step with id={id!r} contains={contains!r}")


def test_daily_fetch_assembles_then_syncs_then_validates_then_exports_then_offloads():
    steps = _steps("daily_fetch.yml")
    assemble = _index(steps, contains="scripts/build_staging_db.py")
    validate = _index(steps, id="validate")
    first_export = _index(steps, contains="scripts/export_canonical_csv.py")
    last_export = _index(steps, contains="scripts/export_local_pages.py")
    offload = _index(steps, contains="scripts/offload_stores.py")
    stage = _index(steps, id="stage")

    for sync in SYNC_IDS:
        assert (
            assemble < _index(steps, id=sync) < validate
        ), f"{sync} is not between assemble and validation"
    assert validate < first_export <= last_export < offload < stage


def test_the_assemble_and_offload_steps_cannot_be_skipped_past():
    steps = _steps("daily_fetch.yml")
    for script in ("scripts/build_staging_db.py", "scripts/offload_stores.py"):
        step = steps[_index(steps, contains=script)]
        assert not step.get("continue-on-error"), f"{script} must stop the run when it fails"


def test_only_the_offload_step_names_the_committed_database():
    for step in _steps("daily_fetch.yml"):
        run = step.get("run", "")
        if "data/belgian_macro.db" in run:
            assert "scripts/offload_stores.py" in run, step["name"]


def test_every_database_step_reads_the_working_copy():
    for workflow in ("daily_fetch.yml", "manual_sources.yml"):
        for step in _steps(workflow):
            run = step.get("run", "")
            # manual_sources.yml's round-trip check builds its own scratch
            # database from one CSV on purpose; it reads nothing committed.
            if "--db " in run and "scripts/load_observations_csv.py" not in run:
                assert '--db "$WORKING_DB"' in run, f"{workflow}: {step['name']}"


def test_partial_data_is_not_auto_merged():
    steps = _steps("daily_fetch.yml")
    sources = _index(steps, id="sources")
    pr = _index(steps, id="pr")
    assert sources < pr
    check = steps[sources]["run"]
    for sync in SYNC_IDS:
        assert f"steps.{sync}.outcome == 'success'" in check, f"{sync} is not in the all_ok check"
    # Every continue-on-error step is one a failure can hide behind.
    hidden = [s.get("id") for s in steps if s.get("continue-on-error")]
    assert sorted(hidden) == sorted(SYNC_IDS)

    automerge = next(s for s in steps if s.get("name") == "Enable auto-merge")
    assert "steps.sources.outputs.all_ok == 'true'" in automerge["if"]


def test_the_manifest_status_is_derived_not_typed():
    steps = _steps("daily_fetch.yml")
    payloads = steps[_index(steps, contains="scripts/export_site_payloads.py")]["run"]
    assert "--validation-status pass" not in payloads
    assert "steps.validate.outcome" in payloads


def test_explorer_payloads_follow_site_payloads_in_every_exporting_workflow():
    for workflow in ("daily_fetch.yml", "manual_sources.yml"):
        steps = _steps(workflow)
        site_payloads = _index(steps, contains="scripts/export_site_payloads.py")
        explorer_payloads = _index(steps, contains="scripts/export_explorer_payloads.py")
        local_pages = _index(steps, contains="scripts/export_local_pages.py")
        assert site_payloads < explorer_payloads < local_pages, workflow


def test_the_police_probe_is_gone():
    text = (WORKFLOWS / "daily_fetch.yml").read_text(encoding="utf-8")
    assert "fetch_police_raw.py" not in text
    assert "police-raw-response" not in text


def test_manual_sources_assembles_and_never_writes_the_committed_database():
    steps = _steps("manual_sources.yml")
    assemble = _index(steps, contains="scripts/build_staging_db.py")
    first_export = _index(steps, contains="scripts/export_communes_csv.py")
    assert assemble < first_export
    text = (WORKFLOWS / "manual_sources.yml").read_text(encoding="utf-8")
    assert "offload_stores.py" not in text
    guard = _index(steps, contains="git diff --exit-code --stat -- data/belgian_macro.db")
    assert guard < len(steps) - 1


def test_ci_assembles_before_it_validates():
    doc = yaml.safe_load((WORKFLOWS / "ci.yml").read_text(encoding="utf-8"))
    steps = doc["jobs"]["fast"]["steps"]
    assert _index(steps, contains="scripts/build_staging_db.py") < _index(
        steps, contains="scripts/validate_data.py"
    )


def test_make_all_assembles_and_never_offloads():
    makefile = (REPO / "Makefile").read_text(encoding="utf-8")
    all_line = next(line for line in makefile.splitlines() if line.startswith("all:"))
    prereqs = all_line.split(":", 1)[1].split()
    assert "assemble" in prereqs
    assert prereqs.index("assemble") < prereqs.index("validate") < prereqs.index("exports")
    assert "offload" not in prereqs
    assert "DB           ?= data/local/working.db" in makefile
