"""The ONEM/WalStat cutover is only safe if four files agree on it -- asserted
here on their text, since there is no cheap way to run Actions locally.

daily_fetch.yml, manual_sources.yml, ci.yml and the Makefile must all read the
assembled working database, never the committed one on its own: since the
cutover (docs/decisions/0006-stores-split-by-volume.md) the committed file
holds 1,939 of ~86,000 observations. One file left behind means either
published pages with ONEM and WalStat silently missing, or a CI run that
fails every PR -- and, through the open-PR check, a daily run that then stops
for good.

Since Dagster step 2 (docs/features/orchestration.md) daily_fetch.yml runs
assemble, every source, validation and every export as one step,
`python -m orchestration.daily`; what that step runs, and in which order, is
held by tests/test_orchestration.py. What stays here is what only the workflow
text can show: the coordinator runs before the offload, nothing can be skipped
past, and the auto-merge gate reads that run's manifest.
"""

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO / ".github" / "workflows"

COORDINATOR = "python -m orchestration.daily"
# Every script the coordinator runs. None may also run as a step of its own:
# the data would be fetched or exported twice, or around the checks.
PIPELINE_SCRIPTS = (
    "scripts/build_staging_db.py",
    "belgian_macro_db.py",
    "scripts/sync_",
    "fetch_stocks.py",
    "scripts/validate_data.py",
    "scripts/revisions_report.py",
    "scripts/export_",
    "src.exporters.metadata",
)


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


def test_daily_fetch_runs_the_coordinator_then_offloads_then_gates_then_opens_the_pr():
    steps = _steps("daily_fetch.yml")
    daily = _index(steps, id="daily")
    assert COORDINATOR in steps[daily]["run"]
    offload = _index(steps, contains="scripts/offload_stores.py")
    sources = _index(steps, id="sources")
    stage = _index(steps, id="stage")
    pr = _index(steps, id="pr")
    assert daily < offload < sources < stage < pr


def test_the_pipeline_runs_only_through_the_coordinator():
    steps = _steps("daily_fetch.yml")
    assert sum(COORDINATOR in step.get("run", "") for step in steps) == 1
    for step in steps:
        for script in PIPELINE_SCRIPTS:
            assert script not in step.get("run", ""), f"{step['name']} runs {script} directly"


def test_nothing_before_the_pr_can_be_skipped_past():
    """No continue-on-error anywhere: a red source is absorbed inside the
    coordinator (exit 3), never by the workflow hiding a failed step."""
    steps = _steps("daily_fetch.yml")
    assert [s["name"] for s in steps if s.get("continue-on-error")] == []
    daily = steps[_index(steps, id="daily")]["run"]
    # Exactly one exit code is let through; every other one stops the run
    # before the offload, as a failed assemble or validation step did.
    assert '[ "$STATUS" -eq 3 ]' in daily
    assert daily.rstrip().endswith("exit $STATUS")


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
    assert sources < _index(steps, id="pr")
    # The gate reads the manifest of THIS run, by the path the coordinator
    # reported -- never a fixed path an older run could have left behind.
    check = steps[sources]["run"]
    assert 'python -m orchestration.manifest "${{ steps.daily.outputs.manifest }}"' in check
    assert '>> "$GITHUB_OUTPUT"' in check
    # No pipe: the default shell has no pipefail, so `gate | tee` would hide
    # a refused manifest behind tee's success.
    assert "|" not in check

    automerge = next(s for s in steps if s.get("name") == "Enable auto-merge")
    assert "steps.sources.outputs.all_ok == 'true'" in automerge["if"]
    last = steps[-1]
    assert last["name"] == "Fail run if any source failed"
    assert last["if"] == "steps.sources.outputs.all_ok != 'true'"


def test_the_manifest_status_is_derived_not_typed():
    """site_payloads takes it from the checks of its own Dagster run
    (tests/test_orchestration.py); the workflow must not type one in."""
    text = (WORKFLOWS / "daily_fetch.yml").read_text(encoding="utf-8")
    assert "--validation-status" not in text


def test_the_runner_installs_dagster_at_the_pinned_version_without_the_ui():
    steps = _steps("daily_fetch.yml")
    install_at = _index(steps, contains="pip install")
    install = steps[install_at]["run"]
    assert "-r requirements.txt" in install
    assert "\"$(grep '^dagster==' requirements-dagster.txt)\"" in install
    assert "webserver" not in install
    assert install_at < _index(steps, id="daily")
    pins = [
        line
        for line in (REPO / "requirements-dagster.txt").read_text(encoding="utf-8").splitlines()
        if line.startswith("dagster")
    ]
    assert len(pins) == 2 and len({line.split("==")[1] for line in pins}) == 1, pins
    pyproject = (REPO / "pyproject.toml").read_text(encoding="utf-8")
    for line in pins:
        assert f'"{line}"' in pyproject, line


def test_dagster_history_stays_in_the_runner_and_off_the_network():
    steps = _steps("daily_fetch.yml")
    env = steps[_index(steps, id="daily")]["env"]
    assert env["DAGSTER_HOME"].startswith("${{ runner.temp }}")
    assert env["DAGSTER_DISABLE_TELEMETRY"]
    assert env["BUILD_ID"] == "${{ github.run_id }}"
    assert env["VALIDATION_SUMMARY"].startswith("${{ runner.temp }}")


def test_explorer_payloads_follow_site_payloads_in_every_exporting_workflow():
    # daily_fetch.yml: the asset graph orders them (tests/test_orchestration.py).
    for workflow in ("manual_sources.yml",):
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


def test_manual_sources_validates_every_store_and_derives_its_status():
    """Pipeline repair part 4: the workflow that publishes a hand refresh used
    to rebuild one store of six and check only its foreign keys, then stamp
    the manifest "unknown". It now runs the same validation as CI and the
    daily run, before exporting, and the manifest reports its outcome."""
    steps = _steps("manual_sources.yml")
    validate = _index(steps, id="validate")
    assert "scripts/validate_data.py" in steps[validate]["run"]
    assert "--record-volume" not in steps[validate]["run"]
    assert validate < _index(steps, contains="scripts/export_communes_csv.py")
    text = (WORKFLOWS / "manual_sources.yml").read_text(encoding="utf-8")
    assert "--validation-status unknown" not in text
    assert "steps.validate.outcome" in text
    assert "PRAGMA foreign_key_check" not in text


def test_the_daily_job_has_room_for_the_dagster_install_and_run():
    """30 minutes: a full run measured ~15 minutes on the maintainer's machine
    once Dagster is installed and every export is rebuilt, so the old 15-minute
    limit would cut a normal day off mid-export."""
    doc = yaml.safe_load((WORKFLOWS / "daily_fetch.yml").read_text(encoding="utf-8"))
    (job,) = doc["jobs"].values()
    assert job["timeout-minutes"] == 30
