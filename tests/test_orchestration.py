"""The Dagster layer (orchestration/, docs/features/orchestration.md).

What these tests hold it to: every asset runs a command the Makefile runs,
or one daily_fetch.yml ran itself before step 2 handed it to the coordinator;
the graph has the workflow's dependencies; the
validation checks are the rule registry; nothing starts by itself; nothing it
writes is committable; and a failing source still lets the exports run while
the day ends red -- proven by running the coordinator, not by reading it.

Sources and exporters are replaced by a recording fake in the scenario tests:
what is under test is the orchestration, and the real scripts need a network
and minutes. tests/test_orchestration_parity.py runs the real ones.
"""

import ast
import csv
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

dg = pytest.importorskip("dagster")

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from export_observations_csv import COLUMNS  # noqa: E402

from orchestration import checks, daily, manifest, run  # noqa: E402
from orchestration.assets import sources_manual  # noqa: E402
from orchestration.commands import COMMANDS, TRACKED  # noqa: E402
from orchestration.definitions import DAILY_CRON, build_defs  # noqa: E402
from orchestration.paths import PipelinePaths  # noqa: E402
from orchestration.policies import fetch_windows, window_days  # noqa: E402
from src.db import migrate  # noqa: E402
from src.stores import extra_csv_stores, in_db_stores, load_stores  # noqa: E402
from src.validation.rules import FAIL, RULES, WARN, Violation  # noqa: E402

GROUPS = {"sources_api", "sources_manual", "reference_data", "canonical", "derived", "website"}
VALIDATED = "validated_working_database"
WORKFLOW = REPO / ".github" / "workflows" / "daily_fetch.yml"


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _steps() -> list[dict]:
    (job,) = _workflow()["jobs"].values()
    return job["steps"]


def _flatten(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\\\n", " "))


def _graph():
    return build_defs().resolve_asset_graph()


def _ancestors(graph, name: str) -> set[str]:
    seen, todo = set(), [dg.AssetKey(name)]
    while todo:
        for parent in graph.get(todo.pop()).parent_keys:
            if parent.to_user_string() not in seen:
                seen.add(parent.to_user_string())
                todo.append(parent)
    return seen


def _group(graph, group: str) -> list[str]:
    return [
        k.to_user_string() for k in graph.get_all_asset_keys() if graph.get(k).group_name == group
    ]


# ── The definitions load, and every asset is in one of the six groups ─────────


def test_definitions_load_and_every_asset_has_a_group():
    defs = build_defs()
    dg.Definitions.validate_loadable(defs)
    graph = defs.resolve_asset_graph()
    groups = {k.to_user_string(): graph.get(k).group_name for k in graph.get_all_asset_keys()}
    assert set(groups.values()) == GROUPS, groups


# ── No drift from the pipeline it wraps ──────────────────────────────────────

MAKE_STYLE = {
    "db": "$(DB)",
    "stores": "$(STORES)",
    "data": "data",
    "public_data": "public/data",
    "local": "local",
    "build_id": '"$${BUILD_ID:-local}"',
    "validation_status": "unknown",
    "today": "-",
}
WORKFLOW_STYLE = {
    **MAKE_STYLE,
    "db": '"$WORKING_DB"',
    "stores": "config/stores.yaml",
    "build_id": '"${{ github.run_id }}"',
    "today": '"$(date -u +%Y-%m-%d)"',
}


# Until Dagster step 2, daily_fetch.yml ran every command itself (develop at
# 5c61e356). Three of them have no Makefile twin with the same arguments; these are the lines it ran,
# frozen, so the coordinator keeps running exactly what production ran.
RETIRED_WORKFLOW_COMMANDS = {
    "staging_db": 'python scripts/build_staging_db.py --working-db "$WORKING_DB"',
    "market_data": "python fetch_stocks.py",
    "revisions_report": (
        'python scripts/revisions_report.py --db "$WORKING_DB" --since "$(date -u +%Y-%m-%d)"'
    ),
}
# Every script that workflow ran between assemble and offload, bar the
# validator (the checks, below).
RETIRED_WORKFLOW_SCRIPTS = {
    "scripts/build_staging_db.py",
    "belgian_macro_db.py",
    "scripts/sync_to_canonical.py",
    "scripts/sync_statbel.py",
    "scripts/sync_onem.py",
    "scripts/sync_onem_rates.py",
    "scripts/sync_walstat.py",
    "fetch_stocks.py",
    "scripts/revisions_report.py",
    "scripts/export_canonical_csv.py",
    "scripts/export_communes_csv.py",
    "scripts/export_communes_history_csv.py",
    "scripts/export_communes_table_json.py",
    "scripts/export_aggregates_csv.py",
    "src.exporters.metadata",
    "scripts/export_percentiles_csv.py",
    "scripts/export_site_payloads.py",
    "scripts/export_explorer_payloads.py",
    "scripts/export_local_pages.py",
}
# The step ids whose outcomes gated auto-merge in that workflow.
RETIRED_GATE_IDS = {
    "fetch_macro",
    "sync_canonical",
    "sync_statbel",
    "sync_onem",
    "sync_onem_rates",
    "sync_walstat",
    "sync_international",
    "fetch_stocks",
}


@pytest.mark.parametrize("name", sorted(COMMANDS))
def test_every_command_is_one_the_makefile_runs_or_production_ran(name):
    argv = COMMANDS[name].argv
    makefile = _flatten((REPO / "Makefile").read_text(encoding="utf-8"))
    as_make = " ".join(["$(PYTHON)", *(t.format(**MAKE_STYLE) for t in argv)])
    as_workflow = " ".join(["python", *(t.format(**WORKFLOW_STYLE) for t in argv)])
    assert as_make in makefile or RETIRED_WORKFLOW_COMMANDS.get(name) == as_workflow, (
        as_make,
        as_workflow,
    )


def test_every_script_production_ran_is_wrapped():
    wrapped = {c.argv[1] if c.argv[0] == "-m" else c.argv[0] for c in COMMANDS.values()}
    assert RETIRED_WORKFLOW_SCRIPTS <= wrapped, RETIRED_WORKFLOW_SCRIPTS - wrapped


# ── Same dependencies as the workflow, never two SQLite writers at once ──────


def test_canonical_sync_depends_on_the_macro_fetch_and_nothing_else():
    graph = _graph()
    parents = {
        p.to_user_string() for p in graph.get(dg.AssetKey("canonical_observations")).parent_keys
    }
    assert parents == {"staging_db", "macro_legacy_fetch"}


def test_every_source_feeds_the_validated_database_and_every_export_hangs_off_it():
    graph = _graph()
    assert {"staging_db", *TRACKED} <= _ancestors(graph, VALIDATED)
    for name in _group(graph, "derived") + _group(graph, "website"):
        assert VALIDATED in _ancestors(graph, name), name
    for name in ("local_pages", "explorer_payloads"):
        assert "site_payloads" in _ancestors(graph, name)


def test_every_job_runs_in_process():
    defs = build_defs()
    for job in ("assemble_working_database", "fetch_sources", "validate_and_export"):
        assert defs.resolve_job_def(job).executor_def.name == "in_process"


# ── Nothing starts by itself ─────────────────────────────────────────────────


def test_the_schedule_is_stopped_and_uses_the_workflow_cron():
    schedule = build_defs().resolve_schedule_def("daily_fetch_sources")
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    doc = _workflow()
    triggers = doc.get("on", doc.get(True))  # PyYAML reads a bare `on:` as True
    assert DAILY_CRON == schedule.cron_schedule == triggers["schedule"][0]["cron"]


def test_no_asset_or_check_carries_an_automation_condition():
    graph = _graph()
    for key in graph.get_all_asset_keys():
        assert graph.get(key).automation_condition is None, key
    for spec in checks.validation_rules.check_specs:
        assert spec.automation_condition is None, spec.name


def test_the_export_job_selects_no_source():
    selected = {
        k.to_user_string()
        for k in build_defs().resolve_job_def("validate_and_export").asset_layer.selected_asset_keys
    }
    manual = {s.key.to_user_string() for s in sources_manual.SPECS}
    assert not selected & {"staging_db", *TRACKED, *manual}


def test_the_tracked_outcomes_are_the_ones_the_workflow_gated_auto_merge_on():
    assert {COMMANDS[n].workflow_step for n in TRACKED} == RETIRED_GATE_IDS
    assert len(TRACKED) == 8
    gate = next(s for s in _steps() if s.get("id") == "sources")
    assert "orchestration.manifest" in gate["run"]


def test_the_runner_reads_the_working_database_the_workflow_names():
    env = _workflow()["env"]
    assert env["WORKING_DB"] == "data/local/working.db"


# ── The checks are the rule registry ─────────────────────────────────────────


def test_one_check_per_rule_blocking_exactly_when_the_rule_fails_the_build():
    specs = {spec.name: spec for spec in checks.validation_rules.check_specs}
    assert set(specs) == set(RULES) | {"store_loads"}
    for name, spec in specs.items():
        severity = FAIL if name == "store_loads" else RULES[name][0]
        assert spec.blocking == (severity == FAIL), name


# ── Manual sources are observed, never materialised ──────────────────────────


def test_one_external_asset_per_hand_loaded_store_and_none_for_in_db_stores():
    stores = load_stores()
    names = {spec.key.to_user_string() for spec in sources_manual.manual_source_specs()}
    assert names == {s.name for s in extra_csv_stores(stores)}
    assert not names & {s.name for s in in_db_stores(stores)}


def test_observing_a_store_reports_its_rows_and_latest_period(tmp_path):
    path = tmp_path / "store.csv"
    rows = [("2022", "2022-12-31"), ("2024-Q1", "2024-03-31"), ("2023", "2023-12-31")]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        for period, end in rows:
            writer.writerow({**dict.fromkeys(COLUMNS, ""), "period": period, "period_end": end})
    described = sources_manual.describe_store_csv(path)
    assert described["rows"] == 3
    # "2024-Q1" sorts above "2023" as a string too; "2024-Q1" < "2024" would not.
    assert described["latest period"] == "2024-Q1"


# ── Freshness is the config's fetch_window_days ──────────────────────────────


def test_freshness_windows_come_from_fetch_window_days():
    graph = _graph()
    windows = fetch_windows()
    walstat = yaml.safe_load((REPO / "config/sources/walstat.yaml").read_text(encoding="utf-8"))
    assert window_days("walstat_observations", windows) == walstat["fetch_window_days"]
    for key in graph.get_all_asset_keys():
        name = key.to_user_string()
        policy = graph.get(key).freshness_policy
        expected = window_days(name, windows) if name in COMMANDS else None
        if expected is None:
            assert policy is None, name
        else:
            assert policy.fail_window.to_timedelta().days == expected, name


# ── Nothing it writes can be committed ───────────────────────────────────────


@pytest.mark.parametrize(
    "path",
    [
        "data/local/dagster_home/storage/runs.db",
        "data/local/dagster_runs/20260913T050000Z-0a1b2c3d/sources.json",
        ".tmp_dagster_home_x1y2/history/runs.db",
        ".dagster/logs/event.log",
    ],
)
def test_dagster_state_is_gitignored(path):
    result = subprocess.run(["git", "check-ignore", "-q", path], cwd=REPO)
    assert result.returncode == 0, f"{path} is not ignored"


def _code_strings(path: Path) -> list[str]:
    """String constants in a module, docstrings excluded."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    return [
        n.value
        for n in ast.walk(tree)
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings
    ]


def test_no_orchestration_code_names_the_committed_database_or_the_offload():
    for module in (REPO / "orchestration").rglob("*.py"):
        for text in _code_strings(module):
            assert "belgian_macro.db" not in text, module
            assert "offload" not in text.lower(), module


# ── run_script runs the Makefile's command line, from the repository ─────────


class _FakePopen:
    calls: list[dict] = []
    exit_code = 0

    def __init__(self, argv, **kwargs):
        _FakePopen.calls.append({"argv": argv, **kwargs})
        self.stdout = iter(["line one\n", "line two\n"])

    def wait(self):
        return _FakePopen.exit_code

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def fake_popen(monkeypatch):
    _FakePopen.calls = []
    _FakePopen.exit_code = 0
    monkeypatch.setattr(run.subprocess, "Popen", _FakePopen)
    return _FakePopen


@pytest.mark.parametrize("name", TRACKED)
def test_a_source_runs_its_workflow_command_from_the_repository_root(fake_popen, name):
    run.run_script(dg.build_asset_context(), PipelinePaths(), name)
    (call,) = fake_popen.calls
    expected = [t.replace("{db}", "data/local/working.db") for t in COMMANDS[name].argv]
    assert call["argv"] == [sys.executable, *expected]
    assert Path(call["cwd"]) == REPO


def test_a_non_zero_exit_turns_the_asset_red(fake_popen):
    fake_popen.exit_code = 3
    with pytest.raises(dg.Failure, match="exited with code 3"):
        run.run_script(dg.build_asset_context(), PipelinePaths(), "onem_observations")


def test_a_repository_only_script_refuses_a_redirected_output(fake_popen, tmp_path):
    with pytest.raises(dg.Failure, match="can only write into the repository"):
        run.run_script(
            dg.build_asset_context(), PipelinePaths(out_root=str(tmp_path)), "page_documents"
        )
    assert fake_popen.calls == []


# ── The coordinator: a red source still exports, and the day ends red ────────


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    db = tmp_path / "working.db"
    migrate.run(db, migrations_dir=REPO / "migrations")
    paths = PipelinePaths(
        out_root=str(tmp_path / "out"), working_db=str(db), runs_dir=str(tmp_path / "runs")
    )
    calls: list[str] = []
    values: dict[str, dict] = {}
    failing: set[str] = set()

    def fake_run_script(context, paths, name, extra=(), **given):
        calls.append(name)
        values[name] = given
        if name in failing:
            raise RuntimeError(f"{name} is down")
        return ""

    monkeypatch.setattr(run, "run_script", fake_run_script)
    monkeypatch.setattr(checks, "run_validation", lambda paths: [])
    monkeypatch.setattr(checks, "record_volume", lambda paths: 0)
    return SimpleNamespace(
        paths=paths,
        defs=build_defs(paths),
        calls=calls,
        values=values,
        failing=failing,
        instance=dg.DagsterInstance.ephemeral(),
        runs=tmp_path / "runs",
    )


def _manifests(runs: Path) -> list[dict]:
    return [manifest.load(p) for p in sorted(runs.glob("*/sources.json"))]


def _metadata(result, name: str) -> dict:
    for event in result.get_asset_materialization_events():
        if event.asset_key.to_user_string() == name:
            return event.step_materialization_data.materialization.metadata
    raise AssertionError(f"{name} was not materialised")


EXPORTS = {"national_csv", "communes_csv", "site_payloads", "local_pages", "site_index"}


def test_one_red_source_still_exports_but_the_day_ends_red(sandbox):
    sandbox.failing.add("onem_observations")

    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_PARTIAL

    (state,) = _manifests(sandbox.runs)
    assert state["sources"]["onem_observations"]["status"] == manifest.FAILED
    assert "onem_observations is down" in state["sources"]["onem_observations"]["message"]
    for name in TRACKED:
        if name != "onem_observations":
            assert state["sources"][name]["status"] == manifest.SUCCESS, name
    assert EXPORTS <= set(sandbox.calls)
    assert state["validate_and_export"]["status"] == manifest.SUCCESS
    latest = sandbox.instance.get_latest_materialization_event(dg.AssetKey(VALIDATED))
    metadata = latest.asset_materialization.metadata
    assert metadata["source_run"].value == state["run_id"]
    assert metadata["red_sources"].value == 1


def test_a_failed_canonical_sync_is_red_like_a_failed_source(sandbox):
    sandbox.failing.add("canonical_observations")

    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_PARTIAL

    (state,) = _manifests(sandbox.runs)
    assert state["sources"]["canonical_observations"]["status"] == manifest.FAILED
    others = [n for n in TRACKED if n != "canonical_observations"]
    assert all(state["sources"][n]["status"] == manifest.SUCCESS for n in others)
    assert EXPORTS <= set(sandbox.calls)


def test_a_failed_macro_fetch_skips_the_canonical_sync_and_that_counts_as_red(sandbox):
    sandbox.failing.add("macro_legacy_fetch")

    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_PARTIAL

    (state,) = _manifests(sandbox.runs)
    assert state["sources"]["macro_legacy_fetch"]["status"] == manifest.FAILED
    assert state["sources"]["canonical_observations"]["status"] == manifest.SKIPPED
    assert "canonical_observations" not in sandbox.calls
    assert manifest.red_sources(state) == ["canonical_observations", "macro_legacy_fetch"]
    assert EXPORTS <= set(sandbox.calls)


def test_in_a_single_run_a_failed_source_would_block_the_exports(sandbox):
    """The negative control: why the fetch and the exports are two runs."""
    sandbox.failing.add("onem_observations")
    job = sandbox.defs.resolve_implicit_global_asset_job_def()
    result = job.execute_in_process(
        instance=sandbox.instance,
        raise_on_error=False,
        asset_selection=[dg.AssetKey("onem_observations"), dg.AssetKey(VALIDATED)],
    )
    materialized = {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}
    assert VALIDATED not in materialized


def test_all_green_exits_zero_and_each_run_gets_its_own_manifest(sandbox):
    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_OK
    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_OK

    states = _manifests(sandbox.runs)
    assert len(states) == 2 and states[0]["run_id"] != states[1]["run_id"]
    assert all(manifest.red_sources(s) == [] for s in states)


def test_a_failed_assemble_stops_before_any_export(sandbox):
    sandbox.failing.add("staging_db")

    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_FAILED

    (state,) = _manifests(sandbox.runs)
    assert state["assemble"]["status"] == manifest.FAILED
    assert state["validate_and_export"]["status"] == manifest.NOT_RUN
    assert sandbox.calls == ["staging_db"]


def test_a_crashed_fetch_job_leaves_every_source_not_run_and_still_exports(sandbox, monkeypatch):
    real = daily._execute

    def crashing(defs, job_name, instance, **kwargs):
        if job_name == "fetch_sources":
            raise RuntimeError("instance went away")
        return real(defs, job_name, instance, **kwargs)

    monkeypatch.setattr(daily, "_execute", crashing)

    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_PARTIAL

    (state,) = _manifests(sandbox.runs)
    assert all(state["sources"][n]["status"] == manifest.NOT_RUN for n in TRACKED)
    assert "instance went away" in state["fetch_crash"]
    assert EXPORTS <= set(sandbox.calls)


# ── Validation still blocks, as the workflow's validation step does ──────────


def test_a_failing_rule_stops_every_export_and_the_volume_baseline(sandbox, monkeypatch):
    violation = Violation("unique_latest", FAIL, "keys with more than one is_latest row", 2)
    monkeypatch.setattr(checks, "run_validation", lambda paths: [violation])
    recorded = []
    monkeypatch.setattr(checks, "record_volume", lambda paths: recorded.append(paths) or 0)

    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_FAILED

    (state,) = _manifests(sandbox.runs)
    assert manifest.red_sources(state) == []
    assert state["validate_and_export"]["status"] == manifest.FAILED
    assert not EXPORTS & set(sandbox.calls)
    assert recorded == [], "a failing run must never become tomorrow's volume baseline"


def test_a_warning_rule_is_reported_and_blocks_nothing(sandbox, monkeypatch):
    violation = Violation("staleness", WARN, "ONEM is 40 days old")
    monkeypatch.setattr(checks, "run_validation", lambda paths: [violation])

    result = sandbox.defs.resolve_job_def("validate_and_export").execute_in_process(
        instance=sandbox.instance, raise_on_error=False
    )

    assert result.success
    assert EXPORTS <= set(sandbox.calls)
    failed = {e.check_name: e for e in result.get_asset_check_evaluations() if not e.passed}
    assert set(failed) == {"staleness"}
    assert failed["staleness"].severity == dg.AssetCheckSeverity.WARN


# ── Step 2: what the runner reads -- manifest path, gate, validation status ──


def test_the_coordinator_hands_the_runner_its_own_manifest(sandbox, tmp_path):
    output = tmp_path / "github_output"
    sandbox.failing.add("onem_observations")

    daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance, github_output=output)

    (state,) = _manifests(sandbox.runs)
    lines = output.read_text(encoding="utf-8").splitlines()
    assert lines == [f"manifest={manifest.path_for(sandbox.runs, state['run_id'])}"]


def test_a_failed_assemble_still_hands_the_runner_a_manifest(sandbox, tmp_path):
    output = tmp_path / "github_output"
    sandbox.failing.add("staging_db")

    daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance, github_output=output)

    path = Path(output.read_text(encoding="utf-8").strip().removeprefix("manifest="))
    assert manifest.gate(manifest.load(path))["all_ok"] == "false"


def _green_manifest() -> dict:
    state = manifest.initial("20260913T050000Z-0a1b2c3d")
    state["assemble"]["status"] = manifest.SUCCESS
    for source in state["sources"].values():
        source["status"] = manifest.SUCCESS
    state["validate_and_export"]["status"] = manifest.SUCCESS
    return state


def test_the_gate_opens_only_when_everything_succeeded():
    state = _green_manifest()
    outputs = manifest.gate(state)
    assert outputs["all_ok"] == "true"
    assert {k for k in outputs if k not in ("all_ok", "summary")} == RETIRED_GATE_IDS
    assert outputs["summary"] == ", ".join(f"{COMMANDS[n].workflow_step}=success" for n in TRACKED)

    for status in (manifest.FAILED, manifest.SKIPPED, manifest.NOT_RUN):
        state = _green_manifest()
        state["sources"]["walstat_observations"]["status"] = status
        outputs = manifest.gate(state)
        assert outputs["all_ok"] == "false", status
        assert outputs["sync_walstat"] == status

    for step in ("assemble", "validate_and_export"):
        state = _green_manifest()
        state[step]["status"] = manifest.FAILED
        assert manifest.gate(state)["all_ok"] == "false", step

    state = _green_manifest()
    del state["sources"]["market_data"]
    assert manifest.gate(state)["all_ok"] == "false", "a missing outcome is not a success"


def test_the_gate_command_prints_outputs_and_refuses_a_missing_manifest(tmp_path, capsys):
    path = manifest.path_for(tmp_path, "20260913T050000Z-0a1b2c3d")
    manifest.write(path, _green_manifest())

    assert manifest.main([str(path)]) == 0
    printed = dict(line.split("=", 1) for line in capsys.readouterr().out.splitlines())
    assert printed["all_ok"] == "true" and printed["fetch_macro"] == "success"

    assert manifest.main([str(tmp_path / "missing.json")]) == 2
    assert manifest.main([""]) == 2
    assert "all_ok" not in capsys.readouterr().out


def test_the_published_validation_status_comes_from_the_checks_of_the_same_run(sandbox):
    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_OK
    assert sandbox.values["site_payloads"] == {"validation_status": "pass"}


def test_a_warning_still_publishes_pass(sandbox, monkeypatch):
    violation = Violation("staleness", WARN, "ONEM is 40 days old")
    monkeypatch.setattr(checks, "run_validation", lambda paths: [violation])

    sandbox.defs.resolve_job_def("validate_and_export").execute_in_process(
        instance=sandbox.instance, raise_on_error=False
    )

    assert sandbox.values["site_payloads"] == {"validation_status": "pass"}


def test_payloads_built_without_the_checks_publish_unknown(sandbox):
    job = sandbox.defs.resolve_implicit_global_asset_job_def()
    result = job.execute_in_process(
        instance=sandbox.instance,
        raise_on_error=False,
        asset_selection=[dg.AssetKey("site_payloads")],
    )
    assert result.success
    assert sandbox.values["site_payloads"] == {"validation_status": "unknown"}
    assert _metadata(result, "site_payloads")["validation_status"].value == "unknown"


def test_a_failed_blocking_check_reads_as_fail(sandbox, monkeypatch):
    violation = Violation("unique_latest", FAIL, "keys with more than one is_latest row", 2)
    monkeypatch.setattr(checks, "run_validation", lambda paths: [violation])

    result = sandbox.defs.resolve_job_def("validate_and_export").execute_in_process(
        instance=sandbox.instance, raise_on_error=False
    )

    assert checks.validation_status(sandbox.instance, result.run_id) == "fail"
    assert "site_payloads" not in sandbox.values


def test_the_checks_write_validate_data_summary_and_annotations(sandbox, monkeypatch, capsys):
    summary = sandbox.runs.parent / "validation-summary.md"
    # A new resource, not model_copy: Dagster rebuilds a resource from the
    # arguments it was constructed with.
    paths = PipelinePaths(
        out_root=sandbox.paths.out_root,
        working_db=sandbox.paths.working_db,
        runs_dir=sandbox.paths.runs_dir,
        validation_summary=str(summary),
    )
    violations = [
        Violation("unique_latest", FAIL, "keys with more than one is_latest row", 2),
        Violation("staleness", WARN, "ONEM is 40 days old"),
    ]
    monkeypatch.setattr(checks, "run_validation", lambda paths: violations)

    build_defs(paths).resolve_job_def("validate_and_export").execute_in_process(
        instance=sandbox.instance, raise_on_error=False
    )

    text = summary.read_text(encoding="utf-8")
    assert "1 failure(s), 1 warning(s)" in text
    assert "keys with more than one is_latest row" in text and "ONEM is 40 days old" in text
    out = capsys.readouterr().out
    assert "::error::" in out and "::warning::" in out


def test_the_runner_environment_reaches_the_paths(monkeypatch):
    from orchestration.definitions import default_paths

    monkeypatch.setenv("WORKING_DB", "data/local/other.db")
    monkeypatch.setenv("BUILD_ID", "34764984140")
    monkeypatch.setenv("VALIDATION_SUMMARY", "/tmp/runner/validation-summary.md")
    paths = default_paths()
    assert paths.working_db == "data/local/other.db"
    assert paths.build_id == "34764984140"
    assert paths.validation_summary == "/tmp/runner/validation-summary.md"


# ── A stale manifest is never attributed to a run ────────────────────────────


def test_a_manual_export_run_ignores_old_manifests_on_disk(sandbox):
    old = manifest.path_for(sandbox.runs, "20260101T000000Z-deadbeef")
    state = manifest.initial("20260101T000000Z-deadbeef")
    for source in state["sources"].values():
        source["status"] = manifest.FAILED
    manifest.write(old, state)
    before = old.read_bytes()

    result = sandbox.defs.resolve_job_def("validate_and_export").execute_in_process(
        instance=sandbox.instance, raise_on_error=False
    )

    assert result.success
    metadata = _metadata(result, VALIDATED)
    assert metadata["source_run"].value == manifest.NO_SOURCE_RUN
    assert "red_sources" not in metadata
    assert old.read_bytes() == before


def _with_manifest(path: str, run_id: str) -> dict:
    return {"ops": {VALIDATED: {"config": {"source_manifest": path, "coordinator_run_id": run_id}}}}


def test_a_configured_manifest_that_does_not_exist_fails(sandbox, tmp_path):
    result = sandbox.defs.resolve_job_def("validate_and_export").execute_in_process(
        instance=sandbox.instance,
        raise_on_error=False,
        run_config=_with_manifest(str(tmp_path / "gone" / "sources.json"), "x"),
    )
    assert VALIDATED in result.get_failed_step_keys()
    assert "site_payloads" not in sandbox.calls


def test_a_manifest_from_another_run_is_refused(sandbox):
    path = manifest.path_for(sandbox.runs, "20260101T000000Z-deadbeef")
    manifest.write(path, manifest.initial("20260101T000000Z-deadbeef"))
    result = sandbox.defs.resolve_job_def("validate_and_export").execute_in_process(
        instance=sandbox.instance,
        raise_on_error=False,
        run_config=_with_manifest(str(path), "20260913T050000Z-0a1b2c3d"),
    )
    assert VALIDATED in result.get_failed_step_keys()


def test_old_run_directories_are_pruned_oldest_first(tmp_path):
    for day in range(1, 36):
        manifest.write(manifest.path_for(tmp_path, f"202608{day:02d}T050000Z-00000000"), {})
    manifest.prune(tmp_path, keep=30)
    remaining = sorted(p.name for p in tmp_path.iterdir())
    assert len(remaining) == 30 and remaining[0].startswith("20260806")


# ── Source metadata never reports an assemble-time reload as a fetch ─────────


def test_the_last_fetch_run_ignores_the_assemble_time_reload(tmp_path):
    import sqlite3

    db = tmp_path / "working.db"
    migrate.run(db, migrations_dir=REPO / "migrations")
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, finished_at, status) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("walstat", "walstat", "2026-09-01T05:00:00+00:00", "2026-09-01T05:01:00+00:00", "ok"),
            # Later, but only build_staging_db.py reloading the committed CSV.
            ("walstat", "rebuild", "2026-09-13T17:22:33+00:00", None, "ok"),
        ],
    )
    conn.commit()
    conn.close()

    snapshot = run.source_snapshot(db, ("walstat",))

    assert snapshot["walstat last fetch run"] == "ok at 2026-09-01T05:01:00+00:00"
