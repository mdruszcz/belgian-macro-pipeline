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

import csv
import inspect
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
from orchestration.assets import canonical, exporter_arguments, sources_manual  # noqa: E402
from orchestration.commands import COMMANDS, TRACKED, Command  # noqa: E402
from orchestration.definitions import DAILY_CRON, build_defs  # noqa: E402
from orchestration.paths import PipelinePaths  # noqa: E402
from orchestration.policies import fetch_windows, window_days  # noqa: E402
from src.db import migrate  # noqa: E402
from src.stores import (  # noqa: E402
    extra_csv_stores,
    in_db_stores,
    load_stores,
    resolve_extra_observations,
)
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
    "committed_db": "$(COMMITTED_DB)",
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
# Every script that workflow ran from assemble to the offload, bar the
# validator (the checks, below). The offload was a workflow step of its own
# until Dagster step 3.
RETIRED_WORKFLOW_SCRIPTS = {
    "scripts/offload_stores.py",
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
# The live set TRACKED (orchestration/commands.py) gates auto-merge on today,
# under Dagster -- RETIRED_GATE_IDS above stays a frozen historical record of
# the pre-Dagster workflow and is never edited for a new source; this is the
# one place a newly tracked daily source (bankruptcies, feat/ns1-bankruptcies;
# population movement, feat/ns3-population-movement; the communal additional
# IPP rate, feat/ns6-ipp-rate; the AGDP leases/transactions datasets,
# feat/ns4-spf-agdp) is added to what the CURRENT gate covers.
CURRENT_GATE_IDS = RETIRED_GATE_IDS | {
    "sync_bankruptcies",
    "sync_population_movement",
    "sync_ipp_rate",
    "sync_spf_agdp",
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
    for job in (
        "assemble_working_database",
        "fetch_sources",
        "validate_and_export",
        "validate_export_and_offload",
    ):
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


def _selected(job: str) -> set[str]:
    keys = build_defs().resolve_job_def(job).asset_layer.selected_asset_keys
    return {k.to_user_string() for k in keys}


def test_the_export_job_selects_no_source():
    manual = {s.key.to_user_string() for s in sources_manual.SPECS}
    for job in ("validate_and_export", "validate_export_and_offload"):
        assert not _selected(job) & {"staging_db", *TRACKED, *manual}, job


def test_the_production_job_is_the_export_job_plus_the_offload():
    """One run: a failed export or blocking check skips committed_stores in that
    same run. validate_and_export, what the UI offers, writes no committed file."""
    exports = _selected("validate_and_export")
    assert "committed_stores" not in exports
    assert _selected("validate_export_and_offload") == exports | {"committed_stores"}
    assert "committed_stores" not in _selected("fetch_sources")
    parents = _graph().get(dg.AssetKey("committed_stores")).parent_keys
    assert {p.to_user_string() for p in parents} == exports


def test_the_tracked_outcomes_are_the_ones_the_workflow_gated_auto_merge_on():
    assert {COMMANDS[n].workflow_step for n in TRACKED} == CURRENT_GATE_IDS
    assert len(TRACKED) == 12
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


# ── call_function: a script's own function, called in this process ───────────


class _Refusal(Exception):
    pass


@pytest.fixture
def fake_script(monkeypatch):
    """Declares COMMANDS["fake"] as a function of a stand-in scripts/ module."""

    def declare(work, refusal: str | None = None) -> None:
        module = SimpleNamespace(__name__="fake_script", work=work, _Refusal=_Refusal)
        monkeypatch.setattr(run, "import_script", lambda name: module)
        command = Command(("scripts/fake.py",), function="fake_script:work", refusal=refusal)
        monkeypatch.setitem(COMMANDS, "fake", command)

    return declare


def test_call_function_turns_the_declared_refusal_into_a_failure(fake_script):
    def refuses(**kwargs):
        raise _Refusal("a committed row would be lost")

    fake_script(refuses, refusal="_Refusal")
    with pytest.raises(
        dg.Failure, match="fake: fake_script refused: a committed row would be lost"
    ):
        run.call_function(dg.build_asset_context(), "fake", working_db=Path("w.db"))


def test_call_function_lets_any_other_exception_through_even_with_a_refusal_declared(fake_script):
    def crashes(**kwargs):
        raise KeyError("indicator_id")

    fake_script(crashes, refusal="_Refusal")
    with pytest.raises(KeyError, match="indicator_id"):
        run.call_function(dg.build_asset_context(), "fake")


def test_call_function_without_a_refusal_class_masks_nothing(fake_script):
    """The exporters' case: no refusal declared, so no except clause at all --
    neither an empty one nor a broad one."""

    def crashes(**kwargs):
        raise ValueError("period 2024-13 is not a month")

    fake_script(crashes)
    with pytest.raises(ValueError, match="2024-13"):
        run.call_function(dg.build_asset_context(), "fake")

    def refuses_undeclared(**kwargs):
        raise _Refusal("undeclared")

    fake_script(refuses_undeclared)
    with pytest.raises(_Refusal):
        run.call_function(dg.build_asset_context(), "fake")

    fake_script(lambda **kwargs: kwargs)
    returned = run.call_function(dg.build_asset_context(), "fake", db_path=Path("a.db"))
    assert returned == {"db_path": Path("a.db")}


def test_every_declared_function_resolves_and_every_script_error_class_is_declared():
    """A script that defines its own exception class raises it on purpose; an
    entry that forgets refusal= would show that refusal as a bare traceback."""
    for name, command in COMMANDS.items():
        if not command.function:
            continue
        module, function = run.script_function(name)
        assert callable(function), name
        own = [
            n
            for n, value in vars(module).items()
            if isinstance(value, type)
            and issubclass(value, Exception)
            and value.__module__ == module.__name__
        ]
        if command.refusal:
            assert issubclass(getattr(module, command.refusal), Exception), name
        else:
            assert own == [], f"{name}: {module.__name__} defines {own}; declare refusal="


EXPORTERS_BY_FUNCTION = [
    "national_csv",
    "communes_csv",
    "aggregates_csv",
    "percentiles_csv",
    "communes_history_full_csv",
    "communes_history_csv",
    "communes_table_json",
]


@pytest.mark.parametrize("name", EXPORTERS_BY_FUNCTION)
def test_an_exporters_arguments_are_its_command_lines_paths(name, tmp_path):
    argv = COMMANDS[name].argv
    for paths in (
        PipelinePaths(out_root=str(tmp_path / "out"), working_db=str(tmp_path / "working.db")),
        PipelinePaths(),
    ):
        arguments = exporter_arguments(name, paths)
        assert arguments["db_path"] == paths.resolve(paths.working_db)
        assert arguments["out_path"] == paths.output(COMMANDS[name].outputs[0])
        assert arguments["db_path"].is_absolute() and arguments["out_path"].is_absolute()
        if "--stores" in argv:
            registry = str(REPO / "config" / "stores.yaml")
            assert arguments["extra_observations"] == resolve_extra_observations([], registry)
            assert arguments["extra_observations"], "the registry declares extra_csv stores"
        else:
            assert "extra_observations" not in arguments
    if COMMANDS[name].function:
        _, function = run.script_function(name)
        signature = inspect.signature(function)
        signature.bind(**arguments)
        assert ("extra_observations" in signature.parameters) == ("--stores" in argv), name


def test_the_full_history_is_the_same_call_with_all_periods():
    """The Makefile's two passes over one script: --all-periods is the only
    difference besides the output file, and it must reach the function."""
    full = exporter_arguments("communes_history_full_csv", PipelinePaths())
    trimmed = exporter_arguments("communes_history_csv", PipelinePaths())
    assert full.pop("all_periods") is True
    assert "all_periods" not in trimmed
    assert full.pop("out_path").name == "communes_history_full.csv"
    assert trimmed.pop("out_path").name == "communes_history.csv"
    assert full == trimmed


def test_the_communes_table_reads_the_full_history_and_the_working_database(tmp_path):
    """Without db_path the table would silently lose its provenance and its
    French and Dutch names: the command line's --db must reach the call."""
    paths = PipelinePaths(out_root=str(tmp_path / "out"), working_db=str(tmp_path / "w.db"))
    arguments = exporter_arguments("communes_table_json", paths)
    assert arguments == {
        "csv_path": paths.output(COMMANDS["communes_history_full_csv"].outputs[0]),
        "out_path": paths.output(COMMANDS["communes_table_json"].outputs[0]),
        "db_path": paths.resolve(paths.working_db),
    }


def test_the_revisions_report_is_called_with_the_command_lines_db_and_today(tmp_path):
    paths = PipelinePaths(working_db=str(tmp_path / "w.db"))
    arguments = canonical.revisions_arguments(paths)
    assert arguments == {"db_path": paths.resolve(paths.working_db), "since": run.today()}
    assert arguments["db_path"].is_absolute()
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", arguments["since"])
    _, function = run.script_function("revisions_report")
    inspect.signature(function).bind(**arguments)


def test_the_revisions_report_refuses_a_command_line_it_cannot_translate(monkeypatch):
    argv = ("scripts/revisions_report.py", "--db", "{db}", "--since", "{today}", "--all")
    monkeypatch.setitem(COMMANDS, "revisions_report", Command(argv))
    with pytest.raises(ValueError, match="revisions_report"):
        canonical.revisions_arguments(PipelinePaths())


def test_the_revisions_report_reaches_the_run_log_and_the_metadata(sandbox, monkeypatch):
    report = "2 revision(s) since 2026-09-14:\n  GDP/be:country/2026: 1.0 -> 2.0\n  POP/be/2026: x"

    def reports(context, name, **given):
        sandbox.values[name] = given
        return [{}, {}], report

    monkeypatch.setattr(run, "call_function", reports)
    result = sandbox.defs.resolve_implicit_global_asset_job_def().execute_in_process(
        instance=sandbox.instance,
        raise_on_error=False,
        asset_selection=[dg.AssetKey("revisions_report")],
    )

    assert result.success
    assert sandbox.values["revisions_report"]["db_path"] == Path(sandbox.paths.working_db)
    metadata = _metadata(result, "revisions_report")
    assert metadata["revisions"].value == 2
    assert "GDP/be:country/2026: 1.0 -> 2.0" in metadata["report"].value
    logged = [e.user_message for e in sandbox.instance.all_logs(result.run_id)]
    assert all(line in logged for line in report.splitlines())


@pytest.mark.parametrize("name", ["staging_db", "site_payloads", "local_pages"])
def test_a_command_line_with_other_flags_is_refused_not_half_translated(name):
    with pytest.raises(ValueError, match=name):
        exporter_arguments(name, PipelinePaths())


@pytest.mark.parametrize(
    "tokens",
    [
        ("--db", "{db}", "--out", "a.csv", "--all-periods", "yes"),  # a switch with a value
        ("--db", "{db}", "--out"),  # a flag without its value
        ("--db", "{db}", "--out", "--all-periods"),  # a flag whose value is a flag
        ("--db", "{db}", "--out", "a.csv", "--out", "b.csv"),  # a flag given twice
        ("--db", "{db}", "--out", "a.csv", "--all-periods", "--all-periods"),
        ("--out", "a.csv"),  # no --db
        ("--db", "{db}", "--communes-history", "h.csv"),  # no --out
    ],
)
def test_a_malformed_exporter_command_line_is_refused(monkeypatch, tokens):
    monkeypatch.setitem(COMMANDS, "fake", Command(("scripts/fake.py", *tokens)))
    with pytest.raises(ValueError, match="fake"):
        exporter_arguments("fake", PipelinePaths())


# ── The coordinator: a red source still exports, and the day ends red ────────


def _sandbox_registry(root: Path) -> Path:
    """The repository's registry in miniature: its first extra_csv store as it
    is, plus an in_db CSV and an in_db directory, both under `root`."""
    (root / "international").mkdir(parents=True, exist_ok=True)
    (root / "walstat.csv").write_text(",".join(COLUMNS) + "\n", encoding="utf-8")
    extra = next(iter(extra_csv_stores(load_stores())))
    rows = {"script": "scripts/sync_walstat.py", "args": ["--reference-rows-only"]}
    stores = {
        extra.name: {
            "path": extra.raw_path,
            "source_id": extra.source_id,
            "mode": "extra_csv",
            "indicators": list(extra.indicators),
            "reference_rows": rows,
        },
        "walstat": {
            "path": str(root / "walstat.csv"),
            "source_id": "walstat",
            "mode": "in_db",
            "indicators": ["MUN_DEBT_TOTAL_PER_CAPITA"],
            "reference_rows": rows,
        },
        "international": {
            "path": str(root / "international"),
            "layout": "one_csv_per_indicator",
            "source_id": "eurostat",
            "mode": "in_db",
            "indicators": ["GDP_A", "HICP_A"],
            "reference_rows": rows,
        },
    }
    path = root / "stores.yaml"
    path.write_text(yaml.safe_dump({"stores": stores}, sort_keys=False), encoding="utf-8")
    return path


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    db = tmp_path / "working.db"
    migrate.run(db, migrations_dir=REPO / "migrations")
    paths = PipelinePaths(
        out_root=str(tmp_path / "out"),
        working_db=str(db),
        runs_dir=str(tmp_path / "runs"),
        committed_db=str(tmp_path / "committed" / "belgian_macro.db"),
        stores=str(_sandbox_registry(tmp_path / "stores")),
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

    def fake_call_function(context, name, **given):
        calls.append(name)
        values[name] = given
        if name in failing:
            raise RuntimeError(f"{name} is down")
        if name == "revisions_report":
            return [], "No revisions."
        return {} if name == "committed_stores" else 0

    monkeypatch.setattr(run, "run_script", fake_run_script)
    monkeypatch.setattr(run, "call_function", fake_call_function)
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
    assert state["offload"]["status"] == manifest.SUCCESS, "a red source still offloads"
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
    assert all(s["offload"]["status"] == manifest.SUCCESS for s in states)
    assert all(manifest.gate(s)["all_ok"] == "true" for s in states)


def test_a_failed_assemble_stops_before_any_export(sandbox):
    sandbox.failing.add("staging_db")

    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_FAILED

    (state,) = _manifests(sandbox.runs)
    assert state["assemble"]["status"] == manifest.FAILED
    assert state["validate_and_export"]["status"] == manifest.NOT_RUN
    assert state["offload"]["status"] == manifest.NOT_RUN
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
    assert state["offload"]["status"] == manifest.SUCCESS


def test_a_failed_export_skips_the_offload_in_the_same_run(sandbox):
    sandbox.failing.add("site_payloads")

    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_FAILED

    (state,) = _manifests(sandbox.runs)
    assert state["validate_and_export"]["status"] == manifest.FAILED
    assert "site_payloads is down" in state["validate_and_export"]["message"]
    assert state["offload"]["status"] == manifest.SKIPPED
    assert "committed_stores" not in sandbox.calls
    assert manifest.gate(state)["all_ok"] == "false"


def test_a_failed_offload_leaves_nothing_publishable(sandbox):
    sandbox.failing.add("committed_stores")

    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_FAILED

    (state,) = _manifests(sandbox.runs)
    assert manifest.red_sources(state) == []
    assert state["validate_and_export"]["status"] == manifest.SUCCESS
    assert state["offload"]["status"] == manifest.FAILED
    assert "committed_stores is down" in state["offload"]["message"]
    assert EXPORTS <= set(sandbox.calls)
    assert manifest.gate(state)["all_ok"] == "false"


def test_the_coordinator_allows_the_offload_for_its_own_run_and_runs_it_last(sandbox):
    assert daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance) == daily.EXIT_OK

    (state,) = _manifests(sandbox.runs)
    latest = sandbox.instance.get_latest_materialization_event(dg.AssetKey("committed_stores"))
    assert latest.asset_materialization.metadata["source_run"].value == state["run_id"]
    assert sandbox.calls[-1] == "committed_stores"


def test_without_fetch_runs_no_source_and_still_offloads(sandbox):
    exit_code = daily.run_daily(sandbox.defs, sandbox.paths, sandbox.instance, fetch=False)

    assert exit_code == daily.EXIT_PARTIAL
    (state,) = _manifests(sandbox.runs)
    assert all(state["sources"][n]["status"] == manifest.NOT_RUN for n in TRACKED)
    assert not set(TRACKED) & set(sandbox.calls)
    assert state["offload"]["status"] == manifest.SUCCESS
    assert manifest.gate(state)["all_ok"] == "false"


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
    assert state["offload"]["status"] == manifest.SKIPPED
    assert "committed_stores" not in sandbox.calls
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


# ── The offload: last, in the export run, and only for the coordinator ───────

RUN_ID = "20260914T050000Z-0a1b2c3d"


def _coordinator_manifest(sandbox, run_id: str = RUN_ID, assemble: str = manifest.SUCCESS) -> Path:
    path = manifest.path_for(sandbox.runs, run_id)
    state = manifest.initial(run_id)
    state["assemble"]["status"] = assemble
    manifest.write(path, state)
    return path


def _allow(path: Path | str, run_id: str = RUN_ID) -> dict:
    config = {
        "allow_committed_writes": True,
        "source_manifest": str(path),
        "coordinator_run_id": run_id,
    }
    return {"ops": {"committed_stores": {"config": config}}}


def _offload(sandbox, run_config: dict | None, paths: PipelinePaths | None = None):
    defs = build_defs(paths) if paths else sandbox.defs
    return defs.resolve_implicit_global_asset_job_def().execute_in_process(
        instance=sandbox.instance,
        raise_on_error=False,
        asset_selection=[dg.AssetKey("committed_stores")],
        run_config=run_config,
    )


def _errors(result, step: str) -> list:
    """The step's error and every error it was raised from."""
    (event,) = [e for e in result.get_step_failure_events() if e.step_key == step]
    error, chain = event.event_specific_data.error, []
    while error is not None:
        chain.append(error)
        error = error.cause
    return chain


def test_the_offload_refuses_without_the_coordinators_permission(sandbox):
    """Materialise from the UI or `dagster job execute`: no committed write."""
    result = _offload(sandbox, None)
    assert any("only by the coordinator" in e.message for e in _errors(result, "committed_stores"))
    assert "committed_stores" not in sandbox.calls


@pytest.mark.parametrize(
    "case", ["no run id", "missing manifest", "another run's manifest", "assemble not a success"]
)
def test_the_offload_refuses_a_permission_it_cannot_tie_to_a_good_run(sandbox, case):
    config = _allow(_coordinator_manifest(sandbox))["ops"]["committed_stores"]["config"]
    if case == "no run id":
        config["coordinator_run_id"] = ""
    elif case == "missing manifest":
        config["source_manifest"] = str(sandbox.runs / "gone" / "sources.json")
    elif case == "another run's manifest":
        config["coordinator_run_id"] = "20260101T000000Z-deadbeef"
    else:
        other = "20260914T060000Z-00000000"
        failed = _coordinator_manifest(sandbox, other, manifest.FAILED)
        config.update(source_manifest=str(failed), coordinator_run_id=other)

    result = _offload(sandbox, {"ops": {"committed_stores": {"config": config}}})

    assert "committed_stores" in result.get_failed_step_keys()
    assert "committed_stores" not in sandbox.calls


def test_the_offload_hands_offload_absolute_paths_and_names_its_run(sandbox):
    result = _offload(sandbox, _allow(_coordinator_manifest(sandbox)))

    assert result.success
    given = sandbox.values["committed_stores"]
    assert given == {
        "working_db": Path(sandbox.paths.working_db),
        "committed_db": Path(sandbox.paths.committed_db),
        "stores_path": Path(sandbox.paths.stores),
    }
    assert all(path.is_absolute() for path in given.values())
    _, offload = run.script_function("committed_stores")
    inspect.signature(offload).bind(**given)
    assert _metadata(result, "committed_stores")["source_run"].value == RUN_ID


@pytest.mark.parametrize("redirected", ["committed_db only", "stores only", "both"])
def test_a_redirected_run_never_offloads_into_the_repository(sandbox, redirected):
    """The registry's store paths resolve against the repository, so a redirected
    committed database with the default registry would still overwrite the
    repository's CSVs."""
    defaults = PipelinePaths()
    paths = PipelinePaths(
        out_root=sandbox.paths.out_root,
        working_db=sandbox.paths.working_db,
        runs_dir=sandbox.paths.runs_dir,
        committed_db=(
            defaults.committed_db if redirected == "stores only" else sandbox.paths.committed_db
        ),
        stores=defaults.stores if redirected == "committed_db only" else sandbox.paths.stores,
    )

    result = _offload(sandbox, _allow(_coordinator_manifest(sandbox)), paths)

    if redirected == "both":
        assert result.success and "committed_stores" in sandbox.calls
    else:
        messages = [e.message for e in _errors(result, "committed_stores")]
        assert any("must not write the committed files" in m for m in messages)
        assert "committed_stores" not in sandbox.calls


def test_a_refused_offload_says_nothing_was_changed(sandbox, monkeypatch):
    def refuses(context, name, **given):
        raise dg.Failure("committed_stores: offload_stores refused: a committed row would be lost")

    monkeypatch.setattr(run, "call_function", refuses)
    result = _offload(sandbox, _allow(_coordinator_manifest(sandbox)))

    messages = " ".join(e.message for e in _errors(result, "committed_stores"))
    assert "a committed row would be lost" in messages
    assert "No committed file was changed." in messages


def test_a_crash_in_the_offload_stays_that_crash(sandbox, monkeypatch):
    def crashes(context, name, **given):
        raise PermissionError("GDP_A.csv is open in another process")

    monkeypatch.setattr(run, "call_function", crashes)
    result = _offload(sandbox, _allow(_coordinator_manifest(sandbox)))

    assert "PermissionError" in {e.cls_name for e in _errors(result, "committed_stores")}
    logged = " ".join(e.user_message for e in sandbox.instance.all_logs(result.run_id))
    # Neutral: an error after a completed publish (the final print) must not be
    # reported as "nothing changed", nor as "everything was put back".
    assert "did not report a partial publication" in logged
    assert "git status" in logged


def test_a_partial_publication_names_only_the_files_left_new_and_the_backups(sandbox, monkeypatch):
    module, _ = run.script_function("committed_stores")
    stores = Path(sandbox.paths.stores).parent
    left_new = [stores / "international" / "GDP_A.csv"]
    backups = Path(sandbox.paths.working_db).parent / "offload_backup" / "20260914T050000Z-1"

    def partial(context, name, **given):
        try:
            raise OSError("disk full while publishing belgian_macro.db")
        except OSError as exc:
            raise module.PartialPublication([stores / "walstat.csv"], left_new, backups) from exc

    monkeypatch.setattr(run, "call_function", partial)
    result = _offload(sandbox, _allow(_coordinator_manifest(sandbox)))

    assert "PartialPublication" in {e.cls_name for e in _errors(result, "committed_stores")}
    logged = [e.user_message for e in sandbox.instance.all_logs(result.run_id)]
    (notice,) = [m for m in logged if m.startswith(canonical.PARTIAL_NOTICE_OPENING)]
    assert f"  {left_new[0]}\n" in notice
    assert "walstat.csv" not in notice, "only the files still new"
    assert str(backups) in notice


def test_the_partial_publication_notice_names_single_files_as_git_names_them(tmp_path):
    inside = [REPO / "data" / "belgian_macro.db", REPO / "data" / "international" / "GDP_A.csv"]
    outside = tmp_path / "walstat.csv"
    notice = canonical.partial_publication_notice(
        PipelinePaths(), [*inside, outside], tmp_path / "backup"
    )
    assert notice.startswith(canonical.PARTIAL_NOTICE_OPENING)
    assert "  data/belgian_macro.db\n" in notice
    assert "  data/international/GDP_A.csv\n" in notice
    assert f"  {outside}\n" in notice
    assert str(tmp_path / "backup") in notice
    assert notice.count("git checkout -- <file>") == 1
    assert "Never restore a whole directory" in notice


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
    state["offload"]["status"] = manifest.SUCCESS
    return state


def test_the_gate_opens_only_when_everything_succeeded():
    state = _green_manifest()
    outputs = manifest.gate(state)
    assert outputs["all_ok"] == "true"
    assert {k for k in outputs if k not in ("all_ok", "summary")} == CURRENT_GATE_IDS
    assert outputs["summary"] == ", ".join(f"{COMMANDS[n].workflow_step}=success" for n in TRACKED)

    for status in (manifest.FAILED, manifest.SKIPPED, manifest.NOT_RUN):
        state = _green_manifest()
        state["sources"]["walstat_observations"]["status"] = status
        outputs = manifest.gate(state)
        assert outputs["all_ok"] == "false", status
        assert outputs["sync_walstat"] == status

    for step in ("assemble", "validate_and_export", "offload"):
        state = _green_manifest()
        state[step]["status"] = manifest.FAILED
        assert manifest.gate(state)["all_ok"] == "false", step

    state = _green_manifest()
    del state["sources"]["market_data"]
    assert manifest.gate(state)["all_ok"] == "false", "a missing outcome is not a success"

    state = _green_manifest()
    del state["offload"]
    assert manifest.gate(state)["all_ok"] == "false", "no offload recorded, no merge"


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
