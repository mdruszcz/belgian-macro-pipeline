"""The alert half of the stale-data item.

The checks already existed and already printed ::warning:: annotations. What
did not exist was an ALERT: a daily run with warnings looked identical from the
outside to a clean one, and nobody opens the log of a build that passed.
Roadmap Block X: "Sources go quiet without announcing it. You want to know
before a client does."
"""

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

from validate_data import _write_summary  # noqa: E402

from src.validation.rules import FAIL, WARN, Violation  # noqa: E402


def test_a_clean_run_still_writes_a_line(tmp_path):
    """Silence would make "validated, nothing wrong" indistinguishable from
    "the validation step never ran" -- which is the same class of failure one
    level up, and the reason this file exists."""
    path = tmp_path / "summary.md"
    _write_summary(path, [], [])
    text = path.read_text(encoding="utf-8")
    assert "Data validation" in text
    assert "rules pass" in text
    assert "No failures, no warnings" in text


def test_failures_and_warnings_are_reported_separately(tmp_path):
    path = tmp_path / "summary.md"
    _write_summary(
        path,
        [Violation("row_collapse", FAIL, "POP dropped from 17000 to 436 rows")],
        [Violation("fetch_silence", WARN, "source 'onem' was last fetched 2026-07-01")],
    )
    text = path.read_text(encoding="utf-8")
    assert "1 failure(s), 1 warning(s)" in text
    assert "### Failures" in text and "### Warnings" in text
    assert "row_collapse" in text and "17000" in text
    assert "fetch_silence" in text and "onem" in text
    # Rendered as a table, so a reader sees which rule fired without parsing prose.
    assert "| Rule | Detail |" in text


def test_a_pipe_in_a_message_cannot_break_the_table(tmp_path):
    """A violation message is source-derived text. An unescaped pipe would
    silently split the row and hide the rest of the message -- the same class
    of bug as the unquoted comma that broke 287 CSV rows."""
    path = tmp_path / "summary.md"
    _write_summary(path, [], [Violation("unit_name_agreement", WARN, "a|b|c mismatch")])
    text = path.read_text(encoding="utf-8")
    row = next(line for line in text.splitlines() if "unit_name_agreement" in line)
    # Two structural pipes at the ends, plus one separating the columns.
    assert row.count("|") - row.count("\\|") == 3, row


def test_the_summary_is_appended_not_overwritten(tmp_path):
    """It is pointed at $GITHUB_STEP_SUMMARY, which other steps also write to.
    Truncating it would delete another step's report."""
    path = tmp_path / "summary.md"
    path.write_text("## Something else\n", encoding="utf-8")
    _write_summary(path, [], [])
    text = path.read_text(encoding="utf-8")
    assert text.startswith("## Something else")
    assert "Data validation" in text


def test_the_row_count_is_carried_through(tmp_path):
    path = tmp_path / "summary.md"
    _write_summary(path, [], [Violation("null_share", WARN, "X is empty", count=91)])
    assert "(91 rows)" in path.read_text(encoding="utf-8")


# --- end to end ------------------------------------------------------------


@pytest.mark.skipif(
    not (REPO / "data" / "belgian_macro.db").is_file(),
    reason="database not built in this working tree",
)
def test_the_cli_writes_a_summary_against_the_real_store(tmp_path):
    path = tmp_path / "summary.md"
    result = subprocess.run(
        [
            sys.executable,
            str(REPO / "scripts" / "validate_data.py"),
            "--db",
            str(REPO / "data" / "belgian_macro.db"),
            "--summary-file",
            str(path),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
    assert path.is_file(), "the CLI accepted --summary-file but wrote nothing"
    text = path.read_text(encoding="utf-8")
    assert "Data validation" in text
    # The three genuinely-behind national series are known and expected; what
    # matters is that they reach the summary rather than only the log.
    assert "staleness" in text


def test_the_workflow_writes_the_summary_even_when_validation_fails():
    """A `run:` block is bash -e, so a failing validation would abort the step
    before the summary was copied -- losing the report in exactly the case it
    matters most. Asserted on the workflow text because there is no cheap way
    to run Actions here."""
    workflow = (REPO / ".github" / "workflows" / "daily_fetch.yml").read_text(encoding="utf-8")
    step = workflow[workflow.index("Validate the data before exporting") :]
    step = step[: step.index("- name:", 10)]
    assert "set +e" in step, "a validation failure would abort before the summary is written"
    assert "STATUS=$?" in step
    assert "exit $STATUS" in step, "the real exit code must still propagate"
    assert 'cat "$SUMMARY" >> "$GITHUB_STEP_SUMMARY"' in step
    # And the daily PR body carries it, so it arrives as a notification rather
    # than only living on a page someone has to visit.
    assert "$validation_summary" in workflow
