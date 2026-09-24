"""The commit-size guard in both workflow files is back at its real 40 MB
threshold (PR: split-communes-history).

#244 temporarily raised it to 80 MB on 2026-09-23 when the new municipal
sources pushed data/communes_history.csv to ~62 MB. The fix (splitting that
file per source store, see scripts/export_communes_history_csv.py's module
docstring) is what removes the need for 80 MB -- this test is what stops the
temporary raise from quietly staying in place after the fix landed.
"""

from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

WORKFLOW_FILES = (
    REPO / ".github" / "workflows" / "daily_fetch.yml",
    REPO / ".github" / "workflows" / "manual_sources.yml",
)


def test_both_workflows_guard_at_40mb_not_80mb():
    for path in WORKFLOW_FILES:
        text = path.read_text(encoding="utf-8")
        assert "+40000k" in text, f"{path.name}: missing the restored +40000k guard"
        assert "+80000k" not in text, f"{path.name}: the temporary +80000k guard is still active"
        assert "exceed 40 MB" in text, f"{path.name}: error message does not say 40 MB"
        assert "exceed 80 MB" not in text, f"{path.name}: error message still says 80 MB"
