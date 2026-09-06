"""
The validation rule catalogue -- Block H, docs/features/validation.md.

Each rule is a small function that inspects the stores and returns
violations. They are registered by name so the runner, the tests and the
documentation cannot drift apart.

SEVERITY IS THE WHOLE DESIGN. The roadmap states the failure mode plainly:
"If every rule fails the build, you will disable validation within a week."
So `fail` is reserved for what is CERTAINLY wrong -- broken referential
integrity, a duplicate key, a value that cannot physically exist -- and
everything that is merely probably wrong, or legitimately wrong sometimes,
is a `warn`. A rule that would fire today on correct data must not be a
`fail`; that is not squeamishness, it is the only way this layer is still
switched on in six months.

Volume rules that need run-to-run history (row_collapse, staleness,
fetch_error) are deliberately NOT here: they need the count columns that
migration 003 adds, and live with the runner that persists them.
"""

import csv
import re
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

FAIL = "fail"
WARN = "warn"

# Period shapes per declared frequency (docs/features/data_model.md).
# 'F' (forecast) is deliberately absent: no forecast observation exists in the
# canonical table yet, and inventing a shape for one would be guessing.
_PERIOD_SHAPES = {
    "A": re.compile(r"^\d{4}$"),
    "Q": re.compile(r"^\d{4}-Q[1-4]$"),
    "M": re.compile(r"^\d{4}-\d{2}$"),
}

# Words in a display name that assert a currency amount.
_CURRENCY_MARKERS = ("meur", "eur", "euro", "€", "million euro")


@dataclass(frozen=True)
class Violation:
    rule: str
    severity: str
    message: str
    count: int = 1

    def __str__(self) -> str:
        suffix = f" ({self.count} rows)" if self.count > 1 else ""
        return f"[{self.severity}] {self.rule}: {self.message}{suffix}"


@dataclass
class Context:
    """Everything a rule may inspect. Assembled by the caller so rules stay
    testable against a temp database and a fixture directory."""

    conn: sqlite3.Connection
    derived_ids: frozenset[str] = frozenset()
    exports: tuple[Path, ...] = ()


RULES: dict[str, tuple[str, Callable[[Context], list[Violation]]]] = {}


def rule(name: str, severity: str):
    def register(func):
        RULES[name] = (severity, func)
        return func

    return register


def _count(ctx: Context, sql: str, params: Iterable = ()) -> int:
    return ctx.conn.execute(sql, tuple(params)).fetchone()[0]


# ── Referential: fail ───────────────────────────────────────────────────────
# Broken references mean numbers attached to nothing -- the automated form of
# the NIS-mismapping risk Block C's audit exists to catch.


@rule("fk_integrity", FAIL)
def fk_integrity(ctx: Context) -> list[Violation]:
    bad = ctx.conn.execute("PRAGMA foreign_key_check").fetchall()
    if not bad:
        return []
    detail = ", ".join(f"{row[0]} rowid={row[1]} -> {row[2]}" for row in bad[:5])
    return [Violation("fk_integrity", FAIL, f"foreign key violations: {detail}", len(bad))]


@rule("observation_has_indicator", FAIL)
def observation_has_indicator(ctx: Context) -> list[Violation]:
    n = _count(
        ctx,
        "SELECT COUNT(*) FROM observations o "
        "LEFT JOIN indicators i ON i.indicator_id = o.indicator_id "
        "WHERE i.indicator_id IS NULL",
    )
    return (
        [Violation("observation_has_indicator", FAIL, "observations with no indicator row", n)]
        if n
        else []
    )


@rule("observation_has_geography", FAIL)
def observation_has_geography(ctx: Context) -> list[Violation]:
    n = _count(
        ctx,
        "SELECT COUNT(*) FROM observations o "
        "LEFT JOIN geographies g ON g.geo_id = o.geo_id WHERE g.geo_id IS NULL",
    )
    return (
        [Violation("observation_has_geography", FAIL, "observations with no geography row", n)]
        if n
        else []
    )


@rule("indicator_has_source", FAIL)
def indicator_has_source(ctx: Context) -> list[Violation]:
    n = _count(
        ctx,
        "SELECT COUNT(*) FROM indicators i "
        "LEFT JOIN sources s ON s.source_id = i.source_id WHERE s.source_id IS NULL",
    )
    return (
        [Violation("indicator_has_source", FAIL, "indicators with no source row", n)] if n else []
    )


@rule("derived_not_stored", FAIL)
def derived_not_stored(ctx: Context) -> list[Violation]:
    """CONTROL G, enforced continuously rather than once. A derived value
    persisted as source data means a formula fix stops propagating and the
    database holds two contradicting truths."""
    if not ctx.derived_ids:
        return []
    stored = {r[0] for r in ctx.conn.execute("SELECT DISTINCT indicator_id FROM observations")}
    leaked = sorted(ctx.derived_ids & stored)
    if not leaked:
        return []
    return [
        Violation(
            "derived_not_stored",
            FAIL,
            f"derived indicators stored as observations: {leaked}",
            len(leaked),
        )
    ]


# ── Structural: fail ────────────────────────────────────────────────────────


@rule("unique_latest", FAIL)
def unique_latest(ctx: Context) -> list[Violation]:
    """Two current values for the same cell would silently double an average."""
    n = _count(
        ctx,
        "SELECT COUNT(*) FROM (SELECT 1 FROM observations WHERE is_latest = 1 "
        "GROUP BY indicator_id, geo_id, period HAVING COUNT(*) > 1)",
    )
    return (
        [Violation("unique_latest", FAIL, "keys with more than one is_latest row", n)] if n else []
    )


@rule("period_matches_frequency", FAIL)
def period_matches_frequency(ctx: Context) -> list[Violation]:
    violations = []
    rows = ctx.conn.execute(
        "SELECT o.indicator_id, i.frequency, o.period, COUNT(*) FROM observations o "
        "JOIN indicators i ON i.indicator_id = o.indicator_id GROUP BY 1, 2, 3"
    ).fetchall()
    offenders = [
        (ind, freq, period, n)
        for ind, freq, period, n in rows
        if freq in _PERIOD_SHAPES and not _PERIOD_SHAPES[freq].match(period)
    ]
    if offenders:
        sample = ", ".join(f"{i} ({f}) has {p!r}" for i, f, p, _ in offenders[:5])
        violations.append(
            Violation(
                "period_matches_frequency",
                FAIL,
                f"period does not match declared frequency: {sample}",
                sum(n for *_, n in offenders),
            )
        )
    return violations


@rule("export_parses", FAIL)
def export_parses(ctx: Context) -> list[Violation]:
    """Every published CSV must parse to a constant field count per row.

    This rule exists because of a real defect: adding indicator names
    containing commas to an exporter that wrote CSV by hand shifted every
    column after the name on 287 rows. The file was still valid UTF-8 with
    the right number of lines. Only parsing it catches that.
    """
    violations = []
    for path in ctx.exports:
        if not path.is_file():
            violations.append(
                Violation("export_parses", FAIL, f"published export missing: {path.name}")
            )
            continue
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.reader(fh))
        if not rows:
            violations.append(Violation("export_parses", FAIL, f"{path.name} is empty"))
            continue
        expected = len(rows[0])
        bad = [i for i, row in enumerate(rows, start=1) if len(row) != expected]
        if bad:
            violations.append(
                Violation(
                    "export_parses",
                    FAIL,
                    f"{path.name}: expected {expected} fields, first bad line {bad[0]}",
                    len(bad),
                )
            )
    return violations


# ── Range: fail ─────────────────────────────────────────────────────────────
# Catches the classic unit error -- a rate stored as 0.052 where its siblings
# use 5.2 -- which is otherwise invisible until a chart looks flat.


@rule("counts_non_negative", FAIL)
def counts_non_negative(ctx: Context) -> list[Violation]:
    n = _count(
        ctx,
        "SELECT COUNT(*) FROM observations o JOIN indicators i "
        "ON i.indicator_id = o.indicator_id WHERE i.unit = 'count' AND o.value < 0",
    )
    return [Violation("counts_non_negative", FAIL, "negative count values", n)] if n else []


@rule("percent_bounded", FAIL)
def percent_bounded(ctx: Context) -> list[Violation]:
    n = _count(
        ctx,
        "SELECT COUNT(*) FROM observations o JOIN indicators i "
        "ON i.indicator_id = o.indicator_id "
        "WHERE i.unit LIKE 'percent%' AND (o.value < -100 OR o.value > 100)",
    )
    return [Violation("percent_bounded", FAIL, "percentages outside -100..100", n)] if n else []


@rule("suppressed_has_no_value", FAIL)
def suppressed_has_no_value(ctx: Context) -> list[Violation]:
    """A suppressed cell must carry NULL, never a number -- above all never 0.

    The schema already enforces the other direction (001_core_schema.sql:86,
    `value IS NOT NULL OR status IN ('suppressed','na')`), so a rule checking
    for stray nulls would duplicate a database constraint and could never
    fire. This is the unguarded direction, and it is the one the data model
    spec singles out: "Statbel suppresses small-cell values. If suppression
    looks like zero, you will publish 'median income EUR 0' for a small
    commune." A zero there is not a missing number, it is a wrong one.
    """
    n = _count(
        ctx,
        "SELECT COUNT(*) FROM observations "
        "WHERE status IN ('suppressed','na') AND value IS NOT NULL",
    )
    return (
        [
            Violation(
                "suppressed_has_no_value",
                FAIL,
                "suppressed/na observations carrying a value instead of NULL",
                n,
            )
        ]
        if n
        else []
    )


# ── Labelling: warn ─────────────────────────────────────────────────────────


@rule("unit_name_agreement", WARN)
def unit_name_agreement(ctx: Context) -> list[Violation]:
    """A display name must not contradict the unit.

    Added because a real defect was invisible to every other rule here: an
    indicator publishing a 2010-based index under a name asserting millions
    of euro. Not a range error (the values were plausible), not structural,
    not referential -- 91 rows reached the published CSV.

    Checks the display NAME, not the indicator id. Six ids still contain
    "MEUR" while holding an index; renaming them is a primary-key migration,
    not a label fix, so flagging them here would make this rule permanently
    noisy for something deliberately deferred.
    """
    violations = []
    for ind, unit, name in ctx.conn.execute("SELECT indicator_id, unit, name_en FROM indicators"):
        lowered = (name or "").lower()
        has_currency = any(marker in lowered for marker in _CURRENCY_MARKERS)
        if (unit or "").startswith("index") and has_currency:
            violations.append(
                Violation(
                    "unit_name_agreement",
                    WARN,
                    f"{ind}: unit is {unit!r} but the name claims a currency amount ({name!r})",
                )
            )
    return violations


@rule("has_trilingual_name", WARN)
def has_trilingual_name(ctx: Context) -> list[Violation]:
    """Every indicator needs a real label in all three languages.

    Would have caught the twelve indicators found in the Block F review whose
    name.en/fr/nl were all set to the raw indicator id -- invisible in an
    English test pass and fatal in a Belgian sales meeting.
    """
    violations = []
    for ind, nl, fr, en in ctx.conn.execute(
        "SELECT indicator_id, name_nl, name_fr, name_en FROM indicators"
    ):
        missing = [lang for lang, v in (("nl", nl), ("fr", fr), ("en", en)) if not v]
        placeholder = [lang for lang, v in (("nl", nl), ("fr", fr), ("en", en)) if v == ind]
        if missing:
            violations.append(
                Violation("has_trilingual_name", WARN, f"{ind}: missing name in {missing}")
            )
        elif placeholder:
            violations.append(
                Violation(
                    "has_trilingual_name",
                    WARN,
                    f"{ind}: name in {placeholder} is just the raw indicator id",
                )
            )
    return violations


# ── Runner ──────────────────────────────────────────────────────────────────


def run_all(ctx: Context, only: Iterable[str] | None = None) -> list[Violation]:
    """Every registered rule, in a stable order. Returns violations rather
    than raising, so the caller decides what blocks -- the runner prints them
    all instead of stopping at the first, because fixing five problems one
    build at a time is how people give up on validation."""
    names = sorted(RULES) if only is None else sorted(only)
    found: list[Violation] = []
    for name in names:
        _severity, func = RULES[name]
        found.extend(func(ctx))
    return found


def has_failures(violations: Iterable[Violation]) -> bool:
    return any(v.severity == FAIL for v in violations)
