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

The volume rules read the run-to-run history in `indicator_volume`
(migration 003). Writing that history is deliberately NOT a rule: it is
`record_volume_snapshot`, called by the runner only after a validation pass
succeeds, so a collapsed count never becomes the new normal.
"""

import csv
import re
import sqlite3
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
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

# Default staleness allowance per declared frequency, in days: THREE
# publication intervals, not the two the spec first proposed. Two was
# measured against the real store and fires on correct data --
# BUSINESS_CONFIDENCE sits 68 days past the end of 2026-06 on the day this
# was written, which is a normal publication lag, not a fault. Three leaves
# exactly the indicators that are genuinely behind (HICP and EC_CONS_CONF_BE
# stuck on 2025-12, EUROSTAT_GDP_Q_MEUR on 2025-Q3) and nothing else.
_STALENESS_DEFAULT_DAYS = {"A": 1095, "Q": 270, "M": 93, "D": 7}

# A drop this large in an indicator's is_latest count is treated as a
# regression rather than a legitimate revision. CONTROL H's scenario
# ("17,000 rows yesterday, 436 today") is a 97% drop.
_COLLAPSE_FRACTION = 0.10

# A source or indicator code whose last log entry predates the newest entry in
# that log by more than this is treated as RETIRED, not failing. Measured
# case: BE_CONSUMER_CONFIDENCE and EU_CONSUMER_CONFIDENCE were tried three
# times each on 2026-03-01, failed, and were renamed to EC_CONS_CONF_BE/_EU
# the same hour. Their final entry is an ERROR that will sit at the top of
# their history forever. Without this window they red-light every build from
# now until the log is truncated -- a permanent red light nobody reads is the
# same as no rule at all.
_FETCH_LOG_ACTIVE_DAYS = 7

# An indicator whose current rows are this overwhelmingly empty has been
# fetched into an empty column, not published sparsely. The minimum row count
# exists because 1-of-1 null is 100% and means nothing.
_NULL_SHARE_FRACTION = 0.90
_NULL_SHARE_MIN_ROWS = 10


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
    # Per-indicator staleness allowances from config (`max_age_days`).
    # Absent an entry, the frequency default above applies.
    max_age_days: Mapping[str, int] = field(default_factory=dict)
    # Per-source fetch windows from config (`fetch_window_days`). A source
    # absent from this map is not checked for silence at all -- see the
    # fetch_silence rule for why that has to be opt-in.
    fetch_window_days: Mapping[str, int] = field(default_factory=dict)
    # Injected so staleness is testable without freezing the clock globally.
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


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


# ── Volume: fail, except staleness ──────────────────────────────────────────
# Volume is measured on the STORE, not the run. fetch_runs.rows_written was
# tried first and is useless for this: nbb writes 0 rows on a normal day
# (insert-only-on-change working correctly), so a "count dropped" rule fires
# constantly and a "count is zero" rule fires daily; statbel's figure swings
# 565 -> 19149 because one number mixes datasets loaded by different scripts.
# The count of is_latest = 1 rows per indicator is stable by comparison
# (LOCAL_UNITS_BY_COMMUNE 565, EC_CONS_CONF_BE 216, CONSUMER_CONFIDENCE 200),
# so a drop in it is genuinely alarming.


def _current_counts(ctx: Context) -> dict[str, int]:
    return dict(
        ctx.conn.execute(
            "SELECT indicator_id, COUNT(*) FROM observations WHERE is_latest = 1 GROUP BY 1"
        )
    )


def _last_snapshot(ctx: Context) -> dict[str, int]:
    """The most recent recorded count per indicator, or {} before any run."""
    if not _has_volume_table(ctx.conn):
        return {}
    return dict(
        ctx.conn.execute(
            "SELECT v.indicator_id, v.new_count FROM indicator_volume v "
            "WHERE v.snapshot_id = (SELECT MAX(v2.snapshot_id) FROM indicator_volume v2 "
            "                       WHERE v2.indicator_id = v.indicator_id)"
        )
    )


def _has_volume_table(conn: sqlite3.Connection) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='indicator_volume'"
        ).fetchone()
    )


@rule("row_collapse", FAIL)
def row_collapse(ctx: Context) -> list[Violation]:
    """CONTROL H's scenario: yesterday 17,000 rows, today 436.

    A fail rather than a warn even though it is a volume rule, because it is
    the single failure mode most likely to publish garbage silently -- the
    site still renders, every remaining number is correct, and most communes
    have simply vanished.

    Indicators that went to zero are left to `indicator_disappeared` so the
    same event is not reported twice.
    """
    current = _current_counts(ctx)
    previous = _last_snapshot(ctx)
    offenders = []
    for ind, prev in sorted(previous.items()):
        now = current.get(ind, 0)
        if prev <= 0 or now == 0:
            continue
        if now < prev * (1 - _COLLAPSE_FRACTION):
            offenders.append(f"{ind}: {prev} -> {now}")
    if not offenders:
        return []
    return [
        Violation(
            "row_collapse",
            FAIL,
            f"is_latest row count fell more than {int(_COLLAPSE_FRACTION * 100)}%: "
            + "; ".join(offenders),
            len(offenders),
        )
    ]


@rule("indicator_disappeared", FAIL)
def indicator_disappeared(ctx: Context) -> list[Violation]:
    """An indicator that had observations now has none.

    Separate from row_collapse because it is qualitatively different: a
    dashboard row does not shrink, it silently stops existing.
    """
    current = _current_counts(ctx)
    previous = _last_snapshot(ctx)
    gone = sorted(ind for ind, prev in previous.items() if prev > 0 and current.get(ind, 0) == 0)
    if not gone:
        return []
    return [
        Violation(
            "indicator_disappeared",
            FAIL,
            f"indicators that had observations and now have none: {gone}",
            len(gone),
        )
    ]


@rule("null_share", FAIL)
def null_share(ctx: Context) -> list[Violation]:
    """More than 90% of an indicator's current rows carrying no value.

    The signature of a source that answered with the right shape and none of
    the content -- a renamed value column, a filter that matched nothing.
    Every row is individually legal (the schema allows NULL where status is
    suppressed/na), so nothing else here would notice.

    Guarded by a minimum row count: 1 of 1 rows null is 100% and means
    nothing, and failing the build on it would be exactly the kind of noise
    that gets this layer switched off. There are zero null values in the
    store today, so this rule cannot fire on correct data.
    """
    rows = ctx.conn.execute(
        "SELECT indicator_id, COUNT(*), SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) "
        "FROM observations WHERE is_latest = 1 GROUP BY 1"
    ).fetchall()
    offenders = [
        f"{ind}: {nulls}/{total} null"
        for ind, total, nulls in rows
        if total >= _NULL_SHARE_MIN_ROWS and nulls > total * _NULL_SHARE_FRACTION
    ]
    if not offenders:
        return []
    return [
        Violation(
            "null_share",
            FAIL,
            f"more than {int(_NULL_SHARE_FRACTION * 100)}% of current rows have no value: "
            + "; ".join(sorted(offenders)),
            len(offenders),
        )
    ]


@rule("staleness", WARN)
def staleness(ctx: Context) -> list[Violation]:
    """Latest period older than the indicator's allowance.

    A warn, never a fail, and the reason is concrete: LOCAL_UNITS_BY_COMMUNE
    is 1,010 days old and CORRECT -- Statbel's standard view is pinned to
    2023-Q4, verified against the live API. A rule that fails the build every
    day on correct data is the exact thing that gets validation switched off.
    That indicator carries an explicit `max_age_days` in its config recording
    why it is frozen, so the warning means something when it changes.
    """
    violations = []
    today = ctx.now.date()
    rows = ctx.conn.execute(
        "SELECT o.indicator_id, i.frequency, MAX(o.period) FROM observations o "
        "JOIN indicators i ON i.indicator_id = o.indicator_id "
        "WHERE o.is_latest = 1 GROUP BY 1, 2"
    ).fetchall()
    for ind, freq, period in rows:
        ends = _period_end(period, freq)
        if ends is None:
            continue
        age = (today - ends).days
        allowance = ctx.max_age_days.get(ind, _STALENESS_DEFAULT_DAYS.get(freq))
        if allowance is None or age <= allowance:
            continue
        violations.append(
            Violation(
                "staleness",
                WARN,
                f"{ind}: latest period {period} ended {age} days ago, "
                f"allowance is {allowance} days",
            )
        )
    return violations


def _period_end(period: str, frequency: str) -> date | None:
    """Last calendar day of a period string, or None if it is not a shape we
    recognise. Age is measured from the end of the period, not its start:
    a monthly series publishing 2026-06 is not 'six months late' on July 1st.
    """
    try:
        if frequency == "A" and _PERIOD_SHAPES["A"].match(period):
            return date(int(period), 12, 31)
        if frequency == "Q" and _PERIOD_SHAPES["Q"].match(period):
            year, quarter = int(period[:4]), int(period[-1])
            return _month_end(year, quarter * 3)
        if frequency == "M" and _PERIOD_SHAPES["M"].match(period):
            return _month_end(int(period[:4]), int(period[5:7]))
    except ValueError:
        return None
    return None


def _month_end(year: int, month: int) -> date:
    return date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)


@rule("fetch_error", FAIL)
def fetch_error(ctx: Context) -> list[Violation]:
    """Read BOTH fetch logs, because one of them lies by omission.

    fetch_runs holds 162 rows, every one 'ok'. legacy_fetch_log holds 83
    ERROR rows over the same history -- DBnomics read timeouts. The canonical
    table only covers adapters refactored in Block D; belgian_macro_db.py
    still logs the legacy path to its own table. A validation layer reading
    only fetch_runs would report all-clear on a day when five indicators
    failed to fetch. It did: those timeouts are why five country-variant
    indicators have configs and zero observations.

    Only the MOST RECENT entry per source/indicator counts, and only if that
    entry is recent in absolute terms too. The 83 historical errors are
    history; and a code that was tried, failed and abandoned six months ago
    is retired, not failing. Failing on either forever would make this a
    permanent red light nobody reads.
    """
    violations = []
    runs = ctx.conn.execute(
        "SELECT source_id, status, started_at FROM fetch_runs f "
        "WHERE f.fetch_run_id = (SELECT MAX(f2.fetch_run_id) FROM fetch_runs f2 "
        "                        WHERE f2.source_id = f.source_id) ORDER BY source_id"
    ).fetchall()
    for source_id, status, started_at in _still_active(runs):
        if status in ("error", "partial", "schema_changed"):
            violations.append(
                Violation(
                    "fetch_error",
                    FAIL,
                    f"fetch_runs: latest run for source {source_id!r} is {status!r} "
                    f"({started_at})",
                )
            )

    if _has_legacy_log(ctx.conn):
        legacy = ctx.conn.execute(
            "SELECT indicator_code, status, fetched_at FROM legacy_fetch_log l "
            "WHERE l.id = (SELECT MAX(l2.id) FROM legacy_fetch_log l2 "
            "              WHERE l2.indicator_code = l.indicator_code) "
            "ORDER BY indicator_code"
        ).fetchall()
        for code, status, fetched_at in _still_active(legacy):
            if (status or "").upper() != "OK":
                violations.append(
                    Violation(
                        "fetch_error",
                        FAIL,
                        f"legacy_fetch_log: latest fetch of {code} is {status!r} ({fetched_at})",
                    )
                )
    return violations


@rule("fetch_silence", WARN)
def fetch_silence(ctx: Context) -> list[Violation]:
    """A source that has stopped being fetched at all, while the rest carry on.

    THE BLIND SPOT THIS FILLS. fetch_error checks the STATUS of each source's
    most recent run, and _still_active deliberately DROPS any source whose last
    entry is far behind the newest entry in the log -- correctly, so a retired
    code does not red-light every build forever. The consequence is that a
    source going quiet is the one failure mode actively filtered out of the
    checks: its last run says 'ok', it is excluded as retired, and nothing
    reports it. The pipeline looks green while one source silently stops
    arriving. Roadmap Block X: "Sources go quiet without announcing it. You
    want to know before a client does."

    MEASURED AGAINST THE NEWEST RUN IN THE LOG, NOT THE WALL CLOCK. The
    database is a committed store, so a clone opened three months from now has
    a fetch log three months old -- against wall-clock every source would warn,
    which is the permanent red light this module keeps refusing to build. Read
    relative to the newest run, the question becomes the one that actually
    matters: did THIS source fall behind while the others were fetched? A
    whole workflow that stops running is visible in GitHub Actions itself.

    OPT-IN PER SOURCE, via `fetch_window_days` in config/sources/*.yaml.
    Sources whose files are hand-downloaded (police) have no fetch_runs rows at
    all and would warn forever; their data freshness is the staleness rule's
    job, not this one. Declaring a window is an editorial statement that CI is
    expected to fetch this source, exactly as `max_age_days` is an editorial
    statement about a source's publication behaviour.
    """
    if not ctx.fetch_window_days:
        return []

    rows = ctx.conn.execute(
        "SELECT source_id, MAX(started_at) FROM fetch_runs GROUP BY source_id"
    ).fetchall()
    last_run = {}
    for source_id, when in rows:
        parsed = _parse_ts(when)
        if parsed is not None:
            last_run[source_id] = parsed

    violations = []

    # A source that declares a window and has NEVER been fetched is a
    # different sentence, and a more alarming one: the adapter has never run.
    for source_id in sorted(ctx.fetch_window_days):
        if source_id not in last_run:
            violations.append(
                Violation(
                    "fetch_silence",
                    WARN,
                    f"source {source_id!r} declares fetch_window_days but has no fetch_runs "
                    "entry at all -- it has never been fetched successfully",
                )
            )

    if not last_run:
        return violations

    newest = max(last_run.values())
    for source_id, when in sorted(last_run.items()):
        allowance = ctx.fetch_window_days.get(source_id)
        if allowance is None:
            continue
        behind = (newest - when).days
        if behind > allowance:
            violations.append(
                Violation(
                    "fetch_silence",
                    WARN,
                    f"source {source_id!r} was last fetched {when.date()}, {behind} days "
                    f"behind the newest run in the log ({newest.date()}); its window is "
                    f"{allowance} days. It has gone quiet while other sources kept arriving",
                )
            )
    return violations


def _still_active(rows: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    """Drop keys whose last log entry is far older than the newest entry in
    the same log -- see _FETCH_LOG_ACTIVE_DAYS. Timestamps are compared as
    parsed datetimes rather than strings, because a log written with mixed
    offsets would sort wrong lexically."""
    stamps = {}
    for key, _status, when in rows:
        parsed = _parse_ts(when)
        if parsed is not None:
            stamps[key] = parsed
    if not stamps:
        return list(rows)
    newest = max(stamps.values())
    cutoff = newest - timedelta(days=_FETCH_LOG_ACTIVE_DAYS)
    return [row for row in rows if stamps.get(row[0]) is None or stamps[row[0]] >= cutoff]


def _parse_ts(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _has_legacy_log(conn: sqlite3.Connection) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='legacy_fetch_log'"
        ).fetchone()
    )


def record_volume_snapshot(conn: sqlite3.Connection, now: datetime | None = None) -> int:
    """Record today's is_latest count per indicator, with the delta against
    the previous snapshot.

    Deliberately NOT a rule, and deliberately called only after validation
    passes: recording a collapsed count would make it the baseline, and the
    alarm would silence itself on the very next run.
    """
    if not _has_volume_table(conn):
        raise RuntimeError(
            "indicator_volume table missing -- run migrations (003_volume_history.sql) first"
        )
    taken_at = (now or datetime.now(timezone.utc)).isoformat()
    ctx = Context(conn=conn)
    current = _current_counts(ctx)
    previous = _last_snapshot(ctx)
    rows = [
        (
            taken_at,
            ind,
            previous.get(ind),
            count,
            None if previous.get(ind) is None else count - previous[ind],
        )
        for ind, count in sorted(current.items())
    ]
    conn.executemany(
        "INSERT INTO indicator_volume "
        "(taken_at, indicator_id, previous_count, new_count, delta) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


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
