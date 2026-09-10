"""Resolve a page document's bindings to real published figures.

Batch 14. This module is a READER, not a calculator, and that is the whole
design. Every number a binding can ask for has already been computed by the
analytics and written into `public/data/`: a commune payload carries each
indicator's periods, its province/region/country comparison with coverage, and
its national/regional percentile with rank and peer count; an indicator payload
carries all 565 communes. So resolution is a lookup.

That is not a shortcut, it is the rule. claude.md rule 20: a block consumes
approved generated payloads, never SQLite and never a raw source. Rule 27: a
block never averages a ratio across geographies. If this module ever needs to
compute an aggregate, the answer is that the exporter should publish it, not
that the browser should work it out.

WHAT IT MUST NEVER DO, and a test asserts each one by scanning this file:
open a database, import `src/fetchers/`, or import `src/analytics/` to compute.
The trust boundary is the same one `semantics.py`'s docstring draws.

The five states are the job (rule 26). Missing, unavailable, suppressed and an
explicit zero are four different answers and this module must never collapse
them -- a withheld figure is not a blank and not a zero, and this repository
has already shipped that bug once (docs/steps, "Withheld figures are visible as
withheld").
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path

from src.pages.schema import PageDocumentError

REPO_ROOT = Path(__file__).resolve().parents[2]

PAYLOAD_ROOT = Path("public") / "data"

#: A NIS code and an indicator code, re-checked HERE and not merely relied on
#: from the schema. The schema does constrain both -- but resolution also runs
#: on documents that were never validated, because POST /api/preview renders an
#: invalid document on purpose. A guard that only holds when the validator ran
#: first is not a guard on the path where it is actually needed.
#:
#: Without this, `Path("communes") / f"{nis}.json"` with an ABSOLUTE nis throws
#: the prefix away entirely (pathlib does that silently), and "../manifest"
#: climbs out of the directory. Both were demonstrated, not theorised.
_NIS_RE = re.compile(r"\A[0-9]{5}\Z")
_INDICATOR_RE = re.compile(r"\A[A-Z][A-Z0-9_]{0,63}\Z")

#: Operations this batch answers, each from a payload that already holds the
#: result. `aggregate` is deliberately absent -- see `_AGGREGATE_REFUSAL`.
RESOLVABLE_OPERATIONS = frozenset(
    {"latest", "history", "comparison", "percentile", "map_values", "table"}
)

#: Said out loud rather than silently returning nothing. A block that shows
#: no reason is indistinguishable from a broken one.
_AGGREGATE_REFUSAL = (
    "A {level}-level figure is not available to a block on its own yet. The "
    "aggregate exists, but it is published only inside each commune's "
    "comparison, not as a payload a block can read directly."
)

_SUPPRESSED_MESSAGE = (
    "The source holds this figure and does not publish it, because the count "
    "is too small to release."
)

_NO_CONTEXT = (
    "This block reads the commune the page is about, and this page does not " "declare one."
)


class PayloadError(PageDocumentError):
    """A payload this resolver needs is missing or malformed."""


class PayloadReader:
    """Reads published payloads, caching each file once per instance.

    Injected rather than reached for, the same way `load_metadata(root)` is, so
    a test can point at a fixture tree -- and so this module has no global
    handle on the repository.
    """

    def __init__(self, root: Path | str | None = None):
        self._base = (Path(root) if root is not None else REPO_ROOT) / PAYLOAD_ROOT
        self._cache: dict[Path, dict | None] = {}

    def _read(self, relative: Path) -> dict | None:
        if relative in self._cache:
            return self._cache[relative]
        path = self._base / relative
        # Belt and braces: even with both names regex-checked above, confirm
        # the resolved path is still inside the payload directory. This is what
        # catches a symlink planted under public/data/, which no amount of
        # name-checking can see.
        try:
            resolved = path.resolve()
            base = self._base.resolve()
        except OSError as exc:
            raise PayloadError(f"cannot resolve payload {relative.as_posix()}") from exc
        if not resolved.is_relative_to(base):
            raise PayloadError(f"payload {relative.as_posix()} resolves outside public/data")

        loaded: dict | None
        try:
            loaded = json.loads(resolved.read_text(encoding="utf-8"))
        except FileNotFoundError:
            loaded = None
        except (OSError, ValueError) as exc:
            # The message names the payload, never the exception: `exc` carries
            # the absolute path on this machine, and service.py puts this string
            # into the data a browser is handed. Nothing about this machine
            # reaches the client (service.py's own rule).
            raise PayloadError(f"payload {relative.as_posix()} could not be read") from exc
        self._cache[relative] = loaded
        return loaded

    def commune(self, nis) -> dict | None:
        if not isinstance(nis, str) or not _NIS_RE.match(nis):
            raise PayloadError("a NIS code must be five digits")
        return self._read(Path("communes") / f"{nis}.json")

    def indicator(self, code) -> dict | None:
        if not isinstance(code, str) or not _INDICATOR_RE.match(code):
            raise PayloadError("an indicator code must be A-Z, digits and underscores")
        return self._read(Path("indicators") / f"{code}.json")

    def national(self) -> dict | None:
        return self._read(Path("national.json"))


def _state(state: str, message: str = "") -> dict:
    out: dict = {"state": state}
    if message:
        out["message"] = message
    return out


#: How each language writes a thousands separator. Substituted rather than
#: taken from Python's `locale`, which is process-global and depends on which
#: locales the host happens to have generated -- the same reason the exporters
#: do it this way.
_GROUPING = {"en": ",", "fr": "\u202f", "nl": "."}


def _format_value(value, meta: Mapping, lang: str = "en") -> str:
    """A figure written the way the PUBLISHED SITE writes it.

    Deliberately mirrors `assets/commune_map.js`'s formatValue, because the
    claim of this batch is that a block shows the same number the live site
    shows -- and a number that differs in magnitude or loses its unit is not
    the same number. An earlier version matched only unit "EUR" and "%", so a
    percentage came out as a bare integer: an 18.16% unemployment rate rendered
    as "18", which a reader can only misread.

    `decimals: null` means "two digits below a thousand, none above", which is
    what the site does and what 13 indicators rely on -- not zero.
    """
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)

    decimals = meta.get("decimals")
    if not isinstance(decimals, int) or isinstance(decimals, bool):
        decimals = 0 if abs(number) >= 1000 else 2

    rendered = f"{number:,.{decimals}f}"
    separator = _GROUPING.get(lang, ",")
    if separator != ",":
        # Swap via a placeholder so the decimal point is not caught by the
        # thousands replacement on its way past.
        rendered = rendered.replace(",", "\x00").replace(".", ",").replace("\x00", separator)

    unit = (meta.get("unit") or "").lower()
    if unit == "eur":
        return f"\u20ac{rendered}"
    if unit.startswith("percent"):
        return f"{rendered}%"
    return rendered


def _provenance(meta: Mapping) -> str:
    """Source and date, per rule 28 and the Statbel licence obligation.

    A DERIVED figure carries its INPUTS' date, attributed to the inputs, and
    never its own -- borrowing an input's date as the figure's own is exactly
    what point 5 of that licence forbids.
    """
    source = meta.get("source") or ""
    if meta.get("derived_from"):
        when = meta.get("inputs_updated")
        return f"derived, computed from figures last updated {when}" if when else "derived"
    updated = meta.get("updated")
    if source and updated:
        return f"{source} — retrieved {updated}"
    return source or ""


def _sorted_periods(periods: Mapping) -> list[str]:
    """Period labels ascending. They sort lexically by construction -- that is
    why data_model.md requires zero-padded YYYY-Qn / YYYY-MM."""
    return sorted(periods)


def _cell_state(entry: Mapping, period: str) -> tuple[str, object]:
    """(state, value) for one period of one indicator.

    `status: suppressed` means the source HOLDS this figure and will not
    publish it. That is not absence and not zero.
    """
    cell = entry.get("periods", {}).get(period)
    if cell is None:
        return "missing", None
    if cell.get("status") == "suppressed":
        return "suppressed", None
    value = cell.get("value")
    if value is None:
        return "missing", None
    return "ready", value


def _label(value, lang: str) -> str:
    """A payload label as text. Geography names in the payloads are trilingual
    objects; rendering one raw would put a dict on the page."""
    if isinstance(value, Mapping):
        return str(value.get(lang) or value.get("en") or next(iter(value.values()), ""))
    return "" if value is None else str(value)


def resolve_document(
    doc: Mapping,
    *,
    metadata,
    lang: str = "en",
    reader: PayloadReader | None = None,
    nis: str | None = None,
) -> dict:
    """`{block_id: payload}` for every block carrying a binding.

    A block WITHOUT a binding gets no entry at all -- `render.py`'s
    `_state_for` already renders those as `ready`, and inventing an entry for
    them would say something about data where there is none to say.

    `nis` RENDERS A TEMPLATED DOCUMENT FOR ONE COMMUNE. A commune-profile
    document declares `context.nis` as the literal `{nis}` placeholder and is
    rendered once per commune; the subject came only from the document, so
    there was no way to say which commune this pass is for without editing the
    document 565 times. Passing it wins over the placeholder and over nothing
    at all, and a document that names a CONCRETE commune still wins over this
    -- a page pinned to Namur stays pinned to Namur however it is rendered.
    """
    reader = reader or PayloadReader()
    out: dict[str, dict] = {}
    declared = (doc.get("context") or {}).get("nis")
    context_nis = declared if declared and declared != "{nis}" else nis
    for section in doc.get("sections") or []:
        for block in section.get("blocks") or []:
            binding = block.get("binding")
            if not isinstance(binding, Mapping):
                continue
            block_id = block.get("id")
            if isinstance(block_id, str):
                out[block_id] = _resolve_binding(binding, context_nis, metadata, lang, reader)
    return out


def _resolve_binding(
    binding: Mapping, context_nis, metadata, lang: str, reader: PayloadReader
) -> dict:
    operation = binding.get("operation")
    code = binding.get("indicator")
    if code is not None and not isinstance(code, str):
        # Reached only through /api/preview, which renders unvalidated
        # documents by design. Without this the dict lands in a Mapping.get()
        # and raises TypeError: unhashable, which escapes as an opaque 500 on
        # the one route whose whole job is to render malformed input.
        return _state("error", "the indicator on this block is not a code")

    provider = binding.get("provider")
    geography = binding.get("geography") or {"mode": "context"}
    level = geography.get("level")

    if operation == "aggregate" or geography.get("mode") == "level":
        # `country` is NOT in this refusal: national.json publishes it, and
        # `geography: {mode: level, level: country}` is the schema's own shape
        # for a national binding. Refusing it would deny a figure that exists.
        if level != "country":
            return _state("unavailable", _AGGREGATE_REFUSAL.format(level=level or "that"))

    if operation not in RESOLVABLE_OPERATIONS:
        return _state("unavailable", f"the {operation!r} operation is not available yet")

    meta = metadata.municipal_indicators.get(code) or metadata.national_indicators.get(code) or {}

    if provider == "national" or level == "country":
        return _resolve_national(code, meta, binding, lang, reader)

    if operation in ("map_values", "table"):
        return _resolve_across_communes(code, meta, lang, reader)

    nis = _subject_nis(binding, context_nis)
    if nis is None:
        return _state("unavailable", _NO_CONTEXT)
    payload = reader.commune(nis)
    if payload is None:
        return _state("missing", f"no published payload for commune {nis}")
    entry = (payload.get("indicators") or {}).get(code)
    if entry is None:
        return _state("missing", f"{code} is not published for this commune")

    if operation == "latest":
        return _resolve_latest(binding, entry, meta, lang)
    if operation == "history":
        return _resolve_history(binding, entry, meta)
    if operation == "comparison":
        return _resolve_comparison(entry, meta, payload, lang)
    return _resolve_percentile(entry)


def _resolve_national(code, meta: Mapping, binding: Mapping, lang: str, reader) -> dict:
    """A national figure, from national.json.

    `reader.national()` existed and was never called: every binding went to the
    commune payload regardless of provider, so a valid, validator-approved
    national binding was told "not published for this commune" for a figure
    that IS published, one directory up.
    """
    payload = reader.national()
    if payload is None:
        return _state("missing", "no national payload is published")
    entry = (payload.get("indicators") or {}).get(code)
    if entry is None:
        return _state("missing", f"{code} is not published at national level")
    operation = binding.get("operation")
    if operation == "history":
        return _resolve_history(binding, entry, meta)
    period = _chosen_period(binding, entry)
    if period is None:
        return _state("missing", "this indicator has no periods")
    state, value = _cell_state(entry, period)
    if state == "suppressed":
        return {"state": "suppressed", "period": period, "message": _SUPPRESSED_MESSAGE}
    if state == "missing":
        return _state("missing", f"no value published for {period}")
    return {
        "state": "ready",
        "value": value,
        "period": period,
        "formatted_value": _format_value(value, meta, lang),
        "provenance": _provenance(meta),
    }


def _subject_nis(binding: Mapping, context_nis):
    geography = binding.get("geography") or {"mode": "context"}
    if geography.get("mode") == "fixed":
        return geography.get("nis")
    if context_nis and context_nis != "{nis}":
        return context_nis
    return None


def _chosen_period(binding: Mapping, entry: Mapping):
    period = binding.get("period") or {"mode": "latest"}
    periods = entry.get("periods") or {}
    if not periods:
        return None
    if period.get("mode") == "fixed":
        return period.get("period")
    return _sorted_periods(periods)[-1]


def _resolve_latest(binding: Mapping, entry: Mapping, meta: Mapping, lang: str) -> dict:
    period = _chosen_period(binding, entry)
    if period is None:
        return _state("missing", "this indicator has no periods for this commune")
    state, value = _cell_state(entry, period)
    if state == "suppressed":
        return {
            "state": "suppressed",
            "period": period,
            "message": _SUPPRESSED_MESSAGE,
        }
    if state == "missing":
        return _state("missing", f"no value published for {period}")
    return {
        "state": "ready",
        "value": value,
        "period": period,
        "formatted_value": _format_value(value, meta, lang),
        "provenance": _provenance(meta),
        # THE SERIES BEHIND THE FIGURE, for a sparkline beside it.
        #
        # A kpi_card asks for `latest` because it shows one number, and it also
        # wants the small line under it -- but those were two different
        # operations and a block carries ONE binding, so the card rendered
        # blank with `history` and drew no line with `latest`.
        #
        # Included here rather than making the card ask twice: the periods are
        # already in the entry this function is reading, so it costs nothing. A
        # single-period indicator yields an empty list and no line is drawn,
        # which is right -- one point is not a trend.
        #
        # Municipal only. `_resolve_national` reads a different entry shape and
        # no national block asks for a sparkline yet; adding it there without a
        # caller would be guessing at that shape.
        "points": _series(entry),
    }


def _series(entry: Mapping) -> list:
    """Every published (period, value) pair, oldest first, suppressed cells
    skipped.

    Shared with `_resolve_history` so one indicator cannot produce two
    different pictures of itself depending on which operation asked.
    """
    points = []
    for label in _sorted_periods(entry.get("periods") or {}):
        state, value = _cell_state(entry, label)
        if state == "ready":
            points.append({"period": label, "value": value})
    return points


def _resolve_history(binding: Mapping, entry: Mapping, meta: Mapping) -> dict:
    periods = entry.get("periods") or {}
    labels = _sorted_periods(periods)
    # `range` is schema-valid and validator-accepted. Ignoring it drew every
    # period in the payload on a chart authored for a specific window --
    # silently showing something other than what was asked for.
    window = binding.get("period") or {}
    if window.get("mode") == "range":
        start, end = window.get("from"), window.get("to")
        labels = [x for x in labels if (not start or x >= start) and (not end or x <= end)]
    if len(labels) < 2:
        return _state(
            "unavailable",
            "this indicator has a single period for this commune, so it has no history to draw",
        )
    points = []
    withheld = []
    for label in labels:
        state, value = _cell_state(entry, label)
        if state == "suppressed":
            withheld.append(label)
            continue
        if state == "ready":
            points.append({"period": label, "value": value})
    out = {
        "state": "ready" if points else "missing",
        "points": points,
        "provenance": _provenance(meta),
    }
    if withheld:
        # Named, not dropped: a gap in a line has to be explicable.
        out["withheld_periods"] = withheld
    return out


def _resolve_comparison(entry: Mapping, meta: Mapping, payload: Mapping, lang: str) -> dict:
    """The subject beside its province, region and country.

    THE SUBJECT'S OWN CELL CARRIES ITS STATE. An earlier version took the value
    and threw the state away, so a commune whose figure the source WITHHOLDS sat
    next to real province and country numbers with an empty box, inside a block
    reporting state "ready" -- a withheld figure presented as nothing at all.
    That is the regression docs/steps records as already shipped once, on a
    different code path.

    EACH ROW CARRIES ITS OWN PERIOD AND COVERAGE. The aggregates are not
    necessarily from the subject's latest period -- 53 mismatches in the first
    150 payloads, some six years apart -- and an aggregate below full coverage
    is a total built from part of the country. Printing either without saying so
    is how a figure becomes a wrong figure.
    """
    comparison = entry.get("comparison") or {}
    if not comparison:
        return _state(
            "unavailable",
            "no aggregate is published for this indicator -- a rate with no additive "
            "parts has no defensible province or region figure",
        )
    period = _chosen_period({}, entry)
    state, own = _cell_state(entry, period) if period else ("missing", None)
    rows = [
        {
            "label": _label(payload.get("name"), lang) or payload.get("nis_code") or "",
            "cells": [_cell_text(state, own, meta, lang)],
            "period": period,
            "is_subject": True,
        }
    ]
    periods_shown = {period} if period else set()
    for scope in ("province", "region", "country"):
        row = comparison.get(scope)
        if not row:
            continue
        coverage = row.get("coverage") or {}
        pct = coverage.get("pct")
        label = _label(row.get("name"), lang) or scope
        if isinstance(pct, (int, float)) and pct < 100:
            # Said on the row, not in a footnote nobody reads.
            label = f"{label} ({coverage.get('n')} of {coverage.get('of')})"
        rows.append(
            {
                "label": label,
                "cells": [_cell_text("ready", row.get("value"), meta, lang)],
                "period": row.get("period"),
                "coverage": coverage or None,
            }
        )
        if row.get("period"):
            periods_shown.add(row.get("period"))

    out = {
        "state": "ready",
        "columns": [period or ""],
        "rows": rows,
        "provenance": _provenance(meta),
    }
    if len(periods_shown) > 1:
        # A single column header over rows from different years would state
        # something false about three of them.
        out["mixed_periods"] = sorted(p for p in periods_shown if p)
    return out


def _cell_text(state: str, value, meta: Mapping, lang: str) -> str:
    """One table cell as text, INCLUDING the not-a-number states.

    A table has no per-cell state machine, so the state has to survive as
    words. Blank is not an option: it reads as "nothing here" for a figure the
    source is deliberately holding back.
    """
    if state == "suppressed":
        return "withheld"
    if state == "missing" or value is None:
        return "no data"
    return _format_value(value, meta, lang)


def _resolve_percentile(entry: Mapping) -> dict:
    percentile = entry.get("percentile") or {}
    national = percentile.get("national")
    if not national:
        return _state("unavailable", "no percentile is published for this indicator")
    return {
        "state": "ready",
        "scopes": percentile,
        "formatted_value": f"{national.get('rank')} of {national.get('peers')}",
        "period": national.get("period"),
    }


def _resolve_across_communes(code, meta: Mapping, lang: str, reader: PayloadReader) -> dict:
    payload = reader.indicator(code)
    if payload is None:
        return _state("missing", f"{code} has no cross-commune payload")
    communes = payload.get("communes") or {}
    values = {}
    suppressed = []
    for nis, cell in communes.items():
        if cell.get("status") == "suppressed":
            # Named, not painted. A withheld figure coloured at the bottom of
            # the scale would state a number the source refused to publish.
            suppressed.append(nis)
            continue
        if cell.get("value") is not None:
            values[nis] = cell["value"]
    return {
        "state": "ready" if values else "missing",
        "indicator": code,
        "values": values,
        "suppressed": suppressed,
        # The map component formats its own tooltips and legend ticks, so it
        # needs what the figure IS, not just the number. Taken from metadata,
        # never guessed -- the same rule the KPI formatting follows.
        "unit": meta.get("unit"),
        "decimals": meta.get("decimals"),
        "direction": meta.get("direction"),
        "provenance": _provenance(meta),
    }
