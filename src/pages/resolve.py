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
from collections.abc import Mapping
from pathlib import Path

from src.pages.schema import PageDocumentError

REPO_ROOT = Path(__file__).resolve().parents[2]

PAYLOAD_ROOT = Path("public") / "data"

#: Operations this batch answers, each from a payload that already holds the
#: result. `aggregate` is deliberately absent -- see `_AGGREGATE_REFUSAL`.
RESOLVABLE_OPERATIONS = frozenset(
    {"latest", "history", "comparison", "percentile", "map_values", "table"}
)

#: Said out loud rather than silently returning nothing. A block that shows
#: no reason is indistinguishable from a broken one.
_AGGREGATE_REFUSAL = (
    "A province- or region-level figure is not available to a block yet. The "
    "aggregate exists, but it is published only inside each commune's own "
    "comparison, not as a payload a block can read on its own."
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
        loaded: dict | None
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            loaded = None
        except (OSError, ValueError) as exc:
            raise PayloadError(f"cannot read payload {relative.as_posix()}: {exc}") from exc
        self._cache[relative] = loaded
        return loaded

    def commune(self, nis: str) -> dict | None:
        return self._read(Path("communes") / f"{nis}.json")

    def indicator(self, code: str) -> dict | None:
        return self._read(Path("indicators") / f"{code}.json")

    def national(self) -> dict | None:
        return self._read(Path("national.json"))


def _state(state: str, message: str = "") -> dict:
    out: dict = {"state": state}
    if message:
        out["message"] = message
    return out


def _format_value(value, meta: Mapping) -> str:
    """A figure written the way its own metadata says to write it.

    Unit and decimals come from `metadata/indicators.json`, never from the
    binding (data_binding.md) and never guessed here.
    """
    if value is None:
        return ""
    decimals = meta.get("decimals")
    if not isinstance(decimals, int) or isinstance(decimals, bool):
        decimals = 0
    try:
        rendered = f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)
    unit = meta.get("unit") or ""
    if unit in ("EUR", "%"):
        return f"{rendered} {unit}" if unit == "EUR" else f"{rendered}{unit}"
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
    doc: Mapping, *, metadata, lang: str = "en", reader: PayloadReader | None = None
) -> dict:
    """`{block_id: payload}` for every block carrying a binding.

    A block WITHOUT a binding gets no entry at all -- `render.py`'s
    `_state_for` already renders those as `ready`, and inventing an entry for
    them would say something about data where there is none to say.
    """
    reader = reader or PayloadReader()
    out: dict[str, dict] = {}
    context_nis = (doc.get("context") or {}).get("nis")
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
    if operation == "aggregate" or (binding.get("geography") or {}).get("mode") == "level":
        return _state("unavailable", _AGGREGATE_REFUSAL)
    if operation not in RESOLVABLE_OPERATIONS:
        return _state("unavailable", f"the {operation!r} operation is not available yet")

    code = binding.get("indicator")
    meta = metadata.municipal_indicators.get(code) or metadata.national_indicators.get(code) or {}

    if operation in ("map_values", "table"):
        return _resolve_across_communes(code, meta, reader)

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
        return _resolve_latest(binding, entry, meta)
    if operation == "history":
        return _resolve_history(entry, meta)
    if operation == "comparison":
        return _resolve_comparison(entry, meta, payload, lang)
    return _resolve_percentile(entry)


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


def _resolve_latest(binding: Mapping, entry: Mapping, meta: Mapping) -> dict:
    period = _chosen_period(binding, entry)
    if period is None:
        return _state("missing", "this indicator has no periods for this commune")
    state, value = _cell_state(entry, period)
    if state == "suppressed":
        return {
            "state": "suppressed",
            "period": period,
            "message": (
                "The source holds this figure and does not publish it, because "
                "the count is too small to release."
            ),
        }
    if state == "missing":
        return _state("missing", f"no value published for {period}")
    return {
        "state": "ready",
        "value": value,
        "period": period,
        "formatted_value": _format_value(value, meta),
        "provenance": _provenance(meta),
    }


def _resolve_history(entry: Mapping, meta: Mapping) -> dict:
    periods = entry.get("periods") or {}
    labels = _sorted_periods(periods)
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
    comparison = entry.get("comparison") or {}
    if not comparison:
        return _state(
            "unavailable",
            "no aggregate is published for this indicator -- a rate with no additive "
            "parts has no defensible province or region figure",
        )
    period = _chosen_period({}, entry)
    _, own = _cell_state(entry, period) if period else ("missing", None)
    rows = [
        {
            "label": _label(payload.get("name"), lang) or payload.get("nis_code") or "",
            "cells": [_format_value(own, meta)],
            "is_subject": True,
        }
    ]
    for scope in ("province", "region", "country"):
        row = comparison.get(scope)
        if not row:
            continue
        rows.append(
            {
                "label": _label(row.get("name"), lang) or scope,
                "cells": [_format_value(row.get("value"), meta)],
            }
        )
    return {
        "state": "ready",
        "columns": [period or ""],
        "rows": rows,
        "provenance": _provenance(meta),
    }


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


def _resolve_across_communes(code, meta: Mapping, reader: PayloadReader) -> dict:
    payload = reader.indicator(code)
    if payload is None:
        return _state("missing", f"{code} has no cross-commune payload")
    communes = payload.get("communes") or {}
    values = {}
    suppressed = []
    for nis, cell in communes.items():
        if cell.get("status") == "suppressed":
            suppressed.append(nis)
            continue
        if cell.get("value") is not None:
            values[nis] = cell["value"]
    return {
        "state": "ready" if values else "missing",
        "values": values,
        "suppressed": suppressed,
        "provenance": _provenance(meta),
    }
