"""Canonical serialization for page documents.

Two saves of an unchanged document must be byte-identical (claude.md rule 35,
invariant 9), so the canonical form is fixed here and nowhere else:
`indent=2`, `sort_keys=True`, `ensure_ascii=False`, a trailing newline.

This DELIBERATELY differs from the exporters' payload form
(`separators=(",", ":")`, no `sort_keys` -- see `src/exporters/metadata.py` and
`scripts/export_site_payloads.py`). A page document is human-diffed source
committed to git; a payload is machine output served to a browser. Do not
"fix" either one to match the other: making page documents minified destroys
the reviewable diff, and sorting/indenting payloads changes published bytes.

`allow_nan=False` is not cosmetic. `json.dumps` defaults to `allow_nan=True`,
which writes `float('nan')` as bare `NaN` -- invalid JSON that Python's own
`json.loads` happily reads back, so a naive round-trip test passes while
producing a file no other parser can read. `loads()` refuses the same three
constants on the way in.
"""

import json

from src.pages.schema import (
    ROOT_PATH,
    PageValidationError,
    check_raw_size,
)


def dumps(doc: dict) -> str:
    """The canonical text for a page document, ending in a newline.

    Raises `ValueError` (from `json`) on a NaN or Infinity float, deliberately
    left as the stdlib's own exception rather than wrapped: a non-finite float
    is a programming error in whatever built the document, not a validation
    finding about a user's page.
    """
    return (
        json.dumps(
            doc,
            sort_keys=True,
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    )


def _reject_constant(name: str):
    raise PageValidationError(
        "schema_violation",
        ROOT_PATH,
        f"{name} is not valid JSON and is not accepted in a page document",
    )


def loads(text: str) -> dict:
    """Parse a page document, with the raw-size and recursion guards applied.

    The inverse of `dumps` for anything `dumps` can produce. Guard steps 1 and
    2 of `schema.py`'s ordering live here so no caller can skip them by
    reaching for `json.loads` directly.
    """
    if not isinstance(text, str):
        raise PageValidationError(
            "schema_violation", ROOT_PATH, f"expected document text, got {type(text).__name__}"
        )

    oversize = check_raw_size(text)
    if oversize:
        raise oversize[0]

    try:
        doc = json.loads(text, parse_constant=_reject_constant)
    except RecursionError as exc:
        # CPython raises this somewhere around 1000 levels of nesting. Convert
        # it: a resource guard that crashes the process has not guarded
        # anything.
        raise PageValidationError(
            "too_deep",
            ROOT_PATH,
            "document nesting exceeded the parser's recursion limit",
        ) from exc
    except json.JSONDecodeError as exc:
        raise PageValidationError("schema_violation", ROOT_PATH, f"not valid JSON: {exc}") from exc
    except ValueError as exc:
        # CPython's json.loads also raises a bare ValueError -- not
        # JSONDecodeError -- for an integer literal beyond
        # sys.get_int_max_str_digits() (a DoS guard added to int() itself,
        # unrelated to and uncoordinated with anything in this module). A
        # security-red-team pass found this reaches a caller as an unhandled
        # exception: a builder route parsing an on-disk draft containing a
        # 5000-digit "version" value 500'd instead of answering the ordinary
        # "not valid JSON" rejection every other malformed-JSON shape gets.
        # Caught here, at the one function this whole module exists to make
        # total, rather than patched at each call site.
        raise PageValidationError("schema_violation", ROOT_PATH, f"not valid JSON: {exc}") from exc

    if not isinstance(doc, dict):
        raise PageValidationError(
            "schema_violation",
            ROOT_PATH,
            f"a page document must be a JSON object, got {type(doc).__name__}",
        )
    return doc
