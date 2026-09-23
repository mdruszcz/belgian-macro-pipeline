"""Statbel's monthly bankruptcies-by-NACE open-data file -- BANKRUPTCIES and
BANKRUPTCY_JOBS_LOST, docs/features/bankruptcies.md.

TWO STEPS, because the download URL is not stable: the landing page
(https://statbel.fgov.be/fr/open-data/evolution-mensuelle-des-faillites-par-nace)
links a zip named `TF_BANKRUPTCIES(<year>).zip`, where `<year>` changes as
Statbel republishes the file. `discover_zip_url()` reads the link off the
page's HTML rather than ever hard-coding a year (CLAUDE.md rule 13): it
matches the plain `TF_BANKRUPTCIES(<year>).zip` link only, and explicitly
rejects the two sibling variants the page also carries,
`TF_BANKRUPTCIES_accdb(<year>).zip` and `TF_BANKRUPTCIES_sqlite(<year>).zip`
-- both share the same `TF_BANKRUPTCIES` prefix and would match a looser
pattern. No link matching all three conditions (right prefix, right
parenthesised year, not one of the two suffixed variants) is a hard failure,
never a fallback to a cached or hard-coded URL.

No HTML-parsing library is a dependency of this pipeline, and adding one for
one page is not worth it: the discovery is a plain regex over `href="..."`
attributes, which is enough for one link on one Statbel page and is exactly
as strict as the rule above requires -- it does not need to understand HTML
structure, only find hrefs.

ONE MEMBER IN THE ZIP, `TF_BANKRUPTCIES.txt`: pipe-separated, 41 columns,
header row present. `_parse` receives the zip's raw bytes (as cached by the
base class) and extracts that one member; a zip with no member of that name
is refused rather than guessed at.

ENCODING IS UTF-8 WITH A BOM -- read as `utf-8-sig`, never cp1252. Measured
2026-09-23: "Liège" is stored as the UTF-8 bytes 4c 69 c3 a8 67 65; cp1252
would decode those same bytes without raising and silently produce "LiÃ¨ge".
`utf-8-sig` also strips the BOM itself, so the first column's header name is
not left with a stray `﻿` prefix that would make the required-column
check below fail to find it.

COLUMNS ARE RESOLVED BY NAME, never position, from the header row: only five
matter here -- `MS_COUNTOF_BANKRUPTCIES`, `MS_COUNTOF_WORKERS`, `CD_YEAR`,
`CD_MONTH`, `CD_MUNTY_REFNIS`. Any of the five missing from the header is a
schema change and fails loudly (CLAUDE.md rule 13), not a silent re-index.

ONE ROW PER (commune x month x employment class x legal form x NACE class x
company duration): `_parse` SUMS `MS_COUNTOF_BANKRUPTCIES` and
`MS_COUNTOF_WORKERS` down to one value per (geo NIS code, period) cell --
that collapse is the whole job of this adapter. It then emits TWO rows per
surviving cell, one per indicator ("BANKRUPTCIES" and
"BANKRUPTCY_JOBS_LOST" in a sixth key, `indicator_id`, alongside the base
contract's four), rather than returning one row with two value columns --
so every row still carries exactly the shape
`{"geo_id","period","value","status"}` plus `indicator_id`, and the caller
can treat both indicators identically. The zero-fill for a commune-month
with NO row in the file at all (a real bankruptcy count of zero, per the
maintainer) is the sync script's job, not this adapter's, because it needs
the full commune list and the file's own observed period range, neither of
which `_parse` has reason to know.

`CD_MONTH` is UNPADDED in the source (`8`, not `08`); the period is built as
f"{year:04d}-{month:02d}" here, not by string-concatenating the raw column.

Geography is NOT resolved inside this adapter. `_parse` returns the raw NIS
string from `CD_MUNTY_REFNIS` as `geo_id` -- deliberately breaking the
MunicipalTimeSeriesSource convention of a real `be:mun:` id in that field,
because this source's rows must be resolved at a PINNED period (see
scripts/sync_bankruptcies.py's module docstring), which only the sync script
knows, and resolve_geo() takes a period this adapter is never given. Every
other MunicipalTimeSeriesSource before this one resolves inline because it
already has the one period it needs (WalStat) or does not need one at all;
this is the first one that does not, so it hands back the raw code and lets
the caller resolve it. tests/test_bankruptcies_source.py documents and
tests this deviation directly; the SHARED base contract test in
tests/test_source_contract.py is satisfied per indicator by filtering this
adapter's rows down to the base four keys before asserting on them (see
that test file's own bankruptcies case).
"""

from __future__ import annotations

import csv
import io
import re
import zipfile

from src.fetchers.base import MunicipalTimeSeriesSource

#: The five column names this adapter needs, resolved by name from the
#: header row -- never a fixed position (CLAUDE.md rule 13: a Statbel column
#: reorder must fail loudly, not silently misread).
REQUIRED_COLUMNS = (
    "MS_COUNTOF_BANKRUPTCIES",
    "MS_COUNTOF_WORKERS",
    "CD_YEAR",
    "CD_MONTH",
    "CD_MUNTY_REFNIS",
)

#: The one member the zip must contain.
MEMBER_NAME = "TF_BANKRUPTCIES.txt"

BANKRUPTCIES = "BANKRUPTCIES"
BANKRUPTCY_JOBS_LOST = "BANKRUPTCY_JOBS_LOST"

#: Matches an href ending in the plain zip -- e.g.
#: "https://.../TF_BANKRUPTCIES%282025%29.zip" (URL-encoded) or
#: ".../TF_BANKRUPTCIES(2025).zip" (literal parens) -- capturing the year.
#: Deliberately anchored on `TF_BANKRUPTCIES` immediately followed by the
#: open-paren (encoded or not) so it cannot also match the `_accdb`/`_sqlite`
#: variants, whose prefix is `TF_BANKRUPTCIES_accdb`/`TF_BANKRUPTCIES_sqlite`.
_ZIP_HREF = re.compile(
    r'href="([^"]*TF_BANKRUPTCIES(?:%28|\()(\d{4})(?:%29|\))\.zip)"',
    re.IGNORECASE,
)
_REJECTED_SUFFIX = re.compile(r"TF_BANKRUPTCIES_(accdb|sqlite)", re.IGNORECASE)


class BankruptcyLinkNotFoundError(Exception):
    """The landing page has no href matching TF_BANKRUPTCIES(<year>).zip."""


class BankruptciesSchemaError(ValueError):
    """The zip or the txt member inside it is not the documented shape."""


def discover_zip_url(html: str) -> str:
    """The href for the plain `TF_BANKRUPTCIES(<year>).zip` link on the
    Statbel landing page.

    Rejects `TF_BANKRUPTCIES_accdb(<year>).zip` and
    `TF_BANKRUPTCIES_sqlite(<year>).zip` explicitly, even though a looser
    pattern would also match their shared prefix, per the handoff. Raises
    BankruptcyLinkNotFoundError -- and never falls back to a cached or
    hard-coded URL -- if no link satisfies every condition.
    """
    candidates = [
        (href, year) for href, year in _ZIP_HREF.findall(html) if not _REJECTED_SUFFIX.search(href)
    ]
    if not candidates:
        raise BankruptcyLinkNotFoundError(
            "No href on the page matches TF_BANKRUPTCIES(<year>).zip (after excluding the "
            "_accdb and _sqlite variants). Refusing to fall back to a cached or hard-coded "
            "URL (CLAUDE.md rule 13)."
        )
    if len(candidates) > 1:
        raise BankruptcyLinkNotFoundError(
            f"{len(candidates)} candidate links matched TF_BANKRUPTCIES(<year>).zip: "
            f"{[href for href, _ in candidates]}. Refusing to guess which one is current."
        )
    href, _year = candidates[0]
    return href.replace("%28", "(").replace("%29", ")")


def _extract_member(raw: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        names = zf.namelist()
        if MEMBER_NAME not in names:
            raise BankruptciesSchemaError(
                f"The zip has no member named {MEMBER_NAME!r}. Its members are {names}. "
                "Refusing to guess a replacement (CLAUDE.md rule 13)."
            )
        return zf.read(MEMBER_NAME)


class BankruptciesSource(MunicipalTimeSeriesSource):
    """Contract deviation, documented above and in the contract test: `geo_id`
    is the raw NIS string from the file, not a resolved `be:mun:` id --
    resolution happens in scripts/sync_bankruptcies.py against a pinned
    period this adapter is never given.
    """

    source_id = "statbel"
    adapter = "bankruptcies"
    raw_extension = "zip"

    def _parse(self, raw: bytes, **kwargs) -> list[dict]:
        member_bytes = _extract_member(raw)
        text = member_bytes.decode("utf-8-sig")
        reader = csv.reader(io.StringIO(text), delimiter="|")
        try:
            header = next(reader)
        except StopIteration as exc:
            raise BankruptciesSchemaError(f"{MEMBER_NAME} is empty -- no header row") from exc

        missing = [c for c in REQUIRED_COLUMNS if c not in header]
        if missing:
            raise BankruptciesSchemaError(
                f"{MEMBER_NAME} is missing required column(s) {missing}. Its header has "
                f"{len(header)} columns: {header}. Refusing to guess a replacement "
                "(CLAUDE.md rule 13)."
            )
        idx = {name: header.index(name) for name in REQUIRED_COLUMNS}

        # (nis, period) -> [bankruptcies_sum, workers_sum]
        totals: dict[tuple[str, str], list[float]] = {}
        for row in reader:
            if not row:
                continue
            nis = row[idx["CD_MUNTY_REFNIS"]].strip()
            year = int(row[idx["CD_YEAR"]])
            month = int(row[idx["CD_MONTH"]])
            period = f"{year:04d}-{month:02d}"
            bankruptcies = float(row[idx["MS_COUNTOF_BANKRUPTCIES"]])
            workers = float(row[idx["MS_COUNTOF_WORKERS"]])

            key = (nis, period)
            if key not in totals:
                totals[key] = [0.0, 0.0]
            totals[key][0] += bankruptcies
            totals[key][1] += workers

        results: list[dict] = []
        for (nis, period), (bankruptcies_sum, workers_sum) in sorted(totals.items()):
            for indicator_id, value in (
                (BANKRUPTCIES, bankruptcies_sum),
                (BANKRUPTCY_JOBS_LOST, workers_sum),
            ):
                results.append(
                    {
                        "geo_id": nis,
                        "period": period,
                        "value": value,
                        "status": "final",
                        # Not one of the base contract's four keys -- this
                        # adapter emits two indicators per cell, so the caller
                        # (scripts/sync_bankruptcies.py) needs to know which is
                        # which. tests/test_bankruptcies_source.py checks the
                        # base four keys are still present on every row.
                        "indicator_id": indicator_id,
                    }
                )
        return results
