"""SPF Finances' AGDP (Administration Generale de la Documentation
Patrimoniale) quarterly patrimony datasets -- Wave 4 loads two of the ~15
AGDP datasets that will eventually share this module: real-estate leases
(52.01.01, MUN_LEASES_NEW_HOUSING / MUN_LEASE_RENT_MEDIAN_HOUSING /
MUN_LEASE_CHARGES_MEDIAN_HOUSING) and real-estate transactions (52.01.02,
MUN_PROPERTY_SALES). docs/features/spf_agdp.md.

GENERIC BY DESIGN, CONFIGURED PER DATASET -- Wave 5 (13 more, mostly annual,
AGDP datasets) reuses every piece here: ATOM discovery, the ranged zip
reader, the Municipality-CSV parser. Only the per-dataset shape (which
columns exist, which row to select for which indicator) varies, and that
shape lives in a `AgdpDatasetConfig`, not in a new subclass per dataset --
this module has exactly one MunicipalTimeSeriesSource, `AgdpSource`, driven
by whichever config its caller hands it.

ATOM FEED, ONE PER DATASET: `https://opendata.fin.belgium.be/download/ATOM/
{uuid}-en.xml`. Each `<link rel="section">` element is one quarter's zip:
`href` is the download URL, `time="{quarter end timestamp}"` (e.g.
"2026-03-31T00:00:00Z") is the ONLY period signal -- no CSV in either
dataset carries a period column -- and `length="{bytes}"` is the
republish-detection signal scripts/sync_spf_agdp.py's incremental fetch
uses (module docstring there). `parse_atom_feed()` below returns every
version as an `AgdpVersion(url, quarter, length)`, quarter already converted
from the raw timestamp (e.g. "2026-03-31..." -> "2026-Q1") -- never resolved
against a commune here, this layer has no db connection.

RANGED ZIP READ, NOT A FULL DOWNLOAD. Each zip holds seven CSVs (national/
regional/provincial/arrondissement/municipality/division/statistical-unit)
plus two TechSpec PDFs; only the Municipality* member is ever wanted, and
the Transactions statistical-unit CSV alone is 4.3 GB uncompressed --
downloading the whole ~205 MB zip to read a 6 MB member is the naive path
this exists to avoid (8.1 GB for a full backfill, measured). `_RangedHttpFile`
is a seekable `io.RawIOBase` over HTTP range requests (the server returns
206 and `Accept-Ranges: bytes`, verified); wrapped in
`io.BufferedReader(..., buffer_size=1<<22)` and handed to `zipfile.ZipFile`,
opening just the Municipality member reads ~6 MB in ~5 range requests
instead of the full ~205 MB. Falls back to a whole-zip GET -- loudly, with a
warning identifying the dataset and quarter -- the moment the server answers
200 instead of 206 for a range request (some proxies strip range support
without notice); `open_municipality_csv()` is the one entry point that
chooses between the two paths, so every caller gets the same fallback
behaviour without re-implementing it.

CSV SHAPE, MEASURED: UTF-8 with BOM (`utf-8-sig`), `;`-delimited, CRLF line
endings, decimal POINT, no thousands separator. Both datasets' Municipality
CSVs share this shape; only the column set differs, which is exactly what
`AgdpDatasetConfig.required_columns` exists to check (CLAUDE.md rule 13: an
unexpected header fails loudly, never a silent re-index).

FIVE STATES, enforced in `_parse` for the one row `AgdpDatasetConfig.select`
picks out per commune per quarter -- see the docstring on `_row_to_observation`
below for the exact mapping. This is a MunicipalTimeSeriesSource but,
because a NIS code's *own-period* validity has to be resolved with a live db
connection this layer never has, `geo_id` here is the raw NIS string from
the CSV's `NISCode` column, exactly the same deviation
src/fetchers/bankruptcies.py and src/fetchers/population_movement.py already
use, for the same reason -- scripts/sync_spf_agdp.py resolves it.

LOT B (Wave 5 lot B, patrimony): land use (52.01.04), building condition
(52.01.05) and property-tax exemptions (52.01.03) -- three more annual
datasets, same generic reader, same `frequency="A"` annual mapping lot A's
Owner Occupants/Property Dynamics already use. A real 1-4 suppression tier
DOES exist in lot B's raw files (measured on the cadastral-income columns of
land use and tax exemptions at low ParcelsNumber -- see
`_annual_row_to_observations`'s own docstring), but it never touches any of
lot B's six chosen columns, which are always published (non-blank) at
TOTAL/ParcelNature=TOTAL/ExemptionType=TOTAL for a live commune -- verified
across 2026, 2025 and 2017. `is_count=True` for all six, same as lot A.
"""

from __future__ import annotations

import csv
import http.client
import io
import re
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from xml.etree import ElementTree

from src.fetchers.base import MunicipalTimeSeriesSource

ATOM_URL_PATTERN = "https://opendata.fin.belgium.be/download/ATOM/{uuid}-en.xml"

_ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}

#: Matches the quarter-end timestamp ATOM's `time=` attribute carries, e.g.
#: "2026-03-31T00:00:00Z" -- only the date matters, the time is always
#: midnight UTC in every version observed.
_QUARTER_END = re.compile(r"^(\d{4})-(\d{2})-\d{2}T")

#: Quarter-end month -> quarter number. AGDP publishes strictly on calendar
#: quarter ends (03/06/09/12); any other month in a `time=` attribute is a
#: schema surprise, refused rather than guessed (CLAUDE.md rule 13).
_QUARTER_END_MONTH = {3: 1, 6: 2, 9: 3, 12: 4}

#: Matches the annual timestamp Wave 5's annual AGDP datasets carry, e.g.
#: "2026-01-01T00:00:00Z" -- month and day must both be 01 (a 1-January
#: snapshot); anything else is a schema surprise, refused rather than
#: guessed (CLAUDE.md rule 13).
_ANNUAL_TIMESTAMP = re.compile(r"^(\d{4})-01-01T")

#: The one CSV member name pattern this module ever reads out of a zip --
#: case varies by dataset ("Municipality.csv" / "Municipality_LEASES.csv"
#: are both observed shapes across AGDP datasets), so this matches by
#: prefix, not by an exact hard-coded name. Division/StatisticalUnit/
#: national/regional/provincial/arrondissement members are deliberately
#: never touched (handoff exclusion).
_MUNICIPALITY_MEMBER = re.compile(r"^Municipality[^/]*\.csv$", re.IGNORECASE)

#: Ranged-read tuning, verified 2026-09-23 against the real server: a 4 MiB
#: buffer keeps zipfile's own central-directory + member reads to ~5 range
#: requests for a 6 MB member inside a ~205 MB zip.
_RANGE_BUFFER_SIZE = 1 << 22
_RANGE_MAX_ATTEMPTS = 3
_RANGE_BACKOFF_SECONDS = (1, 2, 4)
_REQUEST_TIMEOUT = 60


class AgdpSchemaError(ValueError):
    """The ATOM feed, the zip, or the Municipality CSV is not the documented shape."""


class AgdpFetchError(Exception):
    """A ranged (or fallback whole-zip) download failed after exhausting retries."""


@dataclass(frozen=True)
class AgdpVersion:
    """One ATOM `<link rel="section">` entry: one quarter's published zip."""

    url: str
    quarter: str  # "YYYY-Qn"
    length: int  # bytes, from the ATOM `length=` attribute -- republish signal


def _quarter_from_timestamp(raw: str, *, context: str) -> str:
    match = _QUARTER_END.match(raw)
    if not match:
        raise AgdpSchemaError(
            f"{context}: ATOM `time` attribute {raw!r} does not match the expected "
            "YYYY-MM-DDT... shape. Refusing to guess a period (CLAUDE.md rule 13)."
        )
    year, month = int(match.group(1)), int(match.group(2))
    quarter = _QUARTER_END_MONTH.get(month)
    if quarter is None:
        raise AgdpSchemaError(
            f"{context}: ATOM `time` attribute {raw!r} has month {month:02d}, not a "
            "calendar quarter end (03/06/09/12). Refusing to guess a period "
            "(CLAUDE.md rule 13)."
        )
    return f"{year:04d}-Q{quarter}"


def _year_from_timestamp(raw: str, *, context: str) -> str:
    match = _ANNUAL_TIMESTAMP.match(raw)
    if not match:
        raise AgdpSchemaError(
            f"{context}: ATOM `time` attribute {raw!r} does not match the expected "
            "annual YYYY-01-01T... shape (a 1-January snapshot). Refusing to guess a "
            "period (CLAUDE.md rule 13)."
        )
    return match.group(1)


def parse_atom_feed(
    xml_bytes: bytes, *, dataset_label: str, frequency: str = "Q"
) -> list[AgdpVersion]:
    """Every version link in one AGDP dataset's ATOM feed, oldest first.

    Raises AgdpSchemaError on a `<link rel="section">` missing `href`,
    `time`, or `length`, or on zero such links -- an empty or malformed feed
    is refused rather than treated as "no new data" (CLAUDE.md rule 13).

    `frequency` selects the period parser: "Q" (default, unchanged) expects
    a calendar quarter-end timestamp and returns "YYYY-Qn"; "A" expects a
    1-January timestamp and returns "YYYY" (Wave 5's annual datasets).
    """
    if frequency not in ("Q", "A"):
        raise ValueError(f"Unknown frequency {frequency!r}, expected 'Q' or 'A'.")
    try:
        root = ElementTree.fromstring(xml_bytes)
    except ElementTree.ParseError as exc:
        raise AgdpSchemaError(f"{dataset_label}: ATOM feed is not well-formed XML: {exc}") from exc

    versions: list[AgdpVersion] = []
    for link in root.iter("{http://www.w3.org/2005/Atom}link"):
        if link.get("rel") != "section":
            continue
        href = link.get("href")
        time_attr = link.get("time")
        length_attr = link.get("length")
        if not href or not time_attr or not length_attr:
            raise AgdpSchemaError(
                f'{dataset_label}: a <link rel="section"> is missing href/time/length '
                f"(href={href!r}, time={time_attr!r}, length={length_attr!r}). Refusing to "
                "guess a replacement (CLAUDE.md rule 13)."
            )
        try:
            length = int(length_attr)
        except ValueError as exc:
            raise AgdpSchemaError(
                f"{dataset_label}: link length {length_attr!r} is not an integer."
            ) from exc
        if frequency == "A":
            period = _year_from_timestamp(time_attr, context=dataset_label)
        else:
            period = _quarter_from_timestamp(time_attr, context=dataset_label)
        versions.append(AgdpVersion(url=href, quarter=period, length=length))

    if not versions:
        raise AgdpSchemaError(
            f'{dataset_label}: ATOM feed has zero <link rel="section"> entries. Refusing '
            "to treat an empty feed as 'nothing new' (CLAUDE.md rule 13)."
        )
    versions.sort(key=lambda v: v.quarter)
    return versions


# --- ranged HTTP read --------------------------------------------------------


class _RangedHttpFile(io.RawIOBase):
    """A seekable, read-only file-like object over HTTP range requests.

    zipfile.ZipFile only ever needs seek/tell/readinto (and readable/
    seekable) to read one member out of a remote zip without downloading it
    whole -- this implements exactly that surface, nothing else. Every range
    request retries transient failures up to `_RANGE_MAX_ATTEMPTS` times; a
    200 response instead of the requested 206 means the server (or a proxy
    in front of it) silently ignored the Range header, so `_content_length`
    falls back to a whole-body read on the FIRST such response and every
    subsequent read is served out of that buffered copy -- `on_fallback`, if
    given, is called once so the caller can print the loud warning the
    handoff requires (never swallowed silently).
    """

    def __init__(self, url: str, *, on_fallback=None):
        super().__init__()
        self._url = url
        self._pos = 0
        self._length: int | None = None
        self._whole_body: bytes | None = None
        self._on_fallback = on_fallback

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def _request(self, headers: dict) -> tuple[bytes, int]:
        req = urllib.request.Request(self._url, headers=headers)
        last_exc: Exception | None = None
        for attempt in range(_RANGE_MAX_ATTEMPTS):
            try:
                with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
                    return resp.read(), resp.status
            except (urllib.error.URLError, TimeoutError, http.client.HTTPException) as exc:
                last_exc = exc
                if attempt < _RANGE_MAX_ATTEMPTS - 1:
                    time.sleep(_RANGE_BACKOFF_SECONDS[attempt])
                    continue
        raise AgdpFetchError(
            f"Exhausted {_RANGE_MAX_ATTEMPTS} attempts fetching {self._url} "
            f"(headers={headers}): {last_exc}"
        )

    def _ensure_length(self) -> int:
        if self._length is not None:
            return self._length
        # A single-byte range probe both discovers Content-Range's total
        # length and confirms whether the server actually honours ranges.
        body, status = self._request({"Range": "bytes=0-0"})
        if status != 206:
            self._fall_back_to_whole_body()
            assert self._whole_body is not None
            self._length = len(self._whole_body)
            return self._length
        # Content-Range: bytes 0-0/12345
        # (urllib does not expose it directly on the streamed read above, so
        # this reissues the same probe via a HEAD-less GET and reads the
        # header from a fresh request object.)
        req = urllib.request.Request(self._url, headers={"Range": "bytes=0-0"})
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
            content_range = resp.headers.get("Content-Range", "")
        match = re.search(r"/(\d+)$", content_range)
        if not match:
            raise AgdpSchemaError(
                f"Server returned 206 for {self._url} but no parseable Content-Range "
                f"header ({content_range!r}). Refusing to guess the file length."
            )
        self._length = int(match.group(1))
        return self._length

    def _fall_back_to_whole_body(self) -> None:
        if self._whole_body is not None:
            return
        if self._on_fallback is not None:
            self._on_fallback()
        body, _status = self._request({})
        self._whole_body = body

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._ensure_length() + offset
        else:
            raise ValueError(f"Unsupported whence {whence!r}")
        return self._pos

    def tell(self) -> int:
        return self._pos

    def readinto(self, b) -> int:
        n = len(b)
        if n == 0:
            return 0
        if self._whole_body is not None:
            chunk = self._whole_body[self._pos : self._pos + n]
        else:
            total = self._ensure_length()
            if self._pos >= total:
                return 0
            end = min(self._pos + n, total) - 1
            body, status = self._request({"Range": f"bytes={self._pos}-{end}"})
            if status != 206:
                # Ranges were honoured for the length probe but not here --
                # treat it the same way: fall back for the rest of this read.
                self._fall_back_to_whole_body()
                assert self._whole_body is not None
                chunk = self._whole_body[self._pos : self._pos + n]
            else:
                chunk = body
        b[: len(chunk)] = chunk
        self._pos += len(chunk)
        return len(chunk)


def open_municipality_csv(url: str, *, dataset_label: str, quarter: str, warn=print) -> bytes:
    """The raw bytes of the one Municipality*.csv member inside the zip at
    `url`, read via a ranged HTTP file so the rest of the zip (including the
    multi-GB StatisticalUnit member) is never downloaded.

    Falls back to a whole-zip GET, with a loud warning naming `dataset_label`
    and `quarter`, the moment ranging is unavailable (200 instead of 206).
    `warn` defaults to `print` so the fallback is visible in the daily run
    log, per the handoff; tests pass a list-appending callable to assert on
    it without capturing stdout.
    """
    fell_back = False

    def _on_fallback():
        nonlocal fell_back
        fell_back = True
        warn(
            f"::warning::{dataset_label} {quarter}: server did not honour a ranged "
            f"request for {url} (200 instead of 206) -- falling back to a whole-zip "
            "download. This is much slower and should be investigated if it recurs."
        )

    ranged = _RangedHttpFile(url, on_fallback=_on_fallback)
    buffered = io.BufferedReader(ranged, buffer_size=_RANGE_BUFFER_SIZE)
    with zipfile.ZipFile(buffered) as zf:
        members = [n for n in zf.namelist() if _MUNICIPALITY_MEMBER.match(n.split("/")[-1])]
        if len(members) != 1:
            raise AgdpSchemaError(
                f"{dataset_label} {quarter}: expected exactly one Municipality*.csv member, "
                f"found {members} among {zf.namelist()}. Refusing to guess (CLAUDE.md "
                "rule 13)."
            )
        return zf.read(members[0])
    # `fell_back` is intentionally unread here -- open_municipality_csv's
    # contract is the bytes; sync_spf_agdp.py checks the log line itself in
    # its own fallback test via the `warn` callable.


# --- per-dataset config ------------------------------------------------------


@dataclass(frozen=True)
class AgdpDatasetConfig:
    """What varies between AGDP datasets sharing this module.

    `select` is a mapping of column name -> exact value the ONE relevant row
    is picked out by (e.g. Leases: RegistrationType=HousingRegistration,
    LessorType=TOTAL, TakerType=TOTAL) -- a dataset with more than one
    indicator drawn from different rows needs more than one AgdpDatasetConfig
    (Leases below has two: the housing-lease row feeds three indicators, but
    they are all the SAME row, so one config suffices there too).
    `indicators` maps CSV column name -> (indicator_id, is_count). A count
    column follows the CLAUDE.md rule 26 five-state mapping in `_parse`
    below; a non-count (percentile) column follows the suppression mapping.
    Exactly one entry may be `is_count=True` per config -- the one whose CSV
    column IS `count_column` itself (every quarterly/lot-A annual config so
    far has exactly one). An additive companion figure that is NOT derived
    from dividing by the count (lot B: TotalCadastralIncome,
    TaxableCadastralIncome, CentralHeating -- each its own independent
    measured euro/count figure, not a percentile/mean that becomes
    meaningless at zero parcels) is `is_count=False` but listed in
    `always_final_columns` so `_annual_row_to_observations` never maps it to
    `na` at a zero/blank count -- it always reads its own cell, final,
    raising if that cell is unexpectedly blank at all (CLAUDE.md rule 13),
    the same "blank is a schema surprise" guard the count branch now uses.
    `count_column` names the column whose blank/0/1-4/>=5 value decides
    status for every percentile column in `indicators` (Leases: RentsNumber;
    Transactions: ParcelsNumber) -- SPF Finances suppresses every percentile
    on the same row together, keyed off the one row count.

    `frequency` selects both the ATOM period parser (`parse_atom_feed`) and,
    in `_row_to_observations`, which five-state mapping applies: "Q"
    (default) is the quarterly Leases/Transactions mapping with its 1-4
    suppression tier, described above. "A" is Wave 5's annual mapping (Owner
    Occupants / Property Dynamics): a count column is final as-is including a
    real zero, exactly like "Q"; but a non-count column has no suppression
    tier at all -- it is `na`/NULL only when `count_column`'s value is
    missing or literally zero (never a fabricated zero duration/mean), and
    `final` otherwise, always requiring a non-blank cell. There is no 1-4
    tier for these two datasets (the handoff measured none).

    `row_filter`, if given, is called once per raw CSV row (before `select`
    is checked) and must return True to keep the row -- e.g. Owner Occupants
    skips the one Fictious=1/NISCode="N/A" placeholder set and any row whose
    NISCode is not all-digit, rather than special-casing that inside
    `_parse`.
    """

    label: str
    uuid: str
    required_columns: tuple[str, ...]
    select: dict[str, str]
    count_column: str
    indicators: dict[str, tuple[str, bool]]  # csv_column -> (indicator_id, is_count)
    frequency: str = "Q"
    row_filter: object = None  # Callable[[dict[str, str]], bool] | None
    #: CSV columns that are their own independent additive measured figure
    #: (not a percentile/mean/duration derived FROM the row's count, so a
    #: zero/blank count must never map them to `na`) -- see the docstring
    #: above. Empty for every quarterly and lot-A annual config; lot B's
    #: land use and building condition datasets populate it.
    always_final_columns: tuple[str, ...] = ()


LEASES = AgdpDatasetConfig(
    label="SPF Finances real-estate leases (52.01.01)",
    uuid="84d5f470-51ca-11eb-8a67-3448ed25ad7c",
    required_columns=(
        "NISCode",
        "NameFre",
        "NameDut",
        "NameGer",
        "RegistrationType",
        "LessorType",
        "TakerType",
        "RentsNumber",
        "RentP25",
        "RentP50",
        "RentP75",
        "ChargesP25",
        "ChargesP50",
        "ChargesP75",
        "TotalRentP25",
        "TotalRentP50",
        "TotalRentP75",
    ),
    select={
        "RegistrationType": "HousingRegistration",
        "LessorType": "TOTAL",
        "TakerType": "TOTAL",
    },
    count_column="RentsNumber",
    indicators={
        "RentsNumber": ("MUN_LEASES_NEW_HOUSING", True),
        "RentP50": ("MUN_LEASE_RENT_MEDIAN_HOUSING", False),
        "ChargesP50": ("MUN_LEASE_CHARGES_MEDIAN_HOUSING", False),
    },
)

TRANSACTIONS = AgdpDatasetConfig(
    label="SPF Finances real-estate transactions (52.01.02)",
    uuid="89209670-51ca-11eb-beeb-3448ed25ad7c",
    required_columns=(
        "NISCode",
        "NameFre",
        "NameDut",
        "NameGer",
        "TransactionType",
        "ParcelNature",
        "ParcelsNumber",
        "PriceP25",
        "PriceP50",
        "PriceP75",
        "ParcelsAreaP25",
        "ParcelsAreaP50",
        "ParcelsAreaP75",
    ),
    select={"TransactionType": "VENTEIMMEUB", "ParcelNature": "TOTAL"},
    count_column="ParcelsNumber",
    indicators={
        "ParcelsNumber": ("MUN_PROPERTY_SALES", True),
    },
)


#: NIS codes are five-digit strings in every live dataset; the Owner
#: Occupants fictitious placeholder set publishes NISCode "N/A" instead.
_ALL_DIGIT_NIS = re.compile(r"^\d+$")


def _skip_fictitious_and_non_numeric_nis(row: dict[str, str]) -> bool:
    """Owner Occupants row filter (handoff): skip the one Fictious=1/
    NISCode="N/A" placeholder set, and any row whose NISCode is not
    all-digit -- applied before `select`, so `_parse`'s own blank-NIS and
    zero-matched-rows guards never have to special-case it."""
    if row.get("Fictious", "").strip() != "0":
        return False
    nis = row.get("NISCode", "").strip()
    return bool(_ALL_DIGIT_NIS.match(nis))


OWNER_OCCUPANTS = AgdpDatasetConfig(
    label="SPF Finances owner occupants (52.01.14)",
    uuid="a54ced71-dcc5-4b51-99f5-40b391631727",
    required_columns=(
        "NISCode",
        "Fictious",
        "NameFre",
        "NameDut",
        "NameGer",
        "PersonType",
        "HousingRightType",
        "PersonNumber",
    ),
    select={"PersonType": "Total", "HousingRightType": "OCCUPANTPUPES"},
    count_column="PersonNumber",
    indicators={
        "PersonNumber": ("MUN_OWNER_OCCUPIERS", True),
    },
    frequency="A",
    row_filter=_skip_fictitious_and_non_numeric_nis,
)

PROPERTY_DYNAMICS = AgdpDatasetConfig(
    label="SPF Finances real-estate property dynamics (52.01.24)",
    uuid="219cd997-631a-11f0-bb32-00be432db085",
    required_columns=(
        "NISCode",
        "NameFre",
        "NameDut",
        "NameGer",
        "ParcelNature",
        "ParcelsNumber",
        "PropertyDurationP25",
        "PropertyDurationP50",
        "PropertyDurationP75",
        "PropertyDurationMean",
        "PropertyRotationMean",
    ),
    select={"ParcelNature": "TOTAL"},
    count_column="ParcelsNumber",
    indicators={
        "ParcelsNumber": ("MUN_PARCELS_OWNED", True),
        "PropertyDurationP50": ("MUN_OWNERSHIP_DURATION_MEDIAN", False),
        "PropertyRotationMean": ("MUN_OWNERSHIP_ROTATION_MEAN", False),
    },
    frequency="A",
)


# --- Wave 5 lot B: patrimony (land use, building condition, tax exemptions) -

LAND_USE = AgdpDatasetConfig(
    label="SPF Finances land use (52.01.04)",
    uuid="86999d70-51ca-11eb-9238-3448ed25ad7c",
    required_columns=(
        "NISCode",
        "NameFre",
        "NameDut",
        "NameGer",
        "ParcelNature",
        "ParcelsNumber",
        "TotalCadastralIncome",
        "TaxableCadastralIncome",
        "TaxExemptCadastralIncome",
    ),
    select={"ParcelNature": "TOTAL"},
    count_column="ParcelsNumber",
    indicators={
        "ParcelsNumber": ("MUN_CADASTRAL_PARCELS_TOTAL", True),
        "TotalCadastralIncome": ("MUN_CADASTRAL_INCOME_TOTAL", False),
        "TaxableCadastralIncome": ("MUN_CADASTRAL_INCOME_TAXABLE", False),
    },
    frequency="A",
    always_final_columns=("TotalCadastralIncome", "TaxableCadastralIncome"),
)

BUILDING_CONDITION = AgdpDatasetConfig(
    label="SPF Finances building condition (52.01.05)",
    uuid="857b351e-51ca-11eb-a86d-3448ed25ad7c",
    required_columns=(
        "NISCode",
        "NameFre",
        "NameDut",
        "NameGer",
        "ParcelNature",
        "ParcelsNumber",
        "CentralHeating",
    ),
    select={"ParcelNature": "TOTAL"},
    count_column="ParcelsNumber",
    indicators={
        "ParcelsNumber": ("MUN_BUILDINGS_TOTAL", True),
        "CentralHeating": ("MUN_BUILDINGS_CENTRAL_HEATING", False),
    },
    frequency="A",
    always_final_columns=("CentralHeating",),
)

TAX_EXEMPTIONS = AgdpDatasetConfig(
    label="SPF Finances property-tax exemptions (52.01.03)",
    uuid="8607969e-51ca-11eb-8d7d-3448ed25ad7c",
    required_columns=(
        "NISCode",
        "NameFre",
        "NameDut",
        "NameGer",
        "ExemptionType",
        "ParcelsNumber",
    ),
    select={"ExemptionType": "TOTAL"},
    count_column="ParcelsNumber",
    indicators={
        "ParcelsNumber": ("MUN_PARCELS_TAX_EXEMPT", True),
    },
    frequency="A",
)


def _to_int_or_blank(raw: str) -> int | None:
    raw = raw.strip()
    return None if raw == "" else int(raw)


def _to_float_or_blank(raw: str) -> float | None:
    raw = raw.strip()
    return None if raw == "" else float(raw)


def _annual_row_to_observations(
    row: dict[str, str], *, config: AgdpDatasetConfig, quarter: str, context: str
) -> list[dict]:
    """Wave 5's annual five-state mapping (lot A: Owner Occupants / Property
    Dynamics; lot B: land use / building condition / tax exemptions) --
    simpler than the quarterly Leases/Transactions mapping below in one way
    (no blanket 1-4 suppression tier applies to a *count* column here) but
    NOT in every way: lot B measured a real 1-4 tier on the *non-count*
    cadastral-income columns of two lot B datasets (land use, tax
    exemptions) at low ParcelsNumber -- landuse 2026 blanks
    TotalCadastralIncome/TaxableCadastralIncome for 30,171 rows at
    ParcelsNumber 1-4; exempt 2026 blanks TotalCadastralIncome for 156 rows
    at ParcelsNumber 1-4; the relation is exact: 0 -> not blank, 1-4 ->
    blank, >=5 -> not blank. This does NOT apply to any of lot B's six
    chosen columns (verified 2026/2025/2017: never blank at TOTAL), so no
    suppression mapping is added here -- every lot B non-count column stays
    on the same two-branch mapping below (final needs a non-blank cell; blank
    count -> na). It is recorded here only so the next dataset added to this
    function does not assume "no suppression tier exists in the annual
    shape" -- it does, just not on any column this batch reads.

    - count column: a numeric value, including a real 0, is written as-is,
      final (every count in every dataset routed through this function is
      always published for a live commune). A BLANK count cell, however, is
      a schema surprise, not a real zero -- CLAUDE.md rule 26 forbids
      collapsing "missing" into "measured zero" -- so a blank count raises
      AgdpSchemaError naming the dataset, quarter/year and NIS rather than
      silently writing a fabricated 0.0 (fixed here: the previous code
      wrote `0.0 if count is None else float(count)`, which is exactly that
      fabrication; no live cell for any dataset routed through this
      function has ever actually been blank at a count column, per the
      handoff's own verification, so this only changes behaviour if that
      measured invariant is ever violated).
    - non-count column (a percentile/mean/cadastral figure), count column
      >= 1: value is present -> value, final.
    - non-count column, count column blank or 0: SPF writes a literal 0 for
      a duration/mean when the row has no parcels -- that is not a measured
      figure, so it maps to value None, status na, never a fabricated zero.
      A literal 0 count itself is still a genuine measured zero and stays
      final (e.g. tax-exempt parcel counts: NIS 44001 in 2017 has
      ExemptionType=TOTAL ParcelsNumber=0 -- final, not na).
      EXCEPTION: a column named in `config.always_final_columns` (lot B's
      TotalCadastralIncome/TaxableCadastralIncome/CentralHeating -- each its
      own independent additive figure, never derived by dividing by the
      count) is never mapped to na this way: its own cell is read and
      written, final, regardless of the count column's value, and a blank
      cell there is always a schema surprise (raises), never a legitimate
      na state. All six of lot B's chosen columns are additive by the
      handoff's own indicator classification, so none of them should ever
      collapse into an unmeasured/na state at TOTAL -- that state genuinely
      does not arise for these six (see docs/features/spf_agdp.md).

    Raises AgdpSchemaError if a non-count column is unexpectedly blank while
    its count column is >= 1, or if an `always_final_columns` column is
    blank at all -- the measured "the file never blanks a
    percentile/mean/cadastral figure at TOTAL" rule no longer holds
    (CLAUDE.md rule 13).
    """
    nis = row["NISCode"].strip()
    if not nis:
        raise AgdpSchemaError(f"{context}: a data row has a blank NISCode.")

    count_raw = row[config.count_column]
    count = _to_int_or_blank(count_raw)

    results: list[dict] = []
    for csv_column, (indicator_id, is_count) in config.indicators.items():
        if is_count:
            if count is None:
                raise AgdpSchemaError(
                    f"{context}, NIS {nis}: count column {config.count_column!r} is blank. "
                    "A blank count is a schema surprise, not a measured zero -- refusing to "
                    "fabricate a 0.0 (CLAUDE.md rule 26)."
                )
            results.append(
                {
                    "geo_id": nis,
                    "period": quarter,
                    "value": float(count),
                    "status": "final",
                    "indicator_id": indicator_id,
                }
            )
            continue

        raw_cell = row[csv_column].strip()
        if csv_column in config.always_final_columns:
            if raw_cell == "":
                raise AgdpSchemaError(
                    f"{context}, NIS {nis}: {csv_column} is blank -- this column is its own "
                    "independent additive figure (config.always_final_columns), never derived "
                    "from the row count, so a blank cell is always a schema surprise. The "
                    "measured rule (never blank at TOTAL) no longer holds (CLAUDE.md rule 13)."
                )
            results.append(
                {
                    "geo_id": nis,
                    "period": quarter,
                    "value": float(raw_cell),
                    "status": "final",
                    "indicator_id": indicator_id,
                }
            )
        elif count is None or count == 0:
            results.append(
                {
                    "geo_id": nis,
                    "period": quarter,
                    "value": None,
                    "status": "na",
                    "indicator_id": indicator_id,
                }
            )
        else:
            if raw_cell == "":
                raise AgdpSchemaError(
                    f"{context}, NIS {nis}: {csv_column} is blank but "
                    f"{config.count_column} is {count} (>= 1) -- expected a published "
                    "value. The measured rule (no blank percentile/mean when "
                    "ParcelsNumber >= 1) no longer holds (CLAUDE.md rule 13)."
                )
            results.append(
                {
                    "geo_id": nis,
                    "period": quarter,
                    "value": float(raw_cell),
                    "status": "final",
                    "indicator_id": indicator_id,
                }
            )
    return results


def _row_to_observations(
    row: dict[str, str], *, config: AgdpDatasetConfig, quarter: str, context: str
) -> list[dict]:
    """The five-state mapping (CLAUDE.md rule 26), applied once per row to
    every indicator column `config.indicators` names:

    - count column: blank means "the SPF published none of that kind" (every
      live commune already has a row for the quarter -- a missing row is a
      commune that did not exist, handled by the caller, not this function)
      -> value 0.0, status final. A numeric value, including 0, is written
      as-is, final.
    - percentile column, count column >= 5: value is present (SPF only
      blanks a percentile below the 5-count threshold) -> value, final.
    - percentile column, count column in 1..4: percentile is blank by
      construction -> value None, status suppressed.
    - percentile column, count column blank or 0: value None, status na.

    Raises AgdpSchemaError if a percentile column is unexpectedly non-blank
    below the suppression threshold, or unexpectedly blank at or above it --
    either means the measured suppression rule (blank count 1-4, minimum
    published median count exactly 5) no longer holds, and this must fail
    loudly rather than silently mis-map a state.

    Dispatches to `_annual_row_to_observations` when `config.frequency ==
    "A"` -- the quarterly path below this check is otherwise byte-for-byte
    unchanged.
    """
    if config.frequency == "A":
        return _annual_row_to_observations(row, config=config, quarter=quarter, context=context)

    nis = row["NISCode"].strip()
    if not nis:
        raise AgdpSchemaError(f"{context}: a data row has a blank NISCode.")

    count_raw = row[config.count_column]
    count = _to_int_or_blank(count_raw)

    results: list[dict] = []
    for csv_column, (indicator_id, is_count) in config.indicators.items():
        if is_count:
            value = 0.0 if count is None else float(count)
            results.append(
                {
                    "geo_id": nis,
                    "period": quarter,
                    "value": value,
                    "status": "final",
                    "indicator_id": indicator_id,
                }
            )
            continue

        raw_cell = row[csv_column].strip()
        if count is None or count == 0:
            if raw_cell != "":
                raise AgdpSchemaError(
                    f"{context}, NIS {nis}: {csv_column} is {raw_cell!r} but "
                    f"{config.count_column} is {count_raw!r} -- expected a blank "
                    "percentile when the row count is blank/zero. Suppression rule "
                    "no longer matches what was measured (CLAUDE.md rule 13)."
                )
            results.append(
                {
                    "geo_id": nis,
                    "period": quarter,
                    "value": None,
                    "status": "na",
                    "indicator_id": indicator_id,
                }
            )
        elif 1 <= count <= 4:
            if raw_cell != "":
                raise AgdpSchemaError(
                    f"{context}, NIS {nis}: {csv_column} is {raw_cell!r} but "
                    f"{config.count_column} is {count} (1-4) -- expected suppression. "
                    "Suppression rule no longer matches what was measured (CLAUDE.md "
                    "rule 13)."
                )
            results.append(
                {
                    "geo_id": nis,
                    "period": quarter,
                    "value": None,
                    "status": "suppressed",
                    "indicator_id": indicator_id,
                }
            )
        else:
            if raw_cell == "":
                raise AgdpSchemaError(
                    f"{context}, NIS {nis}: {csv_column} is blank but "
                    f"{config.count_column} is {count} (>= 5) -- expected a published "
                    "value. Suppression rule no longer matches what was measured "
                    "(CLAUDE.md rule 13)."
                )
            results.append(
                {
                    "geo_id": nis,
                    "period": quarter,
                    "value": float(raw_cell),
                    "status": "final",
                    "indicator_id": indicator_id,
                }
            )
    return results


class AgdpSource(MunicipalTimeSeriesSource):
    """Contract deviation, documented in the module docstring and tested in
    tests/test_source_contract.py: `geo_id` is the raw NIS string (own-period
    resolution is scripts/sync_spf_agdp.py's job), and each row carries a
    fifth key, `indicator_id`, since one CSV row feeds up to three indicators
    (Leases) or one (Transactions).

    `raw_extension` is "csv" -- the base class's `_cache_raw` writes exactly
    the bytes `_parse` receives, and this adapter is always called directly
    with the already-extracted Municipality CSV bytes (never a whole zip;
    scripts/sync_spf_agdp.py owns the ranged zip read and ATOM discovery,
    since those need per-version state this class does not have).
    """

    source_id = "spf_finances"
    adapter = "spf_agdp"
    raw_extension = "csv"

    def __init__(self, config: AgdpDatasetConfig):
        self.config = config

    def _parse(self, raw: bytes, *, quarter: str, **kwargs) -> list[dict]:
        config = self.config
        text = raw.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        if reader.fieldnames is None:
            raise AgdpSchemaError(f"{config.label} {quarter}: Municipality CSV has no header row.")
        missing = [c for c in config.required_columns if c not in reader.fieldnames]
        if missing:
            raise AgdpSchemaError(
                f"{config.label} {quarter}: Municipality CSV is missing required column(s) "
                f"{missing}. Header has {reader.fieldnames}. Refusing to guess a "
                "replacement (CLAUDE.md rule 13)."
            )

        results: list[dict] = []
        matched_rows = 0
        for row in reader:
            if config.row_filter is not None and not config.row_filter(row):
                continue
            if all(row.get(k) == v for k, v in config.select.items()):
                matched_rows += 1
                results.extend(
                    _row_to_observations(
                        row, config=config, quarter=quarter, context=f"{config.label} {quarter}"
                    )
                )

        if matched_rows == 0:
            raise AgdpSchemaError(
                f"{config.label} {quarter}: zero rows matched selector {config.select}. "
                "Refusing to load an empty series (CLAUDE.md rule 13)."
            )
        return results
