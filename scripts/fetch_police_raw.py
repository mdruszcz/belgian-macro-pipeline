"""Fetch one known-good police.be crime-statistics response -- Block F,
docs/data_catalog.md's police.be row.

WHAT THIS DOES NOT DO: parse anything, or loop over communes. No indicator
has been defined from this source and none should be until a real response
body has been read (CLAUDE.md rule 13). The response is JSON -- the
maintainer's browser confirmed `content-type: application/json`, 11,855
bytes for one commune x 10 years x 12 months -- but its actual keys, its
offence categories, and whether the counts are incidents or something else
have never been seen by anyone here.

WHAT THIS DOES: with a plain requests.Session, visit the site's front page
to pick up whatever session cookie it hands ordinary clients, then issue the
one request the maintainer verified returns 200 from a browser, and cache
the body under data/raw/police/{date}/.

WHY IT EXISTS: this pipeline's own network context is served an HTTP 403
"Maintenance" page by police.be on every request, including the bare front
page, and is handed no cookies at all -- so a session cannot even be
started from here, and the response body's shape cannot be inspected. That
is the same wall statbel.fgov.be and onem.be present (though those fail at
the TCP layer rather than with a 403). Whether GitHub Actions' network is
served the same page is a separate question that only a real run can
answer, exactly as it was for ONEM. If this succeeds, the cached JSON is
what a follow-up needs to write a real parser against; if it 403s there too,
police.be joins the manual-download sources (docs/features/manual_sources.md)
and this step should be removed rather than left running.

ONE PARAMETER IS DELIBERATELY NOT GENERALIZED: the `nis=21012_4` value
carries a `_4` suffix on the NIS code whose meaning is unknown -- police
zone? geography level? a table variant? Guessing at it is precisely the
mistake CLAUDE.md rule 13 exists to prevent (a misread Statbel filename
abbreviation caused a real bug in the Census 2021 CAS table). So this
fetches the maintainer's exact verified request, unchanged, and no attempt
is made to build a per-commune URL until the suffix is understood.
"""

import argparse
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

REQUEST_TIMEOUT = 30
COURTESY_DELAY_SECONDS = 1

HOME_URL = "https://www.police.be/"
STATS_URL = "https://www.police.be/statistiques/fr"
# The maintainer's exact verified request (200 OK, application/json), used
# unchanged -- see the module docstring on the `_4` suffix.
API_URL = (
    "https://www.police.be/statistiques/fr/stats-pol/criminality_table/content"
    "?nis=21012_4&year=2016,2017,2018,2019,2020,2021,2022,2023,2024,2025"
    "&month=1,2,3,4,5,6,7,8,9,10,11,12"
)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
}
XHR_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": HOME_URL,
}

RAW_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "police"


def _get(session: requests.Session, url: str, **kwargs) -> tuple[int, bytes, str]:
    """Returns (status_code, body, error). status_code is 0 on a connection
    failure -- broader than requests.exceptions.RequestException on purpose,
    since this script's whole job is to report on reachability and any
    failure, including a raw OSError from a lower socket layer, must become
    a result rather than an uncaught crash."""
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
    except Exception as exc:
        return 0, b"", str(exc)
    return resp.status_code, resp.content, ""


def probe(session: requests.Session | None = None, out_dir: Path = RAW_CACHE_DIR) -> dict:
    session = session or requests.Session()
    session.headers.update(BROWSER_HEADERS)
    result: dict = {"home": None, "stats": None, "api": None, "cookies": [], "json_bytes": 0}

    for key, url in (("home", HOME_URL), ("stats", STATS_URL)):
        status, body, error = _get(session, url)
        result[key] = status
        print(f"  {url} -> {status or 'connection failed'}{f' ({error})' if error else ''}")
        time.sleep(COURTESY_DELAY_SECONDS)

    result["cookies"] = sorted(session.cookies.get_dict())
    if result["cookies"]:
        print(f"  session cookies received: {', '.join(result['cookies'])}")
    else:
        print("  session cookies received: none -- no session could be started")

    status, body, error = _get(session, API_URL, headers=XHR_HEADERS)
    result["api"] = status
    print(f"  criminality_table -> {status or 'connection failed'}{f' ({error})' if error else ''}")

    if status == 200:
        day_dir = out_dir / date.today().isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        target = day_dir / "criminality_table_nis21012_4.json"
        target.write_bytes(body)
        result["json_bytes"] = len(body)
        print(f"  cached {len(body):,} bytes to {target}")
    return result


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Probe police.be's crime-statistics endpoint and cache one verified "
            "response. Does not parse it -- see module docstring."
        )
    )
    ap.add_argument("--out-dir", type=Path, default=RAW_CACHE_DIR)
    args = ap.parse_args()

    print(f"Probing police.be ({datetime.now(timezone.utc).isoformat()})...")
    result = probe(out_dir=args.out_dir)

    if result["api"] == 200:
        print("\npolice.be IS reachable from this network. The cached JSON can now be read.")
    else:
        print(
            "\n::warning::police.be returned no usable response. If this is the same HTTP 403 "
            "'Maintenance' page this pipeline's own network is served, police.be cannot be "
            "automated and belongs in docs/features/manual_sources.md -- see "
            "docs/data_catalog.md, and remove this step from the workflow."
        )
    # Exit 0 either way: this step reports on reachability while that is
    # still unknown, and must never fail the daily run over it.
    sys.exit(0)


if __name__ == "__main__":
    main()
