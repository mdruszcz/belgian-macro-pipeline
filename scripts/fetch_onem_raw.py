"""Fetch and cache ONEM/RVA's six commune-level unemployment files -- Block F,
docs/data_catalog.md's ONEM/RVA row.

WHAT THIS DOES NOT DO YET: parse the files into indicators. Reads through
these six .xls files' actual column layout have never happened -- this
environment cannot reach onem.be to inspect one (confirmed: DNS resolves,
then the TCP handshake itself times out, the identical signature
statbel.fgov.be gives). CLAUDE.md rule 13 is explicit that a source schema
must never be guessed at, so building `_parse` before a real file has been
seen would be exactly the mistake that rule exists to prevent.

WHAT THIS DOES: attempts a real HTTP fetch of each URL and caches whatever
comes back under data/raw/onem/{date}/, with retry on transient failures.
That is deliberately the whole scope of this script -- it is the test of
whether GitHub Actions' network can reach onem.be, which this environment
cannot answer on its own. If it succeeds, the cached files are exactly what
a follow-up needs to write EXTRACTS for a proper sync_onem.py (the same
shape as scripts/sync_census2021.py); if it fails the same way statbel.fgov.be
does, ONEM becomes a fourth manual-download source (docs/features/manual_sources.md)
rather than an automated one, and this step should be removed or changed to
continue-on-error permanently rather than paged on.

FILENAMES, undecoded rather than guessed at: CCI/CT/TTP/EMPL likely stand for
different unemployment-benefit categories and UP/M likely distinguish two
report granularities, but that is exactly the kind of reading of an
abbreviation that led to a real bug in the Census 2021 CAS table (see
docs/data_catalog.md) -- guessed wrong there, corrected only once the actual
file was opened. So the six filenames are used here exactly as ONEM
publishes them, undecoded, until a downloaded file can be read.
"""

import argparse
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

REQUEST_TIMEOUT = 30
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = (2, 5, 10)

BASE_URL = "https://www.onem.be/sites/default/files/assets/statistiques/113"
FILES = [
    "CCI_Commune_Statut_UP_FR.xls",
    "CCI_Commune_Statut_M_FR.xls",
    "CT_Commune_Statut_UP_FR.xls",
    "CT_Commune_Statut_M_FR.xls",
    "TTP_Commune_Statut_UP_FR.xls",
    "EMPL_Commune_Statut_M_FR.xls",
]

RAW_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "onem"


def _fetch_one(filename: str) -> tuple[bool, str]:
    url = f"{BASE_URL}/{filename}"
    last_error = ""
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        except Exception as exc:
            # Broader than requests.exceptions.RequestException on purpose:
            # this script's whole job is to report on reachability while
            # that is still unknown, so ANY failure to connect -- including a
            # raw OSError a lower socket layer can raise -- must become a
            # per-file result, never an uncaught crash of the whole run.
            last_error = str(exc)
            if attempt < MAX_ATTEMPTS - 1:
                print(f"  {filename}: {exc} (attempt {attempt + 1}/{MAX_ATTEMPTS}, retrying)")
                time.sleep(BACKOFF_SECONDS[attempt])
                continue
            return False, last_error
        if resp.status_code >= 500 and attempt < MAX_ATTEMPTS - 1:
            last_error = f"HTTP {resp.status_code}"
            print(
                f"  {filename}: HTTP {resp.status_code} (attempt {attempt + 1}/{MAX_ATTEMPTS}, retrying)"
            )
            time.sleep(BACKOFF_SECONDS[attempt])
            continue
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        return True, resp.content
    return False, last_error


def fetch_all(out_dir: Path = RAW_CACHE_DIR) -> dict[str, bool]:
    day_dir = out_dir / date.today().isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, bool] = {}
    for filename in FILES:
        ok, payload = _fetch_one(filename)
        results[filename] = ok
        if ok:
            (day_dir / filename).write_bytes(payload)
            print(f"  {filename}: OK, {len(payload):,} bytes")
        else:
            print(f"  {filename}: FAILED -- {payload}")
    return results


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Fetch ONEM/RVA's six commune-level unemployment files, caching whatever "
            "succeeds. Does not parse them -- see module docstring."
        )
    )
    ap.add_argument("--out-dir", type=Path, default=RAW_CACHE_DIR)
    args = ap.parse_args()

    print(f"Fetching {len(FILES)} ONEM/RVA files ({datetime.now(timezone.utc).isoformat()})...")
    results = fetch_all(args.out_dir)

    succeeded = sum(results.values())
    print(f"\n{succeeded}/{len(FILES)} succeeded.")
    if succeeded == 0:
        print(
            "::warning::All ONEM/RVA fetches failed. If this is a connection-level block "
            "(the same signature statbel.fgov.be gives), see docs/data_catalog.md -- ONEM "
            "should be treated as a manual-download source like Census 2021, not an "
            "automated one, and this step removed from the daily workflow."
        )
    # Deliberately exit 0 either way: this step is continue-on-error in the
    # workflow while its reachability is still being established, so a
    # failure here must not fail the whole daily run.
    sys.exit(0)


if __name__ == "__main__":
    main()
