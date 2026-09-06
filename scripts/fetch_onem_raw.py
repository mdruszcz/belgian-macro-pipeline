"""Fetch and cache ONEM/RVA's six commune-level unemployment files -- the
fetch half of scripts/sync_onem.py, which parses what this caches.

Kept as its own module because the two jobs fail differently and are worth
reading separately: this one answers "did onem.be answer?", sync_onem.py
answers "did the file say what we expect?". `fetch_all()` is called by
sync_onem.sync(), and this file also stays runnable on its own for
diagnosing a fetch without touching the database.

WHAT THE FILES TURNED OUT TO BE. This module used to carry a long note
saying their columns had never been read and must not be guessed at
(CLAUDE.md rule 13), because this development environment cannot reach
onem.be at all -- DNS resolves, then the TCP handshake times out, the
identical signature statbel.fgov.be gives. That was resolved the only way it
could be: run 34054441348 proved a GitHub Actions runner CAN reach onem.be,
and run 34056982618 uploaded the six fetched files as an artifact so they
could be downloaded and actually opened. They are legacy BIFF .xls (hence
xlrd, not openpyxl), one sheet per year 2017-2026, with the unit declared in
row 3 and the column headers in row 5. See sync_onem.py's docstring for the
full layout and for what the UP/M filename suffixes really mean -- read off
row 3, not inferred from the abbreviation.

RETRIES, AND WHY THIS STILL NEVER RAISES. Transient failures are retried
with backoff; a 4xx is terminal on the first try, since it will not
succeed on the third either. `fetch_all` returns a per-file success map and
`main` exits 0 regardless, so a diagnostic run reports rather than crashes.
sync_onem.py is the layer that decides a missing file is fatal: it raises
FileNotFoundError on a file it needs, so a silent partial load cannot happen
just because a fetch came back empty.
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
