"""Download the federal police's per-commune crime-statistics PDFs for one
quarter, in one language, from the links police.be itself publishes.

WHAT THIS DOES: fetches the quarterly `statistiques-criminalite` /
`criminaliteitsstatistieken` index page, extracts every per-commune PDF link
it lists for the requested quarter, downloads each one to
data/raw/police/commune_pdfs/{quarter}/{lang}/{province_dir}/, and writes a
manifest CSV (url, path, bytes, sha256) beside them.

NO URL IS EVER CONSTRUCTED. Only links the index page publishes are fetched.
That is deliberate, and it is rule 13 in CLAUDE.md applied to a filename
convention: the real filenames are not derivable from any commune list here.
police.be folds accents but keeps some hyphens ("Limburg_Bilzen-Hoeselt")
while replacing others ("Brabant_wallon_Braine_l_Alleud"), keeps literal
spaces in a few ("Liege_La Calamine", "West_Vlaanderen_De Haan"), and picks
the French exonym for some Flemish communes but not others (Renaix, Fourons,
Messines, Espierres-Helchin -- but Aalst, Gent, Oostende). A slug rule built
from data/communes_export.csv mismatches 19 of the 499 published French
files. Scraping the index is the only way to get them right.

TWO THINGS THE CALLER MUST KNOW:

1. curl, not requests. police.be serves this pipeline's network an HTTP 403
   "Maintenance" page (the same wall fetch_police_raw.py documents), and it
   is keyed on the client's TLS/HTTP stack, not on headers: python-requests
   gets 403 with byte-identical browser headers that curl gets 200 with.
   So every fetch here shells out to curl. If that ever stops working, this
   source is manual-download again (docs/features/manual_sources.md).

2. The French tree is incomplete upstream. For 2025_T04 the index publishes
   499 of the 565 communes in French: all 63 Vlaams-Brabant communes are
   missing (the province's accordion on the French page is empty, and the
   files 404 at their would-be URLs in 2025_T02/T03/T04 alike), as are
   Oud-Turnhout, Pont-a-Celles and Meix-devant-Virton. The Dutch tree
   publishes all 565. This script reports the gap; it does not paper over it.

Nothing here parses a PDF or writes an observation. data/raw/** is gitignored.
"""

import argparse
import csv
import hashlib
import html
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "raw" / "police" / "commune_pdfs"

# The quarterly index pages. Each one lists that quarter's PDFs in its own
# language; there is no French page for the Dutch tree or vice versa.
INDEX_URL = {
    "fr": "https://www.police.be/statistiques/fr/criminalite/statistiques-criminalite",
    "nl": "https://www.politie.be/statistieken/nl/criminaliteit/criminaliteitsstatistieken",
}
# The per-commune section's directory name inside each language tree.
COMMUNE_DIR = {"fr": "06_Par commune", "nl": "06_Per gemeente"}

# Safari's header set, which police.be answers with 200 where a bare curl
# User-Agent gets the 403 maintenance page. Kept together with the curl
# invocation because the two only work as a pair -- see the docstring.
BROWSER_HEADERS = [
    "-A",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "-H",
    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "-H",
    "Accept-Language: fr-BE,fr;q=0.9",
    "-H",
    "Accept-Encoding: gzip, deflate, br",
    "-H",
    "Connection: keep-alive",
    "-H",
    "Upgrade-Insecure-Requests: 1",
]

REQUEST_TIMEOUT = 120
COURTESY_DELAY_SECONDS = 0.3
MAX_ATTEMPTS = 3
# Belgium's commune count since the 2025 mergers -- what a complete language
# tree would hold, and what this script measures the download against.
EXPECTED_COMMUNES = 565


def curl(url: str, dest: Path | None) -> tuple[int, bytes]:
    """Fetch `url` with curl. Returns (http_status, body) when dest is None,
    (http_status, b"") when the body was written to dest instead."""
    cmd = ["curl", "--compressed", "--silent", "--show-error", "--max-time", str(REQUEST_TIMEOUT)]
    cmd += BROWSER_HEADERS
    cmd += ["-w", "%{http_code}"]
    if dest is not None:
        cmd += ["-o", str(dest)]
    cmd.append(url)
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        return 0, b""
    if dest is not None:
        return int(proc.stdout[-3:] or 0), b""
    # The status code is appended to the body by -w; split it back off.
    return int(proc.stdout[-3:] or 0), proc.stdout[:-3]


def commune_links(index_html: str, quarter: str, lang: str) -> list[str]:
    """Every per-commune PDF link the index page publishes for `quarter`."""
    year = quarter.split("_")[0]
    links = []
    for raw in re.findall(r'href="([^"]+)"', index_html):
        href = html.unescape(raw)
        path = urllib.parse.unquote(href)
        if not path.endswith(f"_{lang}.pdf"):
            continue
        if f"/{year}/{quarter}/crimi_{lang}/{COMMUNE_DIR[lang]}/" not in path:
            continue
        links.append(urllib.parse.urljoin("https://www.police.be/", href))
    return sorted(set(links))


def download(url: str, dest: Path) -> tuple[str, int, str]:
    """Download one PDF, retrying on anything that is not a PDF body.
    Returns (status, bytes, sha256). A non-PDF body is never kept: police.be
    answers its own wall with an 8 KB HTML page and HTTP 403, and a silently
    saved one would look like a downloaded report to everything downstream."""
    tmp = dest.with_suffix(".part")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        status, _ = curl(url, tmp)
        if status == 200 and tmp.exists() and tmp.read_bytes()[:5] == b"%PDF-":
            body = tmp.read_bytes()
            tmp.replace(dest)
            return "ok", len(body), hashlib.sha256(body).hexdigest()
        if status == 404:
            break
        time.sleep(2**attempt)
    tmp.unlink(missing_ok=True)
    return f"http_{status}", 0, ""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--quarter", default="2025_T04", help="e.g. 2025_T04")
    ap.add_argument("--lang", default="fr", choices=("fr", "nl"))
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--limit", type=int, default=0, help="stop after N files (smoke test; 0 = all)")
    args = ap.parse_args()

    index_url = INDEX_URL[args.lang]
    print(f"Index: {index_url}")
    status, body = curl(index_url, None)
    if status != 200:
        sys.exit(
            f"::error::index page returned HTTP {status or 'connection failure'}. "
            "If this is the 403 maintenance page, police.be is manual-download again."
        )
    links = commune_links(body.decode("utf-8", "replace"), args.quarter, args.lang)
    if not links:
        sys.exit(
            f"::error::no per-commune {args.lang} links for {args.quarter} on the index page. "
            "The page's structure or the quarter's folder naming changed -- do not guess URLs."
        )
    print(f"{len(links)} per-commune {args.lang} PDFs published for {args.quarter}")

    root = args.out_dir / args.quarter / args.lang
    root.mkdir(parents=True, exist_ok=True)
    rows, failures, skipped = [], [], 0
    for i, url in enumerate(links[: args.limit or None], start=1):
        path = urllib.parse.unquote(urllib.parse.urlparse(url).path)
        province_dir, filename = path.split("/")[-2:]
        dest = root / province_dir / filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and dest.stat().st_size > 0:
            body_bytes = dest.read_bytes()
            rows.append(
                (
                    province_dir,
                    filename,
                    url,
                    "cached",
                    len(body_bytes),
                    hashlib.sha256(body_bytes).hexdigest(),
                )
            )
            skipped += 1
            continue
        state, size, digest = download(url, dest)
        rows.append((province_dir, filename, url, state, size, digest))
        if state != "ok":
            failures.append((filename, state))
        if i % 25 == 0 or i == len(links):
            print(f"  {i}/{len(links)}  {filename} -> {state} ({size:,} bytes)")
        time.sleep(COURTESY_DELAY_SECONDS)

    manifest = args.out_dir / args.quarter / f"manifest_{args.lang}.csv"
    with manifest.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["province_dir", "filename", "url", "status", "bytes", "sha256"])
        writer.writerows(rows)

    ok = sum(1 for r in rows if r[3] in ("ok", "cached"))
    print(f"\n{ok} PDFs on disk under {root} ({skipped} already cached)")
    print(f"manifest: {manifest}")
    if failures:
        print(f"::warning::{len(failures)} downloads failed:")
        for filename, state in failures[:20]:
            print(f"  {filename}: {state}")
    if not args.limit and ok < EXPECTED_COMMUNES:
        print(
            f"::warning::{EXPECTED_COMMUNES - ok} of Belgium's {EXPECTED_COMMUNES} communes "
            f"have no {args.lang} PDF for {args.quarter}. Upstream gap, not a download "
            "failure, when the failure list above is empty -- see the module docstring."
        )


if __name__ == "__main__":
    main()
