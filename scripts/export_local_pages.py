"""Generate one permanent, statically-rendered page per commune -- Block L's
"Permanent URLs" step, docs/features/comparison.md.

WHY STATIC AND NOT JUST local.html?nis=11002. local.html is a JavaScript
application: it fetches a payload and renders in the browser. A crawler that
does not execute JavaScript sees an empty shell, so the entire SEO position
the roadmap depends on ("thousands of genuinely useful pages") does not
exist. These pages carry their commune's real figures in the HTML itself.

ROUTE SHAPE, decided in comparison.md before anything was built:
`local/{nis}/index.html`, giving a clean `/local/11002` URL on GitHub Pages
with no server rewrite rules. 565 files, one per current commune.

THE INDICATOR ROUTE (`/local/{nis}/{indicator}`) IS DELIBERATELY NOT BUILT
HERE. comparison.md computed the file count in advance rather than
discovering it later: 565 commune pages plus up to 7,910 indicator pages is
8,475 files per build today, and ~113,000 at the roadmap's 200-indicator
target, which is not viable as one directory per pair. That route is gated
on a platform-limit check and a subset rule, and the commune route ships
first. Adding it later is a second loop here, not a rewrite.

THIN PAGES ARE BANNED (comparison.md, brought forward from roadmap Block
AD): a commune with no values at all gets no page and therefore no URL for
Google to index and demote the domain over. Measured on the real data, no
current commune actually trips this -- but the rule is enforced rather than
assumed, because the failure it prevents is silent.

THE ATTRIBUTION BLOCK IS EXTRACTED FROM communes.html, NOT RETYPED. It is a
licence condition (docs/data_catalog.md: Statbel's 2015 licence terminates
automatically on non-compliance), and a second hand-written copy would drift
from the first the moment either is edited. One source of truth, lifted at
build time, with the `attrUpdated` placeholder filled with the real date --
which is itself the "date of last update" the same licence requires.

NO PER-PAGE BUILD STAMP, DELIBERATELY. An earlier version wrote
`<!-- build:{id} -->` into every page's footer. That made all 565 files
change on every single run even when not one figure had moved -- 565 files
of git churn per day, on a repository already growing ~18 MB a commit from
the database alone. public/data/manifest.json already records build_id,
git_commit and build_date centrally, so the per-page copy bought nothing.
A page now changes if and only if its DATA changed, which is also what makes
the URL-stability property absolute rather than "identical apart from one
line".

ENGLISH ONLY, FOR NOW, AND THIS IS THE ONE THING WORTH ARGUING WITH. The
payloads already carry trilingual names and the sections config carries
trilingual labels, so generating FR and NL is the same loop with a different
language key -- 1,695 files rather than 565. It is not done here because the
full trilingual interface is roadmap Block X's own step and the attribution
text extracted above exists only in English on communes.html; generating a
French page around an English licence notice would be worse than not
generating it. `_render_page` takes `lang` so that block is a loop, not a
rewrite.
"""

import argparse
import html
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEFAULT_PAYLOAD_DIR = REPO / "public" / "data"
DEFAULT_OUT_DIR = REPO / "local"
DEFAULT_BASE_URL = "https://mdruszcz.github.io/belgian-macro-pipeline"
ATTRIBUTION_SOURCE = REPO / "communes.html"
DEFAULT_DB = REPO / "data" / "belgian_macro.db"

# Depth from local/{nis}/index.html back to the site root, used for every
# asset and payload link on the page.
ROOT_PREFIX = "../.."

LANGS = ("en", "fr", "nl")


def _read_attribution(path: Path = ATTRIBUTION_SOURCE) -> str:
    """The `.attribution` block's inner HTML, lifted from communes.html.

    See the module docstring: this is licence text, and a retyped copy is a
    licence risk rather than a maintenance annoyance.
    """
    source = path.read_text(encoding="utf-8")
    match = re.search(r'<div class="attribution" id="attribution">(.*?)</div>', source, re.S)
    if not match:
        raise ValueError(
            f"{path.name} has no .attribution block to lift. It is a licence condition "
            "(docs/data_catalog.md) and these pages must not invent their own copy -- "
            "refusing to generate pages without it."
        )
    return match.group(1).strip()


def _latest(entry: dict) -> tuple[str, dict] | tuple[None, None]:
    """The most recent period holding a value, and that period's cell.

    Periods sort lexically by the data model's own design (docs/features/
    data_model.md), so "2026" > "2025" and "2024-Q4" > "2024-Q1" both hold.
    A cell with no value (a suppressed one) is skipped rather than shown as
    the latest reading, matching how communes.html renders the same case.
    """
    periods = entry.get("periods") or {}
    for period in sorted(periods, reverse=True):
        cell = periods[period]
        if cell.get("value") is not None:
            return period, cell
    return None, None


def _format_value(value: float, unit: str | None) -> str:
    if value is None:
        return ""
    unit = unit or ""
    if unit == "eur":
        return f"€{value:,.0f}"
    if unit.startswith("percent"):
        return f"{value:,.2f}%".replace(".00%", "%")
    if abs(value - round(value)) < 1e-9:
        return f"{round(value):,}"
    return f"{value:,.2f}"


def _local_name(names: dict | None, lang: str, fallback: str) -> str:
    if not names:
        return fallback
    return names.get(lang) or names.get("en") or fallback


def _has_any_value(commune: dict) -> bool:
    for entry in (commune.get("indicators") or {}).values():
        period, _cell = _latest(entry)
        if period is not None:
            return True
    return False


def _describe(commune: dict, lang: str) -> str:
    """The meta description: real figures, not a template with a name slotted
    in, since a description that says nothing is why thin pages get demoted."""
    name = _local_name(commune.get("name"), lang, commune["nis_code"])
    indicators = commune.get("indicators") or {}
    bits: list[str] = []
    for indicator_id in ("POPULATION_BY_COMMUNE", "AVG_NET_TAXABLE_INCOME", "MEDIAN_HOUSE_PRICE"):
        entry = indicators.get(indicator_id)
        if not entry:
            continue
        period, cell = _latest(entry)
        if period is None:
            continue
        label = _local_name(entry.get("names"), lang, indicator_id)
        bits.append(f"{label} {_format_value(cell['value'], entry.get('unit'))} ({period})")
    covered = sum(1 for e in indicators.values() if _latest(e)[0] is not None)
    lead = f"Municipal statistics for {name}, Belgium"
    if bits:
        lead += ": " + "; ".join(bits)
    return f"{lead}. {covered} indicators with history, sources and comparisons."


SOURCE_HOMEPAGES = {
    "statbel": "https://statbel.fgov.be/",
    "onem": "https://www.onem.be/",
    "police": "https://www.police.be/statistiques/",
}


def _indicator_sources(db_path: Path) -> dict[str, dict[str, str]]:
    """indicator_id -> {source_id, agency}, read from the indicators table.

    Read rather than hardcoded because the mapping changes every time a
    source is added, and a stale hardcoded copy would misattribute a figure
    -- which for a licence notice is the failure that matters.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT i.indicator_id, i.source_id, s.agency FROM indicators i "
            "JOIN sources s ON s.source_id = i.source_id"
        ).fetchall()
    finally:
        conn.close()
    return {row[0]: {"source_id": row[1], "agency": row[2]} for row in rows}


def _creators_for(commune: dict, sources: dict[str, dict[str, str]]) -> list[dict]:
    """The agencies that actually contributed a value to THIS commune's page,
    deduplicated and ordered so two builds produce identical bytes."""
    agencies: dict[str, str] = {}
    for indicator_id, entry in (commune.get("indicators") or {}).items():
        if _latest(entry)[0] is None:
            continue
        meta = sources.get(indicator_id)
        if not meta:
            continue
        agencies[meta["agency"]] = meta["source_id"]
    creators = []
    for agency in sorted(agencies):
        creator = {"@type": "Organization", "name": agency}
        url = SOURCE_HOMEPAGES.get(agencies[agency])
        if url:
            creator["url"] = url
        creators.append(creator)
    return creators


def _json_ld(
    commune: dict,
    lang: str,
    canonical: str,
    base_url: str,
    updated: str | None,
    creators: list[dict],
) -> str:
    """schema.org/Dataset.

    `creator` IS A LIST OF THE AGENCIES THAT ACTUALLY CONTRIBUTED TO THIS
    PAGE, and there is deliberately NO blanket `license`. The first version
    of this generator asserted a single creator (Statbel) and a single
    licence (CC BY 4.0) on every page -- which was true when the only
    municipal source was Statbel and is false now that these pages carry ONEM
    and police.be figures too. ONEM's own conditions are explicitly not CC BY
    4.0 and police.be's are thinner still (docs/data_catalog.md), and the
    VISIBLE attribution on this very page says so in as many words. Structured
    data that contradicts the visible notice is worse than structured data
    that omits the field: a crawler would be told these figures are CC BY 4.0
    when two of the three sources never granted that.

    `dateModified` stays: it is the same date-of-last-update obligation the
    visible attribution satisfies, expressed in the form a crawler reads.
    """
    name = _local_name(commune.get("name"), lang, commune["nis_code"])
    indicators = commune.get("indicators") or {}
    periods = [p for e in indicators.values() for p in (e.get("periods") or {})]
    payload = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"{name} — municipal statistics",
        "description": _describe(commune, lang),
        "url": canonical,
        "identifier": commune["nis_code"],
        "isAccessibleForFree": True,
        "creator": creators,
        "spatialCoverage": {
            "@type": "Place",
            "name": name,
            "identifier": f"NIS {commune['nis_code']}",
            "containedInPlace": {
                "@type": "Place",
                "name": commune.get("province") or commune.get("region") or "Belgium",
            },
        },
        "distribution": {
            "@type": "DataDownload",
            "encodingFormat": "application/json",
            "contentUrl": f"{base_url}/public/data/communes/{commune['nis_code']}.json",
        },
    }
    if periods:
        payload["temporalCoverage"] = f"{min(periods)[:4]}/{max(periods)[:4]}"
    if updated:
        payload["dateModified"] = updated
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _latest_updated(commune: dict) -> str | None:
    dates = [e["updated"] for e in (commune.get("indicators") or {}).values() if e.get("updated")]
    return max(dates) if dates else None


def _section_rows(commune: dict, section: dict, lang: str) -> list[tuple[str, str, str, str]]:
    """(label, value, period, updated) per indicator this section lists that
    this commune actually has a value for. An indicator with no value is
    omitted rather than rendered as an empty row -- the page is a summary,
    and local.html remains the place that explains WHY a figure is missing."""
    indicators = commune.get("indicators") or {}
    ids = [section["headline"]] if section.get("headline") else []
    ids += list(section.get("indicators") or [])

    rows = []
    seen = set()
    for indicator_id in ids:
        if indicator_id in seen:
            continue
        seen.add(indicator_id)
        entry = indicators.get(indicator_id)
        if not entry:
            continue
        period, cell = _latest(entry)
        if period is None:
            continue
        rows.append(
            (
                _local_name(entry.get("names"), lang, indicator_id),
                _format_value(cell["value"], entry.get("unit")),
                period,
                entry.get("updated") or "",
            )
        )
    return rows


def _render_page(
    commune: dict,
    sections: list[dict],
    lang: str,
    base_url: str,
    attribution: str,
    build_id: str,
    creators: list[dict],
) -> str:
    nis = commune["nis_code"]
    name = _local_name(commune.get("name"), lang, nis)
    canonical = f"{base_url}/local/{nis}/"
    description = _describe(commune, lang)
    updated = _latest_updated(commune)
    esc = html.escape

    ancestry = " · ".join(
        part
        for part in (
            commune.get("arrondissement"),
            commune.get("province"),
            commune.get("region"),
            "Belgium",
        )
        if part
    )

    body: list[str] = []
    covered = 0
    for section in sections:
        rows = _section_rows(commune, section, lang)
        if not rows:
            continue
        covered += len(rows)
        label = _local_name(section.get("label"), lang, section["id"])
        body.append(f"<section><h2>{esc(label)}</h2>")
        body.append(
            "<table><thead><tr><th>Indicator</th><th>Value</th><th>Period</th>"
            "<th>Updated</th></tr></thead><tbody>"
        )
        for indicator_label, value, period, indicator_updated in rows:
            body.append(
                f"<tr><td>{esc(indicator_label)}</td><td class='v'>{esc(value)}</td>"
                f"<td>{esc(period)}</td><td>{esc(indicator_updated)}</td></tr>"
            )
        body.append("</tbody></table></section>")

    attribution_html = attribution
    if updated:
        attribution_html = attribution_html.replace(
            '<span id="attrUpdated">&mdash;</span>', esc(updated)
        ).replace('<span id="attrUpdated">—</span>', esc(updated))

    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{esc(name)} — municipal statistics | BelPulse</title>
<meta name="description" content="{esc(description)}">
<link rel="canonical" href="{esc(canonical)}">
<meta property="og:type" content="website">
<meta property="og:title" content="{esc(name)} — municipal statistics">
<meta property="og:description" content="{esc(description)}">
<meta property="og:url" content="{esc(canonical)}">
<meta name="robots" content="index,follow">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Spectral:wght@500;600&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>
  :root{{
    --bg:#f7f5f0; --surface:#fff; --border:#e3ddd0; --text:#241f18;
    --text-muted:#6e6558; --text-faint:#948a76; --accent-ink:#5c3d18; --row-alt:#fbfaf6;
  }}
  @media (prefers-color-scheme: dark){{
    :root{{
      --bg:#1a1815; --surface:#221f1a; --border:#3a352c; --text:#ece6d8;
      --text-muted:#a79d89; --text-faint:#786f5d; --accent-ink:#f0d4a8; --row-alt:#1f1d18;
    }}
  }}
  *{{box-sizing:border-box}} html,body{{margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);
    font-family:"IBM Plex Sans",system-ui,sans-serif;font-size:14px;line-height:1.5}}
  .wrap{{max-width:900px;margin:0 auto;padding:40px 24px 80px}}
  h1{{font-family:"Spectral",Georgia,serif;font-weight:600;font-size:2rem;margin:0 0 4px}}
  .ancestry{{color:var(--text-muted);font-size:.9rem;margin-bottom:8px}}
  .lede{{color:var(--text-muted);max-width:62ch}}
  h2{{font-family:"Spectral",Georgia,serif;font-weight:600;font-size:1.15rem;
    margin:32px 0 8px;padding-bottom:4px;border-bottom:1px solid var(--border)}}
  table{{border-collapse:collapse;width:100%;background:var(--surface);
    border:1px solid var(--border);border-radius:8px;overflow:hidden}}
  th{{text-align:left;font-size:.72rem;text-transform:uppercase;letter-spacing:.06em;
    color:var(--text-muted);padding:8px 12px;border-bottom:1px solid var(--border)}}
  td{{padding:7px 12px;border-bottom:1px solid var(--border);font-size:.85rem}}
  td.v{{font-family:"IBM Plex Mono",monospace;font-variant-numeric:tabular-nums;text-align:right}}
  tbody tr:nth-child(even){{background:var(--row-alt)}}
  tbody tr:last-child td{{border-bottom:none}}
  .interactive{{display:inline-block;margin:18px 0;padding:9px 14px;border-radius:7px;
    border:1px solid var(--border);background:var(--surface);color:var(--accent-ink);
    text-decoration:none;font-weight:500}}
  .attribution{{margin-top:28px;padding:14px 16px;border:1px solid var(--border);
    border-radius:8px;background:var(--row-alt);color:var(--text-muted);
    font-size:.76rem;line-height:1.6}}
  .attribution strong{{color:var(--text)}} .attribution a{{color:var(--accent-ink)}}
  footer{{margin-top:22px;color:var(--text-faint);font-size:.75rem}}
  footer a{{color:var(--text-muted)}}
</style>
<script type="application/ld+json">
{_json_ld(commune, lang, canonical, base_url, updated, creators)}
</script>
</head>
<body>
<div class="wrap">
  <h1>{esc(name)}</h1>
  <p class="ancestry">NIS {esc(nis)} · {esc(ancestry)}</p>
  <p class="lede">{esc(description)}</p>

  <a class="interactive" href="{ROOT_PREFIX}/local.html?nis={esc(nis)}">Open the interactive
    profile — charts, comparisons and peer communes</a>

  {"".join(body)}

  <div class="attribution">{attribution_html}</div>

  <footer>
    {covered} indicators shown. Full history for this commune:
    <a href="{ROOT_PREFIX}/public/data/communes/{esc(nis)}.json">JSON payload</a> ·
    <a href="{ROOT_PREFIX}/communes.html">all communes</a> ·
    <a href="{ROOT_PREFIX}/">BelPulse</a>
  </footer>
</div>
</body>
</html>
"""


def export_local_pages(
    payload_dir: Path = DEFAULT_PAYLOAD_DIR,
    out_dir: Path = DEFAULT_OUT_DIR,
    base_url: str = DEFAULT_BASE_URL,
    lang: str = "en",
    build_id: str = "local",
    attribution_source: Path = ATTRIBUTION_SOURCE,
    db_path: Path = DEFAULT_DB,
) -> dict[str, int]:
    sections = json.loads((payload_dir / "metadata" / "sections.json").read_text(encoding="utf-8"))[
        "sections"
    ]
    attribution = _read_attribution(attribution_source)
    sources = _indicator_sources(db_path)

    written = 0
    skipped_thin = 0
    out_dir.mkdir(parents=True, exist_ok=True)

    for payload_path in sorted((payload_dir / "communes").glob("*.json")):
        commune = json.loads(payload_path.read_text(encoding="utf-8"))
        if not _has_any_value(commune):
            # comparison.md's "no data, no page" rule, enforced rather than
            # assumed -- an empty page is worse than a missing one.
            skipped_thin += 1
            continue
        page = _render_page(
            commune,
            sections,
            lang,
            base_url,
            attribution,
            build_id,
            _creators_for(commune, sources),
        )
        target = out_dir / commune["nis_code"] / "index.html"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(page, encoding="utf-8")
        written += 1

    _write_sitemap(out_dir, base_url, payload_dir)
    return {"written": written, "skipped_thin": skipped_thin}


def _write_sitemap(out_dir: Path, base_url: str, payload_dir: Path) -> None:
    """A sitemap listing every generated route. Roadmap Block AD asks for
    sitemap submission later; emitting it alongside the pages costs nothing
    and means the routes are discoverable the moment they exist."""
    routes = sorted(p.parent.name for p in out_dir.glob("*/index.html"))
    today = datetime.now(timezone.utc).date().isoformat()
    urls = "\n".join(
        f"  <url><loc>{base_url}/local/{nis}/</loc><lastmod>{today}</lastmod></url>"
        for nis in routes
    )
    (out_dir / "sitemap.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{urls}\n"
        "</urlset>\n",
        encoding="utf-8",
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate one static, indexable page per commune (Block L permanent URLs)"
    )
    ap.add_argument("--payload-dir", type=Path, default=DEFAULT_PAYLOAD_DIR)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL)
    ap.add_argument("--lang", default="en", choices=LANGS)
    ap.add_argument("--build-id", default="local")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    args = ap.parse_args()

    result = export_local_pages(
        args.payload_dir,
        args.out_dir,
        args.base_url,
        args.lang,
        args.build_id,
        db_path=args.db,
    )
    print(
        f"Wrote {result['written']} commune pages to {args.out_dir} "
        f"({result['skipped_thin']} skipped as having no data), plus sitemap.xml"
    )


if __name__ == "__main__":
    main()
