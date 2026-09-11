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

THE ATTRIBUTION BLOCK IS READ FROM assets/i18n.js, NOT RETYPED. It is a
licence condition (docs/data_catalog.md: Statbel's 2015 licence terminates
automatically on non-compliance), and a second hand-written copy would drift
from the first the moment either is edited. One source of truth, read at build
time through `src/pages/strings.py` -- shared with the page-document exporter,
so those pages cannot drift from these -- with the `attrUpdated` placeholder
filled with the real date, which is itself the "date of last update" the same
licence requires.

NO PER-PAGE BUILD STAMP, DELIBERATELY. An earlier version wrote
`<!-- build:{id} -->` into every page's footer. That made all 565 files
change on every single run even when not one figure had moved -- 565 files
of git churn per day, on a repository already growing ~18 MB a commit from
the database alone. public/data/manifest.json already records build_id,
git_commit and build_date centrally, so the per-page copy bought nothing.
A page now changes if and only if its DATA changed, which is also what makes
the URL-stability property absolute rather than "identical apart from one
line".

THREE LANGUAGES, ONE LOOP. This module was English-only until the licence
notice moved into `assets/i18n.js` in all three languages -- generating a
French page around an English licence notice would have been worse than not
generating it. With the notice trilingual, `_render_page` already took `lang`
and the change was the loop it was written to allow: 565 pages became 1,695.
English keeps `local/{nis}/`; the others sit at `local/{nis}/{lang}/`.
"""

import argparse
import html
import json
import re
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# Run from anywhere and still import src/. Batch 15c added the `src.pages`
# import below without this line: the module resolved under pytest, whose
# rootdir is already on sys.path, and failed the moment anyone ran
# `make pages` -- which is not something the test suite does. Every other
# script here already does this; this one had needed no src import until 15c.
sys.path.insert(0, str(REPO))

from src.pages import strings as _shared_strings  # noqa: E402

DEFAULT_PAYLOAD_DIR = REPO / "public" / "data"
DEFAULT_OUT_DIR = REPO / "local"
DEFAULT_BASE_URL = "https://mdruszcz.github.io/belgian-macro-pipeline"
ATTRIBUTION_SOURCE = REPO / "communes.html"
DEFAULT_DB = REPO / "data" / "belgian_macro.db"

# Depth from local/{nis}/index.html back to the site root, used for every
# asset and payload link on the page.
# Depth from a page back to the site root. An English page lives at
# local/{nis}/ and a translated one at local/{nis}/{lang}/, so it is computed
# rather than fixed -- a wrong prefix breaks every link and every asset on the
# page while the page itself still renders, which is the kind of breakage that
# ships.
#
# English keeps the existing URL: local/{nis}/ is already indexed and linked,
# and moving it to local/{nis}/en/ would break those links for no gain.
DEFAULT_LANG = "en"

# Every language the static pages are generated in.
LANGS = ("en", "fr", "nl")


def _t(strings: dict | None, lang: str, key: str, **subs: str) -> str:
    """One interface string. Falls back to English and then to the key, so an
    omission is visible rather than rendering an empty cell."""
    tables = strings or {}
    value = (tables.get(lang) or {}).get(key) or (tables.get("en") or {}).get(key) or key
    for name, replacement in subs.items():
        value = value.replace("{" + name + "}", str(replacement))
    return value


def _root_prefix(lang: str) -> str:
    return "../.." if lang == DEFAULT_LANG else "../../.."


def _route(nis: str, lang: str) -> str:
    """The site-relative URL of a commune page in one language."""
    return f"/local/{nis}/" if lang == DEFAULT_LANG else f"/local/{nis}/{lang}/"


# MOVED to src/pages/strings.py, so the page-document exporter reads the same
# licence notice from the same place. Re-exported under the old private names
# because this module's own tests and callers use them, and because a second
# copy of a licence-text reader is the exact drift assets/i18n.js exists to end.
I18N_JS = _shared_strings.I18N_JS


def _interface_strings() -> dict[str, dict[str, str]]:
    """See `src/pages/strings.py`. A thin wrapper, not an alias, so this
    module's own `I18N_JS` stays the patch point its licence-guard test
    substitutes -- an alias would freeze the path at import time and the
    refusal would silently stop being exercised."""
    return _shared_strings.interface_strings(I18N_JS)


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


# Digit grouping and decimal marks per language. Belgium writes a number three
# ways and a page that gets it wrong reads as foreign before a reader has taken
# in a single figure -- which is the whole reason this roadmap step exists.
# Done by substitution rather than through the `locale` module: locale is
# process-global, depends on which locales the machine happens to have
# generated, and would make this exporter's output depend on the host.
_NUMBER_FORMATS = {
    "en": (",", "."),
    "fr": ("\u202f", ","),  # narrow no-break space, as French typography wants
    "nl": (".", ","),
}


def _format_value(value: float, unit: str | None, lang: str = "en") -> str:
    if value is None:
        return ""
    unit = unit or ""
    group, decimal = _NUMBER_FORMATS.get(lang, _NUMBER_FORMATS["en"])

    def render(number: float, places: int) -> str:
        # Formatted English first, then re-punctuated: doing it the other way
        # round means writing a grouping algorithm, and this one is already
        # correct.
        text = f"{number:,.{places}f}"
        return text.replace(",", "\x00").replace(".", decimal).replace("\x00", group)

    if unit == "eur":
        return "€" + render(value, 0)
    if unit == "eur_per_inhabitant":
        # A rate, written as one, so a per-head figure is never read as a
        # total. Mirrors MapUI.formatValue and src/pages/resolve.py; one
        # decimal is what every eur_per_inhabitant config declares.
        return "€" + render(value, 1) + "\u202f/\u202fhab."
    if unit.startswith("percent"):
        return render(value, 2).replace(f"{decimal}00", "") + "%"
    if abs(value - round(value)) < 1e-9:
        return render(round(value), 0)
    return render(value, 2)


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


def _describe(commune: dict, lang: str, strings: dict | None = None) -> str:
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
        bits.append(f"{label} {_format_value(cell['value'], entry.get('unit'), lang)} ({period})")
    covered = sum(1 for e in indicators.values() if _latest(e)[0] is not None)
    table = (strings or {}).get(lang, {})
    lead = table.get("describeLead", "Municipal statistics for {name}, Belgium").replace(
        "{name}", name
    )
    if bits:
        lead += ": " + "; ".join(bits)
    tail = table.get(
        "describeTail", "{n} indicators with history, sources and comparisons."
    ).replace("{n}", str(covered))
    return f"{lead}. {tail}"


def _source_registry(payload_dir: Path) -> dict[str, dict]:
    """The published source registry, or {} if it has not been generated.

    Read from the payloads rather than kept as a dict in this file. There WAS
    such a dict here -- three homepages typed into a script -- and a source
    added anywhere else would have left it silently stale, which for a licence
    notice is the failure that matters most.
    """
    path = payload_dir / "metadata" / "sources.json"
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8")).get("sources") or {}


def _grade_words(payload_dir: Path) -> dict[str, dict]:
    """The A/B/C/D vocabulary, in three languages, from metadata/sources.json.

    Read from the payload rather than restated here, so this page and the app
    cannot disagree about what a "C" means in any language.
    """
    path = payload_dir / "metadata" / "sources.json"
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8")).get("grades") or {}


def _indicator_provenance(payload_dir: Path) -> dict[str, dict]:
    """indicator_id -> its row from the published indicator index.

    Carries grade, source and the retrieval dates. Absent index => {}, and
    every caller falls back to what the page showed before it existed.
    """
    path = payload_dir / "metadata" / "indicators.json"
    if not path.is_file():
        return {}
    return {
        row["indicator_code"]: row
        for row in json.loads(path.read_text(encoding="utf-8")).get("indicators") or []
    }


def _indicator_sources(db_path: Path) -> dict[str, dict[str, str]]:
    """indicator_id -> {source_id, agency}, read from the indicators table.

    Still read from the database rather than the registry: this feeds the
    JSON-LD `creator` list, which must name the agencies that actually
    contributed a value to THIS commune, and the indicators table is the
    authoritative indicator-to-source mapping.
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


def _creators_for(
    commune: dict, sources: dict[str, dict[str, str]], registry: dict[str, dict] | None = None
) -> list[dict]:
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
        url = (registry or {}).get(agencies[agency], {}).get("homepage")
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


# The withheld wording lives in assets/i18n.js, in three languages, beside the
# app's own. The word matters more than the dash: a blank cell reads as a bug,
# and a zero would state a number the source explicitly refused to publish.


def _withheld_periods(entry: dict) -> list[str]:
    """Periods this commune has for the indicator that the source withheld.

    ONEM masks any count below 10 for privacy; those cells carry a null value
    and status "suppressed". They used never to reach the payloads at all, so
    this page could not show them -- while the attribution block it lifts from
    communes.html states that withheld figures "are shown as suppressed, never
    as zero". These pages are the crawler-visible copy of the data, so the
    claim has to be true here too, not only in the interactive app.
    """
    periods = entry.get("periods") or {}
    return sorted(p for p, cell in periods.items() if cell.get("status") == "suppressed")


def _source_cell(
    indicator_id: str,
    registry: dict[str, dict] | None,
    provenance: dict[str, dict] | None,
    lang: str = "en",
    grades: dict[str, dict] | None = None,
) -> str:
    """ "Statbel (A)" -- who published the figure, and how it was made.

    A derived figure names no source: it has none, and attributing a computed
    number to one of its inputs' agencies would say that agency published it.
    It carries its grade word instead, and its inputs' retrieval date lands in
    the Updated column.
    """
    row = (provenance or {}).get(indicator_id) or {}
    grade = row.get("grade")
    source_id = row.get("source")
    entry = ((registry or {}).get(source_id) or {}) if source_id else {}
    # The SHORT label, not the full agency name: "Police Fédérale — Direction de
    # l'information policière et des moyens ICT" is the credit the licence
    # requires and it is in the attribution block, in full, where it belongs.
    # In a table cell it is unreadable. Falls back to the agency for a source
    # with no short label yet.
    label = _local_name(entry.get("label"), lang, "") or entry.get("agency")
    if label and grade:
        return f"{label} ({grade})"
    if label:
        return label
    if grade:
        # The grade WORDS come from metadata/sources.json, which publishes them
        # in all three languages -- so this page and the app cannot disagree
        # about what a "C" means, in any language.
        word = (grades or {}).get(grade) or {}
        return word.get(lang) or word.get("en") or grade
    return ""


def _updated_cell(
    indicator_id: str,
    entry: dict,
    provenance: dict[str, dict] | None,
    strings: dict | None = None,
    lang: str = "en",
) -> str:
    """The retrieval date -- or, for a derived figure, its INPUTS' date, said
    to be theirs.

    Statbel's 2015 licence requires the date of last update of the information
    reused, and the information reused in a derived figure is its inputs. This
    column used to be blank for those, which satisfied neither the clause
    requiring a date nor the one forbidding a wrong one.
    """
    own = entry.get("updated")
    if own:
        return own
    row = (provenance or {}).get(indicator_id) or {}
    if row.get("inputs_updated"):
        return f"{_t(strings, lang, 'inputsPrefix')} {row['inputs_updated']}"
    return ""


def _section_rows(
    commune: dict,
    section: dict,
    lang: str,
    registry: dict[str, dict] | None = None,
    provenance: dict[str, dict] | None = None,
    strings: dict | None = None,
    grades: dict[str, dict] | None = None,
) -> list[tuple[str, str, str, str, bool, str]]:
    """(label, value, period, updated, is_withheld) per indicator in this
    section that this commune has something to say about.

    An indicator with no figure AND nothing withheld is omitted rather than
    rendered as an empty row -- the page is a summary, and local.html remains
    the place that explains a figure the pipeline never collected. An
    indicator the SOURCE withheld is different: that is a fact about the
    commune, and omitting it publishes silence where the source published a
    refusal."""
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
        withheld = _withheld_periods(entry)
        if period is None:
            if not withheld:
                continue
            rows.append(
                (
                    _local_name(entry.get("names"), lang, indicator_id),
                    _t(strings, lang, "withheldCell"),
                    ", ".join(withheld),
                    "",
                    True,
                    _source_cell(indicator_id, registry, provenance, lang, grades),
                )
            )
            continue
        # A figure whose NEWER years were withheld says so in its period
        # cell. Without it the static page shows 2024 on a 2026 site and a
        # reader cannot tell whether the series stopped or the source declined
        # to publish -- the interactive page states this, and these pages are
        # the copy a crawler and a reader without JavaScript actually get.
        later_withheld = [p for p in withheld if p > period]
        period_cell = (
            f"{period} ({', '.join(later_withheld)} {_t(strings, lang, 'withheldSuffix')})"
            if later_withheld
            else period
        )
        rows.append(
            (
                _local_name(entry.get("names"), lang, indicator_id),
                _format_value(cell["value"], entry.get("unit"), lang),
                period_cell,
                _updated_cell(indicator_id, entry, provenance, strings, lang),
                False,
                _source_cell(indicator_id, registry, provenance, lang, grades),
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
    registry: dict[str, dict] | None = None,
    provenance: dict[str, dict] | None = None,
    strings: dict | None = None,
    grades: dict[str, dict] | None = None,
) -> str:
    nis = commune["nis_code"]
    name = _local_name(commune.get("name"), lang, nis)
    canonical = f"{base_url}{_route(nis, lang)}"
    root = _root_prefix(lang)
    description = _describe(commune, lang, strings)

    # hreflang, pointing each language at the others AND at itself, which is
    # what search engines require to treat the three as one page in three
    # languages rather than as duplicates competing with each other. This is
    # the entire reason for generating them: a bourgmestre searches in French.
    alternates = (
        "\n".join(
            f'<link rel="alternate" hreflang="{other}" href="{base_url}{_route(nis, other)}">'
            for other in LANGS
        )
        + f'\n<link rel="alternate" hreflang="x-default" href="{base_url}{_route(nis, DEFAULT_LANG)}">'
    )

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
        rows = _section_rows(commune, section, lang, registry, provenance, strings, grades)
        if not rows:
            continue
        # Real figures only. A page whose only content is withheld cells is
        # still a thin page, and the refusal exists to stop those shipping.
        covered += sum(1 for row in rows if not row[4])
        label = _local_name(section.get("label"), lang, section["id"])
        body.append(f"<section><h2>{esc(label)}</h2>")
        body.append(
            "<table><thead><tr>"
            f"<th>{_t(strings, lang, 'colIndicator')}</th>"
            f"<th>{_t(strings, lang, 'colValue')}</th>"
            f"<th>{_t(strings, lang, 'colPeriod')}</th>"
            f"<th>{_t(strings, lang, 'colSource')}</th>"
            f"<th>{_t(strings, lang, 'colUpdated')}</th>"
            "</tr></thead><tbody>"
        )
        for indicator_label, value, period, indicator_updated, withheld, source in rows:
            css = " class='withheld'" if withheld else ""
            body.append(
                f"<tr{css}><td>{esc(indicator_label)}</td><td class='v'>{esc(value)}</td>"
                f"<td>{esc(period)}</td><td>{esc(source)}</td>"
                f"<td>{esc(indicator_updated)}</td></tr>"
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
{alternates}
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
  <p class="ancestry">{esc(_t(strings, lang, "nis"))} {esc(nis)} · {esc(ancestry)}</p>
  <p class="lede">{esc(description)}</p>

  <a class="interactive" href="{root}/local.html?nis={esc(nis)}">{esc(_t(strings, lang, "openInteractive"))}</a>

  {"".join(body)}

  <div class="attribution">{attribution_html}</div>

  <footer>
    {esc(_t(strings, lang, "staticFooter", n=covered))}
    <a href="{root}/public/data/communes/{esc(nis)}.json">{esc(_t(strings, lang, "jsonPayload"))}</a> ·
    <a href="{root}/communes.html">{esc(_t(strings, lang, "allCommunes"))}</a> ·
    <a href="{root}/">BelPulse</a>
  </footer>
</div>
</body>
</html>
"""


def export_local_pages(
    payload_dir: Path = DEFAULT_PAYLOAD_DIR,
    out_dir: Path = DEFAULT_OUT_DIR,
    base_url: str = DEFAULT_BASE_URL,
    # "all" by default, matching the CLI: these pages exist for search
    # visibility, and a French-language commune page is the highest-value thing
    # this repository publishes into a Belgian search result.
    lang: str = "all",
    build_id: str = "local",
    db_path: Path = DEFAULT_DB,
) -> dict[str, int]:
    sections = json.loads((payload_dir / "metadata" / "sections.json").read_text(encoding="utf-8"))[
        "sections"
    ]
    strings = _interface_strings()
    sources = _indicator_sources(db_path)
    registry = _source_registry(payload_dir)
    provenance = _indicator_provenance(payload_dir)
    grades = _grade_words(payload_dir)

    # One language, or all of them. The default is all: these pages exist for
    # search visibility, and a French-language commune page is the single
    # highest-value thing this repository publishes into a Belgian search
    # result. English keeps its existing URL.
    languages = LANGS if lang == "all" else (lang,)

    written = 0
    skipped_thin = 0
    out_dir.mkdir(parents=True, exist_ok=True)

    for payload_path in sorted((payload_dir / "communes").glob("*.json")):
        commune = json.loads(payload_path.read_text(encoding="utf-8"))
        if not _has_any_value(commune):
            # comparison.md's "no data, no page" rule, enforced rather than
            # assumed -- an empty page is worse than a missing one. Counted
            # once per commune, not once per language.
            skipped_thin += 1
            continue
        creators = _creators_for(commune, sources, registry)
        for page_lang in languages:
            page = _render_page(
                commune,
                sections,
                page_lang,
                base_url,
                strings[page_lang]["attribution"],
                build_id,
                creators,
                registry,
                provenance,
                strings,
                grades,
            )
            relative = _route(commune["nis_code"], page_lang).strip("/").split("/", 1)[1]
            target = out_dir / relative / "index.html"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(page, encoding="utf-8")
            written += 1

    _write_sitemap(out_dir, base_url, payload_dir)
    return {"written": written, "skipped_thin": skipped_thin}


def _data_date(payload_dir: Path, nis: str) -> str:
    """When this commune's figures last changed -- NOT when the build ran.

    `lastmod` used to be `datetime.now()`, identical on all 1,695 entries. That
    rewrote every line of this file on every build even when not one figure had
    moved: `git log` shows `1 file changed, 1695 insertions(+), 1695
    deletions(-)` on days nothing happened. It is the same churn this module's
    docstring says was deliberately removed from the PAGES -- the fix landed
    there and the sitemap kept the timestamp, because
    `test_a_url_is_byte_identical_across_two_rebuilds` reads one commune page
    and never opened the sitemap.

    It was also a false statement to a crawler. `lastmod` means "this page
    changed"; a build date claims all 1,695 changed daily, and a search engine
    that learns the claim is worthless starts ignoring it.

    Every indicator in a commune payload already carries `updated`. The newest
    of them is the date this page's content actually last moved. A payload with
    no dated indicator falls back to the empty string, and the caller omits the
    element rather than inventing one.
    """
    payload = payload_dir / "communes" / f"{nis}.json"
    if not payload.is_file():
        return ""
    indicators = (json.loads(payload.read_text(encoding="utf-8")).get("indicators") or {}).values()
    dates = [entry["updated"] for entry in indicators if entry.get("updated")]
    return max(dates) if dates else ""


def _write_sitemap(out_dir: Path, base_url: str, payload_dir: Path) -> None:
    """A sitemap listing every generated route. Roadmap Block AD asks for
    sitemap submission later; emitting it alongside the pages costs nothing
    and means the routes are discoverable the moment they exist."""
    communes = sorted(p.parent.name for p in out_dir.glob("*/index.html"))

    # EVERY LANGUAGE'S ROUTE, each declaring the others as alternates. A
    # sitemap that listed only the English page would leave the French and
    # Dutch ones discoverable by crawl alone, and would not tell a search
    # engine the three are the same page -- which is the difference between
    # ranking in French and competing with yourself in three languages.
    entries = []
    for nis in communes:
        for lang in LANGS:
            target = out_dir / _route(nis, lang).strip("/").split("/", 1)[1] / "index.html"
            if not target.is_file():
                continue
            links = "".join(
                f'<xhtml:link rel="alternate" hreflang="{other}" '
                f'href="{base_url}{_route(nis, other)}"/>'
                for other in LANGS
            )
            # Omitted entirely rather than emitted empty when a payload
            # carries no dated indicator: `<lastmod></lastmod>` is invalid
            # against the sitemap schema, and a crawler may reject the file.
            changed = _data_date(payload_dir, nis)
            stamp = f"<lastmod>{changed}</lastmod>" if changed else ""
            entries.append(f"  <url><loc>{base_url}{_route(nis, lang)}</loc>{stamp}{links}</url>")

    (out_dir / "sitemap.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"\n'
        '        xmlns:xhtml="http://www.w3.org/1999/xhtml">\n'
        + "\n".join(entries)
        + "\n</urlset>\n",
        encoding="utf-8",
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate one static, indexable page per commune (Block L permanent URLs)"
    )
    ap.add_argument("--payload-dir", type=Path, default=DEFAULT_PAYLOAD_DIR)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL)
    ap.add_argument(
        "--lang",
        default="all",
        choices=(*LANGS, "all"),
        help="One language, or 'all' (the default) for every language.",
    )
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
