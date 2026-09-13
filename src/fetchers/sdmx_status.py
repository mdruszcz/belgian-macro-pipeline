"""
The SDMX CL_OBS_STATUS <-> canonical status enum mapping, shared in both
directions rather than kept as two hand-copied tables that could drift apart:

  - map_obs_status(): SDMX letter -> canonical, for an NBB row on its way
    into the canonical `observations` table (scripts/port_existing_indicators.py,
    scripts/sync_to_canonical.py).
  - sdmx_for_status(): canonical -> SDMX letter, the exact inverse, for a row
    EurostatSource already resolved to canonical status
    (src/fetchers/eurostat.py's own OBS_FLAG mapping) that is about to land
    in `legacy_observations.obs_status` -- which
    scripts/sync_to_canonical.py reads back through map_obs_status() and
    therefore still expects SDMX-lettered, exactly like every other national
    adapter's legacy row (belgian_macro_db.py's fetch_all(), BLOCKER 1 found
    by the audit: writing "final" there instead of "A" made map_obs_status()
    raise and abort the whole canonical sync).

CANONICAL_TO_SDMX is computed as OBS_STATUS_MAP's exact inverse so the two
directions cannot disagree.
"""

# SDMX CL_OBS_STATUS -> canonical status enum. Any code not listed here is a
# hard error, never a silent default (CLAUDE.md rule 13: fail loudly).
OBS_STATUS_MAP = {
    "A": "final",  # Normal value
    "P": "provisional",  # Provisional value
    "E": "estimate",  # Estimated value
    "B": "revised",  # Break in series -- weakest mapping here; re-verify if seen
    "M": "na",  # Missing value
    "S": "suppressed",  # Statistical disclosure control, if ever encountered
}

CANONICAL_TO_SDMX = {canonical: sdmx for sdmx, canonical in OBS_STATUS_MAP.items()}


def map_obs_status(raw: str) -> str:
    raw = (raw or "").strip()
    if raw not in OBS_STATUS_MAP:
        raise ValueError(
            f"Unrecognized SDMX OBS_STATUS code {raw!r}. Refusing to guess "
            "(CLAUDE.md rule 13: fail loudly, never silently coerce). "
            "Add it to OBS_STATUS_MAP after confirming its meaning."
        )
    return OBS_STATUS_MAP[raw]


def sdmx_for_status(canonical: str) -> str:
    """The inverse of map_obs_status(). Raises rather than guessing if a
    canonical status this pipeline produces has no SDMX letter recorded for
    it -- legacy_observations.obs_status must always be one of
    OBS_STATUS_MAP's keys, never a canonical word."""
    if canonical not in CANONICAL_TO_SDMX:
        raise ValueError(
            f"No SDMX OBS_STATUS letter for canonical status {canonical!r}. "
            "legacy_observations.obs_status must stay SDMX-lettered "
            "(scripts/sync_to_canonical.py reads it back through map_obs_status())."
        )
    return CANONICAL_TO_SDMX[canonical]
