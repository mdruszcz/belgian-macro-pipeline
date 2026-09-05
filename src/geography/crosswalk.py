"""
Derivation of the municipal merger crosswalk from official Statbel files.

Belgium's communes are not stable. Two recent waves (1 January 2019 and
1 January 2025) merged 55 communes out of existence, and the 2019 Hainaut
arrondissement reform renumbered a further eleven without merging them. Without
a crosswalk, a 2015 source file about Kruibeke resolves to nothing, and a merged
commune's history silently starts at the merger year -- which is exactly the
"plausible numbers attached to the wrong place" failure that docs/steps' Block C
[RED] step exists to catch.

Two independent methods are combined here, deliberately:

1. **Vintage diff (authoritative for *whether* and *when*).** A commune present
   in REFNIS vintage N and absent in vintage N+1 disappeared at that boundary.
   This is direct documentary evidence from the publisher, and it is the reason
   the maintainer downloaded three vintages rather than one.

2. **NIS6 prefix rule (corroborating, and supplies *where to*).** In the NIS9
   workbook, a sub-municipality code whose first five digits differ from its
   commune code preserves the *former* commune's NIS5. That names the successor,
   which a diff alone cannot do -- a diff only reports what vanished and what
   appeared, not which maps to which.

The prefix rule alone is NOT sufficient, and this is not hypothetical: it finds
53 of the 55 predecessors. It misses `73009` (Borgloon) and `73083` (Tongeren),
whose sub-municipality codes were renumbered under the new commune `73111`
(Tongeren-Borgloon) instead of preserving the old codes. Had the crosswalk been
built on the prefix rule alone, both communes' entire pre-2025 history would
have been silently orphaned. Hence: the diff decides membership, the prefix rule
decorates it, and anything the two do not agree on is flagged for human review
rather than guessed.
"""

# Effective dates of the REFNIS vintages the maintainer downloaded. The wave
# dates are Belgian municipal reorganizations, which always take effect on
# 1 January; the diff between two vintages is attributed to the later date.
VINTAGE_DATES = {
    "REFNIS_DEFINITIEF.csv": "1977-01-01",
    "REFNIS_2019.csv": "2019-01-01",
    "REFNIS_2025.csv": "2025-01-01",
}

# Statbel marks partial territory transfers in NIS6 sub-municipality names.
#
# These annotate the **1977** merger of Belgian municipalities, when the modern
# communes were formed -- roughly 200 of them appear across the country, on
# sub-municipalities of communes that have been stable ever since. They say
# nothing about the 2019 and 2025 waves this crosswalk records.
#
# An earlier version flagged a crosswalk row whenever any of its former
# commune's NIS6 entries carried a marker, which caught five rows by pure
# coincidence of code overlap (52063 Seneffe, 55022 La Louviere, 55039 Silly,
# 82003 Bastogne, 82005 Bertogne) and asked the maintainer to verify lineages
# that were never in doubt. A review gate that cries wolf gets ignored, so the
# column is now informational and no longer gates the review at all -- see
# _carries_partial_marker for why it turned out to carry no signal of its own.
PARTIAL_MARKERS = ("PARTIE DE", "PARTIES DE", "MODIFICATION DE LIMITE", "*")

# `partial` is deliberately NOT one of these. A boundary transfer is orthogonal
# to what happened to the commune itself: 55022 La Louviere was *recoded* by the
# 2019 Hainaut arrondissement reform and separately received part of
# Familleureux. Folding that into the relationship would overwrite the useful
# fact with the incidental one, so it travels as its own column.
RELATIONSHIPS = ("merged", "absorbed", "recoded")


def successor_candidates_from_nis6(nis9_rows: list[dict]) -> dict[str, set[str]]:
    """old NIS5 -> {successor commune codes}, via the NIS6 prefix rule.

    Returns a set per old code because the rule is applied per sub-municipality
    row; a genuine 1:1 lineage yields a single-element set, and anything larger
    means the former commune was split across successors, which must not be
    collapsed silently.
    """
    candidates: dict[str, set[str]] = {}
    for row in nis9_rows:
        nis6 = row.get("nis6")
        commune = row.get("commune_code")
        if not nis6 or not commune or len(nis6) < 5:
            continue
        old_code = nis6[:5]
        if old_code != commune:
            candidates.setdefault(old_code, set()).add(commune)
    return candidates


def _is_partial(name: str | None) -> bool:
    if not name:
        return False
    upper = name.upper()
    return any(marker in upper for marker in PARTIAL_MARKERS)


def partial_transfer_destinations(nis9_rows: list[dict]) -> dict[str, set[str]]:
    """old NIS5 -> the communes that its partially-transferred territory sits in today.

    Each marker-bearing NIS6 row states, by its own commune assignment, where
    that piece of land actually ended up. That is what makes "is this partial
    transfer relevant?" computable rather than a judgement call.
    """
    destinations: dict[str, set[str]] = {}
    for row in nis9_rows:
        nis6 = row.get("nis6")
        commune = row.get("commune_code")
        if not nis6 or not commune or len(nis6) < 5:
            continue
        if _is_partial(row.get("nis6_name")):
            destinations.setdefault(nis6[:5], set()).add(commune)
    return destinations


def _carries_partial_marker(old_code: str, destinations: dict[str, set[str]]) -> bool:
    """Whether any of this former commune's territory is marked as a partial
    transfer. **Informational only** -- it does not gate the review.

    It took a wrong turn to see why. The gate originally stopped on any marker,
    which flagged five lineages that were never in doubt. The obvious fix --
    flag only when the marked land ended up somewhere other than this row's
    successor -- turns out to be unable to fire: the prefix rule derives
    successors from exactly those NIS6 rows, so any territory that went
    elsewhere is *already* in the successor set, and the row is already flagged
    as split across multiple successors.

    So the marker carries no signal the multi-successor check does not. It stays
    as a column because it is true and occasionally useful context, and it is
    out of `unresolved()` because a gate that stops on it stops on nothing real.
    """
    return bool(destinations.get(old_code))


def classify(
    old_name: str,
    successors: set[str],
    names_by_code: dict,
    predecessor_count: int,
) -> str:
    """merged | absorbed | recoded, per docs/features/geography.md.

    `predecessor_count` is how many communes disappeared into this same
    successor in this wave -- which is what distinguishes a merger from an
    absorption, and it cannot be seen from a single crosswalk row. Puurs and
    Sint-Amands each map 1:1 to Puurs-Sint-Amands, but the pair of them formed
    a new entity, so both rows are `merged`, not `absorbed`.

    - merged: the successor took in more than one predecessor.
    - recoded: sole predecessor, same name -- the entity survived and only its
      code changed (the 2019 Hainaut reform: 55022 La Louviere -> 58001).
    - absorbed: sole predecessor, different name -- taken into an existing
      commune (11007 Borsbeek -> 11002 Antwerpen).
    """
    if len(successors) != 1:
        return "merged"
    if predecessor_count > 1:
        return "merged"
    successor = next(iter(successors))
    new_name = names_by_code.get(successor, "")
    if _normalize(old_name) == _normalize(new_name):
        return "recoded"
    return "absorbed"


def _match_successor_by_name(old_name: str, appeared: set[str], names_by_code: dict) -> set[str]:
    """Fallback when the NIS6 prefix rule yields no successor.

    Belgian merged communes are conventionally named after their constituents
    ("Tongeren-Borgloon"), so a predecessor's name appearing inside a newly
    created commune's name is a strong signal. It is still only a signal: rows
    resolved this way stay flagged for the maintainer's [H] verification.
    """
    needle = _normalize(old_name)
    if not needle:
        return set()
    return {code for code in appeared if needle in _normalize(names_by_code.get(code, ""))}


def _normalize(name: str | None) -> str:
    """Case- and punctuation-insensitive form for comparing names across vintages.

    Statbel is not consistent about apostrophes: `REFNIS_DEFINITIEF.csv` writes
    "Arrondissement d'Anvers" with a straight quote and `REFNIS_2025.csv` writes
    it with a typographic one. Comparing raw strings would read that as the
    entity changing identity and split its validity window on punctuation.
    """
    text = (name or "").strip().casefold()
    for curly, straight in (("’", "'"), ("‘", "'"), ("´", "'")):
        text = text.replace(curly, straight)
    return " ".join(text.split())


def canonical_code_map(crosswalk_rows: list[dict]) -> dict[str, str]:
    """old NIS -> the code its territory ended up under, following chains.

    Used to compare an arrondissement's territory across vintages without being
    fooled by mergers *inside* it: when Beveren and Kruibeke become
    Beveren-Kruibeke-Zwijndrecht, arrondissement `46000`'s commune list changes
    but its territory does not, and it must not be treated as a new entity.
    """
    direct = {
        row["old_nis"]: row["new_nis"]
        for row in crosswalk_rows
        if row["new_nis"] and ";" not in row["new_nis"]
    }
    resolved: dict[str, str] = {}
    for code in direct:
        seen = {code}
        current = code
        while current in direct and direct[current] not in seen:
            current = direct[current]
            seen.add(current)
        resolved[code] = current
    return resolved


def derive_entity_windows(
    hierarchies: list[tuple[str, dict[str, dict]]],
    canonical: dict[str, str] | None = None,
) -> dict[str, list[dict]]:
    """Validity windows for *every* entity, aggregates included.

    `hierarchies` is an ordered list of (filename, parse_refnis_hierarchy(...))
    output, oldest first.

    Communes are not the only things that change. Arrondissement `54000`
    (Mouscron) ceased to exist in 2019 and `58000` (La Louvière) was created;
    more subtly, `57000` kept its code but went from "Tournai" with 10 communes
    to "Tournai-Mouscron" with 12. Dating every aggregate to the structural
    epoch -- as an earlier version of this module did, by only ever diffing
    commune rows -- makes `resolve_geo('58000', '2015')` return an entity that
    did not exist, and `resolve_geo('57000', '2010')` return a territory
    two communes larger than the one the source meant. Both are silent
    mismappings of exactly the kind Block C's audit step exists to find.

    A window is closed and a new one opened when an entity disappears, or when
    its name or its set of children changes. Returns
    {nis_code: [{valid_from, valid_to, level, parent_nis, name_fr, name_nl}]},
    oldest window first.
    """
    windows: dict[str, list[dict]] = {}
    canonical = canonical or {}

    def territory_map(hierarchy: dict[str, dict]) -> dict[str, frozenset]:
        """Each entity's transitive set of canonical communes.

        Compared at the leaf level rather than by direct children, because an
        aggregate's children can be renumbered without its territory moving:
        the 2019 reform replaced Hainaut's arrondissement `54000` with `58000`,
        so the province's child list changed while it covered exactly the same
        communes. Rolling down to communes -- and mapping each through the
        merger crosswalk -- compares what the entity actually contains.
        """
        leaves: dict[str, set] = {code: set() for code in hierarchy}
        for code, entity in hierarchy.items():
            if entity["level"] != "municipality":
                continue
            canonical_code = canonical.get(code, code)
            current = code
            while current is not None:
                leaves[current].add(canonical_code)
                current = hierarchy[current]["parent_nis"] if current in hierarchy else None
        return {code: frozenset(found) for code, found in leaves.items()}

    for filename, hierarchy in hierarchies:
        territories = territory_map(hierarchy)
        for code, entity in hierarchy.items():
            entity["territory"] = territories[code]
        as_of = VINTAGE_DATES.get(filename)
        if as_of is None:
            raise ValueError(
                f"No effective date known for REFNIS vintage {filename!r}. "
                "Add it to VINTAGE_DATES rather than guessing."
            )
        for code, entity in hierarchy.items():
            existing = windows.setdefault(code, [])
            open_window = existing[-1] if existing and existing[-1]["valid_to"] is None else None
            if open_window is None:
                existing.append({**entity, "valid_from": as_of, "valid_to": None})
                continue
            # Same code, but a different entity in substance: close and reopen.
            # Compared on canonical territory rather than the raw child list, so
            # communes merging *within* an arrondissement do not masquerade as
            # the arrondissement itself changing.
            if (
                _normalize(open_window["name_nl"]) != _normalize(entity["name_nl"])
                or open_window["parent_nis"] != entity["parent_nis"]
                or open_window["territory"] != entity["territory"]
            ):
                open_window["valid_to"] = as_of
                existing.append({**entity, "valid_from": as_of, "valid_to": None})

        # Anything absent from this vintage ended at its date.
        for code, existing in windows.items():
            if code in hierarchy or not existing:
                continue
            if existing[-1]["valid_to"] is None:
                existing[-1]["valid_to"] = as_of

    return windows


def derive_crosswalk(
    vintages: list[tuple[str, dict[str, str]]],
    nis9_rows: list[dict],
    names_by_code: dict[str, str],
    valid_from_by_code: dict[str, str] | None = None,
    previously_verified: set[tuple[str, str, str]] | None = None,
) -> list[dict]:
    """Build the crosswalk from vintage diffs, decorated by the prefix rule.

    `vintages` is an ordered list of (filename, {nis_code: {"name_fr", "name_nl"}})
    for the commune rows of each REFNIS vintage, oldest first. Both languages are
    carried because a disappeared commune's row is the only surviving record of
    its name, and dropping one language here would put a Dutch name in a Walloon
    commune's French label (CLAUDE.md rule 7).

    Every returned row carries `verified: False` and, where the two methods
    could not agree, a `note` naming the disagreement. Those rows are the
    maintainer's [H] verification list -- CONTROL C must not be claimed while
    any remain unresolved.
    """
    prefix_candidates = successor_candidates_from_nis6(nis9_rows)
    valid_from_by_code = valid_from_by_code or {}
    partial_destinations = partial_transfer_destinations(nis9_rows)
    previously_verified = previously_verified or set()

    rows: list[dict] = []
    for (_, older), (newer_file, newer) in zip(vintages, vintages[1:], strict=False):
        wave_date = VINTAGE_DATES.get(newer_file)
        if wave_date is None:
            raise ValueError(
                f"No effective date known for REFNIS vintage {newer_file!r}. "
                "Add it to VINTAGE_DATES rather than guessing a merger date."
            )
        appeared = set(newer) - set(older)
        disappeared = sorted(set(older) - set(newer))

        # Resolve every predecessor's successor first, so that the number of
        # predecessors per successor is known before anything is classified.
        newer_names = {code: entry["name_nl"] for code, entry in newer.items()}
        resolved: dict[str, tuple[set[str], str]] = {}
        for old_code in disappeared:
            successors = prefix_candidates.get(old_code, set())
            note = ""
            if not successors:
                successors = _match_successor_by_name(
                    older[old_code]["name_nl"], appeared, newer_names
                )
                if successors:
                    note = (
                        "successor not derivable from NIS6 codes; matched by name against "
                        f"commune(s) created on {wave_date} -- verify before relying on it"
                    )
                else:
                    note = (
                        "successor could not be derived from NIS6 codes or by name; "
                        f"{len(appeared)} commune(s) were created on {wave_date}"
                    )
            elif len(successors) > 1:
                note = "former commune split across multiple successors"
            resolved[old_code] = (successors, note)

        predecessors_per_successor: dict[str, int] = {}
        for successors, _ in resolved.values():
            if len(successors) == 1:
                successor = next(iter(successors))
                predecessors_per_successor[successor] = (
                    predecessors_per_successor.get(successor, 0) + 1
                )

        for old_code in disappeared:
            successors, note = resolved[old_code]
            count = (
                predecessors_per_successor.get(next(iter(successors)), 1)
                if len(successors) == 1
                else 1
            )
            rows.append(
                {
                    "old_nis": old_code,
                    "old_name_nl": older[old_code]["name_nl"],
                    "old_name_fr": older[old_code]["name_fr"],
                    "new_nis": ";".join(sorted(successors)) if successors else "",
                    "relationship": classify(
                        older[old_code]["name_nl"], successors, names_by_code, count
                    ),
                    "has_partial_transfer": (
                        "true"
                        if _carries_partial_marker(old_code, partial_destinations)
                        else "false"
                    ),
                    # valid_from is when the predecessor first appears in a
                    # vintage; valid_to is the wave that ended it. They must
                    # differ -- `geographies` enforces valid_to > valid_from.
                    "valid_from": valid_from_by_code.get(old_code, VINTAGE_DATES[vintages[0][0]]),
                    "valid_to": wave_date,
                    "evidence": f"absent from {newer_file}",
                    # A sign-off survives regeneration as long as the lineage it
                    # was given still says the same thing; any change to the
                    # successor or the wave resets it, because that is a
                    # different claim from the one that was checked.
                    "verified": (
                        "true"
                        if (old_code, ";".join(sorted(successors)), wave_date)
                        in previously_verified
                        else "false"
                    ),
                    "note": note,
                }
            )
    return rows


def unresolved(rows: list[dict]) -> list[dict]:
    """Crosswalk rows that need the maintainer's eye before CONTROL C.

    A row qualifies when its successor was guessed (a `note`), when it is split
    across several successors, or when no successor could be derived at all.
    `has_partial_transfer` deliberately does not qualify -- see
    _carries_partial_marker for why it carries no signal of its own.
    """
    return [r for r in rows if r["note"] or ";" in r["new_nis"] or not r["new_nis"]]
