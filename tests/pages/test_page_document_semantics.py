"""The batch-9 semantic rejection list (spec §8) and the related §15 requirements:
every page_type in §8.11, the national/municipal indicator-universe regression, and the
retired-vs-invalid NIS distinction. Structural/resource-exhaustion guards live in
test_page_document_schema.py; unsafe-content method-correctness lives in
tests/security/test_page_document_hardening.py.

Every rejection below asserts the *specific* closed-vocabulary error code (batch spec
§11), never just "validation failed". No indicator id, NIS code or figure is hand-typed
here -- all come from tests/fixtures/pages/real_data.py, read off the real payloads.
"""

import pytest

from src.pages import load_metadata, load_registry, validate_document
from tests.fixtures.pages import builders, real_data


@pytest.fixture(scope="module")
def metadata():
    return load_metadata()


@pytest.fixture(scope="module")
def registry():
    return load_registry()


def _codes(errors) -> set[str]:
    return {e.code for e in errors}


# --- 1. duplicate block ids ----------------------------------------------------


def test_duplicate_block_id_across_sections_is_rejected(metadata, registry):
    hero_a = builders.make_block("hero", block_id="blk-dup", binding=None)
    hero_b = builders.make_block("hero", block_id="blk-dup", binding=None)
    doc = builders.minimal_valid_document()
    doc["sections"] = [
        builders.make_section("sec-1", [hero_a]),
        builders.make_section("sec-2", [hero_b]),
    ]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "duplicate_block_id" in _codes(errors)


# --- 2. unknown block type / unsupported version --------------------------------


def test_unknown_block_type_is_rejected(metadata, registry):
    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"][0]["type"] = "not_a_real_block_type"
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unknown_block_type" in _codes(errors)


def test_unsupported_block_version_is_rejected(metadata, registry):
    """kpi_card ships at least two supported versions (spec §13); one past the
    highest declared version must be rejected by its own code."""
    kpi_entry = builders.registry_entry("kpi_card")
    versions = builders._versions_of(kpi_entry)
    assert len(versions) >= 2, "kpi_card should declare v1 and v2 (spec §13)"
    bad_version = max(versions) + 1
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        version=max(versions),
        binding=builders.municipal_binding(real_data.an_additive_municipal_indicator_id()),
    )
    kpi["version"] = bad_version
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsupported_block_version" in _codes(errors)


def test_kpi_card_both_supported_versions_are_individually_valid(metadata, registry):
    kpi_entry = builders.registry_entry("kpi_card")
    for version in builders._versions_of(kpi_entry):
        kpi = builders.make_block(
            "kpi_card",
            block_id=f"blk-kpi-v{version}",
            version=version,
            binding=builders.municipal_binding(real_data.an_additive_municipal_indicator_id()),
        )
        doc = builders.minimal_valid_document()
        doc["sections"] = [builders.make_section("sec-1", [kpi])]
        errors = validate_document(doc, metadata=metadata, registry=registry)
        assert "unsupported_block_version" not in _codes(errors), (version, errors)


# --- 3. invalid route -----------------------------------------------------------


VALID_ROOT_ROUTES = [
    "/",
    "/index.html",
    "/dashboard.html",
    "/communes.html",
    "/map.html",
    "/local.html",
    "/all_data.html",
    "/about.html",
]


@pytest.mark.parametrize("route", VALID_ROOT_ROUTES)
def test_batch_0_root_route_is_accepted(metadata, registry, route):
    doc = builders.minimal_valid_document(route=route)
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "invalid_route" not in _codes(errors), (route, errors)


def test_templated_commune_routes_are_accepted(metadata, registry):
    nis = real_data.a_municipal_nis_code()
    for route in (f"/local/{nis}/", f"/local/{nis}/fr/", f"/local/{nis}/nl/"):
        doc = builders.minimal_valid_document(route=route, page_type="municipality-profile")
        doc["context"] = {"nis": nis}
        errors = validate_document(doc, metadata=metadata, registry=registry)
        assert "invalid_route" not in _codes(errors), (route, errors)


BAD_ROUTES = [
    "/local/../../etc/passwd",
    "/local/11001/../../secret",
    "https://evil.example.com/",
    "//evil.example.com/",
    "javascript:alert(1)",
    "/local\\11001\\",
    "/local/11001%2f..%2f",
    "/this/route/does/not/exist",
    "",
]


@pytest.mark.parametrize("route", BAD_ROUTES)
def test_bad_route_is_rejected(metadata, registry, route):
    doc = builders.minimal_valid_document(route=route)
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "invalid_route" in _codes(errors), (route, errors)


# --- 5. grid overlap within a section that doesn't permit it --------------------


def test_grid_overlap_within_a_section_is_rejected(metadata, registry):
    hero_a = builders.make_block(
        "hero", block_id="blk-a", binding=None, block_layout=builders.layout()
    )
    hero_b = builders.make_block(
        "hero", block_id="blk-b", binding=None, block_layout=builders.layout()
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [hero_a, hero_b])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "grid_overlap" in _codes(errors)


def test_non_overlapping_blocks_in_the_same_section_pass(metadata, registry):
    # Full-width, stacked vertically at every breakpoint -- true non-overlap
    # regardless of column count (12/8/4), unlike offsetting x, which would
    # itself overflow the 4-column mobile grid for two side-by-side blocks.
    top_layout = builders.layout(desktop=(0, 0, 12, 2), tablet=(0, 0, 8, 2), mobile=(0, 0, 4, 2))
    bottom_layout = builders.layout(desktop=(0, 2, 12, 2), tablet=(0, 2, 8, 2), mobile=(0, 2, 4, 2))
    hero_a = builders.make_block("hero", block_id="blk-a", binding=None, block_layout=top_layout)
    hero_b = builders.make_block("hero", block_id="blk-b", binding=None, block_layout=bottom_layout)
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [hero_a, hero_b])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "grid_overlap" not in _codes(errors), errors


# Per-breakpoint overlap: each block's layout is independently authored per
# breakpoint (there is no reflow engine yet -- Batch 13), so two blocks
# colliding at ONE breakpoint and not the others is a real, hand-authored
# collision at that viewport and must be rejected on its own, not only when
# every breakpoint collides simultaneously.


def _two_hero_blocks(layout_a: dict, layout_b: dict, *, visibility_b: dict | None = None) -> list:
    hero_a = builders.make_block("hero", block_id="blk-a", binding=None, block_layout=layout_a)
    hero_b = builders.make_block(
        "hero",
        block_id="blk-b",
        binding=None,
        block_layout=layout_b,
        block_visibility=visibility_b,
    )
    return [hero_a, hero_b]


def test_grid_overlap_detected_at_mobile_only(metadata, registry):
    layout_a = builders.layout(desktop=(0, 0, 6, 2), tablet=(0, 0, 4, 2), mobile=(0, 0, 4, 2))
    layout_b = builders.layout(desktop=(6, 0, 6, 2), tablet=(4, 0, 4, 2), mobile=(0, 0, 4, 2))
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", _two_hero_blocks(layout_a, layout_b))]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "grid_overlap" in _codes(errors), errors


def test_grid_overlap_detected_at_tablet_only(metadata, registry):
    layout_a = builders.layout(desktop=(0, 0, 6, 2), tablet=(0, 0, 4, 2), mobile=(0, 0, 4, 2))
    layout_b = builders.layout(desktop=(6, 0, 6, 2), tablet=(0, 0, 4, 2), mobile=(0, 2, 4, 2))
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", _two_hero_blocks(layout_a, layout_b))]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "grid_overlap" in _codes(errors), errors


def test_grid_overlap_detected_at_desktop_only(metadata, registry):
    layout_a = builders.layout(desktop=(0, 0, 6, 2), tablet=(0, 0, 4, 2), mobile=(0, 0, 4, 2))
    layout_b = builders.layout(desktop=(0, 0, 6, 2), tablet=(4, 0, 4, 2), mobile=(0, 2, 4, 2))
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", _two_hero_blocks(layout_a, layout_b))]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "grid_overlap" in _codes(errors), errors


def test_grid_overlap_error_names_the_offending_breakpoint(metadata, registry):
    """A mobile-only collision must say "mobile" somewhere in the error (path
    or message), so a failure tells someone which viewport to fix, not just
    that a collision exists somewhere in the document."""
    layout_a = builders.layout(desktop=(0, 0, 6, 2), tablet=(0, 0, 4, 2), mobile=(0, 0, 4, 2))
    layout_b = builders.layout(desktop=(6, 0, 6, 2), tablet=(4, 0, 4, 2), mobile=(0, 0, 4, 2))
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", _two_hero_blocks(layout_a, layout_b))]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    overlap_errors = [e for e in errors if e.code == "grid_overlap"]
    assert overlap_errors, errors
    assert any("mobile" in e.path or "mobile" in e.message for e in overlap_errors), overlap_errors


def test_grid_overlap_at_every_breakpoint_is_still_rejected(metadata, registry):
    """The pre-existing all-breakpoint case (identical layout everywhere)
    must remain rejected once detection tightens to per-breakpoint -- it is
    the strict superset of a single-breakpoint collision, not a separate
    threshold that could be dropped."""
    same_layout_a = builders.layout()
    same_layout_b = builders.layout()
    doc = builders.minimal_valid_document()
    doc["sections"] = [
        builders.make_section("sec-1", _two_hero_blocks(same_layout_a, same_layout_b))
    ]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "grid_overlap" in _codes(errors)


def test_allow_overlap_true_suppresses_the_rejection(metadata, registry):
    same_layout_a = builders.layout()
    same_layout_b = builders.layout()
    doc = builders.minimal_valid_document()
    doc["sections"] = [
        builders.make_section(
            "sec-1", _two_hero_blocks(same_layout_a, same_layout_b), allow_overlap=True
        )
    ]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "grid_overlap" not in _codes(errors), errors


def test_a_block_hidden_at_mobile_does_not_collide_at_mobile(metadata, registry):
    """A block hidden at a breakpoint (visibility.mobile == False) occupies
    nothing there -- an identical mobile position on a block that never
    renders at mobile is not a real collision, even though the desktop and
    tablet positions differ and don't otherwise overlap."""
    layout_a = builders.layout(desktop=(0, 0, 6, 2), tablet=(0, 0, 4, 2), mobile=(0, 0, 4, 2))
    layout_b = builders.layout(desktop=(6, 0, 6, 2), tablet=(4, 0, 4, 2), mobile=(0, 0, 4, 2))
    hidden_at_mobile = builders.visibility(desktop=True, tablet=True, mobile=False)
    doc = builders.minimal_valid_document()
    doc["sections"] = [
        builders.make_section(
            "sec-1", _two_hero_blocks(layout_a, layout_b, visibility_b=hidden_at_mobile)
        )
    ]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "grid_overlap" not in _codes(errors), errors


# --- 6. unknown indicator / union regression -------------------------------------


def test_binding_naming_an_unknown_indicator_is_rejected(metadata, registry):
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(real_data.an_unknown_indicator_id()),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unknown_indicator" in _codes(errors)


def test_municipal_indicator_binding_validates(metadata, registry):
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(real_data.an_additive_municipal_indicator_id()),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unknown_indicator" not in _codes(errors)


def test_national_indicator_binding_validates(metadata, registry):
    """The P1 regression from spec §5: a validator that only consults
    metadata/indicators.json (municipal-only) would wrongly reject every
    national binding -- this is the specific case that would break Batch 3's
    homepage and Batch 6's macro page."""
    national_id = real_data.national_indicator_ids()[0]
    kpi = builders.make_block(
        "kpi_card", block_id="blk-kpi-1", binding=builders.national_binding(national_id)
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unknown_indicator" not in _codes(errors), (national_id, errors)


@pytest.mark.parametrize(
    "national_id",
    ["GDP_ANNUAL_CY", "HICP", "UNEMPLOYMENT_RATE"],
)
def test_named_national_indicators_from_spec_validate(metadata, registry, national_id):
    """The spec calls these three out by name as the regression example.
    Guarded by a real-payload membership assertion first, so this stays a
    real-data check rather than a hand-typed indicator id if the payload
    ever changes."""
    assert (
        national_id in real_data.national_indicator_ids()
    ), f"{national_id} is no longer in national.json -- update the regression example"
    kpi = builders.make_block(
        "kpi_card", block_id="blk-kpi-1", binding=builders.national_binding(national_id)
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unknown_indicator" not in _codes(errors)


# --- 7. NIS validity: invalid / retired / geo-level-mismatch --------------------


def test_binding_naming_an_invalid_nis_code_is_rejected(metadata, registry):
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(
            real_data.an_additive_municipal_indicator_id(), nis=real_data.an_invalid_nis_code()
        ),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "invalid_nis_code" in _codes(errors)


def test_retired_nis_code_yields_retired_not_invalid(metadata, registry):
    """Trap B: a merged commune's old NIS must produce retired_nis_code, with
    the "merged in 2025" wording sourced from the crosswalk -- and must NOT
    produce invalid_nis_code, which would read to Batch 15 / the maintainer
    as a plain typo."""
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(
            real_data.an_additive_municipal_indicator_id(), nis=real_data.a_retired_nis_code()
        ),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    codes = _codes(errors)
    assert "retired_nis_code" in codes
    assert "invalid_nis_code" not in codes


def test_country_level_nis_on_a_municipal_binding_is_geo_level_mismatch(metadata, registry):
    """Trap A: every one of the 622 geographies.json rows has a nis_code, so a
    naive membership test would wrongly accept the country's own NIS (01000)
    for a binding whose provider implies "municipality"."""
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(
            real_data.an_additive_municipal_indicator_id(), nis=real_data.a_country_nis_code()
        ),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    codes = _codes(errors)
    assert "geo_level_mismatch" in codes
    assert "invalid_nis_code" not in codes


def test_region_level_nis_on_a_municipal_binding_is_geo_level_mismatch(metadata, registry):
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(
            real_data.an_additive_municipal_indicator_id(), nis=real_data.a_region_nis_code()
        ),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "geo_level_mismatch" in _codes(errors)


def test_a_real_municipal_nis_on_a_municipal_binding_passes(metadata, registry):
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(
            real_data.an_additive_municipal_indicator_id(), nis=real_data.a_municipal_nis_code()
        ),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    codes = _codes(errors)
    assert "invalid_nis_code" not in codes
    assert "retired_nis_code" not in codes
    assert "geo_level_mismatch" not in codes


# --- 8. forbidden aggregation (§9, corrected reasoning) --------------------------


def test_sum_style_aggregation_of_a_non_additive_indicator_is_forbidden(metadata, registry):
    non_additive = real_data.a_non_additive_municipal_indicator_id()
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(non_additive, aggregate_over="province"),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "forbidden_aggregation" in _codes(errors)


def test_sum_style_aggregation_of_an_additive_indicator_is_allowed(metadata, registry):
    additive = real_data.an_additive_municipal_indicator_id()
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(additive, aggregate_over="province"),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "forbidden_aggregation" not in _codes(errors)


def test_batch_9_does_not_read_aggregation_method_or_reject_the_real_province_average(
    metadata, registry
):
    """§9: aggregation_method is vestigial and must not be read at all; a blunt
    "additive:false -> refuse every aggregate" rule would also wrongly break
    the real province-level AVG_NET_TAXABLE_INCOME figure this pipeline
    already publishes. A plain `latest` binding (no `aggregate`) against a
    non-additive indicator must NOT be rejected as forbidden_aggregation --
    only a binding that explicitly asks to sum across geographies is in
    scope."""
    non_additive = real_data.a_non_additive_municipal_indicator_id()
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=builders.municipal_binding(non_additive),
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [kpi])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "forbidden_aggregation" not in _codes(errors)


# --- 9. missing accessible name on an interactive block (§7) --------------------


def test_interactive_block_missing_accessible_name_is_rejected(metadata, registry):
    """chart is registered interactive (spec §13). Its registry props_schema
    deliberately does NOT list accessible_name under `required` (registry.json's
    own accessible_name_note: a schema `required` would report this as
    schema_violation instead of missing_accessible_name) -- so the plain
    registry-minimal props (no accessible_name key at all) is exactly the
    "missing" case, with no field removal needed."""
    assert builders.is_interactive("chart")
    props = builders.props_for("chart")
    assert "accessible_name" not in props
    chart = builders.make_block(
        "chart",
        block_id="blk-chart-1",
        binding=builders.national_binding(real_data.national_indicator_ids()[0]),
        props=props,
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [chart])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "missing_accessible_name" in _codes(errors)


def test_interactive_block_with_accessible_name_passes(metadata, registry):
    props = builders.props_for("chart")
    props["accessible_name"] = builders.trilingual_instance("chart-accessible-name")
    chart = builders.make_block(
        "chart",
        block_id="blk-chart-1",
        binding=builders.national_binding(real_data.national_indicator_ids()[0]),
        props=props,
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [chart])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "missing_accessible_name" not in _codes(errors)


def test_interactive_map_block_missing_accessible_name_is_rejected(metadata, registry):
    """map is the other interactive type (rule 29). Confirms the check applies
    to every interactive block type, not just chart."""
    assert builders.is_interactive("map")
    props = builders.props_for("map")
    assert "accessible_name" not in props
    map_block = builders.make_block(
        "map",
        block_id="blk-map-1",
        binding=builders.municipal_binding(real_data.an_additive_municipal_indicator_id()),
        props=props,
    )
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [map_block])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "missing_accessible_name" in _codes(errors)


def test_non_interactive_block_does_not_require_an_accessible_name(metadata, registry):
    assert not builders.is_interactive("hero")
    doc = builders.minimal_valid_document()
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "missing_accessible_name" not in _codes(errors)


# --- 10. unsafe content (basic case; full method coverage in hardening test) ----


def test_script_tag_in_a_trilingual_label_is_rejected(metadata, registry):
    doc = builders.minimal_valid_document()
    doc["seo"]["title"]["en"] = "<script>alert(1)</script>"
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" in _codes(errors)


# --- 11. page_type: allowed set, unknown rejected --------------------------------


ALLOWED_PAGE_TYPES = [
    "homepage",
    "macro",
    "micro",
    "municipality-profile",
    "explorer",
    "data-explorer",
    "map-explorer",
    "blank",
]


@pytest.mark.parametrize("page_type", ALLOWED_PAGE_TYPES)
def test_every_allowed_page_type_is_accepted(metadata, registry, page_type):
    doc = builders.minimal_valid_document(page_type=page_type)
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unknown_page_type" not in _codes(errors), (page_type, errors)


def test_unknown_page_type_is_rejected(metadata, registry):
    doc = builders.minimal_valid_document(page_type="not-a-real-page-type")
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unknown_page_type" in _codes(errors)


# --- 12. schema_version handled in test_page_document_schema.py -----------------


# --- §15: trilingual torture string must PASS ------------------------------------


TORTURE_LABEL = {
    "en": "Union of Namur’s neighbourhoods — a case study",
    "fr": (
        "Coïncidence heureuse : l’Union régionale des cœurs de "
        "villages, classée par ORDER (tri) alphabétique, d’Ath à "
        "Zichem — sélection étudiée avec l’œil"
    ),
    "nl": (
        "Bevoegdheidsverdeling tussen de Vlaamse Gemeenschap, "
        "de Franse Gemeenschap en het Brussels Hoofdstedelijk Gewest — "
        "een ‘organisatie’ die niemand één-op-één "
        "kan samenvatten"
    ),
}


def test_belgian_trilingual_torture_string_passes_validation(metadata, registry):
    """A validator that rejects real French/Dutch content containing the
    words "Union", "ORDER" or accented characters is a bug -- this is what
    stops someone implementing SQL-keyword scanning (spec §12)."""
    doc = builders.minimal_valid_document()
    doc["seo"]["title"] = TORTURE_LABEL
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert errors == [], f"legitimate Belgian trilingual content was rejected: {errors}"
