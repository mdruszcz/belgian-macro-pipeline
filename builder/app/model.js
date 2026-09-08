/* BelPulse builder -- document model helpers: structural knowledge of the
 * page-document schema (docs/features/page_document.schema.json) that no
 * endpoint exposes, plus the numeric-fidelity guard.
 *
 * Batch 12b (builder-ui). See history.js's header for the delivery mechanism.
 *
 * WHAT IS HARD-CODED HERE, AND WHY THAT IS NOT THE THING THE SPEC WARNS
 * AGAINST. The batch spec forbids one specific kind of hard-coding: a copy of
 * `route` / `page_type` / `theme`, because those are OPEN allowlists that grow
 * over time in `src/pages/semantics.py`, exposed by no endpoint, and a stale
 * copy would silently reject (or silently accept) the wrong values. Nothing
 * below is that. The block wrapper's own required keys (id/type/version/
 * props/binding/visibility/layout), the binding vocabulary (provider/
 * operation/geography mode/period mode/geo_level/aggregate function) and the
 * grid_cell shape are CLOSED, versioned parts of the document schema itself --
 * there is no endpoint that could serve them because they are not data the
 * server loads, they are the shape of the request body every write route
 * accepts. A document cannot be constructed at all without this knowledge
 * (the same separation tests/fixtures/pages/builders.py makes: block PROPS
 * come from the live registry, the document's own envelope does not). Every
 * value a user chooses through these controls is still re-checked by the
 * server's own POST /api/validate before it is trusted for anything.
 */

(function (shell) {
  "use strict";

  var BINDING_PROVIDERS = [
    "national",
    "municipal",
    "indicator_snapshot",
    "geography",
    "context",
    "comparison",
    "ranking",
  ];

  var BINDING_OPERATIONS = [
    "latest",
    "history",
    "comparison",
    "ranking",
    "percentile",
    "table",
    "map_values",
    "aggregate",
  ];

  var GEO_LEVELS = ["country", "region", "province", "arrondissement", "municipality"];

  var GEOGRAPHY_MODES = ["context", "fixed", "level"];
  var PERIOD_MODES = ["latest", "fixed", "range"];
  var AGGREGATE_FUNCTIONS = ["sum"];

  function deepClone(value) {
    if (typeof structuredClone === "function") {
      return structuredClone(value);
    }
    return JSON.parse(JSON.stringify(value));
  }

  function emptyTrilingual() {
    // Never defaulted from another language (claude.md rule 7): all three
    // start empty and the inspector marks all three as required together.
    return { en: "", fr: "", nl: "" };
  }

  function defaultVisibility() {
    return { desktop: true, tablet: true, mobile: true };
  }

  function defaultLayoutAt(row) {
    // A simple stacked-full-width placement. This is NOT a layout editor --
    // per-breakpoint layout editing is Batch 13's -- it only has to be a
    // legal, non-overlapping grid_cell so a freshly added block validates on
    // grid shape (though it may still fail on required PROPS, which is
    // expected: a new block starts incomplete, not pre-filled).
    var y = row * 2;
    return {
      desktop: { x: 0, y: y, w: 12, h: 2 },
      tablet: { x: 0, y: y, w: 8, h: 2 },
      mobile: { x: 0, y: y, w: 4, h: 2 },
    };
  }

  /* --- grid geometry (Batch 13) ---------------------------------------
   *
   * The grid is the same one the page document declares and the renderer
   * lays out: a fixed column count per breakpoint, rows of arbitrary depth.
   * Column counts are NOT invented here -- they are the widths the existing
   * defaultLayoutAt already places blocks at, and the validator's own
   * grid bounds. A block occupies [x, x+w) x [y, y+h).
   */

  var GRID_COLUMNS = { desktop: 12, tablet: 8, mobile: 4 };
  var BREAKPOINTS = ["desktop", "tablet", "mobile"];

  function columnsFor(breakpoint) {
    return GRID_COLUMNS[breakpoint];
  }

  function cellsOverlap(a, b) {
    // Half-open intervals on both axes: two blocks touching edge-to-edge do
    // NOT overlap, which is the ordinary case of one sitting below another.
    return (
      a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h
    );
  }

  function blocksWithLayout(doc, breakpoint) {
    var out = [];
    (doc.sections || []).forEach(function (section, sectionIndex) {
      (section.blocks || []).forEach(function (block, blockIndex) {
        out.push({
          block: block,
          sectionIndex: sectionIndex,
          blockIndex: blockIndex,
          cell: block.layout[breakpoint],
        });
      });
    });
    return out;
  }

  /**
   * Why a proposed cell cannot be used, or "" if it can.
   *
   * Returns a REASON rather than a boolean so the interface can say what is
   * wrong instead of just refusing -- a silent refusal during a drag reads as
   * a broken builder, and an operator cannot fix what they are not told.
   * Collision is reported against a named block for the same reason.
   */
  function rejectionFor(doc, sectionIndex, blockIndex, breakpoint, cell) {
    var columns = columnsFor(breakpoint);
    if (!cell || cell.w < 1 || cell.h < 1) {
      return "A block must stay at least one column wide and one row tall.";
    }
    if (cell.x < 0 || cell.y < 0) {
      return "A block cannot move above or to the left of the grid.";
    }
    if (cell.x + cell.w > columns) {
      return (
        "That would put the block past the right edge of the " +
        breakpoint +
        " grid, which is " +
        columns +
        " columns wide."
      );
    }
    var section = doc.sections[sectionIndex];
    if (section.allow_overlap) {
      return "";
    }
    var moving = section.blocks[blockIndex];
    var clash = null;
    blocksWithLayout(doc, breakpoint).forEach(function (entry) {
      if (entry.block.id === moving.id || clash) {
        return;
      }
      if (entry.sectionIndex === sectionIndex && cellsOverlap(cell, entry.cell)) {
        clash = entry.block;
      }
    });
    if (clash) {
      return "That would overlap " + clash.id + ", and this section does not allow overlap.";
    }
    return "";
  }

  function lockRejection(block) {
    return block.locked ? "This block is locked. Unlock it to move or resize it." : "";
  }

  /**
   * Place a block at a new cell on ONE breakpoint.
   *
   * Per-breakpoint by design: the three layouts are independently authored
   * (the page document has no reflow engine -- every breakpoint's x/y/w/h is
   * declared), so moving a block on desktop must not silently move it on
   * mobile, where the operator may have arranged something different.
   */
  function setBlockCell(doc, sectionIndex, blockIndex, breakpoint, cell) {
    var next = deepClone(doc);
    var block = next.sections[sectionIndex].blocks[blockIndex];
    block.layout[breakpoint] = { x: cell.x, y: cell.y, w: cell.w, h: cell.h };
    return next;
  }

  function setBlockLocked(doc, sectionIndex, blockIndex, locked) {
    var next = deepClone(doc);
    next.sections[sectionIndex].blocks[blockIndex].locked = !!locked;
    return next;
  }

  /** The cell a block would occupy after a relative nudge. */
  function nudge(cell, dx, dy) {
    return { x: cell.x + dx, y: cell.y + dy, w: cell.w, h: cell.h };
  }

  /** The cell a block would occupy after a relative resize. */
  function grow(cell, dw, dh) {
    return { x: cell.x, y: cell.y, w: cell.w + dw, h: cell.h + dh };
  }

  function slugify(blockType) {
    return String(blockType).replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  }

  function allBlockIds(doc) {
    var ids = {};
    (doc.sections || []).forEach(function (section) {
      (section.blocks || []).forEach(function (block) {
        ids[block.id] = true;
      });
    });
    return ids;
  }

  function allSectionIds(doc) {
    var ids = {};
    (doc.sections || []).forEach(function (section) {
      ids[section.id] = true;
    });
    return ids;
  }

  function freshId(prefix, taken) {
    var n = 1;
    var candidate = prefix + "-" + n;
    while (taken[candidate]) {
      n += 1;
      candidate = prefix + "-" + n;
    }
    return candidate;
  }

  function findBlock(doc, blockId) {
    for (var s = 0; s < (doc.sections || []).length; s += 1) {
      var blocks = doc.sections[s].blocks || [];
      for (var b = 0; b < blocks.length; b += 1) {
        if (blocks[b].id === blockId) {
          return { sectionIndex: s, blockIndex: b, section: doc.sections[s], block: blocks[b] };
        }
      }
    }
    return null;
  }

  function addSection(doc) {
    var next = deepClone(doc);
    var id = freshId("section", allSectionIds(next));
    next.sections = next.sections || [];
    next.sections.push({ id: id, blocks: [] });
    return { doc: next, sectionId: id };
  }

  function removeSection(doc, sectionIndex) {
    var next = deepClone(doc);
    next.sections.splice(sectionIndex, 1);
    return next;
  }

  function moveSection(doc, sectionIndex, direction) {
    var target = sectionIndex + direction;
    if (target < 0 || target >= doc.sections.length) {
      return doc;
    }
    var next = deepClone(doc);
    var moved = next.sections.splice(sectionIndex, 1)[0];
    next.sections.splice(target, 0, moved);
    return next;
  }

  function defaultPropsFor(registry, blockType, version) {
    var entry = registry.block_types[blockType];
    var versionEntry = entry.versions[String(version)];
    var schema = (versionEntry && versionEntry.props_schema) || {};
    var required = schema.required || [];
    var props = {};
    required.forEach(function (key) {
      var propSchema = resolveRef(schema.properties && schema.properties[key], registry.$defs);
      props[key] = minimalValueFor(propSchema, registry.$defs);
    });
    return props;
  }

  function resolveRef(schema, defs) {
    if (schema && schema.$ref) {
      var name = schema.$ref.split("/").pop();
      return defs[name] || {};
    }
    return schema || {};
  }

  function minimalValueFor(schema, defs) {
    schema = resolveRef(schema, defs);
    if (schema.const !== undefined) {
      return schema.const;
    }
    if (schema.enum) {
      return schema.enum[0];
    }
    if (schema.oneOf && schema.oneOf.length) {
      return minimalValueFor(schema.oneOf[0], defs);
    }
    if (schema.anyOf && schema.anyOf.length) {
      return minimalValueFor(schema.anyOf[0], defs);
    }
    if (schema.allOf && schema.allOf.length) {
      var merged = {};
      schema.allOf.forEach(function (sub) {
        var value = minimalValueFor(sub, defs);
        if (value && typeof value === "object") {
          Object.assign(merged, value);
        }
      });
      return merged;
    }
    if (isTrilingualSchema(schema)) {
      return emptyTrilingual();
    }
    if (schema.type === "object") {
      var required = schema.required || [];
      var out = {};
      required.forEach(function (key) {
        out[key] = minimalValueFor(schema.properties && schema.properties[key], defs);
      });
      return out;
    }
    if (schema.type === "array") {
      var minItems = schema.minItems || 0;
      var items = [];
      for (var i = 0; i < minItems; i += 1) {
        items.push(minimalValueFor(schema.items, defs));
      }
      return items;
    }
    if (schema.type === "boolean") {
      return false;
    }
    if (schema.type === "string") {
      return "";
    }
    return null;
  }

  function isTrilingualSchema(schema) {
    if (!schema || schema.type !== "object") {
      return false;
    }
    var required = schema.required || [];
    var hasAll = ["en", "fr", "nl"].every(function (lang) {
      return required.indexOf(lang) !== -1;
    });
    if (!hasAll) {
      return false;
    }
    var props = schema.properties || {};
    return ["en", "fr", "nl"].every(function (lang) {
      return props[lang] && props[lang].type === "string";
    });
  }

  function addBlock(doc, registry, sectionIndex, blockType) {
    var entry = registry.block_types[blockType];
    var version = entry.current_version;
    var next = deepClone(doc);
    var id = freshId(slugify(blockType), allBlockIds(next));
    var row = (next.sections[sectionIndex].blocks || []).length;
    var block = {
      id: id,
      type: blockType,
      version: version,
      props: defaultPropsFor(registry, blockType, version),
      binding: null,
      visibility: defaultVisibility(),
      layout: defaultLayoutAt(row),
      // Required since schema_version 2. A block the operator just placed is
      // one they are about to arrange, so it starts unlocked.
      locked: false,
    };
    next.sections[sectionIndex].blocks.push(block);
    return { doc: next, blockId: id };
  }

  function removeBlock(doc, sectionIndex, blockIndex) {
    var next = deepClone(doc);
    next.sections[sectionIndex].blocks.splice(blockIndex, 1);
    return next;
  }

  function duplicateBlock(doc, sectionIndex, blockIndex) {
    var next = deepClone(doc);
    var original = next.sections[sectionIndex].blocks[blockIndex];
    var copy = deepClone(original);
    copy.id = freshId(slugify(original.type), allBlockIds(next));
    next.sections[sectionIndex].blocks.splice(blockIndex + 1, 0, copy);
    return { doc: next, blockId: copy.id };
  }

  function moveBlock(doc, sectionIndex, blockIndex, direction) {
    var blocks = doc.sections[sectionIndex].blocks;
    var target = blockIndex + direction;
    if (target < 0 || target >= blocks.length) {
      return doc;
    }
    var next = deepClone(doc);
    var list = next.sections[sectionIndex].blocks;
    var moved = list.splice(blockIndex, 1)[0];
    list.splice(target, 0, moved);
    return next;
  }

  function setAtPath(doc, pathParts, value) {
    var next = deepClone(doc);
    var cursor = next;
    for (var i = 0; i < pathParts.length - 1; i += 1) {
      cursor = cursor[pathParts[i]];
    }
    cursor[pathParts[pathParts.length - 1]] = value;
    return next;
  }

  /* --- server error paths ------------------------------------------------
   *
   * `format_path()` in src/pages/schema.py renders a rejection's location as
   * "sections/0/blocks/1/props/heading/fr", or "(root)". Parsed here so a
   * 422 lands on the right block and field instead of a raw dump.
   */
  function parseErrorPath(path) {
    if (!path || path === "(root)") {
      return { root: true, segments: [] };
    }
    var segments = path.split("/");
    if (segments[0] === "sections" && segments.length >= 4 && segments[2] === "blocks") {
      return {
        root: false,
        sectionIndex: parseInt(segments[1], 10),
        blockIndex: parseInt(segments[3], 10),
        rest: segments.slice(4),
      };
    }
    return { root: false, segments: segments };
  }

  /* --- browser JSON numeric fidelity -------------------------------------
   *
   * A JSON number token is "risky" when re-serialising it (as any editor that
   * round-trips through JSON.parse/JSON.stringify must) would not reproduce
   * the same bytes: `1.0` -> 1, `1e3` -> 1000, and an integer past 2^53 loses
   * precision entirely. `String(Number(token)) !== token` catches all three
   * with one rule, because it is exactly the question "does this survive a
   * parse and a plain re-print unchanged".
   *
   * String contents are masked out first (with same-length filler, so
   * indices into the ORIGINAL text stay valid) so a piece of user text that
   * happens to contain digits is never mistaken for a JSON number.
   */
  function maskStrings(text) {
    return text.replace(/"(?:[^"\\]|\\.)*"/g, function (match) {
      return match.charAt(0) + "a".repeat(match.length - 2) + match.charAt(0);
    });
  }

  function scanNumberFidelity(rawText) {
    var masked = maskStrings(rawText);
    var re = /-?\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b/g;
    var match;
    var risky = [];
    while ((match = re.exec(masked)) !== null) {
      var token = rawText.slice(match.index, match.index + match[0].length);
      if (String(Number(token)) !== token) {
        risky.push(token);
      }
    }
    return { risky: risky, safe: risky.length === 0 };
  }

  shell.model = {
    BINDING_PROVIDERS: BINDING_PROVIDERS,
    BINDING_OPERATIONS: BINDING_OPERATIONS,
    GEO_LEVELS: GEO_LEVELS,
    GEOGRAPHY_MODES: GEOGRAPHY_MODES,
    PERIOD_MODES: PERIOD_MODES,
    AGGREGATE_FUNCTIONS: AGGREGATE_FUNCTIONS,
    deepClone: deepClone,
    emptyTrilingual: emptyTrilingual,
    defaultVisibility: defaultVisibility,
    defaultLayoutAt: defaultLayoutAt,
    GRID_COLUMNS: GRID_COLUMNS,
    BREAKPOINTS: BREAKPOINTS,
    columnsFor: columnsFor,
    cellsOverlap: cellsOverlap,
    blocksWithLayout: blocksWithLayout,
    rejectionFor: rejectionFor,
    lockRejection: lockRejection,
    setBlockCell: setBlockCell,
    setBlockLocked: setBlockLocked,
    nudge: nudge,
    grow: grow,
    isTrilingualSchema: isTrilingualSchema,
    resolveRef: resolveRef,
    minimalValueFor: minimalValueFor,
    findBlock: findBlock,
    addSection: addSection,
    removeSection: removeSection,
    moveSection: moveSection,
    addBlock: addBlock,
    removeBlock: removeBlock,
    duplicateBlock: duplicateBlock,
    moveBlock: moveBlock,
    setAtPath: setAtPath,
    parseErrorPath: parseErrorPath,
    scanNumberFidelity: scanNumberFidelity,
  };
})(shell);
