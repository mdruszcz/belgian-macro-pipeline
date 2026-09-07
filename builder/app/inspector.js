/* BelPulse builder -- the inspector: six panels over the selected block
 * (content / layout / appearance / data / accessibility / visibility).
 *
 * Batch 12b (builder-ui). See history.js's header for the delivery mechanism.
 *
 * Every field a block can carry is driven by the registry's OWN declared
 * props schema (fetched from GET /api/registry, never a second copy of the
 * block list). Where a declared prop has no editor here, its value is shown
 * read-only, verbatim, never dropped and never silently rewritten -- the
 * batch spec is explicit that this, not an "unrecognised prop", is the real
 * case, because every props schema is additionalProperties:false.
 *
 * Trilingual {en, fr, nl} fields are three independent inputs. None is ever
 * copied from another (claude.md rule 7).
 */

(function (shell) {
  "use strict";

  var dom = shell.dom;
  var el = dom.el;
  var model = shell.model;

  var PANELS = ["content", "layout", "appearance", "data", "accessibility", "visibility"];
  var PANEL_LABELS = {
    content: "Content",
    layout: "Layout",
    appearance: "Appearance",
    data: "Data",
    accessibility: "Accessibility",
    visibility: "Visibility",
  };

  var uid = 0;
  function nextId(prefix) {
    uid += 1;
    return prefix + "-" + uid;
  }

  /* --- generic field widgets -------------------------------------------- */

  function labelled(labelText, control, help) {
    var id = control.id || (control.id = nextId("f"));
    var children = [el("label", { for: id }, [labelText]), control];
    if (help) {
      children.push(el("p", { className: "bp-help" }, [help]));
    }
    return el("div", { className: "bp-field" }, children);
  }

  function trilingualField(labelText, value, onChange, required) {
    // `current` is shared by all three inputs' handlers for the LIFETIME of
    // this fieldset (this field is a "quiet" control: typing never re-renders
    // it, see the panel-level comment on why). Rebuilding the triple from the
    // ORIGINAL `value` argument on every keystroke -- instead of from what was
    // just typed into the other two inputs -- was a real bug here: typing EN
    // then FR silently reset EN back to empty, because FR's handler still saw
    // the stale pre-edit snapshot. `current` is mutated in place instead, so
    // each field's edit builds on the others' latest values, not their first.
    var current = Object.assign({}, model.emptyTrilingual(), value || {});
    var wrap = el("fieldset", { className: "bp-trilingual" }, [
      el("legend", {}, [labelText + (required ? " (required, all three languages)" : "")]),
    ]);
    ["en", "fr", "nl"].forEach(function (lang) {
      var input = el("input", {
        type: "text",
        value: current[lang] || "",
        "aria-label": labelText + " (" + lang.toUpperCase() + ")",
        oninput: function (evt) {
          current[lang] = evt.target.value;
          onChange(Object.assign({}, current), lang);
        },
      });
      wrap.appendChild(
        el("div", { className: "bp-lang-row" }, [
          el("span", { className: "bp-lang-tag" }, [lang.toUpperCase()]),
          input,
        ])
      );
    });
    return wrap;
  }

  function textField(labelText, value, onChange, opts) {
    opts = opts || {};
    var input = el("input", {
      type: "text",
      value: value || "",
      pattern: opts.pattern || null,
      oninput: function (evt) {
        onChange(evt.target.value);
      },
    });
    return labelled(labelText, input, opts.help);
  }

  function booleanField(labelText, value, onChange) {
    var input = el("input", {
      type: "checkbox",
      checked: value === true,
      onchange: function (evt) {
        onChange(evt.target.checked);
      },
    });
    var id = (input.id = nextId("f"));
    return el("div", { className: "bp-field bp-field-checkbox" }, [
      input,
      el("label", { for: id }, [labelText]),
    ]);
  }

  function enumField(labelText, options, value, onChange) {
    var select = el(
      "select",
      {
        onchange: function (evt) {
          onChange(evt.target.value);
        },
      },
      options.map(function (opt) {
        return el("option", { value: opt, selected: opt === value }, [opt]);
      })
    );
    return labelled(labelText, select);
  }

  function readOnlyField(labelText, value) {
    var pre = el("pre", { className: "bp-readonly" }, [JSON.stringify(value, null, 2)]);
    return el("div", { className: "bp-field" }, [
      el("p", { className: "bp-field-title" }, [labelText]),
      el("p", { className: "bp-help" }, [
        "The builder has no editor for this field yet. Its value is kept exactly as loaded.",
      ]),
      pre,
    ]);
  }

  /* --- classifying a declared prop --------------------------------------- */

  function classifyProp(key, resolvedSchema) {
    if (key === "accessible_name") {
      return "accessibility";
    }
    if (model.isTrilingualSchema(resolvedSchema)) {
      return "content";
    }
    if (resolvedSchema.type === "boolean" || resolvedSchema.enum) {
      return "appearance";
    }
    if (isCtaSchema(resolvedSchema)) {
      return "content";
    }
    if (isRichTextContentSchema(resolvedSchema)) {
      return "content";
    }
    return "content";
  }

  function isCtaSchema(schema) {
    if (!schema || schema.type !== "object") {
      return false;
    }
    var required = schema.required || [];
    return required.indexOf("label") !== -1 && required.indexOf("href") !== -1;
  }

  function isRichTextContentSchema(schema) {
    return !!schema && schema.type === "array" && !!schema.items;
  }

  /* --- one prop's control -------------------------------------------------
   *
   * Returns a DOM node. `onChange(newValue)` replaces this prop's whole
   * value; callers own how that becomes a history push (a text edit
   * coalesces, a discrete change does not).
   */
  function renderPropControl(defs, key, rawSchema, value, onChange) {
    var schema = model.resolveRef(rawSchema, defs);
    if (model.isTrilingualSchema(schema)) {
      return trilingualField(titleCase(key), value, onChange, true);
    }
    if (isCtaSchema(schema)) {
      // Same shared-mutable-snapshot fix as trilingualField, and for the same
      // reason: this control is quiet (no re-render while typing), so label
      // and href must not each rebuild the CTA from a stale pre-edit copy of
      // the other.
      var current = {
        label: (value && value.label) || model.emptyTrilingual(),
        href: (value && value.href) || "",
      };
      var box = el("fieldset", { className: "bp-cta" }, [el("legend", {}, [titleCase(key)])]);
      box.appendChild(
        trilingualField("Label", current.label, function (next) {
          current.label = next;
          onChange({ label: current.label, href: current.href });
        })
      );
      box.appendChild(
        textField(
          "Link (site-relative path, e.g. /about/, or a #fragment)",
          current.href,
          function (next) {
            current.href = next;
            onChange({ label: current.label, href: current.href });
          },
          { pattern: "^(/|#).*" }
        )
      );
      return box;
    }
    if (schema.enum) {
      return enumField(titleCase(key), schema.enum, value, onChange);
    }
    if (schema.type === "boolean") {
      return booleanField(titleCase(key), value, onChange);
    }
    if (schema.type === "string") {
      return textField(titleCase(key), value, onChange);
    }
    return readOnlyField(titleCase(key), value);
  }

  function titleCase(key) {
    return String(key)
      .replace(/_/g, " ")
      .replace(/\b\w/g, function (c) {
        return c.toUpperCase();
      });
  }

  /* --- rich_text content: paragraphs are editable, everything else is kept
   * read-only, verbatim (invariant: never drop a value with no control) --- */

  function renderRichTextContent(value, onChange) {
    // `current` is a shared, mutated-in-place snapshot of the whole array,
    // for the same reason trilingualField and the data panel keep one: a
    // keystroke in one paragraph must not silently undo a keystroke already
    // made in another, which merging against the ORIGINAL `value` argument on
    // every edit would do the moment two paragraphs are edited without a
    // re-render between them.
    var current = (value || []).slice();
    var wrap = el("div", { className: "bp-richtext" });
    current.forEach(function (node, index) {
      if (node && node.node === "paragraph") {
        var item = el("div", { className: "bp-richtext-item" });
        item.appendChild(
          trilingualField("Paragraph " + (index + 1), node.text, function (nextText) {
            // A text edit: quiet, coalesced, no panel re-render.
            current[index] = { node: "paragraph", text: nextText };
            onChange(current.slice(), false, "richtext." + index);
          })
        );
        item.appendChild(
          el(
            "button",
            {
              type: "button",
              className: "bp-btn-small",
              "aria-label": "Remove paragraph " + (index + 1),
              onclick: function () {
                // A structural edit: the panel's own shape changes, so this
                // one DOES re-render (a click, not a keystroke).
                current.splice(index, 1);
                onChange(current.slice(), true);
              },
            },
            ["Remove paragraph"]
          )
        );
        wrap.appendChild(item);
      } else {
        var ro = readOnlyField(
          "Content item " + (index + 1) + " (" + ((node && node.node) || "unknown") + ")",
          node
        );
        ro.appendChild(
          el("p", { className: "bp-help" }, [
            "Only paragraph nodes are editable in this batch; this node type is preserved as-is.",
          ])
        );
        wrap.appendChild(ro);
      }
    });
    wrap.appendChild(
      el(
        "button",
        {
          type: "button",
          className: "bp-btn-small",
          onclick: function () {
            current.push({ node: "paragraph", text: model.emptyTrilingual() });
            onChange(current.slice(), true);
          },
        },
        ["+ Add paragraph"]
      )
    );
    return wrap;
  }

  /* --- data panel: the binding descriptor -------------------------------- */

  function renderDataPanel(store, container, block, blockEntry) {
    var acceptsBinding = !!(blockEntry && blockEntry.accepts_binding);
    var requiresBinding = !!(blockEntry && blockEntry.requires_binding);
    if (!acceptsBinding) {
      container.appendChild(
        el("p", { className: "bp-help" }, ["This block type does not accept a data binding."])
      );
      return;
    }
    var binding = block.binding;
    if (binding === null || binding === undefined) {
      container.appendChild(
        el("p", { className: requiresBinding ? "bp-warning" : "bp-help" }, [
          requiresBinding
            ? "This block type requires a binding. It will not validate until one is set."
            : "No binding is set on this block.",
        ])
      );
      container.appendChild(
        el(
          "button",
          {
            type: "button",
            onclick: function () {
              store.actions.setBinding(
                block.id,
                { provider: model.BINDING_PROVIDERS[0], operation: model.BINDING_OPERATIONS[0] },
                { rerenderInspector: true }
              );
            },
          },
          ["Set a binding"]
        )
      );
      return;
    }

    // `current` is a live snapshot shared by every control below, mutated in
    // place. Free-text edits are quiet (no re-render -- see the fieldset
    // comment in trilingualField for why), so merging a new patch against the
    // ORIGINAL `binding` argument on every keystroke would silently undo
    // whatever the operator had just typed into a DIFFERENT field in this
    // same panel (e.g. type the indicator code, then the NIS code: the second
    // edit would merge onto a `binding` that had never heard about the
    // first). Mutating one shared object avoids that.
    var current = Object.assign({}, binding);

    // Structural changes (a mode select that reveals or hides other fields)
    // re-render the panel; free-text edits coalesce into one undo step per
    // field and never re-render, so a keystroke never steals focus back from
    // the input the operator is typing into.
    function updateStructural(patch) {
      Object.assign(current, patch);
      store.actions.setBinding(block.id, Object.assign({}, current), { rerenderInspector: true });
    }
    function updateText(patch, fieldKey) {
      Object.assign(current, patch);
      store.actions.setBinding(block.id, Object.assign({}, current), {
        coalesceKey: block.id + ".binding." + fieldKey,
        rerenderInspector: false,
      });
    }

    container.appendChild(
      enumField("Provider", model.BINDING_PROVIDERS, binding.provider, function (v) {
        updateStructural({ provider: v });
      })
    );
    container.appendChild(
      textField(
        "Indicator code (e.g. UNEMPLOYMENT_RATE)",
        binding.indicator || "",
        function (v) {
          updateText({ indicator: v }, "indicator");
        },
        { pattern: "^[A-Z][A-Z0-9_]*$" }
      )
    );
    container.appendChild(
      enumField("Operation", model.BINDING_OPERATIONS, binding.operation, function (v) {
        updateStructural({ operation: v });
      })
    );

    // geography
    var geography = binding.geography || { mode: "context" };
    container.appendChild(
      enumField("Geography mode", model.GEOGRAPHY_MODES, geography.mode, function (v) {
        var next = { mode: v };
        if (v === "fixed") {
          next.nis = geography.nis || "";
        }
        if (v === "level") {
          next.level = geography.level || model.GEO_LEVELS[0];
        }
        updateStructural({ geography: next });
      })
    );
    if (geography.mode === "fixed") {
      container.appendChild(
        textField(
          "NIS code (5 digits)",
          geography.nis || "",
          function (v) {
            updateText(
              { geography: Object.assign({}, current.geography, { nis: v }) },
              "geography.nis"
            );
          },
          { pattern: "^[0-9]{5}$" }
        )
      );
    }
    if (geography.mode === "level") {
      container.appendChild(
        enumField("Geography level", model.GEO_LEVELS, geography.level, function (v) {
          updateStructural({ geography: Object.assign({}, geography, { level: v }) });
        })
      );
    }

    // period
    var period = binding.period || { mode: "latest" };
    container.appendChild(
      enumField("Period mode", model.PERIOD_MODES, period.mode, function (v) {
        var next = { mode: v };
        if (v === "fixed") {
          next.period = period.period || "";
        }
        if (v === "range") {
          next.from = period.from || "";
          next.to = period.to || "";
        }
        updateStructural({ period: next });
      })
    );
    if (period.mode === "fixed") {
      container.appendChild(
        textField(
          "Period (YYYY, YYYY-MM, or YYYY-Qn)",
          period.period || "",
          function (v) {
            updateText(
              { period: Object.assign({}, current.period, { period: v }) },
              "period.period"
            );
          },
          { pattern: "^[0-9]{4}(-(0[1-9]|1[0-2])|-Q[1-4])?$" }
        )
      );
    }
    if (period.mode === "range") {
      // Both fields are visible and quiet at once, so each patch is merged
      // against `current.period` (live) rather than the `period` snapshot
      // taken at render time -- otherwise editing From then To would silently
      // erase what was just typed into From.
      container.appendChild(
        textField("From", period.from || "", function (v) {
          updateText(
            { period: Object.assign({}, current.period, { from: v }) },
            "period.from"
          );
        })
      );
      container.appendChild(
        textField("To", period.to || "", function (v) {
          updateText({ period: Object.assign({}, current.period, { to: v }) }, "period.to");
        })
      );
    }

    // aggregate, only meaningful for operation === "aggregate"
    if (binding.operation === "aggregate") {
      var aggregate = binding.aggregate || { function: "sum", over: model.GEO_LEVELS[0] };
      container.appendChild(
        enumField("Aggregate function", model.AGGREGATE_FUNCTIONS, aggregate["function"], function (v) {
          updateStructural({ aggregate: Object.assign({}, aggregate, { function: v }) });
        })
      );
      container.appendChild(
        enumField("Aggregate over", model.GEO_LEVELS, aggregate.over, function (v) {
          updateStructural({ aggregate: Object.assign({}, aggregate, { over: v }) });
        })
      );
    }

    container.appendChild(
      el(
        "button",
        {
          type: "button",
          className: "bp-btn-small",
          onclick: function () {
            store.actions.setBinding(block.id, null, { rerenderInspector: true });
          },
        },
        ["Remove binding"]
      )
    );

    // Live validation feedback against the server's own validator -- the
    // batch deliberately ships no indicator picker (Batch 14), so this is
    // the only correctness signal the data panel gets.
    var feedback = el("div", { className: "bp-validate-feedback", "aria-live": "polite" });
    container.appendChild(feedback);
    store.actions.requestLiveValidate(function (result) {
      if (!feedback.isConnected) {
        return; // the panel was re-rendered before this answer arrived
      }
      dom.clear(feedback);
      if (result === null) {
        feedback.appendChild(el("p", { className: "bp-help" }, ["Checking against the server…"]));
        return;
      }
      if (result.ok) {
        feedback.appendChild(el("p", { className: "bp-ok" }, ["Valid so far."]));
        return;
      }
      var relevant = (result.errors || []).filter(function (e) {
        var parsed = model.parseErrorPath(e.path);
        return !parsed.root && block.id === blockIdAt(store.doc, parsed);
      });
      var list = el("ul", { className: "bp-errors" });
      (relevant.length ? relevant : result.errors || []).forEach(function (e) {
        list.appendChild(el("li", {}, [e.path + ": " + e.message]));
      });
      feedback.appendChild(list);
    });
  }

  function blockIdAt(doc, parsed) {
    if (parsed.root || parsed.sectionIndex === undefined) {
      return null;
    }
    var section = doc.sections[parsed.sectionIndex];
    var block = section && section.blocks[parsed.blockIndex];
    return block ? block.id : null;
  }

  /* --- layout / visibility panels ---------------------------------------- */

  function renderLayoutPanel(container, block) {
    container.appendChild(
      el("p", { className: "bp-help" }, [
        "Per-breakpoint position and size are set here as read-only values. " +
          "Dragging and resizing on the grid is a later batch; nothing here is editable yet.",
      ])
    );
    ["desktop", "tablet", "mobile"].forEach(function (bp) {
      var cell = block.layout[bp];
      container.appendChild(
        readOnlyField(titleCase(bp) + " grid position", cell)
      );
    });
  }

  function renderVisibilityPanel(store, container, block) {
    ["desktop", "tablet", "mobile"].forEach(function (bp) {
      container.appendChild(
        booleanField("Visible on " + bp, block.visibility[bp], function (checked) {
          var next = Object.assign({}, block.visibility);
          next[bp] = checked;
          store.actions.setVisibility(block.id, next, { rerenderInspector: false });
        })
      );
    });
  }

  /* --- top-level render ---------------------------------------------------
   */

  function render(store, container) {
    uid = 0;
    dom.clear(container);
    container.appendChild(el("h2", { className: "bp-panel-heading" }, ["Inspector"]));

    if (!store.doc || !store.selection) {
      container.appendChild(
        el("p", { className: "bp-help" }, ["Select a block in the structure tree to edit it."])
      );
      return;
    }
    var found = model.findBlock(store.doc, store.selection);
    if (!found) {
      container.appendChild(el("p", { className: "bp-help" }, ["That block no longer exists."]));
      return;
    }
    var block = found.block;
    var blockEntry = store.registry.block_types[block.type];

    container.appendChild(
      el("p", { className: "bp-block-caption" }, [block.type + " v" + block.version + " — " + block.id])
    );

    var tabs = el(
      "div",
      { role: "tablist", "aria-label": "Inspector panels", className: "bp-tabs" },
      PANELS.map(function (panel) {
        var selected = panel === store.panel;
        return el(
          "button",
          {
            type: "button",
            role: "tab",
            id: "tab-" + panel,
            "aria-selected": selected ? "true" : "false",
            "aria-controls": "tabpanel-" + panel,
            tabindex: selected ? "0" : "-1",
            className: "bp-tab" + (selected ? " bp-tab-selected" : ""),
            onclick: function () {
              store.actions.setPanel(panel);
            },
            onkeydown: function (evt) {
              var idx = PANELS.indexOf(panel);
              if (evt.key === "ArrowRight") {
                store.actions.setPanel(PANELS[(idx + 1) % PANELS.length]);
                focusTab(container, PANELS[(idx + 1) % PANELS.length]);
              } else if (evt.key === "ArrowLeft") {
                store.actions.setPanel(PANELS[(idx - 1 + PANELS.length) % PANELS.length]);
                focusTab(container, PANELS[(idx - 1 + PANELS.length) % PANELS.length]);
              }
            },
          },
          [PANEL_LABELS[panel]]
        );
      })
    );
    container.appendChild(tabs);

    var panelBody = el("div", {
      role: "tabpanel",
      id: "tabpanel-" + store.panel,
      "aria-labelledby": "tab-" + store.panel,
      className: "bp-tabpanel",
    });
    container.appendChild(panelBody);

    if (store.panel === "content") {
      renderContentPanel(store, panelBody, block, blockEntry);
    } else if (store.panel === "layout") {
      renderLayoutPanel(panelBody, block);
    } else if (store.panel === "appearance") {
      renderAppearanceAndAccessibility(store, panelBody, block, blockEntry, "appearance");
    } else if (store.panel === "data") {
      renderDataPanel(store, panelBody, block, blockEntry);
    } else if (store.panel === "accessibility") {
      renderAppearanceAndAccessibility(store, panelBody, block, blockEntry, "accessibility");
    } else if (store.panel === "visibility") {
      renderVisibilityPanel(store, panelBody, block);
    }
  }

  function focusTab(container, panel) {
    var node = container.querySelector("#tab-" + panel);
    if (node) {
      node.focus();
    }
  }

  function propSchemaFor(blockEntry, block) {
    var versionEntry = blockEntry && blockEntry.versions && blockEntry.versions[String(block.version)];
    return (versionEntry && versionEntry.props_schema) || { properties: {} };
  }

  function renderContentPanel(store, container, block, blockEntry) {
    var schema = propSchemaFor(blockEntry, block);
    var props = block.props || {};
    var keys = Object.keys(props);
    if (keys.length === 0) {
      container.appendChild(el("p", { className: "bp-help" }, ["This block has no content fields."]));
    }
    keys.forEach(function (key) {
      var rawSchema = (schema.properties && schema.properties[key]) || {};
      var resolved = model.resolveRef(rawSchema, store.registry.$defs);
      if (classifyProp(key, resolved) !== "content") {
        return;
      }
      if (isRichTextContentSchema(resolved)) {
        container.appendChild(el("p", { className: "bp-field-title" }, [titleCase(key)]));
        container.appendChild(
          renderRichTextContent(props[key], function (next, structural, coalesceKey) {
            store.actions.updateProp(block.id, key, next, {
              rerenderInspector: !!structural,
              coalesceKey: coalesceKey ? block.id + "." + coalesceKey : undefined,
            });
          })
        );
        return;
      }
      container.appendChild(
        renderPropControl(store.registry.$defs, key, rawSchema, props[key], function (next) {
          // Coalesced per field so a run of keystrokes is one undo step, and
          // never re-rendered so the input never loses focus while typing.
          store.actions.updateProp(block.id, key, next, {
            coalesceKey: block.id + ".props." + key,
            rerenderInspector: false,
          });
        })
      );
    });
    renderAddOptionalProp(store, container, block, schema, "content");
  }

  function renderAppearanceAndAccessibility(store, container, block, blockEntry, which) {
    var schema = propSchemaFor(blockEntry, block);
    var props = block.props || {};
    var keys = Object.keys(props);
    var any = false;
    keys.forEach(function (key) {
      var rawSchema = (schema.properties && schema.properties[key]) || {};
      var resolved = model.resolveRef(rawSchema, store.registry.$defs);
      if (classifyProp(key, resolved) !== which) {
        return;
      }
      any = true;
      container.appendChild(
        renderPropControl(store.registry.$defs, key, rawSchema, props[key], function (next) {
          store.actions.updateProp(block.id, key, next, {
            coalesceKey: block.id + ".props." + key,
            rerenderInspector: false,
          });
        })
      );
    });
    if (!any) {
      container.appendChild(
        el("p", { className: "bp-help" }, [
          which === "accessibility"
            ? "This block declares no accessible-name field."
            : "This block has no appearance fields.",
        ])
      );
    }
    renderAddOptionalProp(store, container, block, schema, which);
  }

  function renderAddOptionalProp(store, container, block, schema, forPanel) {
    var props = block.props || {};
    var required = schema.required || [];
    var available = Object.keys(schema.properties || {}).filter(function (key) {
      if (required.indexOf(key) !== -1) {
        return false;
      }
      if (Object.prototype.hasOwnProperty.call(props, key)) {
        return false;
      }
      var resolved = model.resolveRef(schema.properties[key], store.registry.$defs);
      return classifyProp(key, resolved) === forPanel;
    });
    if (available.length === 0) {
      return;
    }
    var select = el(
      "select",
      { "aria-label": "Add an optional field" },
      [el("option", { value: "" }, ["Add a field…"])].concat(
        available.map(function (key) {
          return el("option", { value: key }, [titleCase(key)]);
        })
      )
    );
    select.addEventListener("change", function () {
      if (!select.value) {
        return;
      }
      var key = select.value;
      var resolved = model.resolveRef(schema.properties[key], store.registry.$defs);
      var value = model.minimalValueFor(resolved, store.registry.$defs);
      store.actions.updateProp(block.id, key, value, { rerenderInspector: true });
    });
    container.appendChild(el("div", { className: "bp-add-field" }, [select]));
  }

  shell.inspector = { render: render, classifyProp: classifyProp, resolveForBlock: propSchemaFor };
})(shell);
