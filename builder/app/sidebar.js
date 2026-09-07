/* BelPulse builder -- the left panel: page selector + new-page form, block
 * library (driven entirely by GET /api/registry), and the structure tree.
 *
 * Batch 12b (builder-ui). See history.js's header for the delivery mechanism.
 *
 * Invariant 3, restated for this file specifically: a block-library entry is
 * a name, a letter-icon derived from that name, and the registry's OWN
 * descriptive text (`why_in_batch_9`) -- never a thumbnail carrying a made-up
 * figure. Nothing here hand-types an indicator value, a commune figure, or a
 * plausible-looking placeholder number.
 */

(function (shell) {
  "use strict";

  var dom = shell.dom;
  var el = dom.el;
  var model = shell.model;

  /* ---------------------------------------------------------------------
   * page selector + new-page form
   * ------------------------------------------------------------------- */

  function renderPageSelector(store, container) {
    container.appendChild(el("h2", { className: "bp-panel-heading" }, ["Pages"]));

    if (store.pages.length === 0) {
      container.appendChild(
        el("p", { className: "bp-help" }, [
          "No pages exist yet. config/pages/ holds only its README. Create the first " +
            "one below -- a page is never blank in the schema sense: it still needs a " +
            "route, a page type, a theme and a non-empty title in all three languages.",
        ])
      );
    }

    var list = el("ul", { className: "bp-page-list", "aria-label": "Existing pages" });
    store.pages.forEach(function (page) {
      var actions = [];
      if (page.has_draft) {
        actions.push(
          el(
            "button",
            {
              type: "button",
              onclick: function () {
                store.actions.openPage(page.page_id, "draft");
              },
            },
            ["Open draft"]
          )
        );
      }
      if (page.has_published) {
        actions.push(
          el(
            "button",
            {
              type: "button",
              onclick: function () {
                store.actions.openPage(page.page_id, "published");
              },
            },
            ["Open published"]
          )
        );
      }
      list.appendChild(
        el("li", { className: "bp-page-item" }, [
          el("span", { className: "bp-page-id" }, [page.page_id]),
          el("span", { className: "bp-page-meta" }, [
            (page.has_draft ? "draft" : "no draft") +
              " · " +
              (page.has_published ? "published" : "unpublished") +
              " · " +
              page.versions +
              " version(s)",
          ]),
          el("span", { className: "bp-page-actions" }, actions),
        ])
      );
    });
    container.appendChild(list);

    if (store.showNewPageForm) {
      container.appendChild(renderNewPageForm(store));
    } else {
      container.appendChild(
        el(
          "button",
          {
            type: "button",
            onclick: function () {
              store.actions.toggleNewPageForm(true);
            },
          },
          ["+ New page"]
        )
      );
    }
  }

  function renderNewPageForm(store) {
    var draft = store.newPageDraft;
    var form = el("form", {
      className: "bp-new-page-form",
      "aria-label": "New page",
      onsubmit: function (evt) {
        evt.preventDefault();
        store.actions.submitNewPage();
      },
    });

    function quietField(labelText, key, help) {
      var input = el("input", {
        type: "text",
        value: draft[key],
        oninput: function (evt) {
          draft[key] = evt.target.value;
        },
      });
      var id = (input.id = "npf-" + key);
      var nodes = [el("label", { for: id }, [labelText]), input];
      if (help) {
        nodes.push(el("p", { className: "bp-help" }, [help]));
      }
      return el("div", { className: "bp-field" }, nodes);
    }

    function quietTrilingual(labelText, key) {
      var wrap = el("fieldset", { className: "bp-trilingual" }, [
        el("legend", {}, [labelText + " (required, all three languages)"]),
      ]);
      ["en", "fr", "nl"].forEach(function (lang) {
        var input = el("input", {
          type: "text",
          value: draft[key][lang],
          "aria-label": labelText + " (" + lang.toUpperCase() + ")",
          oninput: function (evt) {
            draft[key][lang] = evt.target.value;
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

    form.appendChild(
      quietField("Page id", "page_id", "Lowercase letters, digits and hyphens, e.g. about-us.")
    );
    form.appendChild(
      quietField(
        "Route",
        "route",
        "Checked by the server; there is no local copy of the allowed shapes."
      )
    );
    form.appendChild(
      quietField(
        "Page type",
        "page_type",
        "Also checked by the server. Pre-filled with \"blank\", which is what this " +
          "batch's new-page flow creates -- templates are a later batch."
      )
    );
    form.appendChild(
      quietField("Theme", "theme", "A theme name the renderer recognises under assets/belpulse/.")
    );
    form.appendChild(quietTrilingual("SEO title", "title"));
    form.appendChild(quietTrilingual("SEO description", "description"));

    var feedback = el("div", { className: "bp-validate-feedback", "aria-live": "polite" });
    form.appendChild(feedback);
    store.actions.requestNewPageLiveValidate(function (result) {
      if (!feedback.isConnected) {
        return;
      }
      dom.clear(feedback);
      if (result === null) {
        return;
      }
      if (result.ok) {
        feedback.appendChild(el("p", { className: "bp-ok" }, ["This would be a valid page."]));
        return;
      }
      var list = el("ul", { className: "bp-errors" });
      (result.errors && result.errors.length ? result.errors : [{ path: "", message: result.message }]).forEach(
        function (e) {
          list.appendChild(el("li", {}, [(e.path ? e.path + ": " : "") + e.message]));
        }
      );
      feedback.appendChild(list);
    });

    var buttons = el("div", { className: "bp-form-buttons" }, [
      el("button", { type: "submit" }, ["Create page"]),
      el(
        "button",
        {
          type: "button",
          onclick: function () {
            store.actions.toggleNewPageForm(false);
          },
        },
        ["Cancel"]
      ),
    ]);
    form.appendChild(buttons);
    return form;
  }

  /* ---------------------------------------------------------------------
   * block library
   * ------------------------------------------------------------------- */

  function renderBlockLibrary(store, container) {
    container.appendChild(el("h2", { className: "bp-panel-heading" }, ["Block library"]));
    var types = Object.keys(store.registry.block_types).sort();
    if (types.length === 0) {
      container.appendChild(
        el("p", { className: "bp-help" }, ["The registry declares no block types."])
      );
      return;
    }
    var sectionOptions = store.doc.sections.map(function (s, i) {
      return { value: String(i), label: s.id };
    });
    var list = el("ul", { className: "bp-block-library" });
    types.forEach(function (type) {
      var entry = store.registry.block_types[type];
      var select = el(
        "select",
        { "aria-label": "Section to add " + type + " to" },
        sectionOptions.length
          ? sectionOptions.map(function (opt) {
              return el("option", { value: opt.value }, [opt.label]);
            })
          : [el("option", { value: "" }, ["(a new section will be created)"])]
      );
      list.appendChild(
        el("li", { className: "bp-block-library-item" }, [
          el("span", { className: "bp-icon", "aria-hidden": "true" }, [type.charAt(0).toUpperCase()]),
          el("span", { className: "bp-block-type-name" }, [titleCase(type)]),
          el("p", { className: "bp-help" }, [entry.why_in_batch_9 || ""]),
          select,
          el(
            "button",
            {
              type: "button",
              onclick: function () {
                var sectionIndex = select.value === "" ? null : parseInt(select.value, 10);
                store.actions.addBlock(sectionIndex, type);
              },
            },
            ["Add"]
          ),
        ])
      );
    });
    container.appendChild(list);
  }

  function titleCase(key) {
    return String(key)
      .replace(/_/g, " ")
      .replace(/\b\w/g, function (c) {
        return c.toUpperCase();
      });
  }

  /* ---------------------------------------------------------------------
   * structure tree
   * ------------------------------------------------------------------- */

  function renderStructureTree(store, container) {
    container.appendChild(el("h2", { className: "bp-panel-heading" }, ["Structure"]));
    if (store.doc.sections.length === 0) {
      container.appendChild(
        el("p", { className: "bp-help" }, [
          "This page has no sections yet. sections: [] is a valid document -- add one below, " +
            "or add a block from the library above, which creates one for you.",
        ])
      );
    }
    var tree = el("ul", { className: "bp-tree", "aria-label": "Page structure" });
    store.doc.sections.forEach(function (section, sectionIndex) {
      tree.appendChild(renderSectionNode(store, section, sectionIndex));
    });
    container.appendChild(tree);
    container.appendChild(
      el(
        "button",
        {
          type: "button",
          onclick: function () {
            store.actions.addSection();
          },
        },
        ["+ Add section"]
      )
    );
  }

  function renderSectionNode(store, section, sectionIndex) {
    var header = el("div", { className: "bp-tree-section-header" }, [
      el("span", { className: "bp-tree-section-id" }, [section.id]),
      el(
        "button",
        {
          type: "button",
          className: "bp-btn-small",
          disabled: sectionIndex === 0,
          "aria-label": "Move section " + section.id + " up",
          onclick: function () {
            store.actions.moveSection(sectionIndex, -1);
          },
        },
        ["↑"]
      ),
      el(
        "button",
        {
          type: "button",
          className: "bp-btn-small",
          disabled: sectionIndex === store.doc.sections.length - 1,
          "aria-label": "Move section " + section.id + " down",
          onclick: function () {
            store.actions.moveSection(sectionIndex, 1);
          },
        },
        ["↓"]
      ),
      el(
        "button",
        {
          type: "button",
          className: "bp-btn-small",
          "aria-label": "Delete section " + section.id,
          onclick: function () {
            store.actions.confirmDeleteSection(sectionIndex);
          },
        },
        ["Delete section"]
      ),
    ]);

    var blockList = el("ul", { className: "bp-tree-blocks" });
    section.blocks.forEach(function (block, blockIndex) {
      blockList.appendChild(renderBlockNode(store, section, sectionIndex, block, blockIndex));
    });

    return el("li", { className: "bp-tree-section" }, [header, blockList]);
  }

  function renderBlockNode(store, section, sectionIndex, block, blockIndex) {
    var selected = store.selection === block.id;
    var selectButton = el(
      "button",
      {
        type: "button",
        className: "bp-tree-block-select" + (selected ? " bp-tree-block-selected" : ""),
        "aria-current": selected ? "true" : null,
        onclick: function () {
          store.actions.selectBlock(block.id);
        },
      },
      [block.type + " (" + block.id + ")"]
    );

    var toolbar = el("span", { className: "bp-tree-block-toolbar" }, [
      el(
        "button",
        {
          type: "button",
          className: "bp-btn-small",
          disabled: blockIndex === 0,
          "aria-label": "Move " + block.id + " up",
          onclick: function () {
            store.actions.moveBlock(sectionIndex, blockIndex, -1);
          },
        },
        ["↑"]
      ),
      el(
        "button",
        {
          type: "button",
          className: "bp-btn-small",
          disabled: blockIndex === section.blocks.length - 1,
          "aria-label": "Move " + block.id + " down",
          onclick: function () {
            store.actions.moveBlock(sectionIndex, blockIndex, 1);
          },
        },
        ["↓"]
      ),
      el(
        "button",
        {
          type: "button",
          className: "bp-btn-small",
          "aria-label": "Duplicate " + block.id,
          onclick: function () {
            store.actions.duplicateBlock(sectionIndex, blockIndex);
          },
        },
        ["Duplicate"]
      ),
      el(
        "button",
        {
          type: "button",
          className: "bp-btn-small",
          "aria-label": "Delete " + block.id,
          onclick: function () {
            store.actions.confirmDeleteBlock(sectionIndex, blockIndex);
          },
        },
        ["Delete"]
      ),
    ]);

    return el("li", { className: "bp-tree-block" }, [selectButton, toolbar]);
  }

  /* ---------------------------------------------------------------------
   * top-level render
   * ------------------------------------------------------------------- */

  function render(store, container) {
    dom.clear(container);
    renderPageSelector(store, container);
    if (store.doc) {
      renderBlockLibrary(store, container);
      renderStructureTree(store, container);
    }
  }

  shell.sidebar = { render: render };
})(shell);
