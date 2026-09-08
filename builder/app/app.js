/* BelPulse builder -- shell.start(context): wires the store, the four
 * panels (sidebar / inspector / canvas / topbar) and every required state
 * from the batch spec together. Last file in SHELL_JS_FILES on purpose --
 * every other module must already be attached to `shell` when this runs.
 *
 * Batch 12b (builder-ui). See history.js's header for the delivery mechanism
 * and the module boundary: `context.api` is a capability closure, never the
 * token itself, and nothing in this file (or any file under builder/app/)
 * ever reads a token, writes one to storage, or makes a request outside
 * `context.api`.
 */

(function (shell) {
  "use strict";

  var dom = shell.dom;
  var el = dom.el;
  var model = shell.model;
  var api = shell.api;

  var LIVE_VALIDATE_DEBOUNCE_MS = 400;

  function start(context) {
    var root = context.root;
    var topbarEl = document.getElementById("shell-topbar");
    var sidebarEl = document.getElementById("shell-sidebar");
    var canvasEl = document.getElementById("shell-canvas");
    var inspectorEl = document.getElementById("shell-inspector");
    var bodyEl = document.getElementById("shell-body");

    var bannersEl = el("div", { id: "shell-banners", className: "bp-banners" });
    root.insertBefore(bannersEl, bodyEl);

    var previewFrame = context.preview;

    var store = {
      api: context.api,
      previewCss: context.previewCss,
      languages: context.languages,
      schemaVersion: context.schemaVersion,
      lang: "en",
      statusEl: context.status,
      topbarEl: topbarEl,
      sidebarEl: sidebarEl,
      canvasEl: canvasEl,
      inspectorEl: inspectorEl,
      previewFrame: previewFrame,
      bannersEl: bannersEl,

      registry: null,
      pages: [],
      startupError: null,

      pageId: null,
      which: "draft",
      doc: null,
      history: null,
      dirty: false,
      migrated: false,
      migratedAcknowledged: false,
      blocked: null,
      lastPreviewHtml: "",
      previewError: null,

      selection: null,
      panel: "content",
      viewport: "desktop",
      lastErrors: [],

      showNewPageForm: false,
      newPageDraft: null,

      modal: null,

      _liveValidateTimer: null,
      _newPageValidateTimer: null,
    };

    store.actions = buildActions(store);
    window.addEventListener("beforeunload", function (evt) {
      if (store.dirty) {
        evt.preventDefault();
        evt.returnValue = "";
      }
    });

    store.actions.init();
  }

  /* ------------------------------------------------------------------- */

  function renderAll(store) {
    // Note who had focus BEFORE anything is torn down. A modal is opened by
    // setting store.modal and re-rendering, so by the time the dialog is built
    // the button that opened it no longer exists and document.activeElement is
    // already <body>. Only an id survives a re-render.
    var active = document.activeElement;
    if (active && active.id) {
      store._preRenderFocusId = active.id;
    }
    shell.topbar.render(store, store.topbarEl);
    shell.sidebar.render(store, store.sidebarEl);
    shell.inspector.render(store, store.inspectorEl);
    shell.canvas.render(store, store.canvasEl);
    renderBanners(store);
  }

  function renderBanners(store) {
    dom.clear(store.bannersEl);
    if (store.startupError) {
      store.bannersEl.appendChild(
        el("div", { className: "bp-banner bp-banner-error", role: "alert" }, [
          store.startupError,
          el(
            "button",
            {
              type: "button",
              onclick: function () {
                store.actions.init();
              },
            },
            ["Retry"]
          ),
        ])
      );
    }
    if (store.blocked) {
      store.bannersEl.appendChild(
        el("div", { className: "bp-banner bp-banner-error", role: "alert" }, [store.blocked.message])
      );
    }
    if (store.doc && store.migrated) {
      store.bannersEl.appendChild(
        el("div", { className: "bp-banner bp-banner-notice", role: "note" }, [
          "This document's schema_version was upgraded when it loaded (the server " +
            "reported migrated: true). Saving will write the upgraded format to disk. " +
            "This is announced here, not applied silently.",
        ])
      );
    }
    if (store.lastErrors && store.lastErrors.length) {
      var box = el("div", { className: "bp-banner bp-banner-error", role: "alert" }, [
        el("p", {}, ["The document is not valid:"]),
      ]);
      var list = el("ul", {});
      store.lastErrors.forEach(function (e) {
        list.appendChild(el("li", {}, [e.path + ": " + e.message]));
      });
      box.appendChild(list);
      box.appendChild(
        el(
          "button",
          {
            type: "button",
            className: "bp-btn-small",
            onclick: function () {
              store.lastErrors = [];
              renderBanners(store);
            },
          },
          ["Dismiss"]
        )
      );
      store.bannersEl.appendChild(box);
    }
  }

  function refreshTopbarAndMaybeCanvas(store, becameDirty) {
    shell.topbar.render(store, store.topbarEl);
    if (becameDirty) {
      shell.canvas.render(store, store.canvasEl);
    }
  }

  /**
   * commitDocChange -- the ONE place every document edit funnels through.
   * Pushes the new state onto history.js (with optional coalescing), marks
   * the document dirty, and re-renders only what has to change so a
   * keystroke in a text field never loses focus (see inspector.js's own
   * comment on the same point).
   */
  function commitDocChange(store, opts) {
    opts = opts || {};
    var wasDirty = store.dirty;
    store.dirty = true;
    store.history.push(store.doc, { coalesceKey: opts.coalesceKey, label: opts.label });
    refreshTopbarAndMaybeCanvas(store, !wasDirty);
    if (opts.rerenderInspector) {
      shell.inspector.render(store, store.inspectorEl);
    }
  }

  function debounced(store, key, fn, ms) {
    if (store[key]) {
      clearTimeout(store[key]);
    }
    store[key] = setTimeout(fn, ms);
  }

  function findBlockPanelFor(store, key, blockEntry, block) {
    var schema = shell.inspector.resolveForBlock(blockEntry, block);
    var rawSchema = (schema.properties && schema.properties[key]) || {};
    var resolved = model.resolveRef(rawSchema, store.registry.$defs);
    return shell.inspector.classifyProp(key, resolved);
  }

  function applyValidationErrors(store, errors) {
    store.lastErrors = errors || [];
    for (var i = 0; i < store.lastErrors.length; i += 1) {
      var parsed = model.parseErrorPath(store.lastErrors[i].path);
      if (parsed.root || parsed.sectionIndex === undefined) {
        continue;
      }
      var section = store.doc.sections[parsed.sectionIndex];
      var block = section && section.blocks[parsed.blockIndex];
      if (!block) {
        continue;
      }
      store.selection = block.id;
      var rest = parsed.rest || [];
      if (rest[0] === "binding") {
        store.panel = "data";
      } else if (rest[0] === "visibility") {
        store.panel = "visibility";
      } else if (rest[0] === "layout") {
        store.panel = "layout";
      } else if (rest[0] === "props" && rest[1]) {
        var blockEntry = store.registry.block_types[block.type];
        store.panel = findBlockPanelFor(store, rest[1], blockEntry, block);
      } else {
        store.panel = "content";
      }
      break;
    }
  }

  /* ------------------------------------------------------------------- */

  function buildActions(store) {
    var actions = {};

    actions.init = function () {
      // Sequential, deliberately NOT Promise.all: the builder service is a
      // single-threaded http.server (src/builder/service.py's own module
      // docstring explains why -- the recursion guard needs CPython's main
      // stack). It accepts one connection at a time, and a browser that
      // still holds the page's own navigation connection open (ordinary
      // HTTP/1.1 keep-alive behaviour) leaves a second, concurrently-opened
      // connection waiting for that socket's 10-second read timeout before
      // the server can even accept it. Firing requests one at a time lets
      // each reuse a connection that is already open rather than requiring
      // the server to accept a new one, which avoids PILING UP that penalty
      // across multiple calls. It does not remove the one-time cost on a
      // genuinely cold connection -- see docs/implementation/known-risks.md.
      store.startupError = null;
      api.getRegistry(store.api).then(function (registryResult) {
        return api.listPages(store.api).then(function (pagesResult) {
          return [registryResult, pagesResult];
        });
      }).then(function (results) {
        var registryResult = results[0];
        var pagesResult = results[1];
        if (!registryResult.ok) {
          store.startupError =
            "The builder could not load its block registry: " + registryResult.message;
          renderAll(store);
          return;
        }
        store.registry = registryResult.registry;
        if (!pagesResult.ok) {
          store.startupError = "The builder could not list pages: " + pagesResult.message;
          renderAll(store);
          return;
        }
        store.pages = pagesResult.pages;
        renderAll(store);
        dom.announce(store.statusEl, "The builder is ready. " + store.pages.length + " page(s).");
      });
    };

    function reloadPages() {
      return api.listPages(store.api).then(function (result) {
        if (result.ok) {
          store.pages = result.pages;
        }
      });
    }

    function loadPage(pageId, which) {
      store.blocked = null;
      store.lastErrors = [];
      api.getDocument(store.api, pageId, which).then(function (result) {
        if (!result.ok) {
          store.blocked = { message: result.message };
          store.doc = null;
          renderAll(store);
          dom.announce(store.statusEl, result.message);
          return;
        }
        store.pageId = pageId;
        store.which = which;
        store.doc = result.document;
        store.migrated = !!result.migrated;
        store.migratedAcknowledged = false;
        // The bytes this edit starts from. Handed back on save so the service
        // can refuse a write over someone else's change.
        store.baseSha256 = result.sha256 || "";
        store.history = shell.createHistory(store.doc);
        store.dirty = false;
        store.selection = null;
        store.panel = "content";
        store.showNewPageForm = false;
        renderAll(store);
        shell.canvas.refresh(store).then(function () {
          shell.canvas.render(store, store.canvasEl);
        });
        dom.announce(
          store.statusEl,
          "Opened " + pageId + " (" + which + ")" + (store.migrated ? ", migrated on load" : "")
        );
      });
    }

    function withUnsavedGuard(proceed) {
      if (!store.dirty) {
        proceed();
        return;
      }
      store.modal = {
        title: "Discard unsaved changes?",
        body:
          "This page has unsaved changes. Leaving now discards them; they were never " +
          "written to draft.json.",
        confirmLabel: "Discard changes",
        onConfirm: proceed,
      };
      renderAll(store);
    }

    actions.openPage = function (pageId, which) {
      withUnsavedGuard(function () {
        loadPage(pageId, which);
      });
    };

    actions.toggleNewPageForm = function (show) {
      store.showNewPageForm = show;
      if (show) {
        store.newPageDraft = {
          page_id: "",
          route: "",
          page_type: "blank",
          theme: "",
          title: model.emptyTrilingual(),
          description: model.emptyTrilingual(),
        };
      }
      renderAll(store);
    };

    function documentFromNewPageDraft(draft) {
      var seo = { title: draft.title };
      var descNonEmpty = ["en", "fr", "nl"].every(function (lang) {
        return draft.description[lang].trim() !== "";
      });
      if (descNonEmpty) {
        seo.description = draft.description;
      }
      return {
        // From the service, never a literal here: a page created by the shell
        // must be born at the CURRENT version, or it reports itself as
        // "migrated on load" the moment it is reopened.
        schema_version: store.schemaVersion,
        page_id: draft.page_id,
        revision: 1,
        route: draft.route,
        page_type: draft.page_type,
        theme: draft.theme,
        context: {},
        seo: seo,
        sections: [],
      };
    }

    actions.requestNewPageLiveValidate = function (callback) {
      callback(null);
      debounced(
        store,
        "_newPageValidateTimer",
        function () {
          var doc = documentFromNewPageDraft(store.newPageDraft);
          var pageId = store.newPageDraft.page_id || "unsaved-new-page-check";
          api.postValidate(store.api, pageId, doc).then(callback);
        },
        LIVE_VALIDATE_DEBOUNCE_MS
      );
    };

    actions.submitNewPage = function () {
      var draft = store.newPageDraft;
      var doc = documentFromNewPageDraft(draft);
      api.postSave(store.api, draft.page_id, doc).then(function (result) {
        if (!result.ok) {
          if (result.code === "validation_failed") {
            store.lastErrors = result.errors;
            renderAll(store);
          } else {
            store.startupError = "Could not create the page: " + result.message;
            renderAll(store);
          }
          dom.announce(store.statusEl, "Could not create the page.");
          return;
        }
        dom.announce(store.statusEl, "Created page " + draft.page_id + ".");
        reloadPages().then(function () {
          loadPage(draft.page_id, "draft");
        });
      });
    };

    /* --- structure edits (always structural: full render) -------------- */

    actions.addSection = function () {
      var result = model.addSection(store.doc);
      store.doc = result.doc;
      commitDocChange(store, { label: "add section" });
      renderAll(store);
      dom.announce(store.statusEl, "Added section " + result.sectionId + ".");
    };

    actions.confirmDeleteSection = function (sectionIndex) {
      var section = store.doc.sections[sectionIndex];
      store.modal = {
        title: "Delete section?",
        body:
          "Delete section " +
          section.id +
          " and its " +
          section.blocks.length +
          " block(s)? This can be undone with Undo until you save.",
        confirmLabel: "Delete section",
        onConfirm: function () {
          store.doc = model.removeSection(store.doc, sectionIndex);
          if (store.selection && !model.findBlock(store.doc, store.selection)) {
            store.selection = null;
          }
          commitDocChange(store, { label: "delete section" });
          renderAll(store);
          dom.announce(store.statusEl, "Deleted section " + section.id + ".");
        },
      };
      renderAll(store);
    };

    actions.moveSection = function (sectionIndex, direction) {
      store.doc = model.moveSection(store.doc, sectionIndex, direction);
      commitDocChange(store, { label: "move section" });
      renderAll(store);
      dom.announce(store.statusEl, "Moved section.");
    };

    actions.addBlock = function (sectionIndex, blockType) {
      var doc = store.doc;
      if (sectionIndex === null || doc.sections.length === 0) {
        var added = model.addSection(doc);
        doc = added.doc;
        sectionIndex = doc.sections.length - 1;
      }
      var result = model.addBlock(doc, store.registry, sectionIndex, blockType);
      store.doc = result.doc;
      store.selection = result.blockId;
      store.panel = "content";
      commitDocChange(store, { label: "add " + blockType });
      renderAll(store);
      dom.announce(store.statusEl, "Added " + blockType + " block " + result.blockId + ".");
    };

    actions.confirmDeleteBlock = function (sectionIndex, blockIndex) {
      var block = store.doc.sections[sectionIndex].blocks[blockIndex];
      store.modal = {
        title: "Delete block?",
        body: "Delete " + block.type + " block " + block.id + "?",
        confirmLabel: "Delete block",
        onConfirm: function () {
          store.doc = model.removeBlock(store.doc, sectionIndex, blockIndex);
          if (store.selection === block.id) {
            store.selection = null;
          }
          commitDocChange(store, { label: "delete block" });
          renderAll(store);
          dom.announce(store.statusEl, "Deleted block " + block.id + ".");
        },
      };
      renderAll(store);
    };

    actions.duplicateBlock = function (sectionIndex, blockIndex) {
      var result = model.duplicateBlock(store.doc, sectionIndex, blockIndex);
      store.doc = result.doc;
      store.selection = result.blockId;
      commitDocChange(store, { label: "duplicate block" });
      renderAll(store);
      dom.announce(store.statusEl, "Duplicated block as " + result.blockId + ".");
    };

    actions.moveBlock = function (sectionIndex, blockIndex, direction) {
      store.doc = model.moveBlock(store.doc, sectionIndex, blockIndex, direction);
      commitDocChange(store, { label: "move block" });
      renderAll(store);
      dom.announce(store.statusEl, "Moved block.");
    };

    /* --- selection / panels / viewport ---------------------------------- */

    actions.selectBlock = function (blockId) {
      store.selection = blockId;
      shell.sidebar.render(store, store.sidebarEl);
      shell.inspector.render(store, store.inspectorEl);
      shell.canvas.render(store, store.canvasEl);
      if (store.lastPreviewHtml) {
        store.previewFrame.srcdoc = shell.canvas.buildSrcdoc(
          store.previewCss,
          store.lastPreviewHtml,
          store.selection
        );
      }
    };

    actions.setPanel = function (panel) {
      store.panel = panel;
      shell.inspector.render(store, store.inspectorEl);
      var focused = store.inspectorEl.querySelector("#tab-" + panel);
      if (focused) {
        focused.focus();
      }
    };

    actions.setViewport = function (viewport) {
      store.viewport = viewport;
      shell.topbar.render(store, store.topbarEl);
      shell.canvas.render(store, store.canvasEl);
    };

    /* --- field edits (quiet by default; see commitDocChange) ------------ */

    actions.updateProp = function (blockId, key, value, opts) {
      var found = model.findBlock(store.doc, blockId);
      if (!found) {
        return;
      }
      store.doc = model.setAtPath(
        store.doc,
        ["sections", found.sectionIndex, "blocks", found.blockIndex, "props", key],
        value
      );
      commitDocChange(store, opts);
    };

    actions.setBinding = function (blockId, binding, opts) {
      var found = model.findBlock(store.doc, blockId);
      if (!found) {
        return;
      }
      store.doc = model.setAtPath(
        store.doc,
        ["sections", found.sectionIndex, "blocks", found.blockIndex, "binding"],
        binding
      );
      commitDocChange(store, opts);
    };

    actions.setVisibility = function (blockId, visibility, opts) {
      var found = model.findBlock(store.doc, blockId);
      if (!found) {
        return;
      }
      store.doc = model.setAtPath(
        store.doc,
        ["sections", found.sectionIndex, "blocks", found.blockIndex, "visibility"],
        visibility
      );
      commitDocChange(store, opts);
    };

    /* --- undo / redo ------------------------------------------------------ */

    actions.undo = function () {
      var state = store.history.undo();
      if (state === null) {
        return;
      }
      store.doc = state;
      store.dirty = true;
      if (store.selection && !model.findBlock(store.doc, store.selection)) {
        store.selection = null;
      }
      renderAll(store);
      dom.announce(store.statusEl, "Undid last change.");
    };

    actions.redo = function () {
      var state = store.history.redo();
      if (state === null) {
        return;
      }
      store.doc = state;
      store.dirty = true;
      if (store.selection && !model.findBlock(store.doc, store.selection)) {
        store.selection = null;
      }
      renderAll(store);
      dom.announce(store.statusEl, "Redid last change.");
    };

    /* --- live validate (data panel) --------------------------------------- */

    actions.requestLiveValidate = function (callback) {
      callback(null);
      debounced(
        store,
        "_liveValidateTimer",
        function () {
          api.postValidate(store.api, store.pageId, store.doc).then(callback);
        },
        LIVE_VALIDATE_DEBOUNCE_MS
      );
    };

    /* --- validate / save / publish / restore ------------------------------ */

    actions.validateNow = function () {
      api.postValidate(store.api, store.pageId, store.doc).then(function (result) {
        if (result.ok) {
          store.lastErrors = [];
          dom.announce(store.statusEl, "This document is valid.");
        } else if (result.code === "validation_failed") {
          applyValidationErrors(store, result.errors);
          dom.announce(store.statusEl, result.errors.length + " validation error(s) found.");
        } else {
          store.lastErrors = [];
          store.startupError = result.message;
          dom.announce(store.statusEl, result.message);
        }
        renderAll(store);
      });
    };

    function doSave() {
      api.postSave(store.api, store.pageId, store.doc, store.baseSha256).then(function (result) {
        if (!result.ok) {
          if (result.code === "validation_failed") {
            applyValidationErrors(store, result.errors);
            dom.announce(store.statusEl, "Save refused: " + result.errors.length + " validation error(s).");
          } else {
            dom.announce(store.statusEl, "Save failed: " + result.message);
            store.startupError = result.message;
          }
          renderAll(store);
          return;
        }
        store.dirty = false;
        store.migrated = false;
        store.lastErrors = [];
        store.startupError = null;
        // The bytes we just wrote become the base for the next save. Without
        // this, the second save of a session would compare against the hash
        // from load time and refuse our own previous write.
        store.baseSha256 = result.sha256 || "";
        renderAll(store);
        dom.announce(store.statusEl, "Saved (" + result.bytes + " bytes).");
        // Sequential, not concurrent -- see the comment on actions.init for
        // why: the single-threaded service can only accept one connection
        // at a time, and two fetches fired together fight over that.
        shell.canvas.refresh(store).then(function () {
          shell.canvas.render(store, store.canvasEl);
          return reloadPages();
        });
      });
    }

    actions.save = function () {
      if (store.migrated && !store.migratedAcknowledged) {
        store.modal = {
          title: "Save the upgraded document?",
          body:
            "This document's schema_version was upgraded when it loaded. Saving now " +
            "writes the upgraded format to draft.json, replacing the older format.",
          confirmLabel: "Save upgraded document",
          onConfirm: function () {
            store.migratedAcknowledged = true;
            doSave();
          },
        };
        renderAll(store);
        return;
      }
      doSave();
    };

    actions.confirmPublish = function () {
      store.modal = {
        title: "Publish this page?",
        body:
          "Publishing validates the current draft and promotes it to published.json. " +
          "The previous published version is snapshotted and can be restored. This is " +
          "never a side effect of Save -- it only happens here.",
        confirmLabel: "Publish",
        onConfirm: function () {
          api.postPublish(store.api, store.pageId).then(function (result) {
            if (!result.ok) {
              if (result.code === "validation_failed") {
                applyValidationErrors(store, result.errors);
                dom.announce(store.statusEl, "Publish refused: the draft is not valid.");
              } else {
                dom.announce(store.statusEl, "Publish failed: " + result.message);
                store.startupError = result.message;
              }
              renderAll(store);
              return;
            }
            dom.announce(
              store.statusEl,
              "Published. " + (result.version ? "Previous version " + result.version + " snapshotted." : "First publish of this page.")
            );
            reloadPages();
          });
        },
      };
      renderAll(store);
    };

    actions.openRestoreDialog = function () {
      api.listVersions(store.api, store.pageId).then(function (result) {
        if (!result.ok) {
          dom.announce(store.statusEl, "Could not list versions: " + result.message);
          return;
        }
        if (result.versions.length === 0) {
          store.modal = {
            title: "Restore",
            body: "This page has no published versions to restore.",
            confirmLabel: "Close",
            onConfirm: function () {},
          };
          renderAll(store);
          return;
        }
        var latest = result.versions[result.versions.length - 1];
        store.modal = {
          title: "Restore version " + latest + "?",
          body:
            "Restoring overwrites the CURRENT DRAFT with published version " +
            latest +
            ". Unsaved changes in the draft are lost. This does not affect the " +
            "published page until you publish again.",
          list: result.versions,
          confirmLabel: "Restore version " + latest,
          onConfirm: function () {
            api.postRestore(store.api, store.pageId, latest).then(function (restoreResult) {
              if (!restoreResult.ok) {
                dom.announce(store.statusEl, "Restore failed: " + restoreResult.message);
                return;
              }
              dom.announce(store.statusEl, "Restored version " + latest + " into the draft.");
              loadPage(store.pageId, "draft");
            });
          },
        };
        renderAll(store);
      });
    };

    actions.closeModal = function () {
      // Release the focus trap before the dialog is torn down. This was
      // recorded and never read, which left a keydown listener behind on every
      // modal that was opened.
      if (typeof store._releaseModalTrap === "function") {
        store._releaseModalTrap();
      }
      store._releaseModalTrap = null;
      var returnId = store._modalReturnFocusId;
      store._modalReturnFocusId = "";
      store.modal = null;
      renderAll(store);
      // renderAll rebuilt the bar, so the original button object is gone; find
      // its replacement by id. Without this, cancelling a dialog drops a
      // keyboard operator on <body> and they Tab through the whole shell again.
      var back = returnId ? document.getElementById(returnId) : null;
      if (!back && store.topbarEl) {
        back = dom.focusableIn(store.topbarEl)[0] || null;
      }
      if (back) {
        back.focus();
      }
    };

    return actions;
  }

  shell.start = start;
})(shell);
