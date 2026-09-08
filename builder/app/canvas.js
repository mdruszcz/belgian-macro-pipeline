/* BelPulse builder -- the canvas: the sandboxed preview iframe, its viewport
 * sizing, block-selection highlight, and the honest labels the batch spec
 * requires (stand-in data, unhydrated interactive blocks).
 *
 * Batch 12b (builder-ui). See history.js's header for the delivery mechanism.
 *
 * SECURITY, restated because this file is where it would be easiest to get
 * wrong: the preview iframe keeps a BARE `sandbox` attribute, forever. This
 * file must never add either of the two tokens that would opt the frame back
 * into a same origin or a scripting capability -- doing so here, in the
 * script that BUILDS the page, would be just as much a P0 as doing it in the
 * Python skeleton, and this comment deliberately spells neither token out in
 * full: this whole file is later inlined verbatim into the page it describes,
 * where a literal, searchable spelling would be actively misleading to a
 * scanner checking the shipped page never carries one. `/preview` renders
 * unvalidated on-disk content on the same origin as the write API; either
 * opt-out turns a renderer-escaping slip into token theft plus arbitrary
 * writes under config/pages/. `srcdoc` is set as a PROPERTY, never assembled
 * as attribute markup, and the only stylesheet the preview gets is the
 * design-system text `bootstrap_html()` already read and handed to the shell
 * as `context.previewCss` -- nothing here fetches a stylesheet.
 */

(function (shell) {
  "use strict";

  var dom = shell.dom;
  var el = dom.el;

  var VIEWPORTS = {
    desktop: { width: 1280, label: "Desktop (1280px, 12-column grid)" },
    tablet: { width: 834, label: "Tablet (834px, 8-column grid)" },
    mobile: { width: 390, label: "Mobile (390px, 4-column grid)" },
  };

  var BLOCK_ID_RE = /^[a-z][a-z0-9-]{0,63}$/;

  function highlightRuleFor(blockId) {
    if (typeof blockId !== "string" || !BLOCK_ID_RE.test(blockId)) {
      return "";
    }
    return (
      '[data-block-id="' +
      blockId +
      '"]{outline:3px solid #1d4ed8;outline-offset:2px;box-shadow:0 0 0 6px rgba(29,78,216,.12)}'
    );
  }

  function buildSrcdoc(previewCss, fragmentHtml, selectedBlockId) {
    // The fragment is UNVALIDATED on-disk content. It is placed in the body
    // of a document whose only styling is inlined text, never a fetched
    // resource, and whose iframe carries a bare sandbox -- see module header.
    return (
      "<!doctype html><html><head><meta charset=\"utf-8\">" +
      "<style>" +
      previewCss +
      highlightRuleFor(selectedBlockId) +
      "</style></head><body>" +
      fragmentHtml +
      "</body></html>"
    );
  }

  function hasInteractiveBlock(doc, registry) {
    var found = false;
    (doc.sections || []).forEach(function (section) {
      (section.blocks || []).forEach(function (block) {
        var entry = registry.block_types[block.type];
        if (entry && entry.interactive) {
          found = true;
        }
      });
    });
    return found;
  }

  function render(store, container) {
    dom.clear(container);
    container.appendChild(el("h2", { className: "bp-panel-heading" }, ["Canvas"]));

    if (!store.doc) {
      container.appendChild(
        el("p", { className: "bp-help" }, ["Open or create a page to see its preview here."])
      );
      return;
    }

    var notices = el("div", { className: "bp-canvas-notices" });
    notices.appendChild(
      el("p", { className: "bp-notice", role: "note" }, [
        "This preview shows stand-in data, not real figures. Bound blocks display " +
          "their loading / missing / suppressed / not-applicable / zero states exactly " +
          "as the live site would, but no indicator value is resolved until the " +
          "data-binding engine (Batch 14) exists.",
      ])
    );
    if (hasInteractiveBlock(store.doc, store.registry)) {
      notices.appendChild(
        el("p", { className: "bp-notice", role: "note" }, [
          "This document has at least one interactive block type (chart or map). " +
            "It renders as an empty, unhydrated slot here: assets/belpulse/blocks.js " +
            "does not run inside the sandboxed preview.",
        ])
      );
    }
    if (store.dirty) {
      notices.appendChild(
        el("p", { className: "bp-notice bp-notice-dirty" }, [
          "Unsaved changes. This preview already shows them -- it renders the " +
            "document in the browser, not the file on disk.",
        ])
      );
    }
    container.appendChild(notices);

    // tabindex + a label: a scrollable region with no focusable content of
    // its own (the iframe inside it is a black box to the outer page) is
    // otherwise unreachable by keyboard-only scrolling in some browsers --
    // an axe "scrollable-region-focusable" finding caught exactly this.
    var frameHost = el("div", {
      className: "bp-canvas-frame-host",
      tabindex: "0",
      role: "region",
      "aria-label": "Preview canvas, scrollable",
    });
    var frameWidth = VIEWPORTS[store.viewport].width;
    var frame = store.previewFrame;
    frame.style.width = frameWidth + "px";
    frame.setAttribute(
      "aria-label",
      "Page preview, " + VIEWPORTS[store.viewport].label + ". Read-only in this batch."
    );
    frameHost.appendChild(frame);
    container.appendChild(frameHost);

    if (store.previewError) {
      container.appendChild(
        el("p", { className: "bp-error", role: "alert" }, [store.previewError])
      );
    }
  }

  function refresh(store) {
    if (!store.doc) {
      store.previewFrame.srcdoc = "";
      return Promise.resolve();
    }
    // The IN-MEMORY document, not the file: a move must show immediately, and
    // /preview reads from disk (Batch 13a added the POST route for exactly
    // this). Falls back to the disk route only when there is no document to
    // send, which is the "published" view.
    var pending =
      store.which === "draft" && store.doc
        ? shell.api.postPreviewHtml(store.api, store.pageId, store.doc, store.lang || "en")
        : shell.api.getPreviewHtml(store.api, store.pageId, store.which, store.lang || "en");
    return pending
      .then(function (result) {
        if (result.ok) {
          store.previewError = null;
          store.lastPreviewHtml = result.html;
          store.previewFrame.srcdoc = buildSrcdoc(store.previewCss, result.html, store.selection);
        } else {
          store.previewError = "The preview could not be loaded: " + result.message;
          store.previewFrame.srcdoc = "";
        }
      });
  }

  shell.canvas = {
    VIEWPORTS: VIEWPORTS,
    render: render,
    refresh: refresh,
    buildSrcdoc: buildSrcdoc,
  };
})(shell);
