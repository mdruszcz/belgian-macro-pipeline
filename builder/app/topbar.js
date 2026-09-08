/* BelPulse builder -- the top bar: undo/redo (wired to history.js), the
 * preview viewport switcher, and the explicit Validate / Save / Publish /
 * Restore actions. Publish always sits behind its own confirmation and is
 * never a side effect of Save (claude.md rule 32, invariant 10).
 *
 * Batch 12b (builder-ui). See history.js's header for the delivery mechanism.
 */

(function (shell) {
  "use strict";

  var dom = shell.dom;
  var el = dom.el;
  var canvas = shell.canvas;

  function renderModal(store, container) {
    if (!store.modal) {
      return;
    }
    var overlay = el("div", { className: "bp-modal-overlay" });
    var dialog = el("div", {
      className: "bp-modal",
      role: "dialog",
      "aria-modal": "true",
      "aria-labelledby": "bp-modal-title",
    });
    dialog.appendChild(el("h2", { id: "bp-modal-title" }, [store.modal.title]));
    dialog.appendChild(el("p", {}, [store.modal.body]));
    if (store.modal.list) {
      var ul = el("ul", {});
      store.modal.list.forEach(function (item) {
        ul.appendChild(el("li", {}, [item]));
      });
      dialog.appendChild(ul);
    }
    var confirmBtn = el(
      "button",
      {
        type: "button",
        onclick: function () {
          var action = store.modal.onConfirm;
          store.actions.closeModal();
          action();
        },
      },
      [store.modal.confirmLabel || "Confirm"]
    );
    var cancelBtn = el(
      "button",
      {
        type: "button",
        onclick: function () {
          store.actions.closeModal();
        },
      },
      ["Cancel"]
    );
    dialog.appendChild(el("div", { className: "bp-modal-buttons" }, [confirmBtn, cancelBtn]));
    overlay.appendChild(dialog);
    container.appendChild(overlay);

    var release = dom.trapFocus(dialog);
    dialog.addEventListener("keydown", function onKey(evt) {
      if (evt.key === "Escape") {
        store.actions.closeModal();
      }
    });
    // Where focus came from, captured by renderAll before this container was
    // cleared -- reading document.activeElement here is too late, the opening
    // button has already been destroyed and focus has fallen to <body>.
    if (!store._modalReturnFocusId) {
      store._modalReturnFocusId = store._preRenderFocusId || "";
    }
    cancelBtn.focus();
    store._releaseModalTrap = release;
  }

  function render(store, container) {
    dom.clear(container);

    var left = el("div", { className: "bp-topbar-group" }, [
      el(
        "button",
        {
          type: "button",
          disabled: !store.history || !store.history.canUndo(),
          onclick: function () {
            store.actions.undo();
          },
        },
        ["Undo"]
      ),
      el(
        "button",
        {
          type: "button",
          disabled: !store.history || !store.history.canRedo(),
          onclick: function () {
            store.actions.redo();
          },
        },
        ["Redo"]
      ),
    ]);
    container.appendChild(left);

    if (store.doc) {
      var viewportGroup = el(
        "div",
        { role: "radiogroup", "aria-label": "Preview viewport", className: "bp-topbar-group" },
        Object.keys(canvas.VIEWPORTS).map(function (key) {
          var selected = store.viewport === key;
          return el(
            "button",
            {
              type: "button",
              role: "radio",
              "aria-checked": selected ? "true" : "false",
              className: "bp-viewport-btn" + (selected ? " bp-viewport-selected" : ""),
              onclick: function () {
                store.actions.setViewport(key);
              },
            },
            [key.charAt(0).toUpperCase() + key.slice(1)]
          );
        })
      );
      container.appendChild(viewportGroup);

      var actions = el("div", { className: "bp-topbar-group" }, [
        el(
          "button",
          {
            type: "button",
            onclick: function () {
              store.actions.validateNow();
            },
          },
          ["Validate"]
        ),
        el(
          "button",
          {
            // These three can open a confirmation dialog, and closing it
            // re-renders this whole bar -- destroying the button that was
            // focused. A stable id is what lets focus come back here instead
            // of falling to <body>. See app.js's closeModal.
            id: "bp-action-save",
            type: "button",
            onclick: function () {
              store.actions.save();
            },
          },
          ["Save"]
        ),
        el(
          "button",
          {
            id: "bp-action-publish",
            type: "button",
            onclick: function () {
              store.actions.confirmPublish();
            },
          },
          ["Publish…"]
        ),
        el(
          "button",
          {
            id: "bp-action-restore",
            type: "button",
            onclick: function () {
              store.actions.openRestoreDialog();
            },
          },
          ["Restore…"]
        ),
      ]);
      container.appendChild(actions);

      container.appendChild(
        el("span", { className: "bp-dirty-indicator" }, [store.dirty ? "Unsaved changes" : "Saved"])
      );
    }

    renderModal(store, container);
  }

  shell.topbar = { render: render };
})(shell);
