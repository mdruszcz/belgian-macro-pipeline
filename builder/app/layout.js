/* BelPulse builder -- the layout editor: drag, resize and per-breakpoint grid
 * placement, with a keyboard equivalent for every pointer action.
 *
 * Batch 13b. See history.js's header for the delivery mechanism.
 *
 * WHY THIS IS A GRID EDITOR AND NOT A DRAG SURFACE ON TOP OF THE PREVIEW.
 * The preview is an iframe with a BARE sandbox: no scripting, opaque origin,
 * nothing inside it can be measured or listened to from here, and that is a
 * P0 security property this batch does not get to trade away for a nicer
 * interaction. So the thing you drag is a real, honest map of the grid the
 * document declares -- one tile per block, positioned by its own x/y/w/h --
 * sitting beside the preview rather than over it. Dragging over a picture we
 * cannot measure would mean guessing where blocks are, and a builder that
 * guesses puts figures in the wrong place.
 *
 * EVERY POINTER ACTION HAS A KEYBOARD EQUIVALENT, and they are not two
 * implementations: both funnel through `attempt()`, so a rule enforced for one
 * is enforced for the other. Arrow keys move, shift+arrows resize. That is an
 * accessibility requirement of this batch, and it is also the only way the
 * editor is usable on a trackpad-free machine.
 */

(function (shell) {
  "use strict";

  var dom = shell.dom;
  var el = dom.el;
  var model = shell.model;

  // Pointer movement below this many pixels is a click, not a drag. Without
  // it a shaky click on a tile starts a one-cell move nobody asked for.
  var DRAG_THRESHOLD_PX = 4;

  // Row height in the editor. The document's grid rows have no intrinsic
  // pixel size -- the renderer decides that -- so this is a drawing constant
  // for this editor only, never written into a document.
  var ROW_HEIGHT_PX = 28;

  function cellOf(block, breakpoint) {
    return block.layout[breakpoint];
  }

  function describe(block, cell, breakpoint) {
    return (
      block.type +
      " (" +
      block.id +
      "), " +
      breakpoint +
      ": column " +
      (cell.x + 1) +
      " to " +
      (cell.x + cell.w) +
      ", row " +
      (cell.y + 1) +
      " to " +
      (cell.y + cell.h) +
      (block.locked ? ", locked" : "")
    );
  }

  /**
   * Try to put a block at `cell`. The ONE path every move and resize takes,
   * whether it came from a pointer or a key.
   *
   * Refusals are announced rather than swallowed: a drag that silently snaps
   * back is indistinguishable from a broken builder, and the operator cannot
   * fix a rule they were never told about.
   */
  function attempt(store, target, breakpoint, cell, label) {
    var block = store.doc.sections[target.sectionIndex].blocks[target.blockIndex];
    var locked = model.lockRejection(block);
    if (locked) {
      dom.announce(store.statusEl, locked);
      return false;
    }
    var refusal = model.rejectionFor(
      store.doc,
      target.sectionIndex,
      target.blockIndex,
      breakpoint,
      cell
    );
    if (refusal) {
      dom.announce(store.statusEl, refusal);
      return false;
    }
    store.actions.applyLayout(target, breakpoint, cell, label);
    dom.announce(store.statusEl, describe(block, cell, breakpoint));
    return true;
  }

  function tileFor(store, entry, breakpoint) {
    var cell = entry.cell;
    var block = entry.block;
    var selected = store.selection === block.id;
    var tile = el("div", {
      className:
        "bp-grid-tile" +
        (selected ? " is-selected" : "") +
        (block.locked ? " is-locked" : ""),
      tabindex: "0",
      role: "button",
      "data-block-id": block.id,
      "aria-label": describe(block, cell, breakpoint),
      "aria-pressed": selected ? "true" : "false",
    });
    tile.style.gridColumn = cell.x + 1 + " / span " + cell.w;
    tile.style.gridRow = cell.y + 1 + " / span " + cell.h;

    tile.appendChild(el("span", { className: "bp-grid-tile-name" }, [block.type]));
    if (block.locked) {
      // A word, not only an icon: an icon alone is not a label, and the lock
      // state changes what every other control on this tile will do.
      tile.appendChild(el("span", { className: "bp-grid-tile-lock" }, ["locked"]));
    }

    var target = { sectionIndex: entry.sectionIndex, blockIndex: entry.blockIndex };

    tile.addEventListener("pointerdown", function (evt) {
      startPointerDrag(store, evt, tile, target, breakpoint, "move");
    });
    tile.addEventListener("keydown", function (evt) {
      onTileKey(store, evt, target, breakpoint);
    });
    tile.addEventListener("focus", function () {
      // Remembered so the tile can be refocused after a re-render. Every move
      // rebuilds this whole editor, which destroys the focused element -- the
      // same trap that dropped focus to <body> when a modal closed. Without
      // this, only the FIRST arrow key of a run would do anything.
      store._focusTileId = block.id;
      store.actions.selectBlock(block.id);
    });

    var handle = el("span", {
      className: "bp-grid-resize",
      "aria-hidden": "true",
    });
    handle.addEventListener("pointerdown", function (evt) {
      // Stop the tile's own move-drag from starting as well: one gesture is
      // one operation.
      evt.stopPropagation();
      startPointerDrag(store, evt, tile, target, breakpoint, "resize");
    });
    tile.appendChild(handle);
    return tile;
  }

  function onTileKey(store, evt, target, breakpoint) {
    var block = store.doc.sections[target.sectionIndex].blocks[target.blockIndex];
    var cell = cellOf(block, breakpoint);
    var resizing = evt.shiftKey;
    var delta = null;
    if (evt.key === "ArrowLeft") {
      delta = resizing ? model.grow(cell, -1, 0) : model.nudge(cell, -1, 0);
    } else if (evt.key === "ArrowRight") {
      delta = resizing ? model.grow(cell, 1, 0) : model.nudge(cell, 1, 0);
    } else if (evt.key === "ArrowUp") {
      delta = resizing ? model.grow(cell, 0, -1) : model.nudge(cell, 0, -1);
    } else if (evt.key === "ArrowDown") {
      delta = resizing ? model.grow(cell, 0, 1) : model.nudge(cell, 0, 1);
    } else if (evt.key === "l" || evt.key === "L") {
      evt.preventDefault();
      store.actions.toggleLock(target);
      return;
    } else {
      return;
    }
    evt.preventDefault();
    attempt(store, target, breakpoint, delta, resizing ? "resize block" : "move block");
  }

  /**
   * A pointer drag, opened as ONE history transaction.
   *
   * history.js refuses `push` while a transaction is open, so the dozens of
   * intermediate cells a drag passes through cannot each become an undo step
   * -- the operator undoes the drag, not the last pixel of it. An abandoned
   * drag aborts the transaction and records nothing at all.
   */
  function startPointerDrag(store, evt, tile, target, breakpoint, mode) {
    if (evt.button !== 0) {
      return;
    }
    var block = store.doc.sections[target.sectionIndex].blocks[target.blockIndex];
    var locked = model.lockRejection(block);
    if (locked) {
      dom.announce(store.statusEl, locked);
      return;
    }
    var startCell = cellOf(block, breakpoint);
    var origin = { x: evt.clientX, y: evt.clientY };
    var columnWidth = tile.parentNode.clientWidth / model.columnsFor(breakpoint);
    var moved = false;
    var token = null;
    var lastCell = startCell;

    function onMove(moveEvt) {
      var dx = moveEvt.clientX - origin.x;
      var dy = moveEvt.clientY - origin.y;
      if (!moved && Math.abs(dx) < DRAG_THRESHOLD_PX && Math.abs(dy) < DRAG_THRESHOLD_PX) {
        return;
      }
      if (!moved) {
        moved = true;
        // Capture only once the gesture is really a drag, so a plain click
        // still reaches the tile's own focus/selection handling.
        tile.setPointerCapture(moveEvt.pointerId);
        token = store.history.begin(mode === "resize" ? "resize block" : "move block");
      }
      var columns = Math.round(dx / columnWidth);
      var rows = Math.round(dy / ROW_HEIGHT_PX);
      var proposed =
        mode === "resize"
          ? model.grow(startCell, columns, rows)
          : model.nudge(startCell, columns, rows);
      if (
        proposed.x === lastCell.x &&
        proposed.y === lastCell.y &&
        proposed.w === lastCell.w &&
        proposed.h === lastCell.h
      ) {
        return;
      }
      var refusal = model.rejectionFor(
        store.doc,
        target.sectionIndex,
        target.blockIndex,
        breakpoint,
        proposed
      );
      if (refusal) {
        return;
      }
      lastCell = proposed;
      store.actions.previewLayout(target, breakpoint, proposed);
    }

    function onUp() {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      window.removeEventListener("pointercancel", onCancel);
      if (!moved) {
        return;
      }
      store.actions.commitLayout(token, target, breakpoint, lastCell, mode);
    }

    function onCancel() {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      window.removeEventListener("pointercancel", onCancel);
      if (moved) {
        store.actions.abandonLayout(token);
      }
    }

    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    window.addEventListener("pointercancel", onCancel);
  }

  function render(store, container) {
    dom.clear(container);
    if (!store.doc) {
      return;
    }
    var breakpoint = store.viewport;
    var columns = model.columnsFor(breakpoint);

    container.appendChild(
      el("h3", { className: "bp-panel-subheading" }, [
        "Layout — " + breakpoint + " (" + columns + " columns)",
      ])
    );
    container.appendChild(
      el("p", { className: "bp-help" }, [
        "Drag a tile to move it, drag its corner to resize. With a tile focused: " +
          "arrow keys move, shift+arrows resize, L locks. Each breakpoint is " +
          "arranged separately — moving a block here does not move it on the others.",
      ])
    );

    (store.doc.sections || []).forEach(function (section, sectionIndex) {
      container.appendChild(
        el("h4", { className: "bp-grid-section-name" }, [
          section.id + (section.allow_overlap ? " (overlap allowed)" : ""),
        ])
      );
      var grid = el("div", {
        className: "bp-grid",
        role: "group",
        "aria-label": "Grid for section " + section.id,
      });
      grid.style.gridTemplateColumns = "repeat(" + columns + ", 1fr)";
      grid.style.gridAutoRows = ROW_HEIGHT_PX + "px";
      model
        .blocksWithLayout(store.doc, breakpoint)
        .filter(function (entry) {
          return entry.sectionIndex === sectionIndex;
        })
        .forEach(function (entry) {
          grid.appendChild(tileFor(store, entry, breakpoint));
        });
      container.appendChild(grid);
    });

    // Put focus back where it was, but ONLY if this rebuild is what took it
    // away. A move destroys the focused tile and leaves focus on <body>, and
    // without restoring it a keyboard operator is dropped out of the grid
    // after one press.
    //
    // Restoring it UNCONDITIONALLY is a bug, and a subtle one: every re-render
    // would drag focus back into the grid, and because focusing a tile selects
    // its block, it would silently overwrite a selection the operator had just
    // made in the structure tree -- they click a block, start typing, and the
    // inspector switches to a different block underneath them.
    var active = document.activeElement;
    var lostFocus = !active || active === document.body;
    if (store._focusTileId && lostFocus) {
      var refocus = container.querySelector(
        '.bp-grid-tile[data-block-id="' + store._focusTileId + '"]'
      );
      if (refocus) {
        refocus.focus();
      }
    }
  }

  shell.layout = {
    render: render,
    attempt: attempt,
    describe: describe,
    DRAG_THRESHOLD_PX: DRAG_THRESHOLD_PX,
    ROW_HEIGHT_PX: ROW_HEIGHT_PX,
  };
})(shell);
