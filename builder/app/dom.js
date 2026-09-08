/* BelPulse builder -- small DOM construction and announcement helpers.
 *
 * Batch 12b (builder-ui). See history.js's header for the delivery mechanism:
 * this file is concatenated into the same closure as every other shell file,
 * so it declares nothing at top level and attaches everything to `shell`.
 *
 * Security constraint carried from the batch spec: no `innerHTML` with any
 * server- or document-derived string anywhere in the shell, with exactly one
 * exception (the preview `srcdoc`, built in canvas.js). Every element here is
 * built with `document.createElement` and text is set with `textContent`,
 * never markup.
 */

(function (shell) {
  "use strict";

  /**
   * el(tag, attrs, children) -> Element
   *
   * attrs: plain object. Keys starting "on" (e.g. "onclick") are attached as
   * event listeners. "className" sets the class attribute. Anything else is
   * set with setAttribute, EXCEPT booleans, which are only set when true and
   * removed (never set to the string "false") -- this matters for `disabled`,
   * `hidden`, `aria-hidden` and friends, which must not appear at all when
   * false rather than being a suspiciously truthy string "false".
   *
   * children: array of Element | string | null | undefined. Strings become
   * Text nodes.
   */
  function el(tag, attrs, children) {
    var node = document.createElement(tag);
    attrs = attrs || {};
    for (var key in attrs) {
      if (!Object.prototype.hasOwnProperty.call(attrs, key)) {
        continue;
      }
      var value = attrs[key];
      if (value === null || value === undefined || value === false) {
        continue;
      }
      if (key.slice(0, 2) === "on" && typeof value === "function") {
        node.addEventListener(key.slice(2), value);
      } else if (key === "className") {
        node.setAttribute("class", value);
      } else if (value === true) {
        node.setAttribute(key, "");
      } else {
        node.setAttribute(key, String(value));
      }
    }
    (children || []).forEach(function (child) {
      if (child === null || child === undefined || child === false) {
        return;
      }
      if (typeof child === "string") {
        node.appendChild(document.createTextNode(child));
      } else {
        node.appendChild(child);
      }
    });
    return node;
  }

  function text(str) {
    return document.createTextNode(str == null ? "" : String(str));
  }

  function clear(node) {
    while (node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  function replace(node, children) {
    clear(node);
    (children || []).forEach(function (child) {
      if (child === null || child === undefined || child === false) {
        return;
      }
      node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
    });
  }

  /* --- status announcements -------------------------------------------- *
   *
   * #shell-status is already role="status" aria-live="polite" in the Python
   * skeleton. Setting textContent to the SAME string twice in a row does not
   * reliably re-announce in every screen reader, so a repeated message is
   * given a trailing zero-width space that flips on alternating calls -- an
   * invisible change is enough to make the region "changed" again without
   * ever changing what a sighted user reads.
   */
  var announceToggle = false;

  function announce(statusEl, message) {
    if (!statusEl) {
      return;
    }
    announceToggle = !announceToggle;
    var suffix = announceToggle ? "​" : "";
    clear(statusEl);
    statusEl.appendChild(document.createTextNode(String(message) + suffix));
  }

  /* --- a small focus trap for modal dialogs ----------------------------- */

  function focusableIn(container) {
    var selector =
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), ' +
      'textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
    return Array.prototype.slice.call(container.querySelectorAll(selector));
  }

  /**
   * trapFocus(container) -> release()
   *
   * Keeps Tab/Shift+Tab cycling inside `container` while active. Used by
   * modal confirmations (publish, restore) so the preview iframe can never
   * become a keyboard trap and a dialog can never leak focus behind itself.
   */
  function trapFocus(container) {
    function onKeydown(evt) {
      if (evt.key !== "Tab") {
        return;
      }
      var focusable = focusableIn(container);
      if (focusable.length === 0) {
        evt.preventDefault();
        return;
      }
      var first = focusable[0];
      var last = focusable[focusable.length - 1];
      if (evt.shiftKey && document.activeElement === first) {
        evt.preventDefault();
        last.focus();
      } else if (!evt.shiftKey && document.activeElement === last) {
        evt.preventDefault();
        first.focus();
      }
    }
    container.addEventListener("keydown", onKeydown);
    return function release() {
      container.removeEventListener("keydown", onKeydown);
    };
  }

  shell.dom = {
    el: el,
    text: text,
    clear: clear,
    replace: replace,
    announce: announce,
    focusableIn: focusableIn,
    trapFocus: trapFocus,
  };
})(shell);
