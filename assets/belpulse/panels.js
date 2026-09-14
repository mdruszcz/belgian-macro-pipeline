/**
 * BelPulse generic in-page panel switcher -- Batch A1.4
 * (docs/features/site_unification.md, "Macro/Micro : panneaux thématiques").
 *
 * One panel visible at a time inside a page, driven purely by DOM structure:
 * no indicator id, no page-specific string, no framework -- reusable by
 * macro.html today and micro.html later (rule 24, extended to the builder).
 * A page supplies its own markup and calls BPPanels.init(); this file never
 * fetches a payload and never renders a figure.
 *
 * Contract the host page's markup must meet:
 *   - each panel is an element carrying `data-panel` AND a plain `id`
 *     (`<section class="bp-panel" id="croissance" data-panel hidden>`); the
 *     one panel shown by default is the only one NOT marked `hidden` in the
 *     static HTML, so a reader whose JavaScript never runs still gets it;
 *   - the sidebar is `.bp-sidebar-nav a[href="#<panel id>"]` links;
 *   - the optional mobile control is `<select id="panelPick">` with one
 *     `<option value="<panel id>">` per panel.
 *
 * BPPanels.init({panels, storageKey, onShow})
 *   panels:     [{id, legacyAnchors: [...]}] -- `id` is the panel's real,
 *               shareable anchor; `legacyAnchors` are anchors the page used
 *               to expose for that same content before this batch, so an
 *               old bookmark or external link still resolves (rule 31).
 *   storageKey: localStorage key remembering the last panel a reader chose,
 *               used when the URL carries no hash at all. Reads/writes are
 *               try/caught -- a private-browsing tab with storage disabled
 *               falls through to the first panel, never throws.
 *   onShow(panelEl, id): called every time a panel becomes the visible one,
 *               INCLUDING the initial one at boot. A canvas drawn while its
 *               panel carried `hidden` measures width 0, so this is the
 *               host page's one place to redraw whatever lives inside it.
 *
 * Returns {show(idOrHash)} so a page can drive the switcher itself (e.g. a
 * "see the map" link elsewhere on the page) without re-deriving the anchor
 * resolution this module already owns.
 */
(function (global) {
  'use strict';

  function resolveId(panels, raw) {
    if (!raw) return null;
    var id = String(raw).replace(/^#/, '');
    if (!id) return null;
    for (var i = 0; i < panels.length; i++) {
      if (panels[i].id === id) return panels[i].id;
    }
    for (var j = 0; j < panels.length; j++) {
      var legacy = panels[j].legacyAnchors || [];
      if (legacy.indexOf(id) !== -1) return panels[j].id;
    }
    return null;
  }

  function init(opts) {
    opts = opts || {};
    var panels = opts.panels || [];
    var ids = panels.map(function (p) { return p.id; });
    var storageKey = opts.storageKey || 'bp-panel';
    var onShow = typeof opts.onShow === 'function' ? opts.onShow : function () {};

    function storedId() {
      try {
        var v = global.localStorage.getItem(storageKey);
        return ids.indexOf(v) !== -1 ? v : null;
      } catch (e) {
        return null;
      }
    }

    function storeId(id) {
      try { global.localStorage.setItem(storageKey, id); } catch (e) {}
    }

    function currentId() {
      var el = document.querySelector('[data-panel]:not([hidden])');
      return el ? el.id : null;
    }

    function syncChrome(id) {
      document.querySelectorAll('.bp-sidebar-nav a').forEach(function (a) {
        if (a.getAttribute('href') === '#' + id) a.setAttribute('aria-current', 'page');
        else a.removeAttribute('aria-current');
      });
      var select = document.getElementById('panelPick');
      if (select && select.value !== id) select.value = id;
    }

    function show(id, focusHeading) {
      if (ids.indexOf(id) === -1) id = ids[0];
      if (!id) return;
      document.querySelectorAll('[data-panel]').forEach(function (el) {
        el.hidden = el.id !== id;
      });
      syncChrome(id);
      storeId(id);
      var panelEl = document.getElementById(id);
      if (!panelEl) return;
      if (focusHeading) {
        var heading = panelEl.querySelector('h2');
        if (heading) {
          if (!heading.hasAttribute('tabindex')) heading.setAttribute('tabindex', '-1');
          heading.focus();
        }
      }
      try {
        global.dispatchEvent(new CustomEvent('bp:panel-shown', { detail: id }));
      } catch (e) {
        // A browser with no CustomEvent constructor gets no event, not a
        // thrown error mid-navigation.
      }
      // Deferred one frame, not called inline: `hidden` was just cleared on
      // this same synchronous turn, and a host page that measures a canvas
      // (or anything else layout-dependent) the INSTANT it stops being
      // `hidden` can still catch the browser mid-layout in some engines.
      // One requestAnimationFrame is the same "wait for a real layout pass"
      // pattern macro.html's own boot-time redraw already uses -- this is
      // panels.js's copy of it, not a second one counted against that
      // page's own single-rAF budget (tests/test_macro.py).
      global.requestAnimationFrame(function () {
        onShow(panelEl, id);
      });
    }

    function navigate(id, focusHeading) {
      var resolved = resolveId(panels, id) || (ids.indexOf(id) !== -1 ? id : null);
      if (!resolved) return;
      if (resolved !== currentId()) {
        history.pushState(history.state, '', '#' + resolved);
      }
      show(resolved, focusHeading);
    }

    // ---- the sidebar: user choice pushes a new history entry -------------
    document.querySelectorAll('.bp-sidebar-nav a[href^="#"]').forEach(function (a) {
      a.addEventListener('click', function (ev) {
        var id = resolveId(panels, a.getAttribute('href'));
        if (!id) return;
        ev.preventDefault();
        navigate(id, true);
      });
    });

    // ---- the mobile picker: same rule, same history entry -----------------
    var select = document.getElementById('panelPick');
    if (select) {
      select.addEventListener('change', function () {
        navigate(select.value, true);
      });
    }

    // ---- back/forward and any other same-document hash change -------------
    global.addEventListener('popstate', function () {
      show(resolveId(panels, location.hash) || ids[0], false);
    });
    global.addEventListener('hashchange', function () {
      var id = resolveId(panels, location.hash);
      if (id && id !== currentId()) show(id, false);
    });

    // ---- boot: hash, then a legacy anchor, then the last one stored, then
    // the first -- normalised with replaceState so this never adds a back-
    // button entry of its own. -------------------------------------------
    var initial = resolveId(panels, location.hash) || storedId() || ids[0];
    show(initial, false);
    if (location.hash.replace(/^#/, '') !== initial) {
      history.replaceState(history.state, '', '#' + initial);
    }

    return {
      show: function (idOrHash) { navigate(idOrHash, true); },
    };
  }

  global.BPPanels = { init: init };
  if (typeof module !== 'undefined') module.exports = global.BPPanels;
})(typeof window !== 'undefined' ? window : global);
