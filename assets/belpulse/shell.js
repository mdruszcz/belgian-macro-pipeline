/* BelPulse shared shell -- Batch A1.1 (docs/features/site_unification.md).
 *
 * The theme menu, the language menu and the mobile nav toggle that
 * `src/pages/shell.py`'s `render_header()`/`render_footer()` emit markup for.
 * ONE script, loaded on every public page this batch covers -- the two
 * hand-edited pilots (home2.html, macro.html) and every block-built page --
 * so there is one implementation of "how a menu opens, closes and answers the
 * keyboard" to audit, not one copy per page (claude.md rule 17: no framework,
 * but a shared vanilla module is exactly what the existing pages already used
 * for i18n and components).
 *
 * NAMES NO INDICATOR, NO COMMUNE, NO FIGURE (claude.md rules 2, 24, 36). This
 * file only ever touches `localStorage`, `data-theme`, `data-lang` and the
 * menus' own open/closed state -- it has nothing to do with what a block
 * renders.
 *
 * TWO MODES, ONE SCRIPT. A header rendered `mode="client"` (the two
 * hand-edited pages) carries `data-t`/`data-t-aria` and swaps its own text
 * through `I18N.applyStrings()` when a reader picks a language; a header
 * rendered `mode="static"` (every generated page) is already written in its
 * own language, and its language menu holds real per-language links, so
 * picking one just persists the choice before the browser navigates. This
 * script does not need to be told which mode a page is in -- it reads the
 * DOM: a language-menu item that is an `<a>` navigates, one that is a
 * `<button>` re-translates in place.
 */
(function () {
  'use strict';

  // PROGRESSIVE ENHANCEMENT (fixed post-audit). The theme menu, the language
  // menu and the mobile nav toggle all render, in their raw HTML, as a plain
  // in-flow list of real controls with no `hidden` attribute anywhere -- a
  // reader with scripting off gets every link and every choice, visible,
  // with no click needed. This class is the FIRST thing this script does,
  // before wiring anything: assets/belpulse/layout.css keys the collapsed
  // dropdown/toggle behaviour on `:root.bp-js`, so none of that behaviour
  // exists until this line has actually run. If this script never loads (or
  // errors before reaching here), the class never appears and the page stays
  // in its reachable, no-JS shape.
  document.documentElement.classList.add('bp-js');

  var THEME_KEY = 'belpulse-theme';
  var LANG_KEY = 'belpulse-lang';

  /* ---- small helpers ------------------------------------------------------ */

  function qsa(root, sel) {
    return Array.prototype.slice.call(root.querySelectorAll(sel));
  }

  function closeMenu(wrap) {
    var btn = wrap.querySelector('.bp-menu-btn');
    var panel = wrap.querySelector('.bp-menu-panel, .bp-theme-switch, .bp-lang-switch');
    if (!btn || !panel || panel.hidden) return;
    panel.hidden = true;
    btn.setAttribute('aria-expanded', 'false');
  }

  function closeAllMenus(except) {
    qsa(document, '.bp-menu').forEach(function (wrap) {
      if (wrap !== except) closeMenu(wrap);
    });
  }

  function openMenu(wrap) {
    var btn = wrap.querySelector('.bp-menu-btn');
    var panel = wrap.querySelector('.bp-menu-panel, .bp-theme-switch, .bp-lang-switch');
    if (!btn || !panel) return;
    closeAllMenus(wrap);
    panel.hidden = false;
    btn.setAttribute('aria-expanded', 'true');
    var first = panel.querySelector('[role="menuitem"], [role="menuitemradio"]');
    if (first) first.focus();
  }

  function menuItems(wrap) {
    return qsa(wrap, '[role="menuitem"], [role="menuitemradio"]');
  }

  /* ---- generic menu wiring: open/close, Escape, outside click, arrow keys - */

  function wireMenu(wrap) {
    var btn = wrap.querySelector('.bp-menu-btn');
    var panel = wrap.querySelector('.bp-menu-panel, .bp-theme-switch, .bp-lang-switch');
    if (!btn || !panel) return;

    // The panel arrives in the HTML WITHOUT `hidden` (see the `bp-js` class
    // above) -- it is this script's job, not the server's, to close it once
    // it is actually going to behave like a dropdown.
    panel.hidden = true;
    btn.setAttribute('aria-expanded', 'false');

    btn.addEventListener('click', function () {
      if (panel.hidden) openMenu(wrap);
      else closeMenu(wrap);
    });

    panel.addEventListener('keydown', function (e) {
      var items = menuItems(wrap);
      var i = items.indexOf(document.activeElement);
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        items[(i + 1 + items.length) % items.length].focus();
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        items[(i - 1 + items.length) % items.length].focus();
      } else if (e.key === 'Home') {
        e.preventDefault();
        items[0].focus();
      } else if (e.key === 'End') {
        e.preventDefault();
        items[items.length - 1].focus();
      } else if (e.key === 'Escape') {
        e.preventDefault();
        closeMenu(wrap);
        btn.focus();
      } else if (e.key === 'Tab') {
        closeMenu(wrap);
      }
    });

    btn.addEventListener('keydown', function (e) {
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        openMenu(wrap);
      }
    });
  }

  document.addEventListener('click', function (e) {
    qsa(document, '.bp-menu').forEach(function (wrap) {
      if (!wrap.contains(e.target)) closeMenu(wrap);
    });
  });

  /* ---- theme menu ----------------------------------------------------------
   * Applies immediately (no reload needed, unlike a language change on a
   * static page): `data-theme` is what every stylesheet keys off, set here
   * exactly the way SHELL_BOOTSTRAP sets it before paint. */

  function setTheme(choice) {
    document.documentElement.setAttribute('data-theme', choice);
    try {
      localStorage.setItem(THEME_KEY, choice);
    } catch (e) {
      /* privacy mode: the choice still applies for this view */
    }
    qsa(document, '.bp-theme-menu [data-theme-choice]').forEach(function (el) {
      el.setAttribute(
        'aria-checked',
        String(el.getAttribute('data-theme-choice') === choice)
      );
    });
    document.dispatchEvent(new CustomEvent('bp:theme', { detail: { theme: choice } }));
  }

  function wireThemeMenu(wrap) {
    wireMenu(wrap);
    var current = document.documentElement.getAttribute('data-theme') || 'light';
    qsa(wrap, '[data-theme-choice]').forEach(function (el) {
      el.setAttribute('aria-checked', String(el.getAttribute('data-theme-choice') === current));
      el.addEventListener('click', function () {
        setTheme(el.getAttribute('data-theme-choice'));
        closeMenu(wrap);
        wrap.querySelector('.bp-menu-btn').focus();
      });
    });
  }

  /* ---- language menu ---------------------------------------------------- */

  function wireLangMenu(wrap) {
    wireMenu(wrap);
    var panel = wrap.querySelector('.bp-lang-switch');
    if (!panel) return;
    var items = qsa(panel, '[data-lang]');
    var isStatic = items.length > 0 && items[0].tagName === 'A';

    if (isStatic) {
      // A real link: let it navigate. Persisting first means the destination
      // page (a different URL entirely) opens already speaking the chosen
      // language everywhere else on the site (docs/features/i18n.md's goal).
      items.forEach(function (a) {
        a.addEventListener('click', function () {
          try {
            localStorage.setItem(LANG_KEY, a.dataset.lang);
          } catch (e) {
            /* the click has already navigated by the time this would matter */
          }
        });
      });
      return;
    }

    // Client mode: a button per language, re-translating this one document
    // rather than navigating to another.
    function applyLang(lang) {
      try {
        localStorage.setItem(LANG_KEY, lang);
      } catch (e) {
        /* the choice still applies for this view */
      }
      if (typeof I18N !== 'undefined' && typeof I18N.applyStrings === 'function') {
        I18N.applyStrings(lang, document);
      }
      document.documentElement.setAttribute('lang', lang);
      items.forEach(function (btn) {
        btn.setAttribute('aria-checked', String(btn.dataset.lang === lang));
      });
      var code = wrap.querySelector('.bp-menu-current');
      if (code) code.textContent = lang.toUpperCase();
      // FIRES AT BOOT TOO (see the call at the bottom of this function), and
      // that first firing happens WHILE THIS SCRIPT IS STILL RUNNING --
      // before a client-mode page's own `<script>` (further down the body)
      // has even executed, let alone attached its own 'bp:lang' listener. A
      // page cannot rely on that boot-time dispatch to drive its first
      // render; it is only ever useful for a CHANGE after the page has
      // finished loading. Every page in this batch already does the right
      // thing independently -- home2.html and macro.html both read
      // `I18N.initial()` themselves at the top of their own script and use
      // it for their first render, the same value this function is about to
      // recompute -- but a future client-mode page must do the same rather
      // than waiting on this event to fire once at the start.
      document.dispatchEvent(new CustomEvent('bp:lang', { detail: { lang: lang } }));
    }

    items.forEach(function (btn) {
      btn.addEventListener('click', function () {
        applyLang(btn.dataset.lang);
        closeMenu(wrap);
        wrap.querySelector('.bp-menu-btn').focus();
      });
    });

    // The reader's already-chosen language, read the same way I18N.initial()
    // would on a page that loads it -- so the menu shows the truth on first
    // paint instead of always claiming English until a click proves it wrong.
    if (typeof I18N !== 'undefined' && typeof I18N.initial === 'function') {
      applyLang(I18N.initial());
    }
  }

  /* ---- mobile nav toggle -------------------------------------------------- */

  function wireNavToggle() {
    var btn = document.querySelector('.bp-nav-toggle');
    var nav = document.getElementById('bp-nav');
    if (!btn || !nav) return;
    btn.addEventListener('click', function () {
      var open = document.documentElement.getAttribute('data-nav-open') === '1';
      document.documentElement.setAttribute('data-nav-open', open ? '0' : '1');
      btn.setAttribute('aria-expanded', String(!open));
    });
    document.addEventListener('click', function (e) {
      if (!nav.contains(e.target) && !btn.contains(e.target)) {
        document.documentElement.setAttribute('data-nav-open', '0');
        btn.setAttribute('aria-expanded', 'false');
      }
    });
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') {
        document.documentElement.setAttribute('data-nav-open', '0');
        btn.setAttribute('aria-expanded', 'false');
      }
    });
  }

  /* ---- boot ---------------------------------------------------------------- */

  qsa(document, '.bp-theme-menu').forEach(wireThemeMenu);
  qsa(document, '.bp-lang-menu').forEach(wireLangMenu);
  wireNavToggle();
})();
