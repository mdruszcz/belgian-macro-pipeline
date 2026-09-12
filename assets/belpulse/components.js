/**
 * BelPulse shared component behaviour -- Batch 2 (docs/features/page_builder.md).
 *
 * Deliberately small: everything here is generic DOM wiring with no
 * knowledge of an indicator, commune, or data source (rule 24). Anything
 * that needs real data stays in the page/block that calls these.
 */
(function (global) {
  'use strict';

  /** Wires a .bp-tabs / [role=tabpanel] pair. Click or arrow-key a tab,
   * aria-selected and the matching panel's [hidden] update together. */
  function initTabs(root) {
    root = root || document;
    root.querySelectorAll('.bp-tabs').forEach(function (tabs) {
      var buttons = Array.prototype.slice.call(tabs.querySelectorAll('button[role="tab"]'));
      if (!buttons.length) return;

      function select(btn) {
        buttons.forEach(function (b) {
          var on = b === btn;
          b.setAttribute('aria-selected', on ? 'true' : 'false');
          b.tabIndex = on ? 0 : -1;
          var panel = document.getElementById(b.getAttribute('aria-controls'));
          if (panel) panel.hidden = !on;
        });
        btn.focus();
      }

      buttons.forEach(function (btn, i) {
        btn.addEventListener('click', function () {
          select(btn);
        });
        btn.addEventListener('keydown', function (e) {
          if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
            e.preventDefault();
            var next = e.key === 'ArrowRight' ? (i + 1) % buttons.length : (i - 1 + buttons.length) % buttons.length;
            select(buttons[next]);
          }
        });
      });
    });
  }

  /** Two-track theme toggle -- assets/commune_map.css's own pattern, reused
   * verbatim: localStorage overrides prefers-color-scheme via data-theme. */
  function initThemeToggle(root, storageKey) {
    root = root || document;
    storageKey = storageKey || 'belpulse-theme';
    var saved = null;
    try {
      saved = localStorage.getItem(storageKey);
    } catch (e) {
      /* private browsing / storage blocked -- fall back to OS preference */
    }
    // Light unless the reader said otherwise: every design this site is
    // drawn from is light, so the OS preference is honoured only when the
    // reader explicitly asks for 'auto'.
    if (saved === 'auto') {
      document.documentElement.removeAttribute('data-theme');
    } else {
      document.documentElement.setAttribute('data-theme', saved === 'dark' ? 'dark' : 'light');
    }

    root.querySelectorAll('.bp-theme-toggle button[data-theme-choice]').forEach(function (btn) {
      var choice = btn.getAttribute('data-theme-choice');
      btn.setAttribute('aria-pressed', (saved || 'light') === choice ? 'true' : 'false');
      btn.addEventListener('click', function () {
        if (choice === 'auto') {
          document.documentElement.removeAttribute('data-theme');
        } else {
          document.documentElement.setAttribute('data-theme', choice);
        }
        try {
          localStorage.setItem(storageKey, choice);
        } catch (e) {
          /* ignore -- theme still applies for this page view */
        }
        root.querySelectorAll('.bp-theme-toggle button[data-theme-choice]').forEach(function (b) {
          b.setAttribute('aria-pressed', b === btn ? 'true' : 'false');
        });
      });
    });
  }

  /**
   * Drives a .bp-live-counter's displayed number upward on a fixed interval.
   * This is a DECORATIVE ANIMATION, never a data source -- the element MUST
   * already carry data-simulated="true" in its markup (the .bp-simulated-badge
   * is required alongside it, see components.css) or this refuses to run, so
   * a real metric can never be silently wired to it by mistake.
   */
  function initSimulatedCounter(el, opts) {
    if (el.getAttribute('data-simulated') !== 'true') {
      throw new Error('BPComponents.initSimulatedCounter: element is missing data-simulated="true"');
    }
    opts = opts || {};
    var valueEl = el.querySelector('.bp-value');
    if (!valueEl) return;
    var start = opts.start != null ? opts.start : parseFloat(valueEl.textContent.replace(/[^\d.-]/g, '')) || 0;
    var perTick = opts.perTick != null ? opts.perTick : 1;
    var intervalMs = opts.intervalMs != null ? opts.intervalMs : 2000;
    var locale = opts.locale || 'en-GB';
    var count = start;
    var format = function (v) {
      return Math.round(v).toLocaleString(locale);
    };
    valueEl.textContent = format(count);
    var timer = setInterval(function () {
      count += perTick;
      valueEl.textContent = format(count);
    }, intervalMs);
    return function stop() {
      clearInterval(timer);
    };
  }

  global.BPComponents = {
    initTabs: initTabs,
    initThemeToggle: initThemeToggle,
    initSimulatedCounter: initSimulatedCounter,
  };
  if (typeof module !== 'undefined') module.exports = global.BPComponents;
})(typeof window !== 'undefined' ? window : global);
