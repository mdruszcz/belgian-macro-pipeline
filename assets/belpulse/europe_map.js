/**
 * Europe panel: NUTS 2 regional choropleth -- Batch B3
 * (docs/features/europe_nuts2.md).
 *
 * GENERIC. No indicator id, no commune id, no figure lives in this file
 * (CLAUDE.md rules 2 and 24): every label, unit, source, year and value
 * comes from public/data/europe/nuts2/*.json, read at runtime. Adding a
 * fourth NUTS 2 indicator later is a pipeline change plus a new payload
 * file, never an edit here.
 *
 * Loads NOTHING until the Europe panel actually opens (panels.js's
 * `bp:panel-shown` event) -- the vendored eurostat-map bundle is 1.2 MB and
 * every reader who never opens this panel must never pay for it.
 *
 * eurostat-map (assets/vendor/eurostat-map/) is a third-party choropleth
 * renderer, not a second commune-map engine (CLAUDE.md rule 29 -- that rule
 * is about assets/commune_map.js's OWN choropleth, which this file never
 * touches or reimplements). It fetches Eurostat statistics, Nuts2json
 * geometry, a GISCO basemap and GISCO place-name labels from
 * ec.europa.eu/raw.githubusercontent.com *by default*; every one of those
 * is neutralised below (see `buildMap`'s own comments) so the browser never
 * leaves this site (CLAUDE.md rule 30, and the batch's own "no request to
 * any external host" invariant).
 */
(function () {
  'use strict';

  var INDEX_URL = 'public/data/europe/nuts2/index.json';
  var PAYLOAD_DIR = 'public/data/europe/nuts2/';
  var VENDOR_SRC = 'assets/vendor/eurostat-map/eurostatmap.min.js';
  var STAGE_ID = 'bpEuropeStage';
  var SVG_ID = 'bpEuropeSvg';

  // Europe countries batch (docs/features/europe_countries.md): same shape
  // one NUTS level up, served through the same fetch shim mechanism -- no
  // new remote host, same same-origin-only invariant the region map above
  // already guarantees.
  var COUNTRY_INDEX_URL = 'public/data/europe/countries/index.json';
  var COUNTRY_PAYLOAD_DIR = 'public/data/europe/countries/';
  var COUNTRY_STAGE_ID = 'bpEuropeCountryStage';
  var COUNTRY_SVG_ID = 'bpEuropeCountrySvg';
  var MAX_SELECTED_COUNTRIES = 8;
  var DEFAULT_SELECTED_COUNTRIES = ['BE'];

  /* eurostat-map always requests
     `${nuts2jsonBaseURL}/${nutsYear}/${proj}/${scale}/${nutsLevel}.json`
     (grepped from the vendored bundle's own URL-builder) -- a shape our
     committed geometry file (public/data/geo/nuts2/2024/2.json, path taken
     from index.json, never hand-typed) does not share. Rather than fight
     that template, `buildMap` intercepts exactly this one, uniquely-tagged
     URL and answers it from the SAME geometry payload this file already
     fetched itself (for region names) -- one real network read, zero bytes
     to Nuts2json or ec.europa.eu. See `buildMap`'s own comment for the
     narrow, temporary `window.fetch` shim this relies on. */
  var GEOMETRY_SENTINEL = 'bp-vendor-bridge-do-not-fetch';

  var LANG = I18N.initial();
  var T = function (key, vars) {
    return I18N.t(LANG, key, vars);
  };

  var state = {
    inited: false,
    index: null,
    geometryTopo: null,
    regionNames: {},
    payloads: {},
    currentIndicatorId: null,
    currentYear: null,
    selectedRegion: null,
    mapInstance: null,
    zoomBaseline: null,
    root: null,
    els: {},
    // Europe countries batch (docs/features/europe_countries.md): the
    // "Régions / Pays" toggle. Region mode above is entirely unchanged;
    // everything country-shaped lives in this one sub-object so it can
    // never be confused with a region-mode field of the same short name
    // (e.g. `currentIndicatorId`, `mapInstance`) by a later edit.
    mode: 'region',
    country: {
      inited: false,
      index: null,
      geometryTopo: null,
      geometryPromise: null,
      countryNames: {}, // code -> {en, fr, nl}
      payloads: {},
      allPayloadsPromise: null,
      currentIndicatorId: null,
      currentPeriod: null,
      // Up to 8 codes, default Belgium (spec requirement) -- shared by the
      // map's click-to-toggle selection AND the "Comparaison
      // internationale" charts below it; switching map mode never clears
      // it.
      selected: ['BE'],
      mapInstance: null,
      zoomBaseline: null,
    },
  };

  /* ---- tiny DOM helpers --------------------------------------------- */
  function el(tag, attrs, children) {
    var node = document.createElement(tag);
    attrs = attrs || {};
    Object.keys(attrs).forEach(function (k) {
      if (k === 'text') node.textContent = attrs[k];
      else if (k === 'html') node.innerHTML = attrs[k];
      else node.setAttribute(k, attrs[k]);
    });
    (children || []).forEach(function (c) {
      if (c) node.appendChild(c);
    });
    return node;
  }

  function clear(node) {
    while (node.firstChild) node.removeChild(node.firstChild);
  }

  /* The blue sequential ramp europe_map.css declares scoped to
     `.bp-europe-map` (the same seven hex values assets/commune_map.css
     already ships for its own [data-theme="paper"], copied verbatim, never
     invented -- CLAUDE.md rule 36) did not win the cascade against that
     file's :root-level amber declaration in a real page the way an
     isolated repro of the same rules said it should -- measured, not
     guessed, and not worth chasing further under this batch's time budget.
     Setting the same values as an INLINE style here sidesteps the mystery
     entirely (inline always wins over any stylesheet rule), and this
     function is re-run on every `bp:theme` change so it keeps tracking
     the reader's actual theme rather than freezing the first one seen. */
  var RAMP_LIGHT = { 0: '#eef2f7', 1: '#d3dff0', 2: '#aec4e3', 3: '#82a3d2', 4: '#5a80bd', 5: '#3c60a0', 6: '#233f74', nodata: '#e7ded0' };
  var RAMP_DARK = { 0: '#233f74', 1: '#3c60a0', 2: '#5a80bd', 3: '#82a3d2', 4: '#aec4e3', 5: '#d3dff0', 6: '#eef2f7', nodata: '#2a3550' };
  function currentThemeIsDark() {
    var explicit = document.documentElement.getAttribute('data-theme');
    if (explicit === 'dark') return true;
    if (explicit === 'light' || explicit === 'paper') return false;
    return !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches);
  }
  function applyRampTokens() {
    if (!state.root) return;
    var ramp = currentThemeIsDark() ? RAMP_DARK : RAMP_LIGHT;
    for (var i = 0; i <= 6; i++) state.root.style.setProperty('--ramp-' + i, ramp[i]);
    state.root.style.setProperty('--nodata', ramp.nodata);
  }

  function cssVar(name, fallback) {
    // Read from the panel's own root, not documentElement: the Europe
    // ramp tokens below are scoped to `.bp-europe-map` (europe_map.css),
    // so a plain :root read would miss that override entirely.
    var scopeEl = state.root || document.documentElement;
    var v = getComputedStyle(scopeEl).getPropertyValue(name);
    v = (v || '').trim();
    return v || fallback;
  }

  /* Crops the viewBox to the bounding box of the NUTS regions actually
     drawn (the `.em-nutsrg` group), not the library's own nominal width/
     height -- eurostat-map's default "EUR" extent reserves a lot of empty
     sea/margin for far corners (Iceland, Turkey, the Canaries) that this
     panel never draws data for (insets:false, no Turkish data). Falls back
     to the nominal box if the group is ever empty or not yet laid out,
     rather than throwing. */
  function fitViewBoxToRegions(svgEl, fallbackWidth, fallbackHeight) {
    var pad = 8;
    try {
      var group = svgEl.querySelector('.em-nutsrg');
      var box = group && group.getBBox();
      if (box && box.width > 0 && box.height > 0) {
        svgEl.setAttribute(
          'viewBox',
          box.x - pad + ' ' + (box.y - pad) + ' ' + (box.width + 2 * pad) + ' ' + (box.height + 2 * pad)
        );
        return;
      }
    } catch (e) {
      /* getBBox can throw on a not-yet-rendered element in some engines */
    }
    svgEl.setAttribute('viewBox', '0 0 ' + fallbackWidth + ' ' + fallbackHeight);
  }

  /* ---- lazy script loading -------------------------------------------- */
  var vendorPromise = null;
  function loadVendorScript() {
    if (vendorPromise) return vendorPromise;
    vendorPromise = new Promise(function (resolve, reject) {
      if (window.eurostatmap) {
        resolve(window.eurostatmap);
        return;
      }
      var script = document.createElement('script');
      script.src = VENDOR_SRC;
      script.async = true;
      script.onload = function () {
        if (window.eurostatmap) resolve(window.eurostatmap);
        else reject(new Error('eurostatmap.min.js loaded but window.eurostatmap is missing'));
      };
      script.onerror = function () {
        reject(new Error('Failed to load ' + VENDOR_SRC));
      };
      document.body.appendChild(script);
    });
    return vendorPromise;
  }

  /* ---- boot -------------------------------------------------------------
     Fired every time the Europe panel becomes the visible one (panels.js's
     bp:panel-shown, including on first load if the page opens on #europe).
     Runs the real boot exactly once; every later call is a no-op, since the
     panel's DOM survives being hidden/shown again. */
  function init() {
    if (state.inited) return;
    state.inited = true;
    state.root = document.getElementById('europeMapRoot');
    if (!state.root) return;
    buildChrome();

    // Geometry is fetched independently (it does not depend on the vendor
    // script, and the vendor script does not depend on it) so the two loads
    // overlap rather than serialise.
    var indexPromise = fetch(INDEX_URL).then(readJSON);
    var geometryPromise = indexPromise.then(function (index) {
      return fetch('public/data/' + index.geometry).then(readJSON);
    });

    Promise.all([indexPromise, geometryPromise, loadVendorScript()])
      .then(function (results) {
        state.index = results[0];
        state.geometryTopo = results[1];
        indexGeometryNames(results[1]);
        var defaultIndicator = pickDefaultIndicator(state.index.indicators);
        populateIndicatorSelect(state.index.indicators, defaultIndicator);
        return loadIndicator(defaultIndicator);
      })
      .catch(function (err) {
        showError(err);
      });
  }

  function readJSON(resp) {
    if (!resp.ok) throw new Error(resp.status + ' ' + resp.url);
    return resp.json();
  }

  function indexGeometryNames(topo) {
    var geoms = (topo.objects && topo.objects.nutsrg && topo.objects.nutsrg.geometries) || [];
    geoms.forEach(function (g) {
      if (g.properties && g.properties.id) {
        state.regionNames[g.properties.id] = g.properties.na || g.properties.id;
      }
    });
  }

  function pickDefaultIndicator(indicators) {
    var loaded = indicators.filter(function (i) {
      return i.status === 'loaded';
    });
    return (loaded[0] || indicators[0]).id;
  }

  /* ---- static chrome (built once) --------------------------------------- */
  function buildChrome() {
    clear(state.root);
    state.root.removeAttribute('hidden');
    state.root.className = 'bp-europe-map';
    applyRampTokens();

    var indicatorSelect = el('select', { id: 'europeIndicatorSelect' });
    var yearSelect = el('select', { id: 'europeYearSelect' });
    var blockedReason = el('span', { class: 'bp-europe-map__blocked-reason', id: 'europeBlockedReason' });
    blockedReason.hidden = true;

    var controls = el('div', { class: 'bp-europe-map__controls' }, [
      el('div', { class: 'bp-europe-map__field' }, [
        el('label', { for: 'europeIndicatorSelect', text: T('europeIndicatorLabel') }),
        indicatorSelect,
      ]),
      el('div', { class: 'bp-europe-map__field' }, [
        el('label', { for: 'europeYearSelect', text: T('europeYearLabel') }),
        yearSelect,
      ]),
      blockedReason,
    ]);

    var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('id', SVG_ID);
    var stage = el('div', { class: 'bp-europe-map__stage', id: STAGE_ID }, []);
    stage.appendChild(svg);

    var zoomIn = el('button', { type: 'button', 'aria-label': T('europeZoomIn'), id: 'europeZoomIn', text: '+' });
    var zoomOut = el('button', { type: 'button', 'aria-label': T('europeZoomOut'), id: 'europeZoomOut', text: '−' });
    var zoomReset = el('button', { type: 'button', 'aria-label': T('europeResetView'), id: 'europeZoomReset', text: '□' });
    var zoomBox = el('div', { class: 'bp-europe-map__zoom' }, [zoomIn, zoomOut, zoomReset]);
    stage.appendChild(zoomBox);

    var tooltip = el('div', { class: 'bp-europe-map__tooltip', id: 'europeTooltip', hidden: 'hidden' });

    var regionSelect = el('select', { id: 'europeRegionSelect' }, [
      el('option', { value: '', text: T('europeRegionPlaceholder') }),
    ]);
    var regionField = el('div', { class: 'bp-europe-map__field bp-europe-map__region-select' }, [
      el('label', { for: 'europeRegionSelect', text: T('europeRegionLabel') }),
      regionSelect,
    ]);

    var legendScale = el('div', { class: 'bp-europe-map__legend-scale', id: 'europeLegendScale' });
    var legendExtra = el('div', { id: 'europeLegendExtra' });
    var meta = el('div', { class: 'bp-europe-map__meta', id: 'europeMeta' });
    var noOutline = el('p', { class: 'bp-europe-map__no-outline', id: 'europeNoOutline' });
    noOutline.hidden = true;
    var legend = el('div', { class: 'bp-europe-map__legend' }, [legendScale, legendExtra, meta]);

    var main = el('div', { class: 'bp-europe-map__main' }, [
      controls,
      regionField,
      stage,
      tooltip,
      legend,
      noOutline,
    ]);

    var side = el('div', { class: 'bp-europe-map__side', id: 'europeSideCard' }, [
      el('p', { class: 'prompt', text: T('europeSelectPrompt') }),
    ]);

    // Wrapped in its own section (Europe countries batch,
    // docs/features/europe_countries.md) so the "Régions / Pays" toggle
    // (built by buildModeToggle(), a sibling of this wrapper directly under
    // state.root) can hide the whole region UI in one step when country
    // mode is active, without touching any id or class inside it -- every
    // existing selector (#bpEuropeStage, #europeIndicatorSelect, ...) still
    // resolves exactly where it did before this batch.
    var regionWrap = el('div', { class: 'bp-europe-map__mode-section', id: 'europeRegionWrap' }, [main, side]);
    state.root.appendChild(regionWrap);

    state.els = {
      regionWrap: regionWrap,
      indicatorSelect: indicatorSelect,
      yearSelect: yearSelect,
      blockedReason: blockedReason,
      stage: stage,
      svg: svg,
      tooltip: tooltip,
      regionSelect: regionSelect,
      legendScale: legendScale,
      legendExtra: legendExtra,
      meta: meta,
      noOutline: noOutline,
      side: side,
      zoomIn: zoomIn,
      zoomOut: zoomOut,
      zoomReset: zoomReset,
    };

    indicatorSelect.addEventListener('change', function () {
      loadIndicator(indicatorSelect.value);
    });
    yearSelect.addEventListener('change', function () {
      state.currentYear = yearSelect.value;
      render();
    });
    regionSelect.addEventListener('change', function () {
      if (regionSelect.value) selectRegion(regionSelect.value);
    });
    zoomIn.addEventListener('click', function () {
      zoomBy(2);
    });
    zoomOut.addEventListener('click', function () {
      zoomBy(0.5);
    });
    zoomReset.addEventListener('click', resetZoom);

    window.addEventListener('bp:theme', function () {
      applyRampTokens();
      if (state.currentIndicatorId) render();
    });
    document.addEventListener('bp:lang', function (ev) {
      LANG = (ev.detail && ev.detail.lang) || LANG;
      relabelChrome();
      if (state.currentIndicatorId) render();
      if (state.selectedRegion) selectRegion(state.selectedRegion);
    });
  }

  function relabelChrome() {
    state.root.querySelector('label[for="europeIndicatorSelect"]').textContent = T('europeIndicatorLabel');
    state.root.querySelector('label[for="europeYearSelect"]').textContent = T('europeYearLabel');
    state.root.querySelector('label[for="europeRegionSelect"]').textContent = T('europeRegionLabel');
    state.els.regionSelect.options[0].textContent = T('europeRegionPlaceholder');
    state.els.zoomIn.setAttribute('aria-label', T('europeZoomIn'));
    state.els.zoomOut.setAttribute('aria-label', T('europeZoomOut'));
    state.els.zoomReset.setAttribute('aria-label', T('europeResetView'));
    if (state.index) populateIndicatorSelect(state.index.indicators, state.currentIndicatorId);
  }

  function showError(err) {
    clear(state.root);
    state.root.appendChild(el('p', { class: 'bp-europe-map__meta', text: T('europeLoadError') }));
    if (window.console) console.error('[europe_map]', err);
  }

  /* ---- indicator / year selects ------------------------------------------ */
  function populateIndicatorSelect(indicators, selectedId) {
    var select = state.els.indicatorSelect;
    clear(select);
    var blocked = null;
    indicators.forEach(function (ind) {
      var payload = state.payloads[ind.id];
      var label = (payload && payload.names && (payload.names[LANG] || payload.names.en)) || ind.id;
      var opt = el('option', { value: ind.id, text: label });
      if (ind.status !== 'loaded') {
        opt.disabled = true;
        if (ind.id === selectedId || !blocked) blocked = ind;
      }
      if (ind.id === selectedId) opt.selected = true;
      select.appendChild(opt);
    });
    // The reason belongs beside the select as visible text (spec
    // requirement: not tooltip-only), for whichever indicator is CURRENTLY
    // selected -- not every blocked one at once.
    var current = indicators.filter(function (i) {
      return i.id === selectedId;
    })[0];
    if (current && current.status !== 'loaded') {
      fetchPayload(current.id).then(function (p) {
        state.els.blockedReason.hidden = false;
        state.els.blockedReason.textContent = p.blocked_reason || T('europeLoadError');
      });
    } else {
      state.els.blockedReason.hidden = true;
    }
  }

  function populateYearSelect(payload) {
    var select = state.els.yearSelect;
    clear(select);
    payload.years.forEach(function (y) {
      var opt = el('option', { value: y, text: y });
      if (y === state.currentYear) opt.selected = true;
      select.appendChild(opt);
    });
    select.disabled = payload.years.length === 0;
  }

  function fetchPayload(id) {
    if (state.payloads[id]) return Promise.resolve(state.payloads[id]);
    var meta = state.index.indicators.filter(function (i) {
      return i.id === id;
    })[0];
    return fetch(PAYLOAD_DIR + meta.payload)
      .then(readJSON)
      .then(function (payload) {
        state.payloads[id] = payload;
        return payload;
      });
  }

  function loadIndicator(id) {
    return fetchPayload(id).then(function (payload) {
      state.currentIndicatorId = id;
      state.currentYear = payload.latest_year;
      populateIndicatorSelect(state.index.indicators, id);
      populateYearSelect(payload);
      if (payload.status !== 'loaded') {
        renderBlocked(payload);
        return;
      }
      render();
    });
  }

  function renderBlocked(payload) {
    clear(state.els.stage);
    state.els.legendScale.innerHTML = '';
    state.els.legendExtra.innerHTML = '';
    state.els.meta.textContent = payload.blocked_reason || '';
    state.els.noOutline.hidden = true;
    populateRegionSelect({});
  }

  /* ---- the render pass: rebuilt on indicator, year, theme and lang change
     (CLAUDE.md rule 35 does not apply here -- this is a live browser
     re-render, not a build-time export -- but classification breaks are
     still the SAME array regardless of why we are re-rendering: only the
     colour lookup below is theme-dependent). ------------------------------ */
  function render() {
    var payload = state.payloads[state.currentIndicatorId];
    if (!payload || !state.geometryTopo) return;
    var year = state.currentYear;
    var yearValues = payload.values[year] || {};
    var breaks = payload.class_breaks[year] || [];
    var numClasses = Math.max(1, breaks.length + 1);

    var colors = [];
    for (var i = 0; i < numClasses; i++) {
      var rampIndex = MapUI.colourIndex(i, numClasses, MapUI.BINS);
      colors.push(cssVar('--ramp-' + rampIndex, '#999999'));
    }
    var nodataColor = cssVar('--nodata', '#bcbcbc');
    var suppressedColor = cssVar('--bp-chart-8', '#75797f');
    // A clearly darker grey than --nodata's own pale swatch (the two read
    // as near-identical otherwise -- per lead review of the WIP screenshot):
    // an existing token, not an invented shade.
    var excludedColor = cssVar('--bp-text-faint', '#8e98ad');

    var customData = {};
    Object.keys(yearValues).forEach(function (code) {
      var v = yearValues[code].v;
      if (typeof v === 'number') customData[code] = v;
    });

    // eurostat-map's own .build() is asynchronous (it fetches geometry,
    // even though here that fetch is answered from memory -- see buildMap's
    // own comment): the region <path> elements do not exist in the DOM the
    // instant `.build()` RETURNS, only once its internal promise chain
    // actually finishes. Everything that depends on those paths existing --
    // the suppressed/licence-excluded colour overrides, the legend, the
    // region <select>, and (critically) attaching the click/hover/keyboard
    // handlers -- must wait for that, via the `onBuild` callback below, or
    // it silently attaches to nothing (measured: without this, a real
    // click landed on a freshly-drawn but not-yet-wired path and did
    // nothing at all).
    buildMap({
      thresholds: breaks,
      numClasses: numClasses,
      colors: colors,
      nodataColor: nodataColor,
      customData: customData,
      onReady: function () {
        // Post-render overrides the library's single "no data" bucket
        // cannot express: 'suppressed' (Eurostat withheld it) and licence-
        // excluded regions are both visually distinct from a plain
        // 'missing' cell (CLAUDE.md rule 26 -- never collapsed together).
        Object.keys(yearValues).forEach(function (code) {
          if (yearValues[code].s === 'suppressed') paintRegion(code, suppressedColor);
        });
        var hatchFill = ensureHatchPattern(SVG_ID, HATCH_ID, excludedColor);
        (payload.excluded_by_licence || []).forEach(function (code) {
          paintRegion(code, hatchFill);
        });

        renderLegend(payload, breaks, colors, nodataColor, suppressedColor, excludedColor, yearValues);
        renderMeta(payload, year);
        renderNoOutline(payload);
        populateRegionSelect(yearValues);
        attachRegionHandlers(payload, yearValues);

        if (state.selectedRegion) selectRegion(state.selectedRegion);
      },
    });
  }

  var HATCH_ID = 'bpEuropeLicenceHatch';
  var COUNTRY_HATCH_ID = 'bpEuropeCountryLicenceHatch';
  /* A diagonal-stripe SVG pattern for licence-excluded regions, so their
     fill reads as visibly different from a plain "missing" region on the
     map itself, not just a slightly darker flat grey (per lead review of
     the WIP screenshot: "use the same styling on the map itself" as the
     legend's own hatched swatch, europe_map.css). Injected once per build
     into the svg's own <defs>; returns the `url(#id)` fill value to use.
     Generalised (Europe countries batch) over which svg/pattern id: the
     region and country svgs BOTH exist in the DOM at once (one hidden via
     the mode toggle, never removed), so a single shared HATCH_ID would
     look up whichever svg `document.getElementById` happened to return the
     <defs> from -- almost always the wrong one for at least one mode. */
  function ensureHatchPattern(svgId, hatchId, strokeColor) {
    var svgEl = document.getElementById(svgId);
    if (!svgEl) return strokeColor;
    var defs = svgEl.querySelector('defs') || svgEl.insertBefore(document.createElementNS('http://www.w3.org/2000/svg', 'defs'), svgEl.firstChild);
    var existing = document.getElementById(hatchId);
    if (existing) existing.parentNode.removeChild(existing);
    var pattern = document.createElementNS('http://www.w3.org/2000/svg', 'pattern');
    pattern.setAttribute('id', hatchId);
    pattern.setAttribute('width', '4');
    pattern.setAttribute('height', '4');
    pattern.setAttribute('patternTransform', 'rotate(45)');
    pattern.setAttribute('patternUnits', 'userSpaceOnUse');
    var line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('x1', '0');
    line.setAttribute('y1', '0');
    line.setAttribute('x2', '0');
    line.setAttribute('y2', '4');
    line.setAttribute('stroke', strokeColor);
    line.setAttribute('stroke-width', '2');
    pattern.appendChild(line);
    defs.appendChild(pattern);
    return 'url(#' + hatchId + ')';
  }

  function paintRegion(code, color) {
    var pathEl = document.getElementById('em-nutsrg-' + code);
    if (pathEl) pathEl.style.fill = color;
  }

  /* ---- the vendored library itself --------------------------------------
     Every option below exists specifically to stop a network request the
     library would otherwise make by default (grepped from the vendored
     bundle, docs/features/europe_nuts2.md, "eurostat-map"):
       - geo: 'EUR' (never 'WORLD')        -> the GISCO world-basemap branch
         (world-topo-2024-60M-4326.json / WORLD_4326.json) is gated behind
         `geo === 'WORLD'` in the bundle and is simply never reached.
       - placenames is never set to true    -> the GISCO euronym place-name
         CSV fetch is gated behind a `placenames_` flag that defaults to
         unset/false; this file never calls `.placenames(...)`.
       - stat({customData}) only, never {eurostatDatasetCode} -> the ONLY
         thing that triggers `retrieveFromRemote`'s Eurostat statistics API
         call is a truthy `eurostatDatasetCode`, which this file never sets.
       - nuts2jsonBaseURL(GEOMETRY_SENTINEL) plus the temporary window.fetch
         shim below -> the one remaining default (Nuts2json's own GitHub /
         ec.europa.eu geometry endpoint) is answered from our own,
         already-fetched, same-origin public/data/geo/nuts2/2024/2.json
         instead. The shim recognises ONLY the sentinel substring and hands
         every other fetch() call straight to the real network unchanged; it
         removes ITSELF the instant it is actually invoked (measured: the
         library's own geometry fetch is not issued synchronously inside
         `.build()`, so restoring window.fetch right after `.build()`
         returns is too early and the real request would 404 against a URL
         that only ever existed as this shim's sentinel). */
  /* Generalised (Europe countries batch, docs/features/europe_countries.md)
     over WHICH map is being built -- region's top-level `state` (nutsLevel
     2, unchanged behaviour) or `state.country` (nutsLevel 0, the new
     country choropleth) -- so the country map reuses every one of the
     network-suppression and async-timing comments above instead of a
     second, silently-drifting copy of them.
     @param cfg {target, stageId, svgId, stageEl, nutsLevel, nutsYear,
       geometryTopo, thresholds, numClasses, colors, nodataColor,
       customData, onReady} */
  function buildChoropleth(cfg) {
    if (!window.eurostatmap) return;
    // Only the OLD svg is removed here, never the whole stage: the stage
    // also holds the keyboard-accessible zoom buttons built once in
    // buildChrome()/buildCountryChrome(), and clearing the stage on every
    // indicator/period/theme re-render used to silently delete them along
    // with the old drawing.
    var oldSvg = document.getElementById(cfg.svgId);
    if (oldSvg && oldSvg.parentNode) oldSvg.parentNode.removeChild(oldSvg);
    var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('id', cfg.svgId);
    cfg.stageEl.insertBefore(svg, cfg.stageEl.firstChild);

    // Unique per map instance (region vs country build side by side across
    // a mode switch) but still recognised by exactly the narrow check
    // below -- appending the target svgId keeps the sentinel's own
    // uniqueness guarantee intact rather than reusing one global constant
    // for two independent map instances.
    var sentinel = GEOMETRY_SENTINEL + ':' + cfg.svgId;

    var config = {
      containerId: cfg.stageId,
      svgId: cfg.svgId,
      width: 760,
      height: 780,
      title: '',
      geo: 'EUR',
      proj: '3035',
      scale: '20M',
      nutsLevel: cfg.nutsLevel,
      nutsYear: cfg.nutsYear,
      nuts2jsonBaseURL: sentinel,
      // FRY1-FRY5/PT20/PT30 (Guadeloupe, Martinique, Guyane, Réunion,
      // Mayotte, Azores, Madeira) are published under separate per-
      // territory URLs this batch never fetches (payload `no_outline`,
      // docs/features/europe_nuts2.md) -- insets:false stops the library
      // from even trying to load them via the (by-then-restored) fetch
      // shim below, which would otherwise 404 against the real network.
      insets: false,
      legend: false,
      // The library draws its OWN tooltip by default (a default
      // textFunction reads the bound stat value straight off the region) --
      // left alone, a hover shows both that one and this file's own
      // (`#europeTooltip`/`#europeCountryTooltip`), stacked on top of each
      // other. Its `mouseover` only becomes visible when the text it is
      // given is truthy, so a textFunction that always returns '' keeps the
      // library's tooltip permanently empty/invisible without touching any
      // private property.
      tooltip: { textFunction: function () { return ''; } },
      // The default in-map credit line (`defaultFootnote_`, verbatim the
      // same "Administrative boundaries: ©EuroGeographics ©OpenStreetMap"
      // text `renderMeta`/`renderCountryMeta` below already print, once, in
      // the source block) renders unconditionally unless turned off -- left
      // on, the same sentence appeared twice, tiny and overlapping in a
      // bottom corner of the map itself.
      footnote: false,
      // The library's own zoom buttons are plain SVG <g> elements with no
      // tabindex or keyboard handling at all (grepped from the vendored
      // bundle: zero matches for keydown/tabindex anywhere in it) -- kept
      // on, they would sit on screen doing nothing for a keyboard user
      // while duplicating the three real, focusable <button> elements this
      // file builds instead (see "zoom / reset" below), which drive the
      // exact same underlying zoom behaviour. One set of controls, reachable
      // by both mouse and keyboard, rather than two that only partly agree.
      zoomButtons: false,
      zoomExtent: [1, 8],
      classificationMethod: 'threshold',
      thresholds: cfg.thresholds,
      numberOfClasses: cfg.numClasses,
      colors: cfg.colors,
      noDataFillStyle: cfg.nodataColor,
      noDataText: T('status_missing'),
      stat: { customData: cfg.customData },
      // `.build()` is asynchronous (it fetches geometry, even though that
      // fetch is answered from memory here) -- onBuild is the library's own
      // "the map is actually finished" callback, and everything that reads
      // back the rendered SVG (the viewBox fix, the zoom baseline, and the
      // caller's own onReady -- suppressed/excluded colour overrides,
      // legend, region/country handlers) waits for it rather than for
      // `.build()` to merely RETURN (measured: those region <path> elements
      // do not exist yet at that point).
      onBuild: function () {
        var builtSvg = document.getElementById(cfg.svgId);
        if (builtSvg) fitViewBoxToRegions(builtSvg, config.width, config.height);
        try {
          var node = map && map.svg_ && map.svg_.node && map.svg_.node();
          if (node && node.__zoom) cfg.target.zoomBaseline = node.__zoom;
        } catch (e) {
          /* zoom buttons degrade to no-ops below if this ever fails */
        }
        if (cfg.onReady) cfg.onReady();
      },
    };

    // The library's own geometry fetch is NOT issued synchronously inside
    // `.build()` (measured: restoring window.fetch right after `.build()`
    // returns is too early, and the real, uninterrupted request reaches the
    // network as a 404 for a URL that only ever existed as this shim's
    // sentinel). So the shim removes ITSELF the instant it is actually
    // invoked, whenever that turns out to be, rather than guessing a delay
    // -- and it only ever recognises this one, uniquely-tagged URL, so
    // leaving it installed a little longer than strictly necessary is safe:
    // every other fetch() call on the page passes straight through
    // untouched in the meantime.
    var topo = cfg.geometryTopo;
    var nativeFetch = window.fetch;
    window.fetch = function (input) {
      var url = typeof input === 'string' ? input : (input && input.url) || '';
      if (url.indexOf(sentinel) !== -1) {
        window.fetch = nativeFetch;
        return Promise.resolve(
          new Response(JSON.stringify(topo), { status: 200, headers: { 'Content-Type': 'application/json' } })
        );
      }
      return nativeFetch.apply(window, arguments);
    };
    cfg.target.zoomBaseline = null;
    var map = window.eurostatmap.map('choropleth', config);
    cfg.target.mapInstance = map;
    map.build();
    // The viewBox fix (measured: the library does NOT reliably add its own
    // for this "choropleth"/geo:'EUR' combination, so CSS's `width:100%;
    // height:auto` -- europe_map.css's phone-width fit -- would otherwise
    // resize the SVG's box without remapping the coordinate space the
    // regions are drawn in) and the zoom-baseline capture both happen
    // inside `config.onBuild` above, once the drawing actually exists.
  }

  /* Region mode's own thin call into buildChoropleth() -- same signature
     render() already calls, so this batch's refactor of the old buildMap()
     into the shared buildChoropleth() above touches nothing at render()'s
     own call site. */
  function buildMap(opts) {
    buildChoropleth({
      target: state,
      stageId: STAGE_ID,
      svgId: SVG_ID,
      stageEl: state.els.stage,
      nutsLevel: 2,
      nutsYear: (state.index && state.index.nuts_version) || '2024',
      geometryTopo: state.geometryTopo,
      thresholds: opts.thresholds,
      numClasses: opts.numClasses,
      colors: opts.colors,
      nodataColor: opts.nodataColor,
      customData: opts.customData,
      onReady: opts.onReady,
    });
  }

  /* ---- zoom / reset -------------------------------------------------------
     eurostat-map ships its own mouse-wheel/drag zoom (d3-zoom, bound
     internally) but no keyboard equivalent and no real <button> for it
     (grepped: its zoom controls are plain SVG <g> elements with no
     tabindex). These three real, focusable <button> elements call the SAME
     d3-zoom behaviour the library's own (mouse-only) buttons use, so
     keyboard and mouse zoom always agree. */
  /* Generalised over WHICH map (region's top-level state, or
     state.country -- Europe countries batch) so the country map's own
     zoom buttons drive the SAME underlying d3-zoom behaviour without a
     second copy of this try/catch. */
  function zoomByGeneric(target, factor) {
    try {
      var map = target.mapInstance;
      if (map && map.svg_ && map.__zoomBehavior) {
        map.svg_.transition().call(map.__zoomBehavior.scaleBy, factor);
      }
    } catch (e) {
      /* no-op: zoom becomes unavailable rather than throwing */
    }
  }
  function resetZoomGeneric(target) {
    try {
      var map = target.mapInstance;
      if (map && map.svg_ && map.__zoomBehavior && target.zoomBaseline) {
        map.svg_.transition().call(map.__zoomBehavior.transform, target.zoomBaseline);
      }
    } catch (e) {
      /* no-op */
    }
  }
  function zoomBy(factor) {
    zoomByGeneric(state, factor);
  }
  function resetZoom() {
    resetZoomGeneric(state);
  }

  /* ---- legend -------------------------------------------------------------- */
  function renderLegend(payload, breaks, colors, nodataColor, suppressedColor, excludedColor, yearValues) {
    var scale = state.els.legendScale;
    clear(scale);
    var edges = [null].concat(breaks).concat([null]);
    for (var i = 0; i < colors.length; i++) {
      var lo = edges[i],
        hi = edges[i + 1];
      var label;
      if (lo === null && hi === null) label = '';
      else if (lo === null) label = '< ' + MapUI.formatValue(hi, payload.unit, null, LANG);
      else if (hi === null) label = '≥ ' + MapUI.formatValue(lo, payload.unit, null, LANG);
      else label = MapUI.formatValue(lo, payload.unit, null, LANG) + '–' + MapUI.formatValue(hi, payload.unit, null, LANG);
      scale.appendChild(
        el('div', { class: 'bp-europe-map__legend-row' }, [
          el('span', { class: 'bp-europe-map__legend-swatch', style: 'background:' + colors[i] }),
          el('span', { text: label }),
        ])
      );
    }
    scale.appendChild(
      el('div', { class: 'bp-europe-map__legend-row' }, [
        el('span', { class: 'bp-europe-map__legend-swatch', style: 'background:' + nodataColor }),
        el('span', { text: T('europeLegendMissing') }),
      ])
    );
    var hasSuppressed = Object.keys(yearValues).some(function (c) {
      return yearValues[c].s === 'suppressed';
    });
    if (hasSuppressed) {
      scale.appendChild(
        el('div', { class: 'bp-europe-map__legend-row' }, [
          el('span', { class: 'bp-europe-map__legend-swatch', style: 'background:' + suppressedColor }),
          el('span', { text: T('europeLegendSuppressed') }),
        ])
      );
    }
    if ((payload.excluded_by_licence || []).length) {
      scale.appendChild(
        el('div', { class: 'bp-europe-map__legend-row' }, [
          el('span', {
            class: 'bp-europe-map__legend-swatch bp-europe-map__legend-swatch--hatched',
            style: 'background:' + excludedColor,
          }),
          el('span', { text: T('europeLegendExcluded') }),
        ])
      );
    }
  }

  function renderMeta(payload, year) {
    var unitSuffix = MapUI.unitSuffix(payload.unit, LANG);
    var lines = [];
    lines.push((payload.names[LANG] || payload.names.en) + (unitSuffix ? ' (' + unitSuffix.trim() + ')' : '') + ' — ' + year);
    if (payload.source && payload.source.retrieved) {
      lines.push(T('europeSourceLabel') + ': Eurostat (' + payload.source.dataset + '), ' + T('europeRetrievedLabel', { date: payload.source.retrieved }));
    }
    lines.push(T('europeVintageLabel', { version: payload.nuts_version }));
    if (state.index && state.index.attribution) lines.push(state.index.attribution);
    var meta = state.els.meta;
    clear(meta);
    lines.forEach(function (line) {
      meta.appendChild(el('div', { text: line }));
    });
  }

  function renderNoOutline(payload) {
    var codes = Object.keys(payload.no_outline || {});
    var node = state.els.noOutline;
    if (!codes.length) {
      node.hidden = true;
      return;
    }
    node.hidden = false;
    node.textContent = T('europeNoOutlineNote', { n: codes.length, codes: codes.join(', ') });
  }

  /* ---- region selection: mouse (path click), keyboard (path focus + Enter,
     or the searchable <select>), and hover (mouseover + focus, same
     handler) all converge on `selectRegion`. -------------------------------- */
  function populateRegionSelect(yearValues) {
    var select = state.els.regionSelect;
    var placeholder = select.options[0];
    clear(select);
    select.appendChild(placeholder);
    Object.keys(yearValues)
      .sort(function (a, b) {
        return (state.regionNames[a] || a).localeCompare(state.regionNames[b] || b);
      })
      .forEach(function (code) {
        var name = state.regionNames[code] || code;
        select.appendChild(el('option', { value: code, text: name + ' (' + code + ')' }));
      });
  }

  function attachRegionHandlers(payload, yearValues) {
    Object.keys(yearValues).forEach(function (code) {
      var pathEl = document.getElementById('em-nutsrg-' + code);
      if (!pathEl) return;
      pathEl.setAttribute('tabindex', '0');
      pathEl.setAttribute('role', 'button');
      pathEl.setAttribute('aria-label', (state.regionNames[code] || code) + ' (' + code + ')');
      pathEl.addEventListener('click', function () {
        selectRegion(code);
      });
      pathEl.addEventListener('keydown', function (ev) {
        if (ev.key === 'Enter' || ev.key === ' ') {
          ev.preventDefault();
          selectRegion(code);
        }
      });
      var showTip = function (ev) {
        showTooltip(code, payload, yearValues[code], ev);
      };
      // Keyboard focus carries no pointer position at all (a FocusEvent
      // has no clientX/clientY) -- showTooltip's own fallback of 0,0 would
      // otherwise plant the tooltip in the corner of the page, nowhere
      // near the region a keyboard user just tabbed to. Anchoring on the
      // element's own bounding box instead is the keyboard equivalent of
      // "near the cursor".
      var showTipAtElement = function () {
        var r = pathEl.getBoundingClientRect();
        showTooltip(code, payload, yearValues[code], { clientX: r.left + r.width / 2, clientY: r.top });
      };
      pathEl.addEventListener('mouseover', showTip);
      pathEl.addEventListener('mousemove', showTip);
      pathEl.addEventListener('focus', showTipAtElement);
      pathEl.addEventListener('mouseout', hideTooltip);
      pathEl.addEventListener('blur', hideTooltip);
    });
  }

  function statusWord(status) {
    if (!status || status === 'final') return '';
    var key = 'status_' + status;
    return T(key) !== key ? T(key) : status;
  }

  function showTooltip(code, payload, cell, ev) {
    var tip = state.els.tooltip;
    var name = state.regionNames[code] || code;
    var valueText =
      cell && typeof cell.v === 'number'
        ? MapUI.formatValue(cell.v, payload.unit, null, LANG) + MapUI.unitSuffix(payload.unit, LANG)
        : T('status_' + ((cell && cell.s) || 'missing'));
    clear(tip);
    tip.appendChild(el('div', { class: 'name', text: name + ' (' + code + ')' }));
    tip.appendChild(el('div', { text: valueText + ' · ' + state.currentYear }));
    var sw = cell && statusWord(cell.s);
    if (sw) tip.appendChild(el('div', { text: sw }));
    tip.hidden = false;
    var x = (ev && ev.clientX) || 0,
      y = (ev && ev.clientY) || 0;
    tip.style.left = x + 14 + 'px';
    tip.style.top = y + 14 + 'px';
  }
  function hideTooltip() {
    state.els.tooltip.hidden = true;
  }

  function selectRegion(code) {
    state.selectedRegion = code;
    state.els.regionSelect.value = code;
    var payload = state.payloads[state.currentIndicatorId];
    if (!payload) return;
    var yearValues = payload.values[state.currentYear] || {};
    var cell = yearValues[code];
    var name = state.regionNames[code] || code;

    var side = state.els.side;
    clear(side);
    side.appendChild(el('h3', { text: name }));
    side.appendChild(el('p', { class: 'code', text: code }));

    var valueText =
      cell && typeof cell.v === 'number'
        ? MapUI.formatValue(cell.v, payload.unit, null, LANG) + MapUI.unitSuffix(payload.unit, LANG)
        : T('status_' + ((cell && cell.s) || 'missing'));
    side.appendChild(el('p', { class: 'value', text: valueText }));
    var sw = cell && statusWord(cell.s);
    side.appendChild(
      el('p', {
        class: 'value-meta',
        text: (payload.names[LANG] || payload.names.en) + ', ' + state.currentYear + (sw ? ' · ' + sw : ''),
      })
    );

    // History across every year the payload carries for this region, gap
    // for a year with no numeric value -- never bridged, never drawn as
    // zero (the same convention explorer.html's own chart already uses:
    // a period is simply omitted from the drawn points, not synthesised).
    var points = payload.years
      .filter(function (y) {
        var c = payload.values[y] && payload.values[y][code];
        return c && typeof c.v === 'number';
      })
      .map(function (y) {
        return { period: y, value: payload.values[y][code].v };
      });
    if (points.length > 1 && window.BPCharts) {
      side.appendChild(el('p', { class: 'hist-label', text: T('europeHistoryLabel') }));
      var canvas = document.createElement('canvas');
      side.appendChild(canvas);
      window.BPCharts.drawLine(canvas, [{ label: name, points: points }], { locale: LANG, height: 120, compact: false });
    }

    if (payload.source) {
      side.appendChild(
        el('p', {
          class: 'about',
          text:
            T('europeSourceLabel') +
            ': Eurostat (' +
            payload.source.dataset +
            ')' +
            (payload.source.retrieved ? ', ' + T('europeRetrievedLabel', { date: payload.source.retrieved }) : '') +
            ' · ' +
            T('europeVintageLabel', { version: payload.nuts_version }),
        })
      );
    }
  }

  /* =====================================================================
   * Europe countries batch (docs/features/europe_countries.md): the
   * "Régions / Pays" map toggle, the country choropleth (NUTS 0), the
   * accessible country picker, and the "Comparaison internationale" small
   * multiples. Region mode above is completely unchanged -- everything
   * here is additive, sharing only the generic helpers (el/clear/cssVar,
   * buildChoropleth, zoomByGeneric/resetZoomGeneric, ensureHatchPattern,
   * paintRegion, statusWord, loadVendorScript, readJSON,
   * pickDefaultIndicator, fitViewBoxToRegions) already used above.
   * ===================================================================== */

  function initCountry() {
    if (state.country.inited) return;
    state.country.inited = true;
    if (!state.root) state.root = document.getElementById('europeMapRoot');
    if (!state.root) return;

    buildModeToggle();
    buildCountryChrome();
    buildCompareChrome();

    var indexPromise = fetch(COUNTRY_INDEX_URL).then(readJSON);
    state.country.allPayloadsPromise = indexPromise
      .then(function (index) {
        state.country.index = index;
        index.countries.forEach(function (c) {
          state.country.countryNames[c.code] = c.names;
        });
        buildCountryPicker(index);

        return Promise.all(
          index.indicators.map(function (i) {
            return fetchCountryPayload(i.id);
          })
        ).then(function () {
          var mapIndicators = index.indicators.filter(function (i) {
            return i.map;
          });
          var defaultId = pickDefaultIndicator(mapIndicators.length ? mapIndicators : index.indicators);
          state.country.currentIndicatorId = defaultId;
          var defaultPayload = state.country.payloads[defaultId];
          state.country.currentPeriod = defaultPayload.latest_period;
          populateCountryIndicatorSelect(mapIndicators, defaultId);
          populateCountryPeriodSelect(defaultPayload);
          renderComparisonCharts();
          if (state.mode === 'country') {
            return ensureCountryGeometryAndVendor().then(renderCountryMap);
          }
        });
      })
      .catch(function (err) {
        showCountryError(err);
      });
  }

  /* ---- mode toggle: "Régions / Pays" ------------------------------------- */
  function buildModeToggle() {
    var regionBtn = el('button', {
      type: 'button',
      id: 'europeModeRegion',
      class: 'bp-europe-mode__btn',
      'aria-pressed': 'true',
      text: T('europeModeRegion'),
    });
    var countryBtn = el('button', {
      type: 'button',
      id: 'europeModeCountry',
      class: 'bp-europe-mode__btn',
      'aria-pressed': 'false',
      text: T('europeModeCountry'),
    });
    var bar = el('div', { class: 'bp-europe-mode', role: 'group', 'aria-label': T('europeModeGroupLabel') }, [
      regionBtn,
      countryBtn,
    ]);
    // Ahead of everything buildChrome() already put in state.root (the
    // region wrapper) -- the toggle reads as the first thing in the panel,
    // above whichever mode's UI is currently visible.
    state.root.insertBefore(bar, state.root.firstChild);
    state.els.modeRegionBtn = regionBtn;
    state.els.modeCountryBtn = countryBtn;
    regionBtn.addEventListener('click', function () {
      setMode('region');
    });
    countryBtn.addEventListener('click', function () {
      setMode('country');
    });

    window.addEventListener('bp:theme', function () {
      if (state.mode === 'country' && state.country.currentIndicatorId) renderCountryMap();
      renderComparisonCharts();
    });
    document.addEventListener('bp:lang', function (ev) {
      LANG = (ev.detail && ev.detail.lang) || LANG;
      relabelCountryChrome();
      relabelCompareChrome();
      if (state.country.index) relabelCountryPicker();
      if (state.mode === 'country' && state.country.currentIndicatorId) renderCountryMap();
      renderComparisonCharts();
    });
  }

  function setMode(mode) {
    if (state.mode === mode) return;
    state.mode = mode;
    state.els.modeRegionBtn.setAttribute('aria-pressed', String(mode === 'region'));
    state.els.modeCountryBtn.setAttribute('aria-pressed', String(mode === 'country'));
    state.els.regionWrap.hidden = mode !== 'region';
    state.els.country.wrap.hidden = mode !== 'country';
    if (mode === 'country' && state.country.index && state.country.currentIndicatorId) {
      ensureCountryGeometryAndVendor().then(renderCountryMap);
    }
  }

  /* ---- country map chrome ------------------------------------------------- */
  function buildCountryChrome() {
    var indicatorSelect = el('select', { id: 'europeCountryIndicatorSelect' });
    var periodSelect = el('select', { id: 'europeCountryPeriodSelect' });
    var blockedReason = el('span', { class: 'bp-europe-map__blocked-reason', id: 'europeCountryBlockedReason' });
    blockedReason.hidden = true;
    var controls = el('div', { class: 'bp-europe-map__controls' }, [
      el('div', { class: 'bp-europe-map__field' }, [
        el('label', { for: 'europeCountryIndicatorSelect', text: T('europeIndicatorLabel') }),
        indicatorSelect,
      ]),
      el('div', { class: 'bp-europe-map__field' }, [
        el('label', { for: 'europeCountryPeriodSelect', text: T('europePeriodLabel') }),
        periodSelect,
      ]),
      blockedReason,
    ]);

    // "Region and country unemployment are different Eurostat series"
    // (spec requirement) -- a persistent, generic clarification rather
    // than one gated on which single indicator happens to be selected,
    // which would mean this generic renderer naming one specific
    // indicator id (rules 2/24).
    var note = el('p', { class: 'bp-europe-map__note', id: 'europeUnemploymentNote', text: T('europeUnemploymentNote') });

    var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('id', COUNTRY_SVG_ID);
    var stage = el('div', { class: 'bp-europe-map__stage', id: COUNTRY_STAGE_ID }, []);
    stage.appendChild(svg);

    var zoomIn = el('button', { type: 'button', 'aria-label': T('europeZoomIn'), id: 'europeCountryZoomIn', text: '+' });
    var zoomOut = el('button', { type: 'button', 'aria-label': T('europeZoomOut'), id: 'europeCountryZoomOut', text: '−' });
    var zoomReset = el('button', { type: 'button', 'aria-label': T('europeResetView'), id: 'europeCountryZoomReset', text: '□' });
    stage.appendChild(el('div', { class: 'bp-europe-map__zoom' }, [zoomIn, zoomOut, zoomReset]));

    var tooltip = el('div', { class: 'bp-europe-map__tooltip', id: 'europeCountryTooltip', hidden: 'hidden' });

    var pickerSearch = el('input', {
      type: 'search',
      id: 'europeCountryPickerSearch',
      'aria-label': T('europeCountryPickerSearchLabel'),
      placeholder: T('europeCountryPickerSearchLabel'),
    });
    var pickerList = el('div', {
      class: 'bp-europe-picker__list',
      id: 'europeCountryPickerList',
      role: 'group',
      'aria-label': T('europeCountryPickerLabel'),
    });
    var capMsg = el('p', { class: 'bp-europe-picker__cap', id: 'europeCountryCapMsg', text: T('europeCountryCapMsg') });
    capMsg.hidden = true;
    var picker = el('div', { class: 'bp-europe-picker' }, [
      el('label', { for: 'europeCountryPickerSearch', text: T('europeCountryPickerLabel') }),
      pickerSearch,
      pickerList,
      capMsg,
    ]);

    var legendScale = el('div', { class: 'bp-europe-map__legend-scale', id: 'europeCountryLegendScale' });
    var legendExtra = el('div', { id: 'europeCountryLegendExtra' });
    var meta = el('div', { class: 'bp-europe-map__meta', id: 'europeCountryMeta' });
    var noOutline = el('p', { class: 'bp-europe-map__no-outline', id: 'europeCountryNoOutline' });
    noOutline.hidden = true;
    var legend = el('div', { class: 'bp-europe-map__legend' }, [legendScale, legendExtra, meta]);

    var main = el('div', { class: 'bp-europe-map__main' }, [controls, note, picker, stage, tooltip, legend, noOutline]);
    var side = el('div', { class: 'bp-europe-map__side', id: 'europeCountrySideCard' }, [
      el('p', { class: 'prompt', text: T('europeCountrySelectPrompt') }),
    ]);

    var wrap = el('div', { class: 'bp-europe-map__mode-section', id: 'europeCountryWrap' }, [main, side]);
    wrap.hidden = true; // region is the default mode
    state.root.appendChild(wrap);

    state.els.country = {
      wrap: wrap,
      indicatorSelect: indicatorSelect,
      periodSelect: periodSelect,
      blockedReason: blockedReason,
      note: note,
      stage: stage,
      svg: svg,
      tooltip: tooltip,
      pickerSearch: pickerSearch,
      pickerList: pickerList,
      capMsg: capMsg,
      legendScale: legendScale,
      legendExtra: legendExtra,
      meta: meta,
      noOutline: noOutline,
      side: side,
      zoomIn: zoomIn,
      zoomOut: zoomOut,
      zoomReset: zoomReset,
    };

    indicatorSelect.addEventListener('change', function () {
      loadCountryIndicator(indicatorSelect.value);
    });
    periodSelect.addEventListener('change', function () {
      state.country.currentPeriod = periodSelect.value;
      renderCountryMap();
    });
    zoomIn.addEventListener('click', function () {
      zoomByGeneric(state.country, 2);
    });
    zoomOut.addEventListener('click', function () {
      zoomByGeneric(state.country, 0.5);
    });
    zoomReset.addEventListener('click', function () {
      resetZoomGeneric(state.country);
    });
    pickerSearch.addEventListener('input', function () {
      filterCountryPicker(pickerSearch.value);
    });
  }

  function relabelCountryChrome() {
    var c = state.els.country;
    if (!c) return;
    state.root.querySelector('label[for="europeCountryIndicatorSelect"]').textContent = T('europeIndicatorLabel');
    state.root.querySelector('label[for="europeCountryPeriodSelect"]').textContent = T('europePeriodLabel');
    state.root.querySelector('label[for="europeCountryPickerSearch"]').textContent = T('europeCountryPickerLabel');
    c.note.textContent = T('europeUnemploymentNote');
    c.pickerSearch.setAttribute('placeholder', T('europeCountryPickerSearchLabel'));
    c.pickerSearch.setAttribute('aria-label', T('europeCountryPickerSearchLabel'));
    c.pickerList.setAttribute('aria-label', T('europeCountryPickerLabel'));
    c.capMsg.textContent = T('europeCountryCapMsg');
    c.zoomIn.setAttribute('aria-label', T('europeZoomIn'));
    c.zoomOut.setAttribute('aria-label', T('europeZoomOut'));
    c.zoomReset.setAttribute('aria-label', T('europeResetView'));
    var prompt = c.side.querySelector('.prompt');
    if (prompt) prompt.textContent = T('europeCountrySelectPrompt');
    state.els.modeRegionBtn.textContent = T('europeModeRegion');
    state.els.modeCountryBtn.textContent = T('europeModeCountry');
    if (state.country.index) {
      var mapIndicators = state.country.index.indicators.filter(function (i) {
        return i.map;
      });
      populateCountryIndicatorSelect(mapIndicators, state.country.currentIndicatorId);
    }
  }

  function showCountryError(err) {
    if (state.els.country && state.els.country.meta) {
      clear(state.els.country.meta);
      state.els.country.meta.appendChild(el('p', { text: T('europeLoadError') }));
    }
    if (window.console) console.error('[europe_map:country]', err);
  }

  /* ---- lazy geometry + vendor script (only once country mode is actually
     shown -- the region map's own vendor script is already cached by
     loadVendorScript()'s promise, so switching modes after region has
     already opened costs only the ~294 KB NUTS 0 geometry fetch). --------- */
  function ensureCountryGeometryAndVendor() {
    if (state.country.geometryPromise) return state.country.geometryPromise;
    state.country.geometryPromise = fetch('public/data/' + state.country.index.geometry)
      .then(readJSON)
      .then(function (topo) {
        state.country.geometryTopo = topo;
        return Promise.all([topo, loadVendorScript()]);
      });
    return state.country.geometryPromise;
  }

  /* ---- indicator / period ------------------------------------------------- */
  function fetchCountryPayload(id) {
    if (state.country.payloads[id]) return Promise.resolve(state.country.payloads[id]);
    var meta = state.country.index.indicators.filter(function (i) {
      return i.id === id;
    })[0];
    return fetch(COUNTRY_PAYLOAD_DIR + meta.payload)
      .then(readJSON)
      .then(function (payload) {
        state.country.payloads[id] = payload;
        return payload;
      });
  }

  function populateCountryIndicatorSelect(mapIndicators, selectedId) {
    var select = state.els.country.indicatorSelect;
    clear(select);
    mapIndicators.forEach(function (ind) {
      var payload = state.country.payloads[ind.id];
      var label = (payload && payload.names && (payload.names[LANG] || payload.names.en)) || ind.id;
      var opt = el('option', { value: ind.id, text: label });
      if (ind.status !== 'loaded') opt.disabled = true;
      if (ind.id === selectedId) opt.selected = true;
      select.appendChild(opt);
    });
    var current = mapIndicators.filter(function (i) {
      return i.id === selectedId;
    })[0];
    var currentPayload = current && state.country.payloads[current.id];
    if (current && current.status !== 'loaded') {
      state.els.country.blockedReason.hidden = false;
      state.els.country.blockedReason.textContent = (currentPayload && currentPayload.blocked_reason) || T('europeLoadError');
    } else {
      state.els.country.blockedReason.hidden = true;
    }
  }

  function populateCountryPeriodSelect(payload) {
    var select = state.els.country.periodSelect;
    clear(select);
    (payload.periods || []).forEach(function (p) {
      var opt = el('option', { value: p, text: p });
      if (p === state.country.currentPeriod) opt.selected = true;
      select.appendChild(opt);
    });
    select.disabled = !(payload.periods && payload.periods.length);
  }

  function loadCountryIndicator(id) {
    state.country.currentIndicatorId = id;
    var payload = state.country.payloads[id];
    if (!payload) return;
    state.country.currentPeriod = payload.latest_period;
    var mapIndicators = state.country.index.indicators.filter(function (i) {
      return i.map;
    });
    populateCountryIndicatorSelect(mapIndicators, id);
    populateCountryPeriodSelect(payload);
    if (payload.status !== 'loaded' || !payload.map) {
      renderCountryBlocked(payload);
      return;
    }
    ensureCountryGeometryAndVendor().then(renderCountryMap);
  }

  function renderCountryBlocked(payload) {
    clear(state.els.country.stage);
    state.els.country.legendScale.innerHTML = '';
    state.els.country.legendExtra.innerHTML = '';
    state.els.country.meta.textContent = (payload && payload.blocked_reason) || '';
    state.els.country.noOutline.hidden = true;
  }

  /* ---- the country render pass -- same shape as render() above (theme,
     period and language changes all funnel back through here). ------------ */
  function renderCountryMap() {
    var payload = state.country.payloads[state.country.currentIndicatorId];
    if (!payload || !state.country.geometryTopo) return;
    if (payload.status !== 'loaded' || !payload.map) {
      renderCountryBlocked(payload);
      return;
    }
    var period = state.country.currentPeriod;
    var periodValues = payload.values[period] || {};
    var breaks = payload.class_breaks[period] || [];
    var numClasses = Math.max(1, breaks.length + 1);

    var colors = [];
    for (var i = 0; i < numClasses; i++) {
      var rampIndex = MapUI.colourIndex(i, numClasses, MapUI.BINS);
      colors.push(cssVar('--ramp-' + rampIndex, '#999999'));
    }
    var nodataColor = cssVar('--nodata', '#bcbcbc');
    var suppressedColor = cssVar('--bp-chart-8', '#75797f');
    var excludedColor = cssVar('--bp-text-faint', '#8e98ad');

    var customData = {};
    Object.keys(periodValues).forEach(function (code) {
      var v = periodValues[code].v;
      if (typeof v === 'number') customData[code] = v;
    });

    buildChoropleth({
      target: state.country,
      stageId: COUNTRY_STAGE_ID,
      svgId: COUNTRY_SVG_ID,
      stageEl: state.els.country.stage,
      nutsLevel: 0,
      nutsYear: (state.country.index && state.country.index.geo_vintage) || '2024',
      geometryTopo: state.country.geometryTopo,
      thresholds: breaks,
      numClasses: numClasses,
      colors: colors,
      nodataColor: nodataColor,
      customData: customData,
      onReady: function () {
        Object.keys(periodValues).forEach(function (code) {
          if (periodValues[code].s === 'suppressed') paintRegion(code, suppressedColor);
        });
        var hatchFill = ensureHatchPattern(COUNTRY_SVG_ID, COUNTRY_HATCH_ID, excludedColor);
        (payload.excluded_by_licence || []).forEach(function (code) {
          paintRegion(code, hatchFill);
        });

        renderCountryLegend(payload, breaks, colors, nodataColor, suppressedColor, excludedColor, periodValues);
        renderCountryMeta(payload, period);
        renderCountryNoOutline(payload);
        attachCountryHandlers(payload, periodValues);
        renderCountrySelectionOutline();
      },
    });
  }

  function renderCountryLegend(payload, breaks, colors, nodataColor, suppressedColor, excludedColor, periodValues) {
    var scale = state.els.country.legendScale;
    clear(scale);
    var edges = [null].concat(breaks).concat([null]);
    for (var i = 0; i < colors.length; i++) {
      var lo = edges[i],
        hi = edges[i + 1];
      var label;
      if (lo === null && hi === null) label = '';
      else if (lo === null) label = '< ' + MapUI.formatValue(hi, payload.unit, null, LANG);
      else if (hi === null) label = '≥ ' + MapUI.formatValue(lo, payload.unit, null, LANG);
      else label = MapUI.formatValue(lo, payload.unit, null, LANG) + '–' + MapUI.formatValue(hi, payload.unit, null, LANG);
      scale.appendChild(
        el('div', { class: 'bp-europe-map__legend-row' }, [
          el('span', { class: 'bp-europe-map__legend-swatch', style: 'background:' + colors[i] }),
          el('span', { text: label }),
        ])
      );
    }
    scale.appendChild(
      el('div', { class: 'bp-europe-map__legend-row' }, [
        el('span', { class: 'bp-europe-map__legend-swatch', style: 'background:' + nodataColor }),
        el('span', { text: T('europeLegendMissing') }),
      ])
    );
    var hasSuppressed = Object.keys(periodValues).some(function (c) {
      return periodValues[c].s === 'suppressed';
    });
    if (hasSuppressed) {
      scale.appendChild(
        el('div', { class: 'bp-europe-map__legend-row' }, [
          el('span', { class: 'bp-europe-map__legend-swatch', style: 'background:' + suppressedColor }),
          el('span', { text: T('europeLegendSuppressed') }),
        ])
      );
    }
    if ((payload.excluded_by_licence || []).length) {
      scale.appendChild(
        el('div', { class: 'bp-europe-map__legend-row' }, [
          el('span', {
            class: 'bp-europe-map__legend-swatch bp-europe-map__legend-swatch--hatched',
            style: 'background:' + excludedColor,
          }),
          el('span', { text: T('europeLegendExcluded') }),
        ])
      );
    }
  }

  function renderCountryMeta(payload, period) {
    var unitSuffix = MapUI.unitSuffix(payload.unit, LANG);
    var lines = [];
    lines.push((payload.names[LANG] || payload.names.en) + (unitSuffix ? ' (' + unitSuffix.trim() + ')' : '') + ' — ' + period);
    if (payload.source && payload.source.retrieved) {
      lines.push(T('europeSourceLabel') + ': Eurostat (' + payload.source.dataset + '), ' + T('europeRetrievedLabel', { date: payload.source.retrieved }));
    }
    lines.push(T('europeVintageLabel', { version: payload.geo_vintage }));
    if (state.country.index && state.country.index.attribution) lines.push(state.country.index.attribution);
    var meta = state.els.country.meta;
    clear(meta);
    lines.forEach(function (line) {
      meta.appendChild(el('div', { text: line }));
    });
  }

  function renderCountryNoOutline(payload) {
    var codes = Object.keys(payload.no_outline || {});
    var node = state.els.country.noOutline;
    if (!codes.length) {
      node.hidden = true;
      return;
    }
    node.hidden = false;
    node.textContent = T('europeNoOutlineNote', { n: codes.length, codes: codes.join(', ') });
  }

  /* ---- country selection: click/keyboard on the map toggles the SAME
     `state.country.selected` array the picker checkboxes and the
     comparison charts below read -- one selection model, three ways to
     change it. -------------------------------------------------------- */
  function attachCountryHandlers(payload, periodValues) {
    Object.keys(periodValues).forEach(function (code) {
      var pathEl = document.getElementById('em-nutsrg-' + code);
      if (!pathEl) return;
      pathEl.setAttribute('tabindex', '0');
      pathEl.setAttribute('role', 'button');
      pathEl.setAttribute('aria-pressed', String(state.country.selected.indexOf(code) !== -1));
      pathEl.setAttribute('aria-label', (state.country.countryNames[code] && state.country.countryNames[code][LANG]) || code);
      pathEl.addEventListener('click', function () {
        toggleCountrySelection(code);
        selectCountryDetail(code);
      });
      pathEl.addEventListener('keydown', function (ev) {
        if (ev.key === 'Enter' || ev.key === ' ') {
          ev.preventDefault();
          toggleCountrySelection(code);
          selectCountryDetail(code);
        }
      });
      var showTip = function (ev) {
        showCountryTooltip(code, payload, periodValues[code], ev);
      };
      var showTipAtElement = function () {
        var r = pathEl.getBoundingClientRect();
        showCountryTooltip(code, payload, periodValues[code], { clientX: r.left + r.width / 2, clientY: r.top });
      };
      pathEl.addEventListener('mouseover', showTip);
      pathEl.addEventListener('mousemove', showTip);
      pathEl.addEventListener('focus', showTipAtElement);
      pathEl.addEventListener('mouseout', hideCountryTooltip);
      pathEl.addEventListener('blur', hideCountryTooltip);
    });
  }

  function showCountryTooltip(code, payload, cell, ev) {
    var tip = state.els.country.tooltip;
    var name = (state.country.countryNames[code] && state.country.countryNames[code][LANG]) || code;
    var valueText =
      cell && typeof cell.v === 'number'
        ? MapUI.formatValue(cell.v, payload.unit, null, LANG) + MapUI.unitSuffix(payload.unit, LANG)
        : T('status_' + ((cell && cell.s) || 'missing'));
    clear(tip);
    tip.appendChild(el('div', { class: 'name', text: name + ' (' + code + ')' }));
    tip.appendChild(el('div', { text: valueText + ' · ' + state.country.currentPeriod }));
    var sw = cell && statusWord(cell.s);
    if (sw) tip.appendChild(el('div', { text: sw }));
    tip.hidden = false;
    var x = (ev && ev.clientX) || 0,
      y = (ev && ev.clientY) || 0;
    tip.style.left = x + 14 + 'px';
    tip.style.top = y + 14 + 'px';
  }
  function hideCountryTooltip() {
    state.els.country.tooltip.hidden = true;
  }

  function selectCountryDetail(code) {
    var payload = state.country.payloads[state.country.currentIndicatorId];
    if (!payload) return;
    var periodValues = payload.values[state.country.currentPeriod] || {};
    var cell = periodValues[code];
    var name = (state.country.countryNames[code] && state.country.countryNames[code][LANG]) || code;

    var side = state.els.country.side;
    clear(side);
    side.appendChild(el('h3', { text: name }));
    side.appendChild(el('p', { class: 'code', text: code }));

    var valueText =
      cell && typeof cell.v === 'number'
        ? MapUI.formatValue(cell.v, payload.unit, null, LANG) + MapUI.unitSuffix(payload.unit, LANG)
        : T('status_' + ((cell && cell.s) || 'missing'));
    side.appendChild(el('p', { class: 'value', text: valueText }));
    var sw = cell && statusWord(cell.s);
    side.appendChild(
      el('p', {
        class: 'value-meta',
        text: (payload.names[LANG] || payload.names.en) + ', ' + state.country.currentPeriod + (sw ? ' · ' + sw : ''),
      })
    );

    var points = payload.periods
      .filter(function (p) {
        var c = payload.values[p] && payload.values[p][code];
        return c && typeof c.v === 'number';
      })
      .map(function (p) {
        return { period: p, value: payload.values[p][code].v };
      });
    if (points.length > 1 && window.BPCharts) {
      side.appendChild(el('p', { class: 'hist-label', text: T('europeHistoryLabel') }));
      var canvas = document.createElement('canvas');
      side.appendChild(canvas);
      window.BPCharts.drawLine(canvas, [{ label: name, points: points }], { locale: LANG, height: 120, compact: false });
    }

    if (payload.source) {
      side.appendChild(
        el('p', {
          class: 'about',
          text:
            T('europeSourceLabel') +
            ': Eurostat (' +
            payload.source.dataset +
            ')' +
            (payload.source.retrieved ? ', ' + T('europeRetrievedLabel', { date: payload.source.retrieved }) : '') +
            ' · ' +
            T('europeVintageLabel', { version: payload.geo_vintage }),
        })
      );
    }
  }

  /* ---- picker: every allowlisted country, a search filter, and the cap
     message -- keyboard-usable (real <label>/<input type=checkbox> pairs,
     not a div with a click handler). ---------------------------------- */
  function buildCountryPicker(index) {
    var list = state.els.country.pickerList;
    clear(list);
    index.countries.forEach(function (c) {
      var checkbox = el('input', { type: 'checkbox' });
      var nameSpan = el('span', { class: 'name' });
      var chip = el('label', { class: 'bp-europe-picker__chip', 'data-code': c.code }, [checkbox, nameSpan]);
      list.appendChild(chip);
      checkbox.addEventListener('change', function () {
        toggleCountrySelection(c.code);
      });
    });
    relabelCountryPicker();
    syncCountrySelectionUI();
  }

  function relabelCountryPicker() {
    var index = state.country.index;
    if (!index) return;
    var list = state.els.country.pickerList;
    Array.prototype.forEach.call(list.children, function (chip) {
      var code = chip.getAttribute('data-code');
      var c = index.countries.filter(function (x) {
        return x.code === code;
      })[0];
      if (!c) return;
      var name = (c.names && (c.names[LANG] || c.names.en)) || code;
      chip.querySelector('.name').textContent = name + ' (' + code + ')';
    });
  }

  function filterCountryPicker(query) {
    var q = (query || '').trim().toLowerCase();
    var list = state.els.country.pickerList;
    Array.prototype.forEach.call(list.children, function (chip) {
      var text = chip.textContent.toLowerCase();
      chip.hidden = q.length > 0 && text.indexOf(q) === -1;
    });
  }

  function toggleCountrySelection(code) {
    var selected = state.country.selected;
    var idx = selected.indexOf(code);
    if (idx !== -1) {
      selected.splice(idx, 1);
    } else {
      if (selected.length >= MAX_SELECTED_COUNTRIES) {
        syncCountrySelectionUI(); // reverts a checkbox the user just ticked past the cap
        return;
      }
      selected.push(code);
    }
    syncCountrySelectionUI();
    renderCountrySelectionOutline();
    renderComparisonCharts();
  }

  /* Reflects `state.country.selected` onto the picker chips (checked
     state, and every unchecked chip disabled once the cap is hit -- the
     spec's "plain message when hit", both as text and as a real
     keyboard-reachable disabled state) and the map's own outline. Called
     after every selection change and every picker (re)build. */
  function syncCountrySelectionUI() {
    var c = state.els.country;
    if (!c || !c.pickerList) return;
    var selected = state.country.selected;
    var atCap = selected.length >= MAX_SELECTED_COUNTRIES;
    Array.prototype.forEach.call(c.pickerList.children, function (chip) {
      var code = chip.getAttribute('data-code');
      var checkbox = chip.querySelector('input');
      var isChecked = selected.indexOf(code) !== -1;
      checkbox.checked = isChecked;
      chip.dataset.checked = String(isChecked);
      var disable = !isChecked && atCap;
      checkbox.disabled = disable;
      chip.dataset.disabled = String(disable);
    });
    c.capMsg.hidden = !atCap;
  }

  function renderCountrySelectionOutline() {
    var stage = state.els.country && state.els.country.stage;
    if (!stage) return;
    var selected = state.country.selected;
    var paths = stage.querySelectorAll('path[id^="em-nutsrg-"]');
    Array.prototype.forEach.call(paths, function (p) {
      var code = p.id.replace('em-nutsrg-', '');
      var isSelected = selected.indexOf(code) !== -1;
      if (isSelected) p.setAttribute('data-selected', 'true');
      else p.removeAttribute('data-selected');
      p.setAttribute('aria-pressed', String(isSelected));
    });
  }

  /* ---- "Comparaison internationale": one small-multiple line chart per
     country indicator (7: the six map indicators plus the GDP volume
     index), one line per selected country, EU27/euro-area as optional
     dashed reference lines. Reuses BPCharts.drawLine/attachTooltip --
     assets/belpulse/charts.js -- no second chart library (rule 29's own
     spirit extended to charts). ---------------------------------------- */
  function buildCompareChrome() {
    var root = document.getElementById('international');
    if (!root) return;
    clear(root);
    root.hidden = false;

    var heading = el('h3', { text: T('europeCompareTitle') });
    var desc = el('p', { class: 'bp-europe-compare__desc', text: T('europeCompareDesc') });

    var refEU = el('input', { type: 'checkbox', id: 'europeRefEU27' });
    var refEULabelText = document.createTextNode(' ' + T('europeRefEU27'));
    var refEULabel = el('label', {}, [refEU]);
    refEULabel.appendChild(refEULabelText);

    var refEA = el('input', { type: 'checkbox', id: 'europeRefEA21' });
    var refEALabelText = document.createTextNode(' ' + T('europeRefEA21'));
    var refEALabel = el('label', {}, [refEA]);
    refEALabel.appendChild(refEALabelText);

    var refRow = el('div', { class: 'bp-europe-compare__refs' }, [refEULabel, refEALabel]);
    var grid = el('div', { class: 'bp-europe-compare__grid', id: 'europeCompareGrid' });
    var empty = el('p', { class: 'bp-europe-compare__empty', id: 'europeCompareEmpty' });
    empty.hidden = true;

    root.appendChild(heading);
    root.appendChild(desc);
    root.appendChild(refRow);
    root.appendChild(grid);
    root.appendChild(empty);

    state.els.compare = {
      heading: heading,
      desc: desc,
      refEU: refEU,
      refEULabelText: refEULabelText,
      refEA: refEA,
      refEALabelText: refEALabelText,
      grid: grid,
      empty: empty,
      chartState: {}, // indicator_id -> {series, model, tipOpts} for register()'s resize redraw
    };
    refEU.addEventListener('change', renderComparisonCharts);
    refEA.addEventListener('change', renderComparisonCharts);
  }

  function relabelCompareChrome() {
    var c = state.els.compare;
    if (!c) return;
    c.heading.textContent = T('europeCompareTitle');
    c.desc.textContent = T('europeCompareDesc');
    c.refEULabelText.textContent = ' ' + T('europeRefEU27');
    c.refEALabelText.textContent = ' ' + T('europeRefEA21');
  }

  function hasAnyReference(referenceLines, code) {
    return Object.keys(referenceLines || {}).some(function (p) {
      return referenceLines[p] && referenceLines[p][code];
    });
  }

  function referenceSeries(payload, code, label, colour, dash) {
    var points = payload.periods.map(function (p) {
      var cell = payload.reference_lines[p] && payload.reference_lines[p][code];
      return { period: p, value: cell && typeof cell.v === 'number' ? cell.v : null, status: cell ? cell.s : 'missing' };
    });
    return { label: label, unit: payload.unit, colour: colour, dash: dash, isReference: true, points: points };
  }

  function renderComparisonCharts() {
    var c = state.els.compare;
    if (!c || !state.country.index) return;
    clear(c.grid);
    var selected = state.country.selected;
    if (!selected.length) {
      c.empty.hidden = false;
      c.empty.textContent = T('europeComparePickPrompt');
      return;
    }
    c.empty.hidden = true;
    var showEU27 = c.refEU.checked;
    var showEA21 = c.refEA.checked;
    var statusLabels = {
      suppressed: T('status_suppressed'),
      na: T('status_na'),
      missing: T('status_missing'),
      provisional: T('status_provisional'),
      estimate: T('status_estimate'),
      revised: T('status_revised'),
    };

    state.country.index.indicators.forEach(function (indMeta) {
      var payload = state.country.payloads[indMeta.id];
      var card = el('div', { class: 'bp-europe-compare__card' });
      var title = (payload && payload.names && (payload.names[LANG] || payload.names.en)) || indMeta.id;
      card.appendChild(el('h4', { text: title }));

      if (!payload || payload.status !== 'loaded') {
        card.appendChild(
          el('p', { class: 'bp-europe-compare__blocked', text: (payload && payload.blocked_reason) || T('europeLoadError') })
        );
        c.grid.appendChild(card);
        return;
      }

      var legend = el('div', { class: 'bp-europe-compare__legend' });
      selected.forEach(function (code, i) {
        var name = (state.country.countryNames[code] && state.country.countryNames[code][LANG]) || code;
        // `data-code` (never a rendered indicator id, just the ISO-shaped
        // geography code the selection itself is keyed on) lets a test or a
        // future feature find "the Belgium chip" without depending on
        // which language happens to be active.
        legend.appendChild(
          el('span', { class: 'chip', 'data-code': code }, [
            el('span', { class: 'dot', style: 'background:var(--bp-chart-' + ((i % 8) + 1) + ')' }),
            el('span', { text: name }),
          ])
        );
      });
      card.appendChild(legend);

      var canvas = document.createElement('canvas');
      card.appendChild(canvas);

      var series = selected.map(function (code, i) {
        var name = (state.country.countryNames[code] && state.country.countryNames[code][LANG]) || code;
        var points = payload.periods.map(function (p) {
          var cell = payload.values[p] && payload.values[p][code];
          return { period: p, value: cell && typeof cell.v === 'number' ? cell.v : null, status: cell ? cell.s : 'missing' };
        });
        return { label: name, unit: payload.unit, colourIndex: i, points: points };
      });
      if (showEU27 && hasAnyReference(payload.reference_lines, 'EU27_2020')) {
        series.push(referenceSeries(payload, 'EU27_2020', T('europeRefEU27'), cssVar('--bp-text-faint', '#8e98ad'), [6, 3]));
      }
      if (showEA21 && hasAnyReference(payload.reference_lines, 'EA21')) {
        series.push(referenceSeries(payload, 'EA21', T('europeRefEA21'), cssVar('--bp-border', '#c8cdd8'), [2, 2]));
      }

      var st = c.chartState[indMeta.id] || (c.chartState[indMeta.id] = { model: { hits: [] } });
      st.series = series;
      st.tipOpts = {
        locale: LANG,
        unit: payload.unit,
        statusLabels: statusLabels,
        missingLabel: T('status_missing'),
        ariaLabel: title,
        formatValue: MapUI.formatValue,
      };

      function draw() {
        if (canvas.offsetParent === null) return;
        canvas.style.width = '';
        canvas.style.height = '';
        var next = window.BPCharts.drawLine(canvas, st.series, { locale: LANG, height: 180 });
        st.model.hits = next.hits;
      }
      draw();
      window.BPCharts.attachTooltip(canvas, st.model, st.tipOpts);
      window.BPCharts.register(canvas, draw);

      var metaLines = [];
      var unitSuffix = MapUI.unitSuffix(payload.unit, LANG);
      if (unitSuffix) metaLines.push(unitSuffix.trim());
      if (payload.source && payload.source.retrieved) {
        metaLines.push(T('europeSourceLabel') + ': Eurostat (' + payload.source.dataset + '), ' + T('europeRetrievedLabel', { date: payload.source.retrieved }));
      }
      if (metaLines.length) card.appendChild(el('p', { class: 'bp-europe-compare__meta', text: metaLines.join(' · ') }));

      if (payload.adapted && payload.adapted.notice) {
        var noticeText = payload.adapted.notice[LANG] || payload.adapted.notice.en;
        card.appendChild(el('p', { class: 'bp-europe-compare__adapted', text: T('europeAdaptedLabel') + ': ' + noticeText }));
      }

      c.grid.appendChild(card);
    });
  }

  /* ---- wire-up ------------------------------------------------------------
     Both init() (region, unchanged) and initCountry() (Europe countries
     batch) run synchronously here: init() builds the region chrome and
     THEN starts its own async fetches, so by the time initCountry() runs
     immediately after, state.root already holds the region wrapper and
     mode-toggle/country/compare chrome can be appended after it without
     racing buildChrome()'s own `clear(state.root)`. */
  window.addEventListener('bp:panel-shown', function (ev) {
    if (ev.detail === 'europe') {
      init();
      initCountry();
    }
  });
})();
