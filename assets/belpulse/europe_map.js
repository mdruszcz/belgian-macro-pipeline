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

  function cssVar(name, fallback) {
    var v = getComputedStyle(document.documentElement).getPropertyValue(name);
    v = (v || '').trim();
    return v || fallback;
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

    state.root.appendChild(main);
    state.root.appendChild(side);

    state.els = {
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
    var excludedColor = cssVar('--bp-border', '#cccccc');

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
        (payload.excluded_by_licence || []).forEach(function (code) {
          paintRegion(code, excludedColor);
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
  function buildMap(opts) {
    if (!window.eurostatmap) return;
    // Only the OLD svg is removed here, never the whole stage: the stage
    // also holds the keyboard-accessible zoom buttons built once in
    // buildChrome(), and clearing the stage on every indicator/year/theme
    // re-render used to silently delete them along with the old drawing.
    var oldSvg = document.getElementById(SVG_ID);
    if (oldSvg && oldSvg.parentNode) oldSvg.parentNode.removeChild(oldSvg);
    var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('id', SVG_ID);
    state.els.stage.insertBefore(svg, state.els.stage.firstChild);

    var config = {
      containerId: STAGE_ID,
      svgId: SVG_ID,
      width: 760,
      height: 780,
      title: '',
      geo: 'EUR',
      proj: '3035',
      scale: '20M',
      nutsLevel: 2,
      nutsYear: (state.index && state.index.nuts_version) || '2024',
      nuts2jsonBaseURL: GEOMETRY_SENTINEL,
      // FRY1-FRY5/PT20/PT30 (Guadeloupe, Martinique, Guyane, Réunion,
      // Mayotte, Azores, Madeira) are published under separate per-
      // territory URLs this batch never fetches (payload `no_outline`,
      // docs/features/europe_nuts2.md) -- insets:false stops the library
      // from even trying to load them via the (by-then-restored) fetch
      // shim below, which would otherwise 404 against the real network.
      insets: false,
      legend: false,
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
      thresholds: opts.thresholds,
      numberOfClasses: opts.numClasses,
      colors: opts.colors,
      noDataFillStyle: opts.nodataColor,
      noDataText: T('status_missing'),
      stat: { customData: opts.customData },
      // `.build()` is asynchronous (it fetches geometry, even though that
      // fetch is answered from memory here) -- onBuild is the library's own
      // "the map is actually finished" callback, and everything that reads
      // back the rendered SVG (the viewBox fix, the zoom baseline, and
      // render()'s own onReady -- suppressed/excluded colour overrides,
      // legend, region handlers) waits for it rather than for `.build()`
      // to merely RETURN (measured: those region <path> elements do not
      // exist yet at that point).
      onBuild: function () {
        var builtSvg = document.getElementById(SVG_ID);
        if (builtSvg) builtSvg.setAttribute('viewBox', '0 0 ' + config.width + ' ' + config.height);
        try {
          var node = map && map.svg_ && map.svg_.node && map.svg_.node();
          if (node && node.__zoom) state.zoomBaseline = node.__zoom;
        } catch (e) {
          /* zoom buttons degrade to no-ops below if this ever fails */
        }
        if (opts.onReady) opts.onReady();
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
    var topo = state.geometryTopo;
    var nativeFetch = window.fetch;
    window.fetch = function (input) {
      var url = typeof input === 'string' ? input : (input && input.url) || '';
      if (url.indexOf(GEOMETRY_SENTINEL) !== -1) {
        window.fetch = nativeFetch;
        return Promise.resolve(
          new Response(JSON.stringify(topo), { status: 200, headers: { 'Content-Type': 'application/json' } })
        );
      }
      return nativeFetch.apply(window, arguments);
    };
    state.zoomBaseline = null;
    var map = window.eurostatmap.map('choropleth', config);
    state.mapInstance = map;
    map.build();
    // The viewBox fix (measured: the library does NOT reliably add its own
    // for this "choropleth"/geo:'EUR' combination, so CSS's `width:100%;
    // height:auto` -- europe_map.css's phone-width fit -- would otherwise
    // resize the SVG's box without remapping the coordinate space the
    // regions are drawn in) and the zoom-baseline capture both happen
    // inside `config.onBuild` above, once the drawing actually exists.
  }

  /* ---- zoom / reset -------------------------------------------------------
     eurostat-map ships its own mouse-wheel/drag zoom (d3-zoom, bound
     internally) but no keyboard equivalent and no real <button> for it
     (grepped: its zoom controls are plain SVG <g> elements with no
     tabindex). These three real, focusable <button> elements call the SAME
     d3-zoom behaviour the library's own (mouse-only) buttons use, so
     keyboard and mouse zoom always agree. */
  function zoomBy(factor) {
    try {
      var map = state.mapInstance;
      if (map && map.svg_ && map.__zoomBehavior) {
        map.svg_.transition().call(map.__zoomBehavior.scaleBy, factor);
      }
    } catch (e) {
      /* no-op: zoom becomes unavailable rather than throwing */
    }
  }
  function resetZoom() {
    try {
      var map = state.mapInstance;
      if (map && map.svg_ && map.__zoomBehavior && state.zoomBaseline) {
        map.svg_.transition().call(map.__zoomBehavior.transform, state.zoomBaseline);
      }
    } catch (e) {
      /* no-op */
    }
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
          el('span', { class: 'bp-europe-map__legend-swatch', style: 'background:' + excludedColor }),
          el('span', { text: T('europeLegendExcluded') }),
        ])
      );
    }
  }

  function renderMeta(payload, year) {
    var unitSuffix = MapUI.unitSuffix(payload.unit);
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
        ? MapUI.formatValue(cell.v, payload.unit, null, LANG) + MapUI.unitSuffix(payload.unit)
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
        ? MapUI.formatValue(cell.v, payload.unit, null, LANG) + MapUI.unitSuffix(payload.unit)
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

  /* ---- wire-up ------------------------------------------------------------ */
  window.addEventListener('bp:panel-shown', function (ev) {
    if (ev.detail === 'europe') init();
  });
})();
