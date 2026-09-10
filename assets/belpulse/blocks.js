/**
 * Hydration for rendered page documents -- Batch 10
 * (docs/features/page_builder.md).
 *
 * src/pages/render.py emits every block as real HTML. Two block types cannot
 * be finished server-side -- a chart is canvas pixels and a map needs a 1.2 MB
 * boundary file -- so the renderer leaves them a slot marked
 * `data-hydrate="chart"` / `data-hydrate="map"` and this fills it in.
 *
 * LAZY, AND THAT IS THE POINT. Batch 0 measured map.html at 59 on performance
 * because of that boundary file; a page document may hold several map blocks,
 * and fetching the boundaries once per block on load would be far worse. Each
 * slot is hydrated only when it actually scrolls into view, the boundary file
 * is fetched AT MOST ONCE per page and shared between every map block, and a
 * page with no map block never fetches it at all.
 *
 * Nothing here draws a chart or a map itself: charts go through
 * assets/belpulse/charts.js and maps through assets/commune_map.js, the same
 * components every hand-built page uses (claude.md rule 29).
 *
 * A block whose data never arrives is left in the state the renderer gave it.
 * This module never invents a value to fill a slot.
 */
(function (global) {
  'use strict';

  var GEO_URL = 'data/geo/communes.geojson';

  // map.html's own zoom factor, named here rather than repeated twice.
  var ZOOM_STEP = 1.4;

  // One in-flight promise for the boundary file, shared by every map block on
  // the page. Two maps scrolling into view together must not start two 1.2 MB
  // downloads.
  var geoPromise = null;
  function boundaries(prefix) {
    if (!geoPromise) {
      // The prefix matters: a page one directory down asking for
      // "data/geo/..." gets /preview/data/geo/..., which does not exist, and
      // the map then draws NOTHING with no error anywhere -- the block still
      // reports "ready" because the renderer set that before hydration. Third
      // time this exact trap has appeared (stylesheets, block payloads, and
      // now the boundary file), which is why the prefix is threaded through
      // rather than assumed empty.
      geoPromise = fetch((prefix || '') + GEO_URL).then(function (r) {
        if (!r.ok) throw new Error('boundaries unavailable');
        return r.json();
      });
    }
    return geoPromise;
  }

  /* commune_map.js opens with `const MapUI = {}` at the top level of a classic
   * script, and a top-level const is a LEXICAL global: reachable as a bare
   * identifier, never a property of `window`. So `global.MapUI` is undefined
   * in here even though the file loaded fine and a bare `MapUI` one line away
   * works -- which made hydrateMap return at its very first guard and leave a
   * map with no boundaries, silently, with the block still marked ready.
   *
   * Read it through the scope chain instead. Do NOT "fix" this by making
   * commune_map.js assign to window: map.html, home.html, commune.html and
   * communes.html all load that file as it stands, and this module is the
   * newcomer. `typeof` on an undeclared name is the one safe probe here --
   * a bare reference would throw. */
  function mapUI() {
    if (global.MapUI) return global.MapUI;
    return typeof MapUI !== 'undefined' ? MapUI : null;
  }

  function blockOf(el) {
    return el.closest ? el.closest('.bp-block') : null;
  }

  /** The payload a block was given, if the page put one there. Batch 14 owns
   * producing these; this only reads what is already attached. */
  function payloadFor(block, provided) {
    if (!block) return null;
    var id = block.getAttribute('data-block-id');
    return (provided && Object.prototype.hasOwnProperty.call(provided, id)) ? provided[id] : null;
  }

  /* A donut and a ranking are not time series: both want LABEL/VALUE pairs,
     where a line and a bar want period/value points. The resolver has no
     operation that returns "these named things and their sizes" -- a binding
     names exactly one indicator -- so a chart of either kind is fed from
     `data.segments` (donut) or `data.items` (ranking), and neither exists yet.
     Wired now so the drawing code is reachable the day a part-of-whole binding
     lands; until then the block says so rather than drawing an empty circle. */
  function labelledPairs(data, key) {
    var rows = data && data[key];
    if (!Array.isArray(rows)) return null;
    var out = rows.filter(function (row) {
      return row && typeof row.value === 'number' && row.label;
    });
    return out.length ? out : null;
  }

  function hydrateChart(slot, data, opts) {
    var canvas = slot.querySelector('canvas');
    if (!canvas || !global.BPCharts) return;
    var kind = slot.getAttribute('data-chart-type') || 'line';

    if (kind === 'donut' || kind === 'ranking') {
      var pairs = labelledPairs(data, kind === 'donut' ? 'segments' : 'items');
      if (!pairs) {
        /* Not an error -- the binding resolved, it simply cannot express this
           shape. Says so in the block's own message rather than leaving a
           blank canvas, which reads as broken. */
        var block = blockOf(slot);
        if (block) block.setAttribute('data-state', 'unavailable');
        return;
      }
      if (kind === 'donut') BPCharts.drawDonut(canvas, pairs, { locale: opts.lang });
      else BPCharts.drawRanking(canvas, pairs, { locale: opts.lang });
      return;
    }

    var points = data && data.points;
    if (!points || points.length < 2) return;   // never a one-point "trend"
    var series = [{ label: (data && data.label) || '', points: points }];
    if (kind === 'bar') {
      BPCharts.drawBar(canvas, points.map(function (p) {
        return { label: p.period, value: p.value };
      }), { locale: opts.lang });
    } else {
      BPCharts.drawLine(canvas, series, { locale: opts.lang });
    }
  }

  /* The indicator payload for a picker change. Same file communes.html and
     map.html already fetch -- a published block reads the published payload
     (rule 20), it does not learn a second way to get data. */
  function indicatorPayload(prefix, code) {
    return fetch(prefix + 'public/data/indicators/' + encodeURIComponent(code) + '.json')
      .then(function (r) { return r.ok ? r.json() : null; });
  }

  /* CommuneMap.setData wants ROWS -- nis -> {value, period, status} -- and
     tests `typeof row.value === 'number'`. Hand it bare numbers and every
     commune falls through to `var(--nodata)`: a complete, silent, grey map
     with a legend that says nothing carries a value. Both converters below
     exist to produce that one shape, from the two payloads this module sees.

     A SUPPRESSED CELL IS A ROW, not an absent key. The component counts
     withheld figures separately and its coverage note says how many were
     withheld; drop them and a withheld figure is reported as "no data",
     collapsing two of the five states claude.md rule 26 keeps distinct.
     Painting one is the other error -- the component already refuses that,
     because a null value never satisfies its number test. */

  /** The resolver's payload (`values`: nis -> number, plus `suppressed`). */
  function rowsFromResolved(data) {
    var out = {};
    var values = (data && data.values) || {};
    Object.keys(values).forEach(function (nis) {
      out[nis] = { value: values[nis], status: 'final' };
    });
    ((data && data.suppressed) || []).forEach(function (nis) {
      out[nis] = { value: null, status: 'suppressed' };
    });
    return out;
  }

  /** A published indicator payload, fetched whole by the picker. Its cells
      are ALREADY the shape the component wants, so they are passed through
      rather than rebuilt -- including their period and status. */
  function rowsFromIndicator(payload) {
    var out = {};
    var communes = (payload && payload.communes) || {};
    Object.keys(communes).forEach(function (nis) {
      if (communes[nis]) out[nis] = communes[nis];
    });
    return out;
  }

  function wireZoom(slot, map) {
    var toolbar = slot.parentNode && slot.parentNode.querySelector('.bp-map-zoom');
    if (!toolbar) return;
    /* The component already had zoomBy() and resetView(); v1 of the map block
       simply rendered no controls to reach them, so a published map could be
       panned but not zoomed while map.html could. */
    toolbar.querySelectorAll('[data-map-zoom]').forEach(function (button) {
      button.addEventListener('click', function () {
        var action = button.getAttribute('data-map-zoom');
        if (action === 'reset') map.resetView();
        else map.zoomBy(action === 'in' ? 1 / ZOOM_STEP : ZOOM_STEP);
      });
    });
  }

  function wirePicker(slot, map, data, opts) {
    var select = slot.parentNode && slot.parentNode.querySelector('[data-map-picker]');
    if (!select) return;
    var prefix = opts.assetPrefix || '';
    fetch(prefix + 'public/data/metadata/indicators.json')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (index) {
        if (!index) return;
        /* Built from the published metadata, never a list typed into this file
           -- rule 24: no indicator id inside generic block logic. */
        (index.indicators || []).forEach(function (meta) {
          var option = document.createElement('option');
          option.value = meta.indicator_code;
          option.textContent = (meta.names && meta.names[opts.lang]) || meta.indicator_code;
          if (meta.indicator_code === (data && data.indicator)) option.selected = true;
          select.appendChild(option);
        });
        select.disabled = false;
        select.addEventListener('change', function () {
          indicatorPayload(prefix, select.value).then(function (payload) {
            if (!payload) return;
            map.setData(rowsFromIndicator(payload), {
              unit: payload.unit, decimals: payload.decimals, direction: payload.direction,
            });
          });
        });
      });
  }

  function hydrateMap(slot, data, opts) {
    var ui = mapUI();
    if (!ui) return;
    var svg = slot.querySelector('svg');
    var box = slot.querySelector('.mapbox');
    if (!svg || !box) return;
    var prefix = opts.assetPrefix || '';
    return boundaries(prefix).then(function (geojson) {
      var map = new ui.CommuneMap({
        svg: svg,
        mapbox: box,
        tip: slot.querySelector('.map-tip'),
        swatches: slot.querySelector('.swatches'),
        ticks: slot.querySelector('.ticks'),
        /* The coverage sentence -- "N of 565 communes, M withheld". Omit it
           and the difference between a withheld figure and a missing one
           never reaches the reader. */
        legendNote: slot.querySelector('.legend .note'),
      }, {
        lang: opts.lang,
        /* Off unless the document asked for it: a map that should not navigate
           simply passes nothing, which is the component's own contract. */
        onSelect: slot.getAttribute('data-map-click-through')
          ? function (nis) { global.location.href = prefix + 'local/' + nis + '/'; }
          : null,
      }).build(geojson);
      if (data && data.values) {
        map.setData(rowsFromResolved(data), {
          unit: data.unit, decimals: data.decimals, direction: data.direction,
        });
      }
      if (slot.getAttribute('data-map-zoom-enabled')) wireZoom(slot, map);
      if (slot.getAttribute('data-map-picker-enabled')) wirePicker(slot, map, data, opts);
      return map;
    }).catch(function () {
      var block = blockOf(slot);
      if (block) block.setAttribute('data-state', 'error');
    });
  }

  /* The comparison picker's options are 565 commune names. They come from the
     published geography metadata -- the same file local.html and map.html
     already read -- rather than being inlined into every page that carries a
     picker. Fetched once and shared, like the boundary file above. */
  var geographiesPromise = null;
  function geographies(prefix) {
    if (!geographiesPromise) {
      geographiesPromise = fetch((prefix || '') + 'public/data/metadata/geographies.json')
        .then(function (r) { return r.ok ? r.json() : null; });
    }
    return geographiesPromise;
  }

  function hydrateComparisonPicker(slot, data, opts) {
    var selects = slot.querySelectorAll('[data-compare-slot]');
    if (!selects.length) return;
    return geographies(opts.assetPrefix || '').then(function (index) {
      if (!index) return;
      /* Municipalities only: a region is not something this page compares
         against in the same sense, and the aggregate rows already cover
         province, region and country. */
      var communes = (index.geographies || []).filter(function (g) {
        return g && g.level === 'municipality' && g.nis_code;
      }).sort(function (a, b) {
        return String(nameOf(a, opts.lang)).localeCompare(String(nameOf(b, opts.lang)));
      });
      selects.forEach(function (select) {
        communes.forEach(function (g) {
          var option = document.createElement('option');
          option.value = g.nis_code;
          option.textContent = nameOf(g, opts.lang);
          select.appendChild(option);
        });
        select.disabled = false;
      });
      var button = slot.querySelector('button[type="submit"]');
      if (button) button.disabled = false;
      /* The chosen peers go in the URL, so a comparison someone assembled is a
         link they can send. Same contract local.html already uses. */
      slot.addEventListener('submit', function (event) {
        event.preventDefault();
        var chosen = [];
        selects.forEach(function (s) { if (s.value) chosen.push(s.value); });
        var url = new URL(global.location.href);
        if (chosen.length) url.searchParams.set('vs', chosen.join(','));
        else url.searchParams.delete('vs');
        global.location.href = url.toString();
      });
    });
  }

  function nameOf(geo, lang) {
    var names = geo && geo.name;
    if (!names) return geo && geo.nis_code;
    return names[lang] || names.en || geo.nis_code;
  }

  /* The small line under a KPI figure. Same series the headline came from, so
     the number and the shape cannot disagree. Compact and axis-free: at this
     size a scale would be unreadable, and the point is the direction. */
  function hydrateSpark(slot, data, opts) {
    var canvas = slot.querySelector('canvas');
    if (!canvas || !global.BPCharts) return;
    var points = data && data.points;
    if (!points || points.length < 2) return;   // one point is not a trend
    BPCharts.drawLine(canvas, [{ label: '', points: points }], {
      locale: opts.lang, height: 40, compact: true,
    });
  }

  var HYDRATORS = {
    chart: hydrateChart,
    map: hydrateMap,
    comparison_picker: hydrateComparisonPicker,
    spark: hydrateSpark,
  };

  /**
   * @param root  element containing rendered blocks (a page, or the builder's
   *              canvas -- the same function serves both, which is what makes
   *              the preview and the published page behave identically).
   * @param opts  {lang, data}  `data` maps block id -> resolved payload.
   */
  function hydrate(root, opts) {
    root = root || document;
    opts = opts || {};
    opts.lang = opts.lang || document.documentElement.lang || 'en';

    var slots = Array.prototype.slice.call(root.querySelectorAll('[data-hydrate]'));
    if (!slots.length) return;

    function run(slot) {
      if (slot.getAttribute('data-hydrated') === '1') return;
      slot.setAttribute('data-hydrated', '1');
      var fn = HYDRATORS[slot.getAttribute('data-hydrate')];
      if (!fn) return;
      var block = blockOf(slot);
      try {
        fn(slot, payloadFor(block, opts.data), opts);
      } catch (e) {
        // Same isolation rule the server-side renderer follows: one block
        // failing to hydrate must not stop the others on the page.
        if (block) block.setAttribute('data-state', 'error');
      }
    }

    if (!global.IntersectionObserver) {
      slots.forEach(run);   // no observer: correctness over laziness
      return;
    }
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (!entry.isIntersecting) return;
        io.unobserve(entry.target);
        run(entry.target);
      });
    }, { rootMargin: '200px' });   // start just before it is needed
    slots.forEach(function (s) { io.observe(s); });
  }

  global.BPBlocks = { hydrate: hydrate };
  if (typeof module !== 'undefined') module.exports = global.BPBlocks;
})(typeof window !== 'undefined' ? window : globalThis);
