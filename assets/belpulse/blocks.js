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

  // One in-flight promise for the boundary file, shared by every map block on
  // the page. Two maps scrolling into view together must not start two 1.2 MB
  // downloads.
  var geoPromise = null;
  function boundaries() {
    if (!geoPromise) {
      geoPromise = fetch(GEO_URL).then(function (r) {
        if (!r.ok) throw new Error('boundaries unavailable');
        return r.json();
      });
    }
    return geoPromise;
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

  function hydrateChart(slot, data, opts) {
    var canvas = slot.querySelector('canvas');
    if (!canvas || !global.BPCharts) return;
    var points = data && data.points;
    if (!points || points.length < 2) return;   // never a one-point "trend"
    var kind = slot.getAttribute('data-chart-type') || 'line';
    var series = [{ label: (data && data.label) || '', points: points }];
    if (kind === 'bar') {
      BPCharts.drawBar(canvas, points.map(function (p) {
        return { label: p.period, value: p.value };
      }), { locale: opts.lang });
    } else {
      BPCharts.drawLine(canvas, series, { locale: opts.lang });
    }
  }

  function hydrateMap(slot, data, opts) {
    if (!global.MapUI) return;
    var svg = slot.querySelector('svg');
    var box = slot.querySelector('.mapbox');
    if (!svg || !box) return;
    return boundaries().then(function (geojson) {
      var map = new MapUI.CommuneMap({
        svg: svg,
        mapbox: box,
        tip: slot.querySelector('.tip'),
        swatches: slot.querySelector('.swatches'),
        ticks: slot.querySelector('.ticks'),
      }, { lang: opts.lang }).build(geojson);
      if (data && data.values) {
        map.setData(data.values, {
          unit: data.unit, decimals: data.decimals, direction: data.direction,
        });
      }
      return map;
    }).catch(function () {
      var block = blockOf(slot);
      if (block) block.setAttribute('data-state', 'error');
    });
  }

  var HYDRATORS = { chart: hydrateChart, map: hydrateMap };

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
