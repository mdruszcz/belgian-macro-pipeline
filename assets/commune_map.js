/* The shared commune-map component.
 *
 * Block X asked for "one shared map component", and there are now two pages
 * that draw the same 565 communes: map.html, which maps one indicator's latest
 * value across the country, and communes.html, which maps whatever column of
 * its table you are looking at, IN THE YEAR YOU HAVE SELECTED. The second is
 * not a duplicate of the first -- communes.html holds the full history, so its
 * map can go back through time, which map.html's latest-value payloads cannot.
 *
 * Everything both pages need lives here so neither can drift from the other.
 * The classification in particular is not a styling detail: a choropleth is a
 * picture, and a picture of a wrong break table looks exactly as convincing as
 * a picture of a right one. Two copies of it would eventually disagree, and
 * the same commune would sit in different bands on two pages of the same site.
 *
 * MapUI.* is DOM-free and unit-tested under Node (tests/test_map_ui_logic.py).
 * MapUI.CommuneMap owns the SVG, and is given its elements rather than looking
 * them up, so one page can hold more than one map without them colliding.
 */

/* The shared interface strings, taken from the global that i18n.js defines.
   Read through one reference so this component holds no English of its own --
   a second copy of these sentences is how a translated page ends up half
   translated.

   NOT require()d as a fallback: the Node test harness concatenates these files
   and evaluates them, so a relative require would resolve against the working
   directory rather than this file. The harness prepends i18n.js instead, and
   MapUI.text degrades to the key if it is ever genuinely absent, so a missing
   strings file costs a label rather than taking the whole map down. */
const I18N_SRC = (typeof I18N !== 'undefined') ? I18N : null;

const MapUI = {};

/* One indirection, so a missing strings file degrades to the key rather than
   throwing and taking the whole map down. */
MapUI.text = function(lang, key, vars){
  return I18N_SRC ? I18N_SRC.t(lang || 'en', key, vars) : key;
};

MapUI.BINS = 7;          // matches the seven --ramp-* tokens in commune_map.css
MapUI.SWATCH_PX = 64;    // must match .swatches div in commune_map.css

/* --- projection ---------------------------------------------------------
   Equirectangular, with longitude scaled by cos(mean latitude) so Belgium is
   not stretched sideways. Over a country four degrees wide the error against
   a proper conic is smaller than the 50 m simplification already applied, so
   a heavier projection would be false precision. */
MapUI.meanLatitude = function(features){
  let sum = 0, n = 0;
  const walk = c => {
    if(typeof c[0] === 'number'){ sum += c[1]; n++; return; }
    c.forEach(walk);
  };
  features.forEach(f => walk(f.geometry.coordinates));
  return n ? sum / n : 0;
};

MapUI.lonScale = lat => Math.cos(lat * Math.PI / 180);

MapUI.geometryToPath = function(geom, lonScale){
  const point = (lon, la) => (lon * lonScale).toFixed(4) + ' ' + (-la).toFixed(4);
  const ring = r => {
    let out = '';
    for(let i = 0; i < r.length; i++) out += (i ? 'L' : 'M') + point(r[i][0], r[i][1]);
    return out + 'Z';
  };
  const polys = geom.type === 'Polygon' ? [geom.coordinates] : geom.coordinates;
  let d = '';
  for(const poly of polys) for(const r of poly) d += ring(r);
  return d;
};

/* --- formatting ---------------------------------------------------------
   Mirrors communes.html so the same number reads the same on both pages. */
/* FORMATTED FOR THE READER'S LANGUAGE, not the browser's locale. A French page
   read in a browser set to English would otherwise print "35,363" where a
   Belgian reader expects "35 363", and Dutch expects "35.363". Passing
   undefined to toLocaleString takes the browser's locale, which is the one
   thing on the page the reader did not choose. */
MapUI.formatValue = function(num, unit, decimals, lang){
  if(num === null || num === undefined || isNaN(num)) return '\u2014';
  const declared = !(decimals === null || decimals === undefined);
  const digits = declared ? decimals : (Math.abs(num) >= 1000 ? 0 : 2);
  /* A DECLARED number of decimals is how many the indicator is published to,
     so it is a minimum as well as a maximum: GDP growth of exactly 1.0 %
     belongs beside 0.5 % and 2.2 % as "1,0", not as "1". This is what
     src/pages/resolve.py's _format_value has always done for the static
     pages; the two mirrors disagreed until Batch 6. With no declared
     decimals the old guess stands, and a trailing "0,00" would be noise. */
  const body = num.toLocaleString(lang || undefined, declared
    ? {minimumFractionDigits: digits, maximumFractionDigits: digits}
    : {maximumFractionDigits: digits});
  const u = (unit || '').toLowerCase();
  if(u === 'eur') return '\u20ac' + body;
  if(u.startsWith('percent')) return body + '%';
  return body;
};

MapUI.tickLabel = function(num, unit, compactAxis, lang){
  const u = (unit || '').toLowerCase();
  const body = compactAxis
    ? num.toLocaleString(lang || undefined, {notation: 'compact', maximumFractionDigits: 1})
    : num.toLocaleString(lang || undefined, {maximumFractionDigits: Math.abs(num) < 100 ? 1 : 0});
  if(u === 'eur') return '\u20ac' + body;
  if(u.startsWith('percent')) return body + '%';
  return body;
};

MapUI.unitSuffix = function(unit){
  const u = (unit || '').toLowerCase();
  if(!u || u === 'count' || u === 'eur' || u.startsWith('percent')) return '';
  return ' ' + unit.replace(/_/g, ' ');
};

MapUI.pickName = function(names, fallback){
  if(!names) return fallback || '';
  return names.en || names.fr || names.nl || fallback || '';
};

/* --- classification -----------------------------------------------------
   Quantiles, not equal intervals. Belgian municipal figures are heavily
   skewed -- a handful of communes hold values many times the median, and an
   equal-interval scale puts 550 of them in the lightest band and shows
   nothing. Quantiles show rank, which is what the legend then says out loud.
   Ties are collapsed, so an indicator where half the communes share a value
   gets FEWER bands rather than empty ones that a reader would read as real. */
MapUI.quantileBreaks = function(sorted, bins){
  const min = sorted[0], max = sorted[sorted.length - 1];
  const breaks = [];
  for(let i = 1; i < bins; i++){
    const pos = (sorted.length - 1) * (i / bins);
    const lo = Math.floor(pos), hi = Math.ceil(pos);
    breaks.push(sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo));
  }
  // Kept only if strictly inside the data. A break sitting ON the minimum
  // makes the bottom band unreachable -- every commune, the smallest
  // included, lands above it -- so the map would draw a colour no commune
  // can have while giving the reader one fewer real distinction than the
  // legend claims.
  const unique = [];
  for(const b of breaks){
    if(b <= min || b > max) continue;
    if(!unique.length || b > unique[unique.length - 1]) unique.push(b);
  }
  return unique;
};

MapUI.equalIntervalBreaks = function(sorted, bins){
  const min = sorted[0], max = sorted[sorted.length - 1];
  const step = (max - min) / bins;
  const breaks = [];
  for(let i = 1; i < bins; i++){
    const b = min + step * i;
    if(b > min && b <= max && (!breaks.length || b > breaks[breaks.length - 1])) breaks.push(b);
  }
  return breaks;
};

/* Quantiles first, equal intervals only as a RESCUE.

   Quantiles collapse when most communes share one value -- the recorded-crime
   rates have long runs of zeros, and every quantile of such a series lands on
   the same number. Collapsing is the correct response to ties, but collapsing
   all the way to one or two bands paints 90% and 10% of the country in the
   same colour and shows the reader nothing that is there. Equal intervals do
   separate them, so the page switches method rather than draw a flat map, and
   says which method it used -- the two answer different questions and a reader
   comparing two maps has to know which one they are looking at. */
MapUI.MIN_USEFUL_BANDS = 3;

MapUI.classify = function(sorted, bins){
  if(!sorted.length) return {breaks: [], method: 'none'};
  if(sorted[0] === sorted[sorted.length - 1]) return {breaks: [], method: 'uniform'};
  const quantile = MapUI.quantileBreaks(sorted, bins);
  if(quantile.length + 1 >= MapUI.MIN_USEFUL_BANDS) return {breaks: quantile, method: 'quantile'};
  const equal = MapUI.equalIntervalBreaks(sorted, bins);
  if(equal.length > quantile.length) return {breaks: equal, method: 'equal'};
  return {breaks: quantile, method: 'quantile'};
};

MapUI.bandFor = function(value, breaks){
  let i = 0;
  while(i < breaks.length && value >= breaks[i]) i++;
  return i;
};

/* Spread whatever bands survived tie-collapsing across the full ramp, so a
   three-band indicator still runs light to dark instead of using three
   near-identical colours from one end of it. */
MapUI.colourIndex = function(band, bands, rampLength){
  if(bands <= 1) return rampLength - 1;
  return Math.round(band * (rampLength - 1) / (bands - 1));
};

/* The indicator to open with. Config first (the headline list local.html
   already drives its hero row from), then simply the first indicator -- so
   this page never names one itself. */
MapUI.defaultIndicator = function(requested, headlines, available){
  const known = new Set(available);
  if(requested && known.has(requested)) return requested;
  const headline = (headlines || []).find(h => known.has(h));
  return headline || available[0] || null;
};


/* ---------------------------------------------------------------------------
   The rendered map.

   Given a set of DOM elements and a boundary GeoJSON, draws 565 SVG paths and
   keeps them coloured. No mapping library and no tiles: a viewBox does
   everything panning and zooming need, with no third-party request from a page
   that publishes licensed public data.
--------------------------------------------------------------------------- */

MapUI.CommuneMap = class CommuneMap {
  /* `elements` carries the DOM nodes this instance owns:
       svg, tip, mapbox            -- required, the map itself
       swatches, ticks, legendNote -- optional, the legend
       coverage                    -- optional, the "no data" explanation
     `onSelect(nis)` is called for a click that was not the end of a pan;
     a page that should not navigate simply passes nothing. */
  constructor(elements, options = {}) {
    this.el = elements;
    this.onSelect = options.onSelect || null;
    this.lang = options.lang || 'en';
    this.features = [];
    this.byNis = {};
    this.values = {};
    this.meta = {};
    this.visible = null;   // null = every commune; otherwise a Set of nis
    this.view = null;
    this.home = null;
    this.drag = null;
    this.ramp = Array.from({length: MapUI.BINS}, (_, i) => `var(--ramp-${i})`);
    this._wire();
  }

  /* --- construction ------------------------------------------------------ */

  build(geojson) {
    const svg = this.el.svg;
    // Mean latitude first: the projection constant is derived from the data,
    // so no path can be built until it is known.
    const scale = MapUI.lonScale(MapUI.meanLatitude(geojson.features));

    const frag = document.createDocumentFragment();
    this.features = [];
    for (const f of geojson.features) {
      const el = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      el.setAttribute('d', MapUI.geometryToPath(f.geometry, scale));
      el.setAttribute('fill', 'var(--nodata)');
      el.dataset.nis = f.properties.nis;
      frag.appendChild(el);
      this.features.push({
        nis: f.properties.nis,
        name: f.properties.name_nl,
        name_fr: f.properties.name_fr,
        el,
      });
    }
    svg.appendChild(frag);

    // Fitted to what was actually drawn rather than to a hardcoded bounding
    // box for Belgium, so a new boundary vintage still frames itself.
    const box = svg.getBBox();
    const pad = Math.max(box.width, box.height) * 0.02;
    this.home = {x: box.x - pad, y: box.y - pad, w: box.width + pad * 2, h: box.height + pad * 2};
    svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');
    svg.style.aspectRatio = (this.home.w / this.home.h).toFixed(4);
    this.resetView();

    this.byNis = Object.fromEntries(this.features.map(f => [f.nis, f]));
    return this;
  }

  /* --- data -------------------------------------------------------------- */

  /* `values` is nis -> {value, period, status}; `meta` describes the indicator
     (unit, decimals, direction, and a `name` for messages). Both pages build
     these from their own payloads, which is the only part that differs. */
  setData(values, meta) {
    this.values = values || {};
    this.meta = meta || {};
    this.paint();
    return this;
  }

  /* Restrict the map to a subset -- communes.html's region filter and search
     box. Passing null restores every commune. */
  /* Re-language in place. Cheaper and less jarring than rebuilding: the paths
     and the view stay exactly as the reader left them, only the words change. */
  setLang(lang) {
    this.lang = lang || 'en';
    this.paint();
    return this;
  }

  setVisible(nisSet) {
    this.visible = nisSet;
    this.paint();
    return this;
  }

  _shown() {
    return this.visible
      ? this.features.filter(f => this.visible.has(f.nis))
      : this.features;
  }

  paint() {
    const shown = this._shown();
    const shownSet = this.visible;

    const nums = [];
    let withheld = 0;
    for (const f of shown) {
      const row = this.values[f.nis];
      if (row && typeof row.value === 'number') nums.push(row.value);
      else if (row && row.value === null && row.status === 'suppressed') withheld++;
    }
    nums.sort((a, b) => a - b);

    const {breaks, method} = MapUI.classify(nums, MapUI.BINS);
    const bands = breaks.length + 1;
    const colourFor = band => this.ramp[MapUI.colourIndex(band, bands, this.ramp.length)];

    let withValue = 0;
    for (const f of this.features) {
      const hidden = shownSet && !shownSet.has(f.nis);
      f.el.classList.toggle('filtered-out', Boolean(hidden));
      const row = hidden ? null : this.values[f.nis];
      if (row && typeof row.value === 'number') {
        f.el.setAttribute('fill', colourFor(MapUI.bandFor(row.value, breaks)));
        withValue++;
      } else {
        f.el.setAttribute('fill', 'var(--nodata)');
      }
    }

    this.method = method;
    this.withheld = withheld;
    this._drawLegend(breaks, bands, colourFor, nums);
    this._reportCoverage(withValue, shown.length, withheld);
    return this;
  }

  /* --- legend ------------------------------------------------------------ */

  _drawLegend(breaks, bands, colourFor, nums) {
    // Each of the three is INDEPENDENTLY optional, which is what the
    // constructor's own docstring has always claimed. It used to bail on the
    // whole legend unless all three were supplied, so a caller that wanted
    // the colour bar without the several-sentence prose note -- a compact
    // card, where that note swamps the card -- silently got no legend at all.
    const {swatches, ticks, legendNote} = this.el;
    if (!swatches && !ticks && !legendNote) return;

    if (swatches) swatches.innerHTML = '';
    if (ticks) ticks.innerHTML = '';
    if (!nums.length) {
      if (legendNote) legendNote.textContent = MapUI.text(this.lang, 'mapNoneCarryValue');
      return;
    }

    if (swatches) {
      for (let i = 0; i < bands; i++) {
        const cell = document.createElement('div');
        cell.style.background = colourFor(i);
        swatches.appendChild(cell);
      }
    }

    // One notation for the whole axis, decided from its largest value, so the
    // ticks do not mix "8,302" with "11.5K".
    const compactAxis = Math.abs(nums[nums.length - 1]) >= 10000;
    // A tick per INTERNAL boundary, at the seam between its two swatches. The
    // ends carry none: the lowest and highest are written out in the note,
    // where they have room to be exact rather than compacted.
    if (ticks) {
      ticks.style.width = (bands * MapUI.SWATCH_PX) + 'px';
      for (let i = 0; i < breaks.length; i++) {
        const span = document.createElement('span');
        span.textContent = MapUI.tickLabel(breaks[i], this.meta.unit, compactAxis, this.lang);
        span.style.left = ((i + 1) * MapUI.SWATCH_PX) + 'px';
        ticks.appendChild(span);
      }
    }

    const direction = {
      higher_is_better: MapUI.text(this.lang, 'mapDirHigher'),
      lower_is_better: MapUI.text(this.lang, 'mapDirLower'),
      contextual: MapUI.text(this.lang, 'mapDirContextual'),
    }[this.meta.direction] || '';

    const exact = v => MapUI.formatValue(v, this.meta.unit, this.meta.decimals, this.lang) +
                       MapUI.unitSuffix(this.meta.unit);
    const methodNote = MapUI.text(
      this.lang, this.method === 'equal' ? 'mapMethodEqual' : 'mapMethodQuantile');
    // Said out loud because it changes what a colour MEANS: with a filter on,
    // the bands rank the communes shown, not all 565, so the same commune can
    // be dark here and pale on the unfiltered map. A reader comparing two
    // screenshots has to be told that.
    const basis = this.visible ? ' ' + MapUI.text(this.lang, 'mapBasisFiltered') : '';

    if (legendNote) {
      legendNote.textContent =
        MapUI.text(this.lang, 'mapRange', {lo: exact(nums[0]), hi: exact(nums[nums.length - 1])}) +
        ' ' + MapUI.text(this.lang, 'mapColourRuns') + ' ' +
        methodNote + basis + (direction ? ' ' + direction : '');
    }
  }

  _reportCoverage(withValue, total, withheld) {
    const box = this.el.coverage;
    if (!box) return;
    const missing = total - withValue;
    if (missing === 0) {
      box.classList.add('map-hidden');
      box.textContent = '';
      return;
    }
    box.classList.remove('map-hidden');
    /* This sentence used to describe withholding in prose while the data it
       received could not distinguish it. It can now, so the two are counted
       separately -- "the source masked 152 communes" and "13 were never
       measured" are different facts about an indicator, and lumping them
       together as one number was the vaguer half of the old wording. */
    const uncollected = missing - (withheld || 0);
    let text = MapUI.text(this.lang, 'mapMissingLead', {n: missing, total}) + ' ';
    if (withheld) {
      text += MapUI.text(this.lang, 'mapMissingWithheld', {n: withheld}) + ' ';
    }
    if (uncollected > 0) {
      text += MapUI.text(this.lang, 'mapMissingUncollected', {n: uncollected});
    }
    box.textContent = text.trim();
  }

  coverage() {
    const shown = this._shown();
    const withValue = shown.filter(f => {
      const row = this.values[f.nis];
      return row && typeof row.value === 'number';
    }).length;
    return {shown: shown.length, withValue, total: this.features.length};
  }

  /* --- view -------------------------------------------------------------- */

  _setView(v) {
    this.view = v;
    this.el.svg.setAttribute('viewBox', `${v.x} ${v.y} ${v.w} ${v.h}`);
  }

  resetView() {
    this._setView({...this.home});
    return this;
  }

  zoomBy(factor, cx, cy) {
    const v = this.view;
    const nx = cx === undefined ? v.x + v.w / 2 : cx;
    const ny = cy === undefined ? v.y + v.h / 2 : cy;
    let w = v.w * factor, h = v.h * factor;
    if (w > this.home.w || h > this.home.h) { this.resetView(); return this; }
    const minW = this.home.w / 60;
    if (w < minW) { w = minW; h = minW * (this.home.h / this.home.w); }
    this._setView({
      x: nx - (nx - v.x) * (w / v.w),
      y: ny - (ny - v.y) * (h / v.h),
      w, h,
    });
    return this;
  }

  /* Mark one commune WITHOUT moving the view -- a locator, not a search
     result. focus() frames the commune, because framing is what a search
     wants; a profile header wants the country with its own commune picked out
     of it. Same class, so the two cannot look like different things. */
  locate(nis) {
    const f = this.byNis[nis];
    if (!f) return false;
    this.el.svg.querySelectorAll('path.hit').forEach(p => p.classList.remove('hit'));
    f.el.classList.add('hit');
    return true;
  }

  /* Frame a SET of communes -- a commune and its neighbours. The union of
     their boxes, padded, so the group fills the map with a margin; nothing is
     outlined here, since which of them is the subject is the caller's own
     styling decision (commune.html marks it with locate-me). */
  frame(nisList) {
    const boxes = nisList.map(n => this.byNis[n]).filter(Boolean).map(f => f.el.getBBox());
    if (!boxes.length) return false;
    const x0 = Math.min(...boxes.map(b => b.x)), y0 = Math.min(...boxes.map(b => b.y));
    const x1 = Math.max(...boxes.map(b => b.x + b.width)), y1 = Math.max(...boxes.map(b => b.y + b.height));
    const pad = Math.max(x1 - x0, y1 - y0) * 0.08;
    this._setView({x: x0 - pad, y: y0 - pad, w: x1 - x0 + pad * 2, h: y1 - y0 + pad * 2});
    return true;
  }

  /* Frame one commune and outline it -- what a search result should do. */
  focus(nis) {
    const f = this.byNis[nis];
    if (!f) return false;
    this.el.svg.querySelectorAll('path.hit').forEach(p => p.classList.remove('hit'));
    f.el.classList.add('hit');
    const box = f.el.getBBox();
    const pad = Math.max(box.width, box.height) * 1.2;
    this._setView({x: box.x - pad, y: box.y - pad, w: box.width + pad * 2, h: box.height + pad * 2});
    return true;
  }

  /* --- interaction ------------------------------------------------------- */

  _tipHtml(f) {
    const row = this.values[f.nis];
    const name = f.name === f.name_fr ? f.name : `${f.name} / ${f.name_fr}`;
    /* THREE STATES, NOT TWO. A commune with no figure is either one the
       source WITHHELD (ONEM masks any count below 10 for privacy) or one
       never measured. Both are drawn in the no-data colour because neither
       can be placed on a scale, but they are different facts and the reader
       has to be able to tell which they are looking at. */
    if (row && row.value === null && row.status === 'suppressed') {
      return `<span class="n">${name}</span>` +
             `<span class="m">${MapUI.text(this.lang, 'mapWithheld')}` +
             `${row.period ? ' · ' + row.period : ''}</span>`;
    }
    if (!row || typeof row.value !== 'number') {
      return `<span class="n">${name}</span>` +
             `<span class="m">${MapUI.text(this.lang, 'mapNoValueHere')}</span>`;
    }
    const status = row.status && row.status !== 'final' ? ` · ${row.status}` : '';
    return `<span class="n">${name}</span>` +
           `<span class="v">${MapUI.formatValue(row.value, this.meta.unit, this.meta.decimals, this.lang)}` +
           `${MapUI.unitSuffix(this.meta.unit)}</span>` +
           `<span class="m">${row.period || ''}${status}</span>`;
  }

  _wire() {
    const {svg, tip, mapbox} = this.el;

    svg.addEventListener('mousemove', e => {
      const nis = e.target && e.target.dataset ? e.target.dataset.nis : null;
      const f = nis ? this.byNis[nis] : null;
      if (!f || (this.visible && !this.visible.has(nis))) { tip.classList.add('map-hidden'); return; }
      tip.innerHTML = this._tipHtml(f);
      tip.classList.remove('map-hidden');
      const box = mapbox.getBoundingClientRect();
      let x = e.clientX - box.left + 14, y = e.clientY - box.top + 14;
      if (x + tip.offsetWidth > box.width) x = e.clientX - box.left - tip.offsetWidth - 14;
      if (y + tip.offsetHeight > box.height) y = e.clientY - box.top - tip.offsetHeight - 14;
      tip.style.left = x + 'px';
      tip.style.top = y + 'px';
    });
    svg.addEventListener('mouseleave', () => tip.classList.add('map-hidden'));

    svg.addEventListener('click', e => {
      if (this.drag && this.drag.panning) return;   // finishing a pan is not a click
      const nis = e.target && e.target.dataset ? e.target.dataset.nis : null;
      if (nis && this.onSelect) this.onSelect(nis);
    });

    svg.addEventListener('wheel', e => {
      e.preventDefault();
      const p = this._svgPoint(e);
      this.zoomBy(e.deltaY > 0 ? 1.2 : 1 / 1.2, p.x, p.y);
    }, {passive: false});

    /* Pointer capture is taken only ONCE THE POINTER HAS ACTUALLY MOVED.
       Capturing it on pointerdown retargets every later event to the <svg>,
       which silently breaks both the hover tooltip and the click-through --
       e.target is then the SVG and carries no data-nis. The threshold is also
       what separates a click from a drag, so a shaky click still opens the
       commune rather than nudging the map. */
    svg.addEventListener('pointerdown', e => {
      if (e.button !== 0) return;
      this.drag = {
        origin: {x: e.clientX, y: e.clientY},
        p: this._svgPoint(e),
        v: {...this.view},
        panning: false,
      };
    });

    svg.addEventListener('pointermove', e => {
      if (!this.drag) return;
      if (!this.drag.panning) {
        const far = Math.hypot(e.clientX - this.drag.origin.x, e.clientY - this.drag.origin.y);
        if (far < MapUI.DRAG_THRESHOLD_PX) return;
        this.drag.panning = true;
        svg.classList.add('dragging');
        svg.setPointerCapture(e.pointerId);
        tip.classList.add('map-hidden');
      }
      const r = svg.getBoundingClientRect();
      const dx = (e.clientX - r.left) / r.width * this.drag.v.w;
      const dy = (e.clientY - r.top) / r.height * this.drag.v.h;
      this._setView({x: this.drag.p.x - dx, y: this.drag.p.y - dy, w: this.drag.v.w, h: this.drag.v.h});
    });

    const endDrag = e => {
      if (!this.drag) return;
      if (this.drag.panning) {
        svg.classList.remove('dragging');
        if (e && e.pointerId !== undefined && svg.hasPointerCapture(e.pointerId)) {
          svg.releasePointerCapture(e.pointerId);
        }
      }
      this.drag = null;
    };
    svg.addEventListener('pointerup', endDrag);
    svg.addEventListener('pointercancel', endDrag);
  }

  _svgPoint(e) {
    const r = this.el.svg.getBoundingClientRect(), v = this.view;
    return {
      x: v.x + (e.clientX - r.left) / r.width * v.w,
      y: v.y + (e.clientY - r.top) / r.height * v.h,
    };
  }
};

MapUI.DRAG_THRESHOLD_PX = 4;

if (typeof module !== 'undefined') module.exports = MapUI;
