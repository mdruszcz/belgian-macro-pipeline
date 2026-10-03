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
  /* Euro placement is LANGUAGE-specific, not just a symbol substitution:
     English reads a currency symbol before the number ("\u20ac40,125.7"), French
     and Dutch both read the symbol AFTER the number with a non-breaking
     space ("40 125,7 \u20ac") -- this is Belgian typographic convention for
     both languages alike, not an EN-vs-non-EN split. commune.html's own
     fmt() has always post-processed MapUI.formatValue()'s English-style
     output this same way for fr/nl (stripping a leading '\u20ac' and
     re-appending 'number NBSP \u20ac'); this was the ONLY caller doing that
     work, so a page that calls MapUI.formatValue() directly (micro.html's
     fmtValue()) printed the English form even on the fr/nl page. Built
     here instead, once, so every caller gets it for free -- and
     commune.html's fmt() below no longer needs to redo it. U+202F is a
     NARROW no-break space, matching commune.html's own NNBSP constant. */
  const NNBSP = '\u202f';
  const euro = function(withBody){ return (lang === 'fr' || lang === 'nl') ? (withBody + NNBSP + '\u20ac') : ('\u20ac' + withBody); };
  if(u === 'eur') return euro(body);
  /* Euros PER INHABITANT (WalStat's municipal accounts), written as a rate so
     it can never be read as a total: Namur's 2 319,7 is what the commune
     raises per resident, not its budget. Mirrored in src/pages/resolve.py.
     fr/nl: the '/hab.' rate suffix sits directly against the euro sign, no
     extra space before the slash -- "2 319,7 \u20ac/hab." -- matching
     commune.html's own fmt() (which builds this by slicing its raw
     "\u20ac2,319.7/hab." apart at the '/' and reassembling), not the
     "number NBSP \u20ac" + " / hab." shape a naive concatenation would give. */
  if(u === 'eur_per_inhabitant') return (lang === 'fr' || lang === 'nl') ? (body + NNBSP + '\u20ac/hab.') : ('\u20ac' + body + '\u202f/\u202fhab.');
  /* Euros PER MONTH (SPF Finances housing-leases batch: median rent and
     charges on new residential leases), same on-number rate treatment as
     eur_per_inhabitant above. Mirrored in src/pages/resolve.py. Dutch's
     rate word is 'maand', not the English 'mo.' abbreviation -- matching
     commune.html's fmt(), which picks the word by LANG the same way. */
  if(u === 'eur_per_month') return (lang === 'fr' || lang === 'nl') ? (body + NNBSP + '\u20ac/' + (lang === 'nl' ? 'maand' : 'mois')) : ('\u20ac' + body + '\u202f/\u202fmo.');
  // pct_of_gdp (Europe countries batch, docs/features/europe_countries.md,
  // GOV_DEBT_EUROPE): deliberately not spelled "percent*" in the indicator
  // config (percent_bounded validation fails above 100, and several
  // countries' debt ratios exceed it), but it is still a percentage for
  // display purposes -- same "%" appended to the number.
  if(u.startsWith('percent') || u === 'pct_of_gdp') return body + '%';
  return body;
};

MapUI.tickLabel = function(num, unit, compactAxis, lang){
  const u = (unit || '').toLowerCase();
  const body = compactAxis
    ? num.toLocaleString(lang || undefined, {notation: 'compact', maximumFractionDigits: 1})
    : num.toLocaleString(lang || undefined, {maximumFractionDigits: Math.abs(num) < 100 ? 1 : 0});
  if(u === 'eur' || u === 'eur_per_inhabitant') return (lang === 'fr' || lang === 'nl') ? (body + '\u202f\u20ac') : ('\u20ac' + body);
  if(u.startsWith('percent')) return body + '%';
  return body;
};

MapUI.unitSuffix = function(unit, lang){
  const u = (unit || '').toLowerCase();
  if(!u || u === 'count' || u === 'eur' || u === 'eur_per_inhabitant' || u.startsWith('percent')) return '';
  // Delegates the actual wording to unitLabel() below -- ONE unit
  // vocabulary, not two, and a trilingual one now that a caller (the
  // Europe NUTS 2 map, Batch B3) needs "pps_per_inhabitant"/"persons"
  // read in the reader's own language rather than the raw English code
  // with underscores turned to spaces. `lang` is optional and defaults
  // through unitLabel/I18N.t to English, so every existing call site that
  // never passed one keeps behaving exactly as before.
  const label = MapUI.unitLabel(unit, lang);
  return label ? ' ' + label : '';
};

/* A unit AS ITS OWN WORD, independent of any number beside it -- "%", "pp",
   "2021=100", the translated word for a balance, or (for anything with no
   shorter reading) the raw code with its underscores turned to spaces, so a
   reader never sees an internal unit code like "percent_yy" (claude.md rule
   7). Batch A1.3 (docs/features/site_unification.md): moved out of
   macro.html's own unitHint(), which duplicated this for its KPI-card
   hints -- ONE unit vocabulary, used both there and by a chart tooltip/
   subtitle that has no number to hang the symbol on.

   Distinct from unitSuffix() above, which is a suffix APPENDED AFTER an
   already-formatted number and stays silent for percent/eur/count because
   formatValue() already put their symbol ON the number -- showing "%" again
   here, standing alone, is not that redundancy. */
MapUI.unitLabel = function(unit, lang){
  const u = (unit || '').toLowerCase();
  if(!u || u === 'count') return '';
  if(u === 'pp_contribution') return 'pp';
  // The Europe countries batch's GDP_VOLUME_EUROPE index carries an
  // explicit base VALUE in its unit code too ("index_2015_100", always
  // rebased to 100 -- see export_europe_countries.py's
  // _rebase_to_2015_index), not just the base year every earlier
  // "index_YYYY" unit used -- the trailing "_100" is optional so both
  // spellings read the same way.
  const idx = /^index_(\d{4})(?:_100)?$/.exec(u);
  if(idx) return idx[1] + '=100';
  if(u === 'balance') return MapUI.text(lang, 'macroBalance');
  if(u === 'eur') return '€';
  if(u === 'eur_per_inhabitant') return '€ / hab.';
  if(u.startsWith('percent')) return '%';
  // Batch B3 (docs/features/europe_nuts2.md): the two Eurostat NUTS 2
  // units with no earlier entry here. Trilingual per CLAUDE.md rule 7,
  // added HERE (the shared vocabulary) rather than in the Europe map's
  // own file, so a second map reading the same unit code never has to
  // duplicate the wording.
  if(u === 'pps_per_inhabitant') return MapUI.text(lang, 'unitPpsPerInhabitant');
  if(u === 'persons') return MapUI.text(lang, 'unitPersons');
  // Europe countries batch (docs/features/europe_countries.md):
  // GOV_DEBT_EUROPE's "pct_of_gdp" (see formatValue's own comment for why
  // it cannot be spelled "percent*"). unitSuffix() does not special-case
  // it, so this word appears once, in the meta line ("... (% of GDP) --
  // 2023"), while formatValue keeps printing a plain "%" on the number
  // itself -- no double percent sign.
  if(u === 'pct_of_gdp') return MapUI.text(lang, 'unitPctOfGdp');
  // Eurostat additional domains batch (docs/data_catalog.md, 2026-09-15),
  // wired into the Europe panel: the unit codes those indicator configs
  // introduced, trilingual here for the same reason as the NUTS 2 ones
  // above. index_0_100 (the Gini coefficient) is a bounded score, not a
  // "base year = 100" index, so it deliberately does not go through the
  // index_YYYY regex above.
  if(u === 'thousand_persons') return MapUI.text(lang, 'unitThousandPersons');
  if(u === 'kt_co2eq') return MapUI.text(lang, 'unitKtCo2eq');
  if(u === 'fte') return MapUI.text(lang, 'unitFte');
  if(u === 'per_mille') return '‰';
  if(u === 'years') return MapUI.text(lang, 'unitYears');
  // Indicator definitions batch: an average number of persons per
  // household, e.g. 2.14 -- not an integer count, so 'count' (which implies
  // a whole, non-negative tally -- see counts_non_negative in
  // src/validation/rules.py) was the wrong unit for a household-size-style
  // average. No existing entry in this vocabulary fit "a ratio of persons
  // to households", so this is a new one, formatted through the plain
  // numeric path in formatValue() (no currency-style prefix/suffix on the
  // number itself) with only its label added here, the same as 'years' or
  // 'fte' above.
  if(u === 'persons_per_household') return MapUI.text(lang, 'unitPersonsPerHousehold');
  if(u === 'meur_clv2010') return MapUI.text(lang, 'unitMeurClv2010');
  // meur (public-finance batch, docs/features/public_finance_live.md): a
  // million-euro LEVEL at current prices -- unlike meur_clv2010's 2010-
  // volumes measure, this is nominal. Same plain-number treatment as
  // meur_clv2010 in formatValue() above (no inline currency symbol, no
  // percent branch): only the label differs. CLAUDE.md rule 41, a level is
  // never a count.
  if(u === 'meur') return MapUI.text(lang, 'unitMeur');
  if(u === 'index_0_100') return MapUI.text(lang, 'unitIndex0100');
  return unit.replace(/_/g, ' ');
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
    // PALETTE, optional. Every existing caller (map.html, communes.html,
    // home.html, home2.html, profiles.html, commune.html) passes nothing and
    // gets exactly the var(--ramp-N) lookup this always did -- the CSS
    // variables live in commune_map.css and each page/theme already sets
    // them. A caller that supplies `options.palette` (an array of CSS colour
    // strings, light-to-dark, "more" at the high end) draws with those
    // instead -- used by commune.html's own Portrait ink/slate ramp so it
    // does not have to fork the shared component to change its look. Length
    // need not be 7: colourIndex() below already spreads however many bands
    // survive across whatever ramp it is given.
    this.ramp = (options.palette && options.palette.length)
      ? options.palette.slice()
      : Array.from({length: MapUI.BINS}, (_, i) => `var(--ramp-${i})`);
    // DIVERGING PALETTE, optional -- for signed indicators (a balance, or any
    // value range that genuinely crosses zero) where "more" has no single
    // direction: {neg: [...], zero: '#fff', pos: [...]}, negative values
    // classified on their own quantiles below zero, positive above, zero
    // itself always the given neutral colour. Undeclared (the default),
    // every caller keeps the single sequential ramp above, unchanged.
    this.divergingPalette = options.divergingPalette || null;
    this.nodataColour = options.nodataColour || null; // null = var(--nodata), unchanged
    this.strokeColour = options.strokeColour || null; // null = var(--map-stroke) via CSS, unchanged
    // HOW MANY CLASSES, AND WHERE THEY CUT. Defaults to the seven the ramp has
    // colours for and to the component's own choice of where to cut them, so a
    // caller that says nothing behaves exactly as before. A caller may ask for
    // a different NUMBER (setClassification({bins})) or dictate the cuts
    // outright (setClassification({breaks})). colourIndex already spreads
    // however many bands survive across the whole ramp, so fewer classes need
    // no second palette.
    this.bins = options.bins || MapUI.BINS;
    this.manualBreaks = null;
    this.breaks = [];
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

  /* Ask for a different number of classes, or for cuts of your own. Passing
     {breaks: null} returns to the component's own choice. Repaints, because
     the question "how many classes" has no answer that is not a picture. */
  setClassification({bins, breaks} = {}) {
    if (bins) this.bins = bins;
    if (breaks !== undefined) this.manualBreaks = breaks && breaks.length ? breaks.slice() : null;
    this.paint();
    return this;
  }

  /* Swap the palette in place and repaint -- for a caller whose palette is
     derived from the page's own light/dark theme (a JS colour array, not a
     CSS var, so it does not repaint itself the way the shared var(--ramp-N)
     default already does on a theme switch). Every field is optional and
     only replaces what is given, so a caller can pass just the one thing
     that changed. */
  setPalette({palette, divergingPalette, nodataColour, strokeColour} = {}) {
    if (palette) this.ramp = palette.slice();
    if (divergingPalette !== undefined) this.divergingPalette = divergingPalette;
    if (nodataColour !== undefined) this.nodataColour = nodataColour;
    if (strokeColour !== undefined) this.strokeColour = strokeColour;
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

    // Diverging path: only taken when the caller supplied a divergingPalette
    // AND the data genuinely straddles zero (a balance can still be all
    // positive or all negative for one indicator's current period, in which
    // case the ordinary single-hue ramp already tells the right story and
    // there is no "negative side" to give a different hue). Every caller
    // that never passes divergingPalette skips this block entirely -- same
    // classify()/colourFor() path as before.
    const isDiverging = this.divergingPalette && nums.length && nums[0] < 0 && nums[nums.length - 1] > 0;

    let breaks, method, colourFor;
    if (isDiverging) {
      const neg = nums.filter(v => v < 0).sort((a, b) => a - b);
      const pos = nums.filter(v => v > 0).sort((a, b) => a - b);
      const negBins = Math.max(1, Math.round(this.bins / 2));
      const posBins = Math.max(1, this.bins - negBins);
      const negBreaks = neg.length ? MapUI.quantileBreaks(neg, negBins) : [];
      const posBreaks = pos.length ? MapUI.quantileBreaks(pos, posBins) : [];
      // One combined breaks array crossing zero, so bandFor()'s existing
      // half-open-upward rule still works unmodified: everything <0 falls
      // into a "neg" band, exactly 0 its own band, everything >0 a "pos" band.
      breaks = negBreaks.concat([0]).concat(posBreaks);
      method = 'diverging';
      const negRamp = this.divergingPalette.neg || [];
      const posRamp = this.divergingPalette.pos || [];
      const zeroColour = this.divergingPalette.zero || negRamp[negRamp.length - 1] || '#fff';
      const zeroBandIndex = negBreaks.length; // the band whose lower edge is 0
      colourFor = band => {
        if (band === zeroBandIndex) return zeroColour;
        if (band < zeroBandIndex) {
          // darkest (most negative) at band 0, lightest near zero
          const i = MapUI.colourIndex(negBreaks.length - band, negBreaks.length + 1, negRamp.length);
          return negRamp[Math.min(negRamp.length - 1, Math.max(0, i))];
        }
        const posBand = band - zeroBandIndex - 1; // 0-based within the positive side
        const posBandCount = posBreaks.length + 1;
        const i = MapUI.colourIndex(posBand, posBandCount, posRamp.length);
        return posRamp[Math.min(posRamp.length - 1, Math.max(0, i))];
      };
    } else if (this.manualBreaks && this.manualBreaks.length && nums.length) {
      // Manual cuts are used AS GIVEN -- that is what manual means -- but only
      // the ones that fall inside the data, because a break above the maximum
      // would print a class in the legend that no commune can ever be in.
      breaks = this.manualBreaks
        .filter(v => v > nums[0] && v <= nums[nums.length - 1])
        .sort((a, b) => a - b);
      method = 'manual';
      const bands = breaks.length + 1;
      colourFor = band => this.ramp[MapUI.colourIndex(band, bands, this.ramp.length)];
    } else {
      ({breaks, method} = MapUI.classify(nums, this.bins));
      const bands = breaks.length + 1;
      colourFor = band => this.ramp[MapUI.colourIndex(band, bands, this.ramp.length)];
    }
    const bands = breaks.length + 1;

    const nodataFill = this.nodataColour || 'var(--nodata)';
    let withValue = 0;
    for (const f of this.features) {
      const hidden = shownSet && !shownSet.has(f.nis);
      f.el.classList.toggle('filtered-out', Boolean(hidden));
      const row = hidden ? null : this.values[f.nis];
      if (row && typeof row.value === 'number') {
        f.el.setAttribute('fill', colourFor(MapUI.bandFor(row.value, breaks)));
        withValue++;
      } else {
        f.el.setAttribute('fill', nodataFill);
      }
    }
    if (this.strokeColour) {
      for (const f of this.features) f.el.setAttribute('stroke', this.strokeColour);
    }

    this.method = method;
    this.breaks = breaks;
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
    const {swatches, ticks, legendNote, legendRows} = this.el;
    if (!swatches && !ticks && !legendNote && !legendRows) return;

    if (swatches) swatches.innerHTML = '';
    if (ticks) ticks.innerHTML = '';
    if (legendRows) legendRows.innerHTML = '';
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

    // THE SAME BANDS, READ DOWN INSTEAD OF ACROSS. A caller that supplies a
    // `legendRows` element gets one row per band -- swatch plus the range that
    // band actually covers -- instead of (or as well as) the colour bar. It is
    // built HERE, from the very `breaks` and `colourFor` that just painted the
    // map, because the alternative was for a page to re-run MapUI.classify on
    // its own copy of the numbers: a second implementation of exactly the
    // thing one shared component exists to prevent, and one that would drift
    // silently the first time the banding rule changed. Every other caller
    // passes no such element and is unaffected.
    if (legendRows) {
      const edge = v => MapUI.formatValue(v, this.meta.unit, this.meta.decimals, this.lang);
      for (let i = 0; i < bands; i++) {
        const row = document.createElement('div');
        row.className = 'legend-row';
        const chip = document.createElement('i');
        chip.style.background = colourFor(i);
        const label = document.createElement('span');
        // Half-open upwards, matching MapUI.bandFor, which is inclusive at the
        // lower edge: a value exactly on a break belongs to the band ABOVE it.
        if (bands === 1) label.textContent = edge(nums[0]);
        else if (i === 0) label.textContent = '< ' + edge(breaks[0]);
        else if (i === bands - 1) label.textContent = '≥ ' + edge(breaks[breaks.length - 1]);
        else label.textContent = edge(breaks[i - 1]) + ' – < ' + edge(breaks[i]);
        row.appendChild(chip);
        row.appendChild(label);
        legendRows.appendChild(row);
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
    const methodNote = MapUI.text(this.lang, {
      equal: 'mapMethodEqual',
      manual: 'mapMethodManual',
    }[this.method] || 'mapMethodQuantile');
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
