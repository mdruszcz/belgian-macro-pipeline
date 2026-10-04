/* BPLiveCounters -- the simulated "live" public-finance counters engine.
 *
 * Mockup batch: public-finance-live. Written production-grade because PR 2
 * moves this file into the real site almost as-is (belpulse-lead handoff).
 *
 * This module knows NOTHING about which counter is which. No indicator id,
 * no counter id and no figure is ever written here (CLAUDE.md rules 2, 24,
 * 36) -- it only knows the payload SHAPE produced by
 * scripts/export_live_counters.py against docs/features/
 * live_counters.schema.json: a counter is a {id, label, unit, state,
 * segments[]} object, a segment is {start_ms, end_ms, v0, v1, rate_per_ms},
 * and v(t) on a segment is v0 + rate_per_ms*(t - start_ms). All arithmetic
 * is linear interpolation of numbers the Python pipeline already computed;
 * the browser computes no statistic of its own.
 *
 * API:
 *   BPLiveCounters.load(url)              -> Promise<payload|null>
 *   BPLiveCounters.valueAt(segments, t)   -> {state, value, ratePerMs}
 *   BPLiveCounters.mount(container, payload, opts) -> {setLang, pause, resume, destroy}
 *
 * Timing: ONE module-level scheduler (a setTimeout re-armed each time,
 * aligned to the next whole second). No setInterval anywhere in this file.
 * The scheduler stops entirely while document.hidden, and each mounted
 * region additionally stops updating itself (independent of the scheduler)
 * while it is scrolled off-screen (IntersectionObserver) or while its own
 * Pause button is pressed.
 */
(function(global){
  'use strict';

  var NNBSP = ' ';

  // ---- payload loading -----------------------------------------------------

  /* The one place "is this payload usable" is decided -- reused by load()
   * below and by any caller that already fetched the JSON itself (e.g. as
   * part of a page's own Promise.all of several payloads) and does not
   * want a second network round trip just to get this same check. */
  function validate(payload){
    if(!payload) return null;
    if(payload.schema_version !== 1) return null;
    if(payload.simulated !== true) return null;
    return payload;
  }

  function load(url){
    return fetch(url)
      .then(function(r){ return r.ok ? r.json() : null; })
      .then(validate)
      .catch(function(){ return null; });
  }

  // ---- pure segment math ----------------------------------------------------

  /* overlap of two closed-open intervals [a0,a1) and [b0,b1), clamped at 0. */
  function overlapMs(a0, a1, b0, b1){
    var lo = Math.max(a0, b0), hi = Math.min(a1, b1);
    return hi > lo ? (hi - lo) : 0;
  }

  /* {state, value, ratePerMs} for one counter's segments at time t (ms).
   * state is 'not-started' before the first segment, 'running' inside one,
   * 'expired' at or after the last segment's end. A counter with no
   * segments at all (should not happen for an 'available' counter, but
   * defended here rather than assumed) reports 'not-started' with a null
   * value, never a fabricated zero. */
  function valueAt(segments, t){
    if(!segments || !segments.length) return {state: 'not-started', value: null, ratePerMs: null};
    var first = segments[0], last = segments[segments.length - 1];
    if(t < first.start_ms) return {state: 'not-started', value: first.v0, ratePerMs: first.rate_per_ms};
    if(t >= last.end_ms) return {state: 'expired', value: last.v1, ratePerMs: last.rate_per_ms};
    for(var i = 0; i < segments.length; i++){
      var s = segments[i];
      if(t >= s.start_ms && t < s.end_ms){
        return {state: 'running', value: s.v0 + s.rate_per_ms * (t - s.start_ms), ratePerMs: s.rate_per_ms};
      }
    }
    // A gap between two declared segments: neither running nor meaningfully
    // "not started" against a specific next segment -- treated as expired-
    // at-the-previous-value rather than inventing a number inside the gap.
    for(i = segments.length - 1; i >= 0; i--){
      if(t >= segments[i].end_ms) return {state: 'expired', value: segments[i].v1, ratePerMs: segments[i].rate_per_ms};
    }
    return {state: 'not-started', value: first.v0, ratePerMs: first.rate_per_ms};
  }

  /* Sum, over every segment, of rate_per_ms * overlap([openedAt, now], segment).
   * Every rate_per_ms in this payload is >= 0 (a trend extrapolation of a
   * growing total), so this can only grow, including across the turn of a
   * year where one segment ends and the next (a different annual rate)
   * begins -- it is never computed as "now's value minus openedAt's value",
   * which WOULD go negative if a counter's pace slowed between segments. */
  function sinceOpened(segments, openedAt, now){
    if(!segments || !segments.length || now <= openedAt) return 0;
    var total = 0;
    for(var i = 0; i < segments.length; i++){
      var s = segments[i];
      total += s.rate_per_ms * overlapMs(openedAt, now, s.start_ms, s.end_ms);
    }
    return total;
  }

  /* The payload declares its totals in a published unit (e.g. "meur") but
   * the segments themselves are always expressed in that unit's BASE
   * magnitude (a "meur" declaration means the segment values are raw euros,
   * a "count" declaration means the segment values are already a plain
   * count) -- this is a fixed convention of this payload format, not a
   * per-counter / per-indicator fact, so stating it here does not reach
   * into indicator-specific knowledge (rule 24). */
  function baseUnitFor(unit){
    return unit === 'meur' ? 'eur' : (unit || 'count');
  }

  // ---- scheduler: one timer, aligned to the next whole second --------------

  var SCHED = {timer: null, regions: []};

  function tickAll(){
    var now = Date.now();
    SCHED.regions.forEach(function(region){
      if(region.destroyed || region.userPaused || region.offscreen) return;
      renderRegionValues(region, now);
    });
  }

  function armTimer(){
    if(SCHED.timer !== null) return;
    if(typeof document !== 'undefined' && document.hidden) return;
    var delay = 1000 - (Date.now() % 1000);
    SCHED.timer = setTimeout(function(){
      SCHED.timer = null;
      tickAll();
      armTimer();
    }, delay);
  }

  if(typeof document !== 'undefined'){
    document.addEventListener('visibilitychange', function(){
      if(document.hidden){
        if(SCHED.timer !== null){ clearTimeout(SCHED.timer); SCHED.timer = null; }
      } else {
        tickAll();
        armTimer();
      }
    });
  }

  // ---- formatting helpers ----------------------------------------------------

  /* Rate line, picking the smallest unit of time whose figure reads with at
   * least `minMagnitude` of headroom: per second first, then per hour, then
   * per day. A plain integer display (baseUnit 'count', 0 decimals) needs a
   * bigger minMagnitude than a money display (2 decimals already carry
   * enough significant digits on their own) -- otherwise a slow count, like
   * a population trend, rounds to a single digit ("+5 / hour" for an
   * underlying 4.80/hour) instead of "+115 / day". Picking the LARGEST
   * window whose magnitude clears the bar, not the first one that clears
   * a bare >=1, is what keeps that second significant digit. */
  function pickRateWindow(ratePerMs, minMagnitude){
    minMagnitude = minMagnitude || 1;
    var windows = [
      {amount: ratePerMs * 1000, window: 'second'},
      {amount: ratePerMs * 3600000, window: 'hour'},
      {amount: ratePerMs * 86400000, window: 'day'},
    ];
    for(var i = 0; i < windows.length; i++){
      if(Math.abs(windows[i].amount) >= minMagnitude) return windows[i];
    }
    return windows[windows.length - 1];
  }

  function labelText(label, lang){
    if(!label) return '';
    return label[lang] || label.en || '';
  }

  // ---- DOM: one mounted region ------------------------------------------------

  function resolveCounters(payload, opts){
    if(opts.counters) return opts.counters;
    var ids = (payload.placements || {})[opts.placement] || [];
    var byId = {};
    (payload.counters || []).forEach(function(c){ byId[c.id] = c; });
    return ids.map(function(id){ return byId[id]; }).filter(Boolean);
  }

  function buildCounterNode(counter){
    var wrap = document.createElement('div');
    wrap.className = 'bp-live-counter';
    wrap.dataset.counterId = counter.id;
    wrap.innerHTML =
      '<div class="bp-live-counter-label">' +
        '<span class="bp-live-counter-name"></span>' +
      '</div>' +
      '<div class="bp-value" aria-hidden="true" data-raw="">—</div>' +
      '<div class="bp-live-counter-rate" aria-hidden="true"></div>' +
      '<div class="bp-live-counter-since" aria-hidden="true"></div>' +
      '<p class="bp-sr-only"></p>';
    return wrap;
  }

  function renderOneCounter(region, entry, now){
    var counter = entry.counter;
    // Guarded (round-2 review, P3-8): opts.buildNode is a page-level
    // extension point (see mount() below) and a future custom node shape
    // could omit this class by mistake -- better a silently-unnamed row
    // than a thrown exception that stops every OTHER counter in the region
    // from rendering too.
    var nameEl = entry.node.querySelector('.bp-live-counter-name');
    if(nameEl) nameEl.textContent = labelText(counter.label, region.lang);

    var valueEl = entry.node.querySelector('.bp-value');
    var rateEl = entry.node.querySelector('.bp-live-counter-rate');
    var sinceEl = entry.node.querySelector('.bp-live-counter-since');
    var srEl = entry.node.querySelector('.bp-sr-only');
    var L = region.labels || {};

    if(counter.state === 'unavailable'){
      valueEl.textContent = '—';
      valueEl.removeAttribute('data-raw');
      var reasonText = (counter.reason && (counter.reason[region.lang] || counter.reason.en)) || L.unavailable || '';
      rateEl.textContent = reasonText;
      sinceEl.textContent = '';
      entry.node.dataset.counterState = 'unavailable';
      if(srEl) srEl.textContent = (nameEl ? nameEl.textContent : '') + (reasonText ? (': ' + reasonText) : '');
      return;
    }

    var baseUnit = baseUnitFor(counter.unit);
    var at = valueAt(counter.segments, now);
    entry.node.dataset.counterState = at.state;

    if(at.state === 'not-started'){
      valueEl.textContent = L.notStarted || '—';
      valueEl.removeAttribute('data-raw');
      rateEl.textContent = '';
      sinceEl.textContent = '';
      if(srEl) srEl.textContent = ((nameEl ? nameEl.textContent : '') + ': ' + (L.notStarted || ''));
      return;
    }

    var floored = Math.floor(at.value);
    valueEl.dataset.raw = String(at.value);
    valueEl.textContent = region.formatValue(floored, baseUnit, 0, region.lang);

    if(at.state === 'expired'){
      rateEl.textContent = L.expired || '';
    } else {
      // A plain count (e.g. population) needs two clear significant digits
      // to read as a real rate ("+115 / day", not "+5 / hour" rounded down
      // from 4.80); a money value already carries that in its own decimals.
      var win = pickRateWindow(at.ratePerMs, baseUnit === 'count' ? 10 : 1);
      var amountText = region.formatValue(Math.abs(win.amount), baseUnit, baseUnit === 'count' ? 0 : 2, region.lang);
      var tmpl = win.window === 'second' ? L.perSecond : (win.window === 'hour' ? L.perHour : L.perDay);
      rateEl.textContent = tmpl ? tmpl.replace('{amount}', amountText) : ('+' + amountText);
    }

    // Floor BEFORE the zero check: a few cents or a fraction of a unit
    // accrued since the page opened must never surface as "+€0" / "+0
    // since you opened" -- the line simply stays blank until there is a
    // whole unit to show (rule: never show an explicit, misleading zero).
    var since = Math.floor(sinceOpened(counter.segments, region.openedAt, now));
    if(since > 0 && L.sinceOpened){
      sinceEl.textContent = L.sinceOpened.replace('{amount}', region.formatValue(since, baseUnit, 0, region.lang));
    } else {
      sinceEl.textContent = '';
    }

    /* Extension point for page-level domain wording this library must not
     * know about itself (rule 24): e.g. a {kind:"difference"} counter
     * reading as "deficit" or "surplus" depending on its live SIGN. The
     * library only hands back the generic {state,value,ratePerMs} and the
     * node it built; what a positive or negative difference MEANS is a
     * page/config decision, never a hardcoded counter id here. */
    if(typeof region.onCounterRendered === 'function'){
      region.onCounterRendered(counter, entry.node, at);
    }

    // Plain, non-live informational text for this one counter (e.g. a
    // screen reader user tabbing onto this specific tile hears it) -- NOT
    // a live region, so updating it every tick is harmless; nothing here
    // is announced automatically. The one-per-region role=status element
    // that IS announced is updated separately, far less often (see
    // updateRegionStatus below).
    if(srEl) srEl.textContent = (nameEl ? nameEl.textContent : '') + ': ' + valueEl.textContent;
  }

  function renderRegionValues(region, now){
    region.entries.forEach(function(entry){ renderOneCounter(region, entry, now); });
  }

  /* The ONE live-region announcement per mounted region (CLAUDE.md
   * accessibility rule: no per-second text in any live region). Built from
   * each counter's already-rendered name/value/rate text -- generic DOM
   * reads, never a counter id or indicator fact -- and updated only at the
   * call sites below (mount, pause/resume, language change), never from
   * the per-second scheduler tick. */
  function regionStatusText(region){
    var parts = region.entries.map(function(entry){
      var state = entry.node.dataset.counterState;
      var nameEl = entry.node.querySelector('.bp-live-counter-name');
      var valueEl = entry.node.querySelector('.bp-value');
      var rateEl = entry.node.querySelector('.bp-live-counter-rate');
      var name = nameEl ? nameEl.textContent : '';
      var value = valueEl ? valueEl.textContent : '';
      var rate = rateEl ? rateEl.textContent : '';
      if(state === 'unavailable' || state === 'not-started'){
        return rate ? (name + ': ' + rate) : (name + ': ' + value);
      }
      return rate ? (name + ': ' + value + ' (' + rate + ')') : (name + ': ' + value);
    });
    var text = parts.join('. ');
    if(region.userPaused && region.labels && region.labels.pausedSuffix){
      text += ' ' + region.labels.pausedSuffix;
    }
    return text;
  }

  function updateRegionStatus(region){
    if(region.statusEl) region.statusEl.textContent = regionStatusText(region);
  }

  function applyLabels(region){
    region.entries.forEach(function(entry){
      var nameEl = entry.node.querySelector('.bp-live-counter-name');
      if(nameEl) nameEl.textContent = labelText(entry.counter.label, region.lang);
    });
    if(region.pauseBtn){
      region.pauseBtn.textContent = region.userPaused ? (region.labels.resume || 'Resume') : (region.labels.pause || 'Pause');
      region.pauseBtn.setAttribute('aria-label', region.userPaused ? (region.labels.resumeAria || region.labels.resume || '') : (region.labels.pauseAria || region.labels.pause || ''));
    }
  }

  /* Round-2 a11y review (NEW P2-K): a paused region's values are frozen on
   * screen (by design) but its per-counter data-counter-state attribute
   * used to keep whatever running/expired/not-started value it had at the
   * moment of pausing -- a CSS or test hook keyed on data-counter-state
   * could not tell "genuinely ticking" from "frozen because paused" apart.
   * Generic UI state, not a counter id or indicator fact (rule 24 is about
   * the latter, not this), so it belongs in the shared engine. */
  function markEntriesPaused(region){
    region.entries.forEach(function(entry){ entry.node.dataset.counterState = 'paused'; });
  }

  function setRegionPaused(region, paused){
    region.userPaused = paused;
    if(region.pauseBtn){
      region.pauseBtn.setAttribute('aria-pressed', paused ? 'true' : 'false');
      // Reuse applyLabels' own two lines (text + aria-label) rather than
      // repeating only the text here: a toggle that updates the visible
      // label but leaves the OLD aria-label in place reads backwards to a
      // screen reader (WCAG 2.5.3 Label in Name) -- caught in review.
      applyLabels(region);
    }
    if(paused){
      markEntriesPaused(region);
    } else {
      renderRegionValues(region, Date.now());
    }
    updateRegionStatus(region);
  }

  /* Round-2 a11y/rules review (P3-8): the original check caught display:none
   * and visibility:hidden but not the other common ways to keep an element
   * in the DOM while hiding it from sighted readers -- opacity:0, a
   * collapsed 0x0 box, or shifting it off-screen (e.g. position:absolute;
   * left:-9999px). A badge hidden any of those ways is exactly as absent,
   * for this guard's purpose, as one that is display:none. */
  function isTrulyVisible(el){
    if(!el || el.hidden) return false;
    if(typeof getComputedStyle === 'function'){
      var style = getComputedStyle(el);
      if(style.display === 'none' || style.visibility === 'hidden') return false;
      if(parseFloat(style.opacity) === 0) return false;
    }
    if(typeof el.getBoundingClientRect === 'function'){
      var rect = el.getBoundingClientRect();
      if(rect.width <= 0 || rect.height <= 0) return false;
      if(rect.right <= 0 || rect.bottom <= 0) return false;
    }
    return true;
  }

  function mount(container, payload, opts){
    opts = opts || {};
    if(container.getAttribute('data-simulated') !== 'true'){
      throw new Error('BPLiveCounters.mount: container is missing data-simulated="true"');
    }
    var badge = container.querySelector('.bp-simulated-badge');
    var badgeVisible = badge && badge.textContent.trim().length > 0 && isTrulyVisible(badge);
    if(!badgeVisible){
      throw new Error('BPLiveCounters.mount: container has no visible, non-empty .bp-simulated-badge');
    }

    var counters = resolveCounters(payload, opts);
    var region = {
      container: container,
      counters: counters,
      lang: opts.lang || 'en',
      labels: opts.labels || {},
      formatValue: opts.formatValue || function(v){ return String(v); },
      openedAt: Date.now(),
      userPaused: !!(global.matchMedia && global.matchMedia('(prefers-reduced-motion: reduce)').matches),
      offscreen: false,
      destroyed: false,
      entries: [],
      onCounterRendered: opts.onCounterRendered,
    };

    var list = opts.listEl || container;
    // Extension point (same spirit as onCounterRendered below, rule 24):
    // a caller that needs extra, page-specific markup inside each counter's
    // node (e.g. a breakdown row's colour swatch and share percentage --
    // facts this generic engine must never know about) supplies
    // opts.buildNode(counter, index) instead of relying on the default
    // buildCounterNode(). Whatever it returns only needs to contain
    // descendants with the SAME class names buildCounterNode uses
    // (.bp-live-counter-name, .bp-value, .bp-live-counter-rate,
    // .bp-live-counter-since, .bp-sr-only) -- renderOneCounter finds them
    // by class, not by the node's own shape, so the ticking/pause/sr
    // machinery below is identical either way.
    region.entries = counters.map(function(counter, index){
      var node = opts.buildNode ? opts.buildNode(counter, index) : buildCounterNode(counter);
      list.appendChild(node);
      return {counter: counter, node: node};
    });

    if(opts.pauseBtn){
      region.pauseBtn = opts.pauseBtn;
      region.pauseBtn.setAttribute('aria-pressed', region.userPaused ? 'true' : 'false');
      region.pauseBtn.addEventListener('click', function(){
        setRegionPaused(region, !region.userPaused);
      });
    }

    applyLabels(region);

    // The ONE live region for this mounted region (accessibility rule: no
    // per-second text in any live region). Placed in the container that
    // already carries data-simulated="true", appended last so it never
    // disturbs the visual layout (it is visually hidden, .bp-sr-only).
    region.statusEl = opts.statusEl || document.createElement('p');
    region.statusEl.className = 'bp-sr-only';
    region.statusEl.setAttribute('role', 'status');
    region.statusEl.setAttribute('aria-live', 'polite');
    if(!opts.statusEl) container.appendChild(region.statusEl);

    if(typeof IntersectionObserver !== 'undefined'){
      region.io = new IntersectionObserver(function(entries){
        entries.forEach(function(e){
          if(e.target === container) region.offscreen = !e.isIntersecting;
        });
      }, {threshold: 0});
      region.io.observe(container);
    }

    renderRegionValues(region, Date.now());
    // A region that starts paused (prefers-reduced-motion, checked above)
    // never gets a tick from the scheduler to mark it so itself -- without
    // this, its counters would read data-counter-state="running" forever
    // despite never actually advancing (round-2 a11y review, NEW P2-K).
    if(region.userPaused) markEntriesPaused(region);
    updateRegionStatus(region);
    SCHED.regions.push(region);
    armTimer();

    return {
      setLang: function(lang, labels){
        region.lang = lang;
        if(labels) region.labels = labels;
        applyLabels(region);
        renderRegionValues(region, Date.now());
        if(region.userPaused) markEntriesPaused(region);
        updateRegionStatus(region);
      },
      pause: function(){ setRegionPaused(region, true); },
      resume: function(){ setRegionPaused(region, false); },
      destroy: function(){
        region.destroyed = true;
        if(region.io) region.io.disconnect();
        SCHED.regions = SCHED.regions.filter(function(r){ return r !== region; });
      },
    };
  }

  global.BPLiveCounters = {
    load: load,
    validate: validate,
    valueAt: valueAt,
    sinceOpened: sinceOpened,
    baseUnitFor: baseUnitFor,
    mount: mount,
  };
  if(typeof module !== 'undefined') module.exports = global.BPLiveCounters;
})(typeof window !== 'undefined' ? window : global);
