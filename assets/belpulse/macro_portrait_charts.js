/* Copied from commune.html: SVG charts, sparks, interactions, reveal and chapter palette.
   The factory supplies only language/translation; no indicator metadata lives here. */
window.BPMacroPortraitCharts = function(options){
  var T = options.translate;
  function localeTag(){ return options.locale(); }
  function escapeHtml(value){ return String(value == null ? '' : value).replace(/[&<>"']/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
  function isDarkTheme(){ return document.documentElement.getAttribute('data-theme') === 'dark'; }
  var THEME_HUES = ['#3a67e0','#1c7f86','#7a5ce0','#278a53','#b8811f','#d2691e','#a23b8f','#1b8bb8','#6b7f1a','#5b6b80','#8a5a3b'];
  var THEME_RAMP_VARS = [0,1,2,3,4,5,6].map(function(i){ return 'var(--th-ramp-' + i + ')'; });
  function hexToRgb(h){
    h = String(h).replace('#','');
    if(h.length === 3) h = h.split('').map(function(c){ return c + c; }).join('');
    return [parseInt(h.slice(0,2),16), parseInt(h.slice(2,4),16), parseInt(h.slice(4,6),16)];
  }
  function rgbToHex(r){
    return '#' + r.map(function(v){ var s = Math.max(0, Math.min(255, Math.round(v))).toString(16); return s.length < 2 ? '0' + s : s; }).join('');
  }
  function mixHex(a, b, t){
    var A = hexToRgb(a), B = hexToRgb(b);
    return rgbToHex([A[0] + (B[0]-A[0])*t, A[1] + (B[1]-A[1])*t, A[2] + (B[2]-A[2])*t]);
  }
  function themeHue(index){
    var n = THEME_HUES.length;
    return THEME_HUES[((index % n) + n) % n];
  }
  function themeRamp(hue, dark){
    var out = [];
    for(var i = 0; i < 7; i++){
      if(dark) out.push(i <= 3 ? mixHex('#1c1b24', hue, 0.22 + i * 0.26) : mixHex(hue, '#ffffff', (i - 3) * 0.2));
      else out.push(i <= 3 ? mixHex('#ffffff', hue, 0.12 + i * 0.27) : mixHex(hue, '#14213d', (i - 3) * 0.2));
    }
    return out;
  }
  var themedEls = [];
  function applyThemeVars(el, index){
    var hue = themeHue(index);
    var dark = isDarkTheme();
    el.style.setProperty('--th', dark ? mixHex(hue, '#ffffff', 0.3) : hue);
    el.style.setProperty('--th-soft', dark ? mixHex(hue, '#1c1b24', 0.72) : mixHex(hue, '#ffffff', 0.86));
    themeRamp(hue, dark).forEach(function(c, i){ el.style.setProperty('--th-ramp-' + i, c); });
    if(!themedEls.some(function(t){ return t.el === el; })) themedEls.push({el: el, index: index});
  }
  function niceGridlines(lo, hi, targetCount){
    if(hi <= lo) return [lo];
    var rawStep = (hi - lo) / targetCount;
    var mag = Math.pow(10, Math.floor(Math.log(rawStep) / Math.LN10));
    var residual = rawStep / mag;
    var step;
    if(residual > 5) step = 10 * mag;
    else if(residual > 2) step = 5 * mag;
    else if(residual > 1) step = 2 * mag;
    else step = mag;
    var start = Math.ceil(lo / step) * step;
    var out = [];
    for(var v = start; v <= hi + step * 1e-9; v += step){ out.push(Math.round(v / step) * step); }
    if(!out.length) out.push(lo);
    return out;
  }
  function niceGridlinesBracketing(dataMin, dataMax, targetCount){
    var vals = niceGridlines(dataMin, dataMax, targetCount);
    var step;
    if(vals.length >= 2){
      step = vals[1] - vals[0];
    } else {
      var rawStep = (dataMax - dataMin) / targetCount;
      var mag = Math.pow(10, Math.floor(Math.log(rawStep) / Math.LN10));
      var residual = rawStep / mag;
      step = residual > 5 ? 10 * mag : residual > 2 ? 5 * mag : residual > 1 ? 2 * mag : mag;
    }
    var out = vals.slice();
    if(!out.length) out.push(Math.ceil(dataMin / step) * step);
    if(out[0] > dataMin) out.unshift(out[0] - step);
    if(out[out.length - 1] < dataMax) out.push(out[out.length - 1] + step);
    return out;
  }
  function periodToTime(period){
    var s = String(period);
    var m;
    if((m = /^(\d{4})-Q([1-4])$/.exec(s))) return +m[1] + (+m[2] - 1) / 4;
    if((m = /^(\d{4})-(\d{2})(?:-(\d{2}))?$/.exec(s))){
      var year = +m[1], month = +m[2], day = m[3] ? +m[3] : 15;
      return year + (month - 1) / 12 + (day - 1) / 365;
    }
    if((m = /^(\d{4})$/.exec(s))) return +m[1];
    return null;
  }
  var SCOPE_MARK = {
    province: {colour: 'var(--bp-text-muted)', glyph: 'tri'},
    region:   {colour: 'var(--bp-text-faint)', glyph: 'sq'},
    country:  {colour: 'var(--bp-text)',       glyph: 'dotc'},
  };
  function fmtAxisNum(v, decimals){
    var d = (decimals == null) ? (Math.abs(v) < 10 ? 1 : 0) : decimals;
    return (Math.abs(v) < Math.pow(10, -d) / 2 ? 0 : v).toLocaleString(localeTag(), {maximumFractionDigits: d});
  }
  function buildLineChartSVG(opts){
    var W = opts.width || 640, H = opts.height || 260;
    var hasCompare = (opts.compareSeries || []).some(function(s){ return s.points.some(function(p){ return typeof p.value === 'number'; }); });
    var padL = 8, padR = hasCompare ? (W < 500 ? 104 : 150) : 8, padT = 16, padB = 26;
    var innerW = W - padL - padR, innerH = H - padT - padB;
    var pts = opts.points;
    /* Comparable-communes grey lines (PR C, spec 4.4). Included in the
       y-range up front so a peer's extreme value is never clipped -- the
       page reads the number, it never redraws the axis to hide it. */
    var simSeries = opts.simSeries || [];
    var allSeries = [{points: pts, main: true}]
      .concat((opts.compareSeries || []).map(function(s){ return {points: s.points, label: s.label}; }))
      .concat(simSeries.map(function(s){ return {points: s.points}; }));
    var allVals = [];
    allSeries.forEach(function(s){ s.points.forEach(function(p){ if(typeof p.value === 'number') allVals.push(p.value); }); });
    if(!allVals.length) return '';
    var dataMin = Math.min.apply(null, allVals), dataMax = Math.max.apply(null, allVals);
    if(dataMin === dataMax){ dataMin -= 1; dataMax += 1; }
    var niceValsForDomain = niceGridlinesBracketing(dataMin, dataMax, 4);
    var vMin = niceValsForDomain[0], vMax = niceValsForDomain[niceValsForDomain.length - 1];
    if(vMax <= vMin){ vMin -= 1; vMax += 1; }
    var finalPad = (vMax - vMin) * 0.04;
    vMin -= finalPad; vMax += finalPad;
    var n = pts.length;
    var times = pts.map(function(p){ return periodToTime(p.period); });
    var timesOk = times.every(function(t){ return t !== null; });
    var steps = [];
    if(timesOk) for(var si = 1; si < times.length; si++) steps.push(times[si] - times[si-1]);
    steps.sort(function(a,b){ return a-b; });
    var medianStep = steps.length ? steps[Math.floor(steps.length/2)] : 0;
    /* isBigGap: the pre-existing dashed-bridge threshold (>1.9x median),
       UNCHANGED for a gap under 3x -- above 3x the gap is compressed
       instead (isCompressedBreak below) and gets a break marker, not a
       dashed bridge, so isBigGap excludes that range now. */
    var isBigGap = function(i, j){
      return timesOk && medianStep > 0 && (times[j] - times[i]) > medianStep * 1.9 && (times[j] - times[i]) <= medianStep * 3;
    };
    /* A5: axis breaks. A gap of more than 3x the median step (e.g. the
       empty 2000-2017 stretch on the Sécurité/burglary chart the maintainer
       drew "BREAK" over) is compressed to exactly 2x the median step in a
       COMPRESSED-TIME x-scale -- not just a dashed bridge, which still
       spends most of the chart's width on years with no data. A gap
       between 1.9x and 3x keeps today's dashed-bridge behaviour unchanged
       (isBigGap above), with no compression and no break marker: only a
       genuinely long empty stretch is shortened. compressedTimes[i] is the
       x-axis coordinate actually used below; realTimes[i] (== times[i])
       stays what tooltips/hits report, so a reader still sees the true
       year, just not spaced proportionally to it.
       Rule 35 (byte-identical rebuilds): this is a pure function of the
       series' own periods, so it stays deterministic across runs. */
    var isCompressedBreak = function(i, j){
      return timesOk && medianStep > 0 && (times[j] - times[i]) > medianStep * 3;
    };
    var compressedTimes = times;
    var breaks = [];
    if(timesOk && medianStep > 0){
      compressedTimes = times.slice();
      var shift = 0;
      for(var ci = 1; ci < times.length; ci++){
        var realGap = times[ci] - times[ci - 1];
        if(realGap > medianStep * 3){
          var compressedGap = medianStep * 2;
          shift += (realGap - compressedGap);
          breaks.push({fromIdx: ci - 1, toIdx: ci, fromPeriod: pts[ci-1].period, toPeriod: pts[ci].period});
        }
        compressedTimes[ci] = times[ci] - shift;
      }
    }
    var tMin = timesOk ? Math.min.apply(null, compressedTimes) : 0;
    var tMax = timesOk ? Math.max.apply(null, compressedTimes) : (n - 1);
    var tSpan = tMax - tMin;
    var xOf = function(i){
      if(n <= 1) return padL + innerW / 2;
      if(!timesOk || tSpan === 0) return padL + (innerW * i) / (n - 1);
      return padL + (innerW * (compressedTimes[i] - tMin)) / tSpan;
    };
    var yOf = function(v){ return padT + innerH - ((v - vMin) / (vMax - vMin)) * innerH; };

    var svg = [];
    svg.push('<svg viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="none" role="img" aria-label="' + escapeHtml(opts.ariaLabel || '') + '">');

    var compareLines = (opts.compareSeries || []).map(function(s){ return s.label; }).filter(Boolean);
    var hits = [];
    pts.forEach(function(p, i){
      if(typeof p.value !== 'number') return;
      hits.push({
        x: xOf(i), y: yOf(p.value), period: p.period, value: p.value,
        valueText: fmtAxisNum(p.value, opts.decimals) + (opts.unitSuffix || ''),
        compareLines: compareLines,
      });
    });

    niceValsForDomain.forEach(function(v){
      var y = yOf(v);
      svg.push('<line x1="' + padL + '" y1="' + y.toFixed(1) + '" x2="' + (W - padR) + '" y2="' + y.toFixed(1) + '" stroke="var(--bp-border)" stroke-width="1"/>');
      svg.push('<text x="0" y="' + (y - 4).toFixed(1) + '" font-family="Inter,sans-serif" font-size="11" fill="var(--bp-text-faint)">' +
        escapeHtml(fmtAxisNum(v, opts.decimals)) + '</text>');
    });

    /* A5 tick order: first, last, each segment's start (the point right
       after a compressed break, so a reader can see the axis picks back up
       there), THEN the usual intermediate picks -- skipping any label
       whose x sits within 44px of one already kept, so a break's own
       nearby tick can't double up with the 33%/66% picks. */
    var xIdx = [0];
    if(n > 1) xIdx.push(n - 1);
    breaks.forEach(function(b){ xIdx.push(b.fromIdx, b.toIdx); });
    if(n > 2) xIdx.push(Math.round((n - 1) * 0.33), Math.round((n - 1) * 0.66));
    xIdx = Array.from(new Set(xIdx));
    var keptX = [];
    var keptIdx = [];
    xIdx.forEach(function(i){
      var x = xOf(i);
      if(keptX.some(function(kx){ return Math.abs(kx - x) < 44; })) return;
      keptX.push(x); keptIdx.push(i);
    });
    keptIdx.sort(function(a, b){ return a - b; });
    keptIdx.forEach(function(i){
      var anchor = i === 0 ? 'start' : (i === n - 1 ? 'end' : 'middle');
      svg.push('<text x="' + xOf(i).toFixed(1) + '" y="' + (H - 6) + '" font-family="Inter,sans-serif" font-size="11" fill="var(--bp-text-faint)" text-anchor="' + anchor + '">' + escapeHtml(String(pts[i].period)) + '</text>');
    });

    /* ---- comparable-communes grey lines (spec 4.4) -----------------------
       Drawn after the gridlines/axis text and BEFORE the scope-comparison
       markers, so the commune's own line and end dot always paint on top.
       No peer runs and no peer values enter the y-range above if
       !timesOk -- xOfTime has no meaning without a real time axis, so a
       grey line would be pixel-meaningless rather than merely absent. */
    var simHits = [];
    var simDrawn = 0;
    if(simSeries.length && timesOk){
      var xOfTime = function(period){
        var t = periodToTime(period);
        if(t === null || t < tMin || t > tMax) return null;
        return padL + (innerW * (t - tMin)) / (tSpan || 1);
      };
      // BPSimilar.peerRuns compares consecutive gaps in whatever units
      // xOfTime returns (pixels here, not raw time) -- medianStep must be
      // converted to that same pixel space, or every step looks like an
      // axis-break-sized gap (innerW/tSpan pixels per time unit vs. 1).
      var pxPerTimeUnit = tSpan > 0 ? innerW / tSpan : 0;
      var medianStepPx = medianStep * pxPerTimeUnit;
      svg.push('<g class="sim-lines" aria-hidden="true">');
      simSeries.forEach(function(s){
        var runs = BPSimilar.peerRuns(s.points, xOfTime, medianStepPx);
        var drawnAny = false;
        runs.forEach(function(run){
          if(run.length < 2){
            if(run.length === 1){
              var p0 = run[0];
              var x0 = xOfTime(p0.period), y0 = yOf(p0.value);
              svg.push('<circle class="sim-dot" data-nis="' + escapeHtml(s.nis) + '" data-first="' + escapeHtml(String(p0.period)) + '" data-last="' + escapeHtml(String(p0.period)) + '" cx="' + x0.toFixed(1) + '" cy="' + y0.toFixed(1) + '" r="1.6"/>');
              simHits.push({nis: s.nis, name: s.name, rank: s.rank, x: x0, y: y0, period: p0.period, value: p0.value, status: p0.status, valueText: fmtAxisNum(p0.value, opts.decimals) + (opts.unitSuffix || '')});
              drawnAny = true;
            }
            return;
          }
          var d = run.map(function(p, k){ return (k === 0 ? 'M' : 'L') + xOfTime(p.period).toFixed(1) + ',' + yOf(p.value).toFixed(1); }).join(' ');
          svg.push('<path class="sim-line" data-nis="' + escapeHtml(s.nis) + '" data-first="' + escapeHtml(String(run[0].period)) + '" data-last="' + escapeHtml(String(run[run.length-1].period)) + '" d="' + d + '"/>');
          run.forEach(function(p){
            simHits.push({nis: s.nis, name: s.name, rank: s.rank, x: xOfTime(p.period), y: yOf(p.value), period: p.period, value: p.value, status: p.status, valueText: fmtAxisNum(p.value, opts.decimals) + (opts.unitSuffix || '')});
          });
          drawnAny = true;
        });
        if(drawnAny) simDrawn++;
      });
      svg.push('</g>');
    }

    var markers = (opts.compareSeries || [])
      .map(function(s){
        var v = null;
        for(var i = s.points.length - 1; i >= 0; i--){ if(typeof s.points[i].value === 'number'){ v = s.points[i].value; break; } }
        return v === null ? null : {
          label: s.label, nameLabel: s.nameLabel || s.label, valueLabel: s.valueLabel || '',
          scope: s.scope, dotY: yOf(v), labelY: yOf(v),
        };
      })
      .filter(Boolean)
      .sort(function(a, b){ return a.dotY - b.dotY; });
    var MIN_GAP = 14;
    for(var mi = 1; mi < markers.length; mi++){
      var minY = markers[mi-1].labelY + MIN_GAP;
      if(markers[mi].labelY < minY) markers[mi].labelY = minY;
    }
    /* A4: the downward pass above can push the LAST label below the plot
       area (padT..H-padB) when several markers cluster near the bottom --
       there is nothing further down to push it into. An upward pass fixes
       that by pulling labels back up from the bottom, then both passes are
       clamped inside the plot so no label sits outside it either way. */
    for(var mj = markers.length - 2; mj >= 0; mj--){
      var maxY = markers[mj+1].labelY - MIN_GAP;
      if(markers[mj].labelY > maxY) markers[mj].labelY = maxY;
    }
    markers.forEach(function(m){
      m.labelY = Math.max(padT + 4, Math.min(H - padB - 4, m.labelY));
    });
    var markerX = xOf(n - 1);
    // A4: the space actually left in the viewBox for a label starting at
    // markerX+8 -- fitEndLabels() (wireLineChart, below) needs this to know
    // when a label overflows the chart's own width, not just an arbitrary
    // guess.
    var endLabelBudget = Math.max(0, W - (markerX + 8) - 4);
    svg.push('<g class="bp-anim-fade bp-cmpmarkers" data-label-budget="' + endLabelBudget.toFixed(1) + '">');
    markers.forEach(function(m){
      var mk = SCOPE_MARK[m.scope] || SCOPE_MARK.region;
      var mx = markerX, my = m.dotY;
      if(mk.glyph === 'tri') svg.push('<path d="M' + mx.toFixed(1) + ',' + (my - 4.5).toFixed(1) + ' L' + (mx + 4.5).toFixed(1) + ',' + (my + 3.5).toFixed(1) + ' L' + (mx - 4.5).toFixed(1) + ',' + (my + 3.5).toFixed(1) + ' Z" fill="' + mk.colour + '"/>');
      else if(mk.glyph === 'sq') svg.push('<rect x="' + (mx - 3.5).toFixed(1) + '" y="' + (my - 3.5).toFixed(1) + '" width="7" height="7" fill="' + mk.colour + '"/>');
      else svg.push('<circle cx="' + mx.toFixed(1) + '" cy="' + my.toFixed(1) + '" r="3.5" fill="' + mk.colour + '"/>');
      if(Math.abs(m.labelY - m.dotY) > 0.5){
        svg.push('<path d="M' + (markerX + 3).toFixed(1) + ',' + m.dotY.toFixed(1) +
          ' L' + (markerX + 6).toFixed(1) + ',' + m.labelY.toFixed(1) +
          '" fill="none" stroke="var(--bp-text-faint)" stroke-width="1"/>');
      }
      // A4: name and value as separate tspans (measured/shortened
      // independently after mount by fitEndLabels() below, since a string's
      // rendered width is not knowable from the markup alone) plus a
      // <title> carrying the untouched full text for anyone who cannot see
      // the shortened version (a screen reader, or a hover).
      svg.push('<text class="bp-endlabel" data-full="' + escapeHtml(m.label) + '" x="' + (markerX + 8).toFixed(1) + '" y="' + (m.labelY + 3.5).toFixed(1) + '" font-family="Inter,sans-serif" font-size="11" fill="' + mk.colour + '">' +
        '<title>' + escapeHtml(m.label) + '</title>' +
        '<tspan class="bp-endlabel-name">' + escapeHtml(m.nameLabel) + (m.valueLabel ? ' ' : '') + '</tspan>' +
        (m.valueLabel ? '<tspan class="bp-endlabel-value">' + escapeHtml(m.valueLabel) + '</tspan>' : '') +
        '</text>');
    });
    svg.push('</g>');

    var runs = [], cur = [];
    pts.forEach(function(p, i){
      if(typeof p.value === 'number'){
        if(cur.length && (isBigGap(cur[cur.length - 1], i) || isCompressedBreak(cur[cur.length - 1], i))) { runs.push(cur); cur = []; }
        cur.push(i);
      } else { if(cur.length) runs.push(cur); cur = []; }
    });
    if(cur.length) runs.push(cur);
    runs.forEach(function(run){
      if(run.length < 2){
        if(run.length === 1){
          var i0 = run[0];
          svg.push('<circle cx="' + xOf(i0).toFixed(1) + '" cy="' + yOf(pts[i0].value).toFixed(1) + '" r="3" fill="var(--th)"/>');
        }
        return;
      }
      var d = run.map(function(i, k){ return (k === 0 ? 'M' : 'L') + xOf(i).toFixed(1) + ',' + yOf(pts[i].value).toFixed(1); }).join(' ');
      svg.push('<path class="bp-line-draw" d="' + d + '" fill="none" stroke="var(--th)" stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round"/>');
    });
    for(var r = 0; r < runs.length - 1; r++){
      var endI = runs[r][runs[r].length - 1], startI = runs[r+1][0];
      var gapX0 = Math.min(xOf(endI), xOf(startI)) - 4, gapX1 = Math.max(xOf(endI), xOf(startI)) + 4;
      svg.push('<g class="bp-anim-clip bp-gapclip" data-x0="' + gapX0.toFixed(1) + '" data-x1="' + gapX1.toFixed(1) + '" style="clip-path:inset(0 ' + (W - gapX0).toFixed(1) + 'px 0 0)">');
      if(isCompressedBreak(endI, startI)){
        /* A5: no dashed bridge across a compressed break -- two short
           slanted strokes on the x baseline, a faint dotted vertical line
           through the plot, and a <title> naming the real years the axis
           skipped (v4AxisBreak). */
        var breakMidX = (xOf(endI) + xOf(startI)) / 2;
        var baseline = H - padB;
        var slant = 5;
        svg.push('<g class="bp-axis-break">');
        svg.push('<title>' + escapeHtml(T('v4AxisBreak', {from: pts[endI].period, to: pts[startI].period})) + '</title>');
        svg.push('<line x1="' + breakMidX.toFixed(1) + '" y1="' + padT.toFixed(1) + '" x2="' + breakMidX.toFixed(1) + '" y2="' + baseline.toFixed(1) + '" stroke="var(--bp-border)" stroke-width="1" stroke-dasharray="1 3"/>');
        [-3, 3].forEach(function(dx){
          svg.push('<line x1="' + (breakMidX + dx - slant/2).toFixed(1) + '" y1="' + (baseline + slant/2).toFixed(1) +
            '" x2="' + (breakMidX + dx + slant/2).toFixed(1) + '" y2="' + (baseline - slant/2).toFixed(1) +
            '" stroke="var(--bp-text-muted)" stroke-width="1.4" stroke-linecap="round"/>');
        });
        svg.push('</g>');
      } else if(isBigGap(endI, startI)){
        svg.push('<path d="M' + xOf(endI).toFixed(1) + ',' + yOf(pts[endI].value).toFixed(1) +
          ' L' + xOf(startI).toFixed(1) + ',' + yOf(pts[startI].value).toFixed(1) +
          '" fill="none" stroke="var(--bp-text-faint)" stroke-width="1.4" stroke-dasharray="3 3"/>');
      } else {
        var gx = (xOf(endI) + xOf(startI)) / 2;
        svg.push('<line x1="' + (gx-3).toFixed(1) + '" y1="' + (padT).toFixed(1) + '" x2="' + (gx+3).toFixed(1) + '" y2="' + (H-padB).toFixed(1) + '" stroke="var(--bp-border)" stroke-width="1" stroke-dasharray="2 2"/>');
      }
      svg.push('</g>');
    }
    var lastIdx = -1;
    pts.forEach(function(p, i){ if(typeof p.value === 'number') lastIdx = i; });
    svg.push('<g class="bp-anim-fade bp-enddot">');
    if(lastIdx >= 0){
      svg.push('<circle cx="' + xOf(lastIdx).toFixed(1) + '" cy="' + yOf(pts[lastIdx].value).toFixed(1) + '" r="4.5" fill="var(--th)"/>');
    }
    svg.push('</g>');
    svg.push('</svg>');
    var out = svg.join('');
    if(simDrawn > 0){
      var ariaSuffix = ' — ' + T('simAria', {n: simDrawn});
      out = out.replace(/(aria-label="[^"]*)(")/, function(m, p1, p2){ return p1 + escapeHtml(ariaSuffix) + p2; });
    }
    return {svg: out, hits: hits, simHits: simHits, simDrawn: simDrawn};
  }

  /* A4: end-of-line comparison labels ("Région de Bruxelles-Capitale
     34 926 €") measured up to 57px outside the chart column. A string's
     rendered width cannot be known from the markup alone (font metrics
     vary by engine/OS), so this runs after mount -- and again once webfonts
     finish loading, since a fallback-font measurement taken before that can
     under- or over-estimate the eventual width. */
  function fitEndLabels(svg){
    var group = svg.querySelector('g.bp-cmpmarkers');
    if(!group) return;
    var budget = parseFloat(group.dataset.labelBudget);
    if(!budget || !isFinite(budget)) return;
    group.querySelectorAll('text.bp-endlabel').forEach(function(text){
      var nameTspan = text.querySelector('.bp-endlabel-name');
      var valueTspan = text.querySelector('.bp-endlabel-value');
      if(!nameTspan) return;
      var fullName = (nameTspan.textContent || '').replace(/\s+$/, '');
      var sep = valueTspan ? ' ' : '';
      var valueText = valueTspan ? valueTspan.textContent : '';
      function totalWidth(){ return text.getComputedTextLength(); }
      nameTspan.textContent = fullName + sep;
      if(totalWidth() <= budget) return;
      // Shorten the NAME first, keeping the value intact, down to nothing;
      // only if the value ALONE still does not fit does it disappear too
      // (the <title> above always keeps the untouched full text).
      var lo = 0, hi = fullName.length, best = 0;
      while(lo <= hi){
        var mid = Math.floor((lo + hi) / 2);
        nameTspan.textContent = (mid > 0 ? fullName.slice(0, mid) + '…' : '') + sep;
        if(totalWidth() <= budget){ best = mid; lo = mid + 1; } else { hi = mid - 1; }
      }
      nameTspan.textContent = (best > 0 ? fullName.slice(0, best) + '…' : '') + sep;
      if(best === 0 && valueTspan && totalWidth() > budget){
        // Even the value alone does not fit: show the value alone (no
        // name, no ellipsis for a name that isn't there).
        nameTspan.textContent = '';
        if(totalWidth() > budget) valueTspan.style.display = 'none';
      }
    });
  }
  function wireLineChart(container, hits, opts){
    var svg = container.querySelector('svg');
    if(!svg) return;
    fitEndLabels(svg);
    if(document.fonts && document.fonts.ready) document.fonts.ready.then(function(){ fitEndLabels(svg); });
    var noAnim = opts && opts.noAnim;
    svg.querySelectorAll('path.bp-line-draw').forEach(function(path){
      armLineDraw(path);
      observeLineDraw(path);
    });
    svg.querySelectorAll('g.bp-gapclip').forEach(function(g){
      var x1 = parseFloat(g.dataset.x1);
      var W = svg.viewBox.baseVal.width;
      var openInset = 'inset(0 ' + Math.max(0, W - x1).toFixed(1) + 'px 0 0)';
      if(REDUCE_MOTION || !revealObserver){ g.style.clipPath = openInset; return; }
      var obs = new IntersectionObserver(function(entries){
        entries.forEach(function(entry){
          if(!entry.isIntersecting) return;
          g.style.clipPath = openInset;
          obs.disconnect();
        });
      }, REVEAL_IO_OPTS);
      obs.observe(g);
    });
    svg.querySelectorAll('g.bp-anim-fade').forEach(observeReveal);
    // Comparable-communes grey lines (spec 4.4's opts.noAnim): a redraw on
    // a list switch shows the final state immediately -- no draw-in, fade
    // or clip reveal -- while the FIRST paint (profiles finish loading
    // after the chart already exists) fades in over 180ms.
    var simLinesG = svg.querySelector('g.sim-lines');
    if(simLinesG){
      if(noAnim || REDUCE_MOTION){ simLinesG.style.opacity = '1'; }
      else {
        simLinesG.style.opacity = '0';
        simLinesG.style.transition = 'opacity .18s ease-out';
        requestAnimationFrame(function(){ requestAnimationFrame(function(){ simLinesG.style.opacity = '1'; }); });
      }
    }

    if(!hits.length) return;
    var crosshair = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    crosshair.setAttribute('class', 'bp-crosshair');
    var vb = svg.viewBox.baseVal;
    crosshair.setAttribute('y1', vb.y); crosshair.setAttribute('y2', vb.y + vb.height);
    svg.appendChild(crosshair);
    var dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    dot.setAttribute('class', 'bp-crosshair-dot');
    dot.setAttribute('r', '4');
    svg.appendChild(dot);
    var live = makeLiveRegion(container);
    wireChartInteraction(svg, crosshair, dot, hits, {
      title: opts && opts.title, ariaBase: (opts && opts.ariaBase) || svg.getAttribute('aria-label'), live: live,
      simHits: opts && opts.simHits,
    });
  }

  function buildComparisonBarSVG(rows, opts){
    var W = opts.width || 640, H = opts.height || 260;
    var padX = 4;
    var withVal = rows.filter(function(r){ return typeof r.value === 'number'; });
    if(!withVal.length) return {svg: '', hits: []};
    var maxV = Math.max.apply(null, withVal.map(function(r){ return Math.abs(r.value); }).concat([1]));
    var n = rows.length;
    var rowH = Math.min(56, (H - 8) / n);
    var barH = Math.max(10, rowH * 0.38);
    /* A4: the value column used to reserve a flat 90px regardless of the
       actual text -- a wide value ("2,42 pers./ménage") could still end
       past the chart's own width. The bars now shrink to reserve however
       much the WIDEST value in this chart actually needs (a 13px/600-weight
       monospace-figure estimate: exact glyph metrics are not knowable
       before the SVG is in the DOM, and this chart is built as a string,
       not measured after mount the way fitEndLabels() above is). */
    var widestValueChars = Math.max.apply(null, rows.map(function(r){
      return typeof r.value === 'number' ? (r.valueText || '').length : 1;
    }).concat([1]));
    var valueColumnW = Math.max(36, widestValueChars * 7.6);
    var barMaxW = Math.max(20, W - padX * 2 - valueColumnW - 10);
    var svg = [];
    var hits = [];
    svg.push('<svg viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="xMidYMid meet" role="img" aria-label="' + escapeHtml(opts.ariaLabel || '') + '">');
    rows.forEach(function(r, i){
      var cy = 8 + rowH * i + rowH / 2;
      var labelY = cy - barH / 2 - 6;
      var barY = cy - barH / 2;
      var w = typeof r.value === 'number' ? (Math.abs(r.value) / maxV) * barMaxW : 0;
      var color = r.self ? 'var(--th)' : 'var(--bp-text-faint)';
      // A4: the row label ellipsised with a <title> holding the full name,
      // rather than letting it run past the chart -- textLength clamps the
      // rendered width without needing a post-mount measurement pass.
      var labelMaxLen = 30;
      var labelShort = r.label.length > labelMaxLen ? r.label.slice(0, labelMaxLen - 1) + '…' : r.label;
      svg.push('<text x="' + padX + '" y="' + labelY.toFixed(1) + '" font-family="Inter,sans-serif" font-size="12" font-weight="' + (r.self ? '600' : '500') + '" fill="var(--bp-text)">' +
        (labelShort !== r.label ? '<title>' + escapeHtml(r.label) + '</title>' : '') + escapeHtml(labelShort) + '</text>');
      svg.push('<rect class="bp-anim-grow" style="transform-box:fill-box; transform-origin:0 50%" x="' + padX + '" y="' + barY.toFixed(1) + '" width="' + Math.max(2, w).toFixed(1) + '" height="' + barH.toFixed(1) + '" rx="3" fill="' + color + '"/>');
      var valText = typeof r.value === 'number' ? r.valueText : '—';
      svg.push('<text class="bp-anim-fade" x="' + (padX + w + 10).toFixed(1) + '" y="' + (barY + barH / 2).toFixed(1) + '" dominant-baseline="middle" font-family="Inter,sans-serif" font-size="13" font-weight="600" font-variant-numeric="tabular-nums" fill="var(--bp-text)">' + escapeHtml(valText) + '</text>');
      hits.push({x: padX + w / 2, y: cy, period: r.label, value: r.value, valueText: valText});
    });
    svg.push('</svg>');
    return {svg: svg.join(''), hits: hits};
  }

  /* A4 backstop: buildComparisonBarSVG's value column is sized from an
     estimated character width (the SVG is built as a string, before it is
     in the DOM to measure). After mount, getComputedTextLength() gives the
     REAL width; if a value text still ends past the viewBox despite the
     estimate, it is nudged left just enough to stay inside. */
  function fitBarValueLabels(svg){
    var vb = svg.viewBox.baseVal;
    if(!vb || !vb.width) return;
    svg.querySelectorAll('text.bp-anim-fade').forEach(function(text){
      var x = parseFloat(text.getAttribute('x'));
      var right = x + text.getComputedTextLength();
      var overflow = right - vb.width;
      if(overflow > 0) text.setAttribute('x', (x - overflow - 2).toFixed(1));
    });
  }
  function wireBarChart(container, hits, opts){
    var svg = container.querySelector('svg');
    if(!svg) return;
    fitBarValueLabels(svg);
    svg.querySelectorAll('.bp-anim-grow, .bp-anim-fade').forEach(observeReveal);
    if(!hits.length) return;
    var crosshair = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    crosshair.setAttribute('class', 'bp-crosshair');
    var vb = svg.viewBox.baseVal;
    crosshair.setAttribute('y1', vb.y); crosshair.setAttribute('y2', vb.y + vb.height);
    svg.appendChild(crosshair);
    var dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    dot.setAttribute('class', 'bp-crosshair-dot');
    dot.setAttribute('r', '4');
    svg.appendChild(dot);
    var live = makeLiveRegion(container);
    wireChartInteraction(svg, crosshair, dot, hits, {
      title: opts && opts.title, ariaBase: (opts && opts.ariaBase) || svg.getAttribute('aria-label'), live: live,
    });
  }

  function sparkPoints(pts){
    var p = pts.slice(-16);
    var times = p.map(function(q){ return periodToTime(q.period); });
    if(p.length < 3 || times.some(function(t){ return t === null; })) return p;
    var steps = [];
    for(var i = 1; i < times.length; i++) steps.push(times[i] - times[i-1]);
    var sorted = steps.slice().sort(function(a, b){ return a - b; });
    var med = sorted[Math.floor(sorted.length / 2)];
    var cut = 0;
    for(var j = 1; j < times.length; j++){ if(med > 0 && times[j] - times[j-1] > med * 3) cut = j; }
    var rest = p.slice(cut);
    return rest.filter(function(q){ return typeof q.value === 'number'; }).length >= 2 ? rest : p;
  }
  function buildSparklineSVG(pts, decimals, unitSfx, opts){
    var W = 200, H = (opts && opts.height) || 56, padX = 4, padY = 6;
    var vals = pts.map(function(p){ return p.value; }).filter(function(v){ return typeof v === 'number'; });
    if(!vals.length) return {svg: '', hits: []};
    var vMin = Math.min.apply(null, vals), vMax = Math.max.apply(null, vals);
    if(vMin === vMax){ vMin -= 1; vMax += 1; }
    var n = pts.length;
    var times = pts.map(function(p){ return periodToTime(p.period); });
    var timesOk = times.every(function(t){ return t !== null; });
    var tMin = timesOk ? Math.min.apply(null, times) : 0;
    var tMax = timesOk ? Math.max.apply(null, times) : (n - 1);
    var tSpan = tMax - tMin;
    var xOf = function(i){
      if(n <= 1) return W/2;
      if(!timesOk || tSpan === 0) return padX + ((W - padX*2) * i) / (n - 1);
      return padX + ((W - padX*2) * (times[i] - tMin)) / tSpan;
    };
    var yOf = function(v){ return padY + (H - padY*2) - ((v - vMin) / (vMax - vMin)) * (H - padY*2); };
    var steps = [];
    if(timesOk) for(var si = 1; si < times.length; si++) steps.push(times[si] - times[si-1]);
    steps.sort(function(a,b){ return a-b; });
    var medianStep = steps.length ? steps[Math.floor(steps.length/2)] : 0;
    var isBigGap = function(i, j){ return timesOk && medianStep > 0 && (times[j] - times[i]) > medianStep * 1.9; };
    var runs = [], cur = [];
    pts.forEach(function(p, i){
      if(typeof p.value === 'number'){
        if(cur.length && isBigGap(cur[cur.length - 1], i)) { runs.push(cur); cur = []; }
        cur.push(i);
      } else { if(cur.length) runs.push(cur); cur = []; }
    });
    if(cur.length) runs.push(cur);
    var hits = [];
    pts.forEach(function(p, i){
      if(typeof p.value !== 'number') return;
      hits.push({x: xOf(i), y: yOf(p.value), period: p.period, value: p.value, valueText: fmtAxisNum(p.value, decimals) + (unitSfx || '')});
    });
    var out = '<svg viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="none">';
    runs.forEach(function(run){
      if(run.length < 2) return;
      var d = run.map(function(i, k){ return (k === 0 ? 'M' : 'L') + xOf(i).toFixed(1) + ',' + yOf(pts[i].value).toFixed(1); }).join(' ');
      out += '<path class="bp-line-draw" d="' + d + '" fill="none" stroke="var(--th)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>';
    });
    for(var r = 0; r < runs.length - 1; r++){
      var endI = runs[r][runs[r].length - 1], startI = runs[r+1][0];
      if(isBigGap(endI, startI)){
        var gapX0b = Math.min(xOf(endI), xOf(startI)) - 4, gapX1b = Math.max(xOf(endI), xOf(startI)) + 4;
        out += '<g class="bp-anim-clip bp-gapclip" data-x1="' + gapX1b.toFixed(1) + '" style="clip-path:inset(0 ' + (W - gapX0b).toFixed(1) + 'px 0 0)">';
        out += '<path d="M' + xOf(endI).toFixed(1) + ',' + yOf(pts[endI].value).toFixed(1) +
          ' L' + xOf(startI).toFixed(1) + ',' + yOf(pts[startI].value).toFixed(1) +
          '" fill="none" stroke="var(--bp-text-faint)" stroke-width="1.2" stroke-dasharray="2 2"/>';
        out += '</g>';
      }
    }
    var lastIdx = -1;
    pts.forEach(function(p, i){ if(typeof p.value === 'number') lastIdx = i; });
    out += '<g class="bp-anim-fade bp-enddot">';
    if(lastIdx >= 0) out += '<circle cx="' + xOf(lastIdx).toFixed(1) + '" cy="' + yOf(pts[lastIdx].value).toFixed(1) + '" r="3" fill="var(--th)"/>';
    out += '</g>';
    out += '</svg>';
    return {svg: out, hits: hits};
  }
  function wireSparkline(container, hits, staggerIndex){
    var svg = container.querySelector('svg');
    if(!svg) return;
    svg.querySelectorAll('path.bp-line-draw').forEach(function(path){
      armLineDraw(path, 'bp-spark-duration');
      if(!REDUCE_MOTION && staggerIndex != null) path.style.transitionDelay = (staggerIndex * 60) + 'ms';
      observeLineDraw(path);
    });
    svg.querySelectorAll('g.bp-gapclip').forEach(function(g){
      var x1 = parseFloat(g.dataset.x1), W = svg.viewBox.baseVal.width;
      var openInset = 'inset(0 ' + Math.max(0, W - x1).toFixed(1) + 'px 0 0)';
      if(!REDUCE_MOTION && staggerIndex != null) g.style.transitionDelay = (staggerIndex * 60) + 'ms';
      if(REDUCE_MOTION || !revealObserver){ g.style.clipPath = openInset; return; }
      var obs = new IntersectionObserver(function(entries){
        entries.forEach(function(entry){ if(entry.isIntersecting){ g.style.clipPath = openInset; obs.disconnect(); } });
      }, REVEAL_IO_OPTS);
      obs.observe(g);
    });
    svg.querySelectorAll('g.bp-anim-fade').forEach(function(el){
      if(!REDUCE_MOTION && staggerIndex != null) el.style.transitionDelay = ((staggerIndex * 60) + 550) + 'ms';
      observeReveal(el);
    });
    if(!hits.length) return;
    var crosshair = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    crosshair.setAttribute('class', 'bp-crosshair');
    var vb = svg.viewBox.baseVal;
    crosshair.setAttribute('y1', vb.y); crosshair.setAttribute('y2', vb.y + vb.height);
    svg.appendChild(crosshair);
    var dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    dot.setAttribute('class', 'bp-crosshair-dot');
    dot.setAttribute('r', '3');
    svg.appendChild(dot);
    wireChartInteraction(svg, crosshair, dot, hits, {});
  }

  var REDUCE_MOTION = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var REVEAL_IO_OPTS = {threshold: 0, rootMargin: '0px 0px 160px 0px'};

  var revealObserver = (!REDUCE_MOTION && 'IntersectionObserver' in window) ?
    new IntersectionObserver(function(entries){
      entries.forEach(function(entry){
        if(!entry.isIntersecting) return;
        entry.target.classList.add('bp-anim-ready');
        revealObserver.unobserve(entry.target);
      });
    }, REVEAL_IO_OPTS) : null;
  function observeReveal(el){
    if(!el) return;
    if(!revealObserver){ el.classList.add('bp-anim-ready'); return; }
    revealObserver.observe(el);
  }
  function armLineDraw(path, durationClass){
    if(REDUCE_MOTION || !path) return;
    var len;
    try{ len = path.getTotalLength(); }catch(e){ return; }
    if(!len) return;
    path.style.strokeDasharray = len + ' ' + len;
    path.style.strokeDashoffset = len;
    path.classList.add('bp-anim-line');
    if(durationClass) path.classList.add(durationClass);
    requestAnimationFrame(function(){
      path.dataset.drawnOffset = '0';
    });
  }
  var lineDrawObserver = (!REDUCE_MOTION && 'IntersectionObserver' in window) ?
    new IntersectionObserver(function(entries){
      entries.forEach(function(entry){
        if(!entry.isIntersecting) return;
        entry.target.style.strokeDashoffset = '0';
        lineDrawObserver.unobserve(entry.target);
      });
    }, REVEAL_IO_OPTS) : null;
  function observeLineDraw(path){
    if(REDUCE_MOTION || !path){ if(path){ path.style.strokeDasharray = ''; path.style.strokeDashoffset = ''; } return; }
    if(!lineDrawObserver){ path.style.strokeDashoffset = '0'; return; }
    lineDrawObserver.observe(path);
  }
  function svgPoint(svg, evt){
    var rect = svg.getBoundingClientRect();
    var vb = svg.viewBox.baseVal;
    var scaleX = vb.width / rect.width;
    var clientX = (evt.touches && evt.touches[0]) ? evt.touches[0].clientX : evt.clientX;
    return vb.x + (clientX - rect.left) * scaleX;
  }
  function nearestSvgHit(hits, x){
    if(!hits || !hits.length) return null;
    var best = hits[0], bestD = Math.abs(hits[0].x - x);
    for(var i = 1; i < hits.length; i++){
      var d = Math.abs(hits[i].x - x);
      if(d < bestD){ bestD = d; best = hits[i]; }
    }
    return best;
  }
  function tipHtmlFor(hit, opts, hot){
    var rows = [];
    if(opts.title) rows.push('<span>' + escapeHtml(opts.title) + '</span>');
    rows.push('<span>' + escapeHtml(String(hit.period)) + '</span>');
    if(hot){
      // A hot comparable commune replaces the commune's own value line with
      // the peer's own name and value -- never both at once, so a reader
      // never has to guess which figure belongs to which place.
      rows.push('<span>' + escapeHtml(hot.name) + '</span>');
      rows.push('<span class="bp-chart-tip-value">' + escapeHtml(hot.valueText != null ? hot.valueText : (typeof hot.value === 'number' ? String(hot.value) : '—')) + '</span>');
      if(hot.status && hot.status !== 'final') rows.push('<span>' + escapeHtml((T('v4Status_' + hot.status) || hot.status).toLocaleLowerCase(localeTag())) + '</span>');
      rows.push('<span>' + escapeHtml(T('cpOpenHint')) + '</span>');
      return rows.join('');
    }
    rows.push('<span class="bp-chart-tip-value">' + escapeHtml(hit.valueText != null ? hit.valueText : '—') + '</span>');
    (hit.compareLines || []).forEach(function(line){ rows.push('<span>' + escapeHtml(line) + '</span>'); });
    if(opts.simTipMedianText) rows.push('<span>' + escapeHtml(opts.simTipMedianText) + '</span>');
    return rows.join('');
  }
  function svgPointY(svg, evt){
    var rect = svg.getBoundingClientRect();
    var vb = svg.viewBox.baseVal;
    var scaleY = vb.height / rect.height;
    var clientY = (evt.touches && evt.touches[0]) ? evt.touches[0].clientY : evt.clientY;
    return vb.y + (clientY - rect.top) * scaleY;
  }
  function wireChartInteraction(svg, crosshairLine, crosshairDot, hits, opts){
    opts = opts || {};
    if(!hits.length) return;
    svg.classList.add('bp-ichart');
    svg.tabIndex = 0;
    svg.setAttribute('role', 'img');
    var lastHit = hits[hits.length - 1];
    var ariaTitle = opts.title != null ? opts.title : (opts.ariaBase || '');
    function ariaFor(hit){
      return ariaTitle + (hit.valueText ? ' — ' + hit.period + ' : ' + hit.valueText : '');
    }
    svg.setAttribute('aria-label', ariaFor(lastHit));
    var activeIndex = -1;
    var live = opts.live;
    // Comparable-communes hover/touch/keyboard (spec 4.9): opts.simHits is
    // only present on a line lead with the layer switched on. Every OTHER
    // caller of wireChartInteraction passes no simHits and is unaffected.
    var simHits = opts.simHits || [];
    var hotNis = null;
    function setHot(nis){
      if(hotNis === nis) return;
      if(hotNis !== null){
        svg.querySelectorAll('[data-nis="' + hotNis + '"]').forEach(function(el){ el.classList.remove('is-hot'); });
      }
      hotNis = nis;
      if(hotNis !== null){
        svg.querySelectorAll('[data-nis="' + hotNis + '"]').forEach(function(el){
          el.classList.add('is-hot');
          if(el.parentNode && el.tagName === 'path') el.parentNode.appendChild(el);
        });
      }
    }

    function show(hit, hot){
      crosshairLine.setAttribute('x1', hit.x); crosshairLine.setAttribute('x2', hit.x);
      crosshairLine.classList.add('on');
      var dotHit = hot || hit;
      if(crosshairDot){
        crosshairDot.setAttribute('cx', dotHit.x);
        if(typeof dotHit.y === 'number') crosshairDot.setAttribute('cy', dotHit.y);
        crosshairDot.classList.add('on');
      }
      var tipEl = document.querySelector('.bp-chart-tip') || (function(){
        var d = document.createElement('div'); d.className = 'bp-chart-tip'; d.setAttribute('role', 'tooltip'); d.hidden = true;
        document.body.appendChild(d); return d;
      })();
      tipEl.innerHTML = tipHtmlFor(hit, opts, hot);
      tipEl.hidden = false;
      var rect = svg.getBoundingClientRect();
      var vb = svg.viewBox.baseVal;
      var px = rect.left + (dotHit.x - vb.x) * (rect.width / vb.width);
      var py = rect.top + ((typeof dotHit.y === 'number' ? dotHit.y : vb.height / 2) - vb.y) * (rect.height / vb.height);
      tipEl.style.left = '0px'; tipEl.style.top = '0px';
      var tw = tipEl.offsetWidth, th = tipEl.offsetHeight;
      var margin = 8, vw = document.documentElement.clientWidth, vh = document.documentElement.clientHeight;
      var left = px - tw / 2;
      if(left + tw > vw - margin) left = px - tw - 10;
      if(left < margin) left = Math.min(margin, vw - tw - margin);
      var top = py - th - 14;
      if(top < margin) top = py + 14;
      if(top + th > vh - margin) top = Math.max(margin, vh - th - margin);
      tipEl.style.left = (left + window.scrollX) + 'px';
      tipEl.style.top = (top + window.scrollY) + 'px';
      if(live){
        live.textContent = hot ? (hot.name + ', ' + hot.period + ': ' + (hot.valueText || '')) : (hit.period + ', ' + (hit.valueText || ''));
      }
    }
    function hide(){
      crosshairLine.classList.remove('on');
      if(crosshairDot) crosshairDot.classList.remove('on');
      var tipEl = document.querySelector('.bp-chart-tip');
      if(tipEl) tipEl.hidden = true;
      setHot(null);
    }
    function candidatesAt(hit){
      return simHits.filter(function(h){ return h.period === hit.period; });
    }
    svg.addEventListener('pointermove', function(ev){
      if(ev.pointerType === 'touch') return;
      var x = svgPoint(svg, ev);
      var hit = nearestSvgHit(hits, x);
      if(!hit) return;
      activeIndex = hits.indexOf(hit);
      var pointerY = svgPointY(svg, ev);
      var hot = simHits.length ? BPSimilar.nearestPeer(candidatesAt(hit), pointerY, 8, hit.y) : null;
      setHot(hot ? hot.nis : null);
      show(hit, hot);
    });
    svg.addEventListener('pointerleave', function(ev){ if(ev.pointerType !== 'touch') hide(); });
    svg.addEventListener('pointerdown', function(ev){
      if(ev.pointerType === 'touch'){
        var x = svgPoint(svg, ev);
        var hit = nearestSvgHit(hits, x);
        if(hit){
          activeIndex = hits.indexOf(hit);
          var pointerY = svgPointY(svg, ev);
          var hot = simHits.length ? BPSimilar.nearestPeer(candidatesAt(hit), pointerY, 14, hit.y) : null;
          setHot(hot ? hot.nis : null);
          show(hit, hot);
          ev.preventDefault();
        }
        return;
      }
      if(hotNis !== null){
        // A click with a hot peer opens that commune; the tap/touch path
        // above never navigates (spec 4.9) -- that is the list links' job.
        window.location.search = '?nis=' + hotNis;
      }
    });
    svg.addEventListener('keydown', function(ev){
      if(ev.key === 'ArrowRight'){
        activeIndex = activeIndex < 0 ? 0 : Math.min(hits.length - 1, activeIndex + 1);
        setHot(null); show(hits[activeIndex]); ev.preventDefault();
      } else if(ev.key === 'ArrowLeft'){
        activeIndex = activeIndex < 0 ? hits.length - 1 : Math.max(0, activeIndex - 1);
        setHot(null); show(hits[activeIndex]); ev.preventDefault();
      } else if(simHits.length && activeIndex >= 0 && (ev.key === 'ArrowDown' || ev.key === 'ArrowUp')){
        var hit = hits[activeIndex];
        var cands = candidatesAt(hit).slice().sort(function(a, b){
          return (b.value - a.value) || (a.rank - b.rank);
        });
        var order = [null].concat(cands.map(function(c){ return c.nis; }));
        var curPos = order.indexOf(hotNis);
        var nextPos;
        if(ev.key === 'ArrowDown') nextPos = (curPos + 1) % order.length;
        else nextPos = (curPos - 1 + order.length) % order.length;
        var nextNis = order[nextPos];
        setHot(nextNis);
        show(hit, nextNis ? cands.filter(function(c){ return c.nis === nextNis; })[0] : null);
        ev.preventDefault();
      } else if(ev.key === 'Enter' && hotNis !== null){
        window.location.search = '?nis=' + hotNis;
        ev.preventDefault();
      } else if(ev.key === 'Escape'){
        if(hotNis !== null){ setHot(null); if(activeIndex >= 0) show(hits[activeIndex]); }
        else { activeIndex = -1; hide(); if(live) live.textContent = ''; }
        ev.preventDefault();
      }
    });
    svg.addEventListener('blur', hide);
  }
  function makeLiveRegion(afterEl){
    var live = document.createElement('span');
    live.className = 'bp-sr-only';
    live.setAttribute('aria-live', 'polite');
    if(afterEl && afterEl.parentNode) afterEl.insertAdjacentElement('afterend', live);
    return live;
  }

  new MutationObserver(function(){ themedEls = themedEls.filter(function(t){return t.el.isConnected;}); themedEls.forEach(function(t){applyThemeVars(t.el,t.index);}); }).observe(document.documentElement,{attributes:true,attributeFilter:['data-theme']});
  return {line:buildLineChartSVG,wireLine:wireLineChart,spark:buildSparklineSVG,sparkPoints:sparkPoints,wireSpark:wireSparkline,bars:buildComparisonBarSVG,wireBars:wireBarChart,theme:applyThemeVars};
};
