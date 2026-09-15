/**
 * BelPulse shared chart rendering -- Batch 2 (docs/features/page_builder.md),
 * extended by Batch A1.3 (docs/features/site_unification.md, "Graphiques :
 * ameliorer une seule couche") with one shared interactive layer.
 *
 * Generalises local.html's existing gc()/niceSteps()/drawSeries() (a single
 * line chart) into four canvas renderers that share the same axis/label
 * maths, so a block author never hand-rolls a second implementation. Reads
 * colour exclusively from --bp-chart-1..8 and the other --bp-* tokens --
 * never a hardcoded hex -- so a chart re-themes for free when tokens.css
 * changes (claude.md rule: no raw colour outside tokens.css).
 *
 * Every function takes ALREADY-RESOLVED {period, value}/{label, value} data.
 * Nothing here fetches, aggregates, or knows an indicator ID (rule 24) --
 * that is the data-binding layer's job (docs/features/data_binding.md).
 *
 * INTERACTIVE LAYER (Batch A1.3). Every draw function below still draws
 * exactly what it drew before AND now also returns
 * `{hits:[{x,y,w,h,point:{series,label,period,value,unit,status}}]}`,
 * computed from the same scales used to paint the pixels -- so a tooltip can
 * never show a number the picture disagrees with. `point.status` is
 * 'missing' only when charts.js itself could not find a numeric value at
 * that position; every other status word (suppressed, na, provisional, ...)
 * is opaque here and comes from whatever the caller put on the point --
 * charts.js holds no indicator-specific vocabulary (rule 2/24) and no i18n
 * table of its own, so `opts.statusLabels` (and `opts.missingLabel`) is how
 * a page supplies the words a reader sees for a non-final status. A gap
 * (null/undefined value) NEVER becomes 0 -- it stays a gap in the line and
 * yields a hit whose `value` is `null`.
 *
 * ALIGNMENT. A multi-series line chart aligns every series on the UNION of
 * the periods actually present, sorted, looked up by period key -- never by
 * array index. A single-series chart (the common case today) sees no change
 * at all: its own periods are that union. See `alignPeriods`.
 */
(function (global) {
  'use strict';

  function gc(name, fallback) {
    var v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return v || fallback || 'currentColor';
  }

  function chartColour(i) {
    return gc('--bp-chart-' + ((i % 8) + 1));
  }

  function niceSteps(mn, mx, n) {
    var rg = mx - mn || 1,
      ro = rg / n,
      mg = Math.pow(10, Math.floor(Math.log10(ro)));
    var rs = ro / mg,
      ni = rs <= 1.5 ? mg : rs <= 3 ? 2 * mg : rs <= 7 ? 5 * mg : 10 * mg;
    var steps = [],
      v = Math.ceil(mn / ni) * ni;
    while (v <= mx) {
      steps.push(parseFloat(v.toFixed(4)));
      v += ni;
    }
    return steps;
  }

  function fmtNum(v, locale) {
    return v.toLocaleString(locale || 'en-GB', { maximumFractionDigits: 1 });
  }

  /** Sizes a canvas for its parent's width at a fixed CSS height, DPR-aware.
   * Every renderer below starts by calling this, so a block never repeats
   * the devicePixelRatio dance local.html's drawSeries established. */
  function sizeCanvas(canvas, height) {
    var ctx = canvas.getContext('2d'),
      dpr = window.devicePixelRatio || 1;
    // The canvas's OWN laid-out width, not the parent's clientWidth.
    // components.css sets `.bp-chart-canvas-wrap canvas{width:100%}`, which
    // overrides the inline style.width this function writes -- so if the two
    // disagree the browser silently squashes the drawing to fit, and every
    // label and gridline lands in the wrong place while the chart still
    // looks plausible. Measuring what is actually on screen cannot disagree
    // with what is actually on screen. Falls back to the parent for a canvas
    // that stylesheet has not stretched, and to a sane default if neither has
    // been laid out yet.
    var W =
      Math.round(canvas.getBoundingClientRect().width) ||
      (canvas.parentElement && canvas.parentElement.clientWidth) ||
      300;
    var H = height;
    canvas.width = Math.round(W * dpr);
    canvas.height = Math.round(H * dpr);
    canvas.style.width = W + 'px';
    canvas.style.height = H + 'px';
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, W, H);
    return { ctx: ctx, W: W, H: H };
  }

  /* ------------------------------------------------------------------ *
   * Pure geometry -- no canvas, no DOM. Exposed on BPCharts so the
   * alignment and hit-test maths has a test surface that does not require
   * mocking a <canvas> or `document` (tests/test_charts_logic.py runs these
   * directly under Node). drawLine() below is a thin wrapper that calls
   * this for its numbers, then paints them, so the pixels and the hits can
   * never disagree.
   * ------------------------------------------------------------------ */

  /** The sorted union of every period key present across all series. A
   * single series's own (already-sorted) periods pass through unchanged --
   * this is only observable when two series' periods differ. */
  function alignPeriods(seriesArr) {
    var set = {};
    seriesArr.forEach(function (s) {
      (s.points || []).forEach(function (p) {
        set[p.period] = true;
      });
    });
    return Object.keys(set).sort();
  }

  /** Contiguous runs of numeric (non-null) indices in an aligned row array
   * -- a line is drawn (and area-filled) per run, so a gap breaks the path
   * instead of drawing a straight line across missing periods. */
  function numericRuns(rows) {
    var runs = [],
      cur = null;
    rows.forEach(function (r, i) {
      if (r.value != null) {
        if (!cur) {
          cur = [];
          runs.push(cur);
        }
        cur.push(i);
      } else {
        cur = null;
      }
    });
    return runs;
  }

  /**
   * @param seriesArr [{label, unit?, points:[{period,value,status?}]}]
   * @param W, H      plot size in CSS px
   * @param opts      {compact, unit}
   */
  function computeLineLayout(seriesArr, W, H, opts) {
    opts = opts || {};
    var compact = !!opts.compact;
    var periods = alignPeriods(seriesArr);
    var n = periods.length;

    var aligned = seriesArr.map(function (s) {
      var byPeriod = {};
      (s.points || []).forEach(function (p) {
        byPeriod[p.period] = p;
      });
      return periods.map(function (period) {
        var p = byPeriod[period];
        if (p && typeof p.value === 'number') {
          return { period: period, value: p.value, status: p.status || 'final' };
        }
        return { period: period, value: null, status: (p && p.status) || 'missing' };
      });
    });

    var allVals = [];
    aligned.forEach(function (rows) {
      rows.forEach(function (r) {
        if (r.value != null) allVals.push(r.value);
      });
    });
    var vMin = allVals.length ? Math.min.apply(null, allVals) : 0;
    var vMax = allVals.length ? Math.max.apply(null, allVals) : 1;
    var pad = (vMax - vMin) * 0.15 || Math.abs(vMax || 1) * 0.05;
    vMin -= pad;
    vMax += pad;
    var vR = vMax - vMin || 1;

    var pL = compact ? 2 : 62,
      pR = compact ? 2 : 10,
      pT = compact ? 4 : 12,
      pB = compact ? 4 : 26;
    var cW = W - pL - pR,
      slW = n > 1 ? cW / (n - 1) : 0;
    var yOf = function (v) {
      return pT + ((vMax - v) / vR) * (H - pT - pB);
    };
    var xOf = function (i) {
      return pL + slW * i;
    };
    var midY = pT + (H - pT - pB) / 2;

    var hits = [];
    seriesArr.forEach(function (s, si) {
      aligned[si].forEach(function (r, i) {
        hits.push({
          x: xOf(i),
          y: r.value != null ? yOf(r.value) : midY,
          w: Math.max(slW, 6),
          h: H - pT - pB,
          point: {
            series: s.label != null ? s.label : null,
            label: s.label != null ? s.label : null,
            period: r.period,
            value: r.value,
            unit: s.unit != null ? s.unit : opts.unit != null ? opts.unit : null,
            status: r.status,
          },
        });
      });
    });

    return {
      periods: periods,
      n: n,
      aligned: aligned,
      vMin: vMin,
      vMax: vMax,
      vR: vR,
      pL: pL,
      pR: pR,
      pT: pT,
      pB: pB,
      slW: slW,
      xOf: xOf,
      yOf: yOf,
      hits: hits,
    };
  }

  /** Nearest hit to pointer position `x`, by x-distance only -- the picking
   * rule this batch's spec calls for on a line/bar chart, and the one thing
   * the Node hit-test suite hand-computes. Pure: no DOM. */
  function nearestHit(hits, x) {
    if (!hits || !hits.length) return null;
    var best = hits[0],
      bestD = Math.abs(hits[0].x - x);
    for (var i = 1; i < hits.length; i++) {
      var d = Math.abs(hits[i].x - x);
      if (d < bestD) {
        bestD = d;
        best = hits[i];
      }
    }
    return best;
  }

  /**
   * Line chart with a filled area under the last series -- the exact shape
   * of local.html's single-indicator chart, generalised to N series so a
   * multi-country comparison (macro.md's international GDP chart) reuses it
   * instead of drawing N separate canvases.
   *
   * @param canvas   <canvas> element
   * @param series   [{label, unit?, points:[{period, value, status?}], colourIndex?,
   *                   colour?, dash?, isReference?}]  `colour` (a CSS colour string)
   *                   overrides the palette cycle; `dash` is a canvas setLineDash
   *                   pattern; `isReference` skips the area fill and point markers.
   *                   All three exist for a reference-average line (e.g. an EU27
   *                   average alongside up to 8 real, palette-coloured series) and
   *                   default to the plain palette-cycled behaviour otherwise.
   * @param opts     {locale}
   */
  /**
   * @param opts.height    chart height in CSS px (default 210). A KPI-sized
   *   card cannot afford a full-height chart, and scaling one down in CSS
   *   would distort its text; this draws at the size it will be shown.
   * @param opts.compact   drop the axis labels and gridlines, leaving the
   *   line alone -- for a sparkline inside a small card, where the figure is
   *   printed beside the chart and the axis would be unreadable anyway.
   * @returns {hits} -- see the file header.
   */
  function drawLine(canvas, series, opts) {
    opts = opts || {};
    if (!series.length || !series[0].points.length) return { hits: [] };
    var compact = !!opts.compact;
    var dims = sizeCanvas(canvas, opts.height || 210),
      ctx = dims.ctx,
      W = dims.W,
      H = dims.H;

    var layout = computeLineLayout(series, W, H, opts);
    var periods = layout.periods,
      n = layout.n,
      aligned = layout.aligned,
      xOf = layout.xOf,
      yOf = layout.yOf,
      pL = layout.pL,
      pB = layout.pB;
    if (!n) return { hits: [] };

    var gridC = gc('--bp-border'),
      labelC = gc('--bp-text-faint');

    if (!compact) {
      niceSteps(layout.vMin, layout.vMax, 4).forEach(function (v) {
        var y = Math.round(yOf(v)) + 0.5;
        ctx.strokeStyle = gridC;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(pL, y);
        ctx.lineTo(W - layout.pR, y);
        ctx.stroke();
        ctx.font = '10px "IBM Plex Mono",monospace';
        ctx.fillStyle = labelC;
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        ctx.fillText(fmtNum(v, opts.locale), pL - 9, y);
      });
    }

    ctx.font = '10px "IBM Plex Mono",monospace';
    ctx.fillStyle = labelC;
    ctx.textBaseline = 'top';
    // Skip interval from the MEASURED widest label, not a guessed constant.
    // A guess sized for "2019" collides as soon as the periods are quarterly
    // ("2020-Q3") or monthly, which is most of this pipeline's series.
    var widest = 0;
    periods.forEach(function (p) {
      widest = Math.max(widest, ctx.measureText(String(p)).width);
    });
    var xSkip = Math.max(1, Math.ceil((widest + 10) / Math.max(layout.slW, 1)));
    // The last label is right-anchored (see below), so its LEFT edge sits at
    // xOf(n-1) - widest, not at its centre -- a plain i % xSkip test placed
    // an ordinary centred label within that span for a dense series (24
    // quarterly points), and the two ran together. Drop any interior
    // candidate whose centre falls inside the last label's own footprint.
    var lastLabelLeftEdge = xOf(n - 1) - widest;
    periods.forEach(function (p, i) {
      if (compact) return; // a sparkline carries no axis labels at all
      if (i === n - 1) {
        // always shown, below
      } else if (i % xSkip !== 0 || xOf(i) + widest / 2 > lastLabelLeftEdge) {
        return;
      }
      // Only the LAST label needs re-anchoring: centred on the right plot
      // edge, half of it falls outside the canvas and renders clipped. The
      // first label stays centred -- it spills left into the y-axis gutter,
      // which is empty, and left-anchoring it instead pushed it right into
      // the next label.
      ctx.textAlign = i === n - 1 ? 'right' : 'center';
      ctx.fillText(p, xOf(i), H - pB + 8);
    });
    ctx.textAlign = 'center';

    series.forEach(function (s, si) {
      // `s.colour` (an explicit CSS colour string) overrides the palette
      // cycle entirely -- the Europe countries comparison charts
      // (docs/features/europe_countries.md) use it for the EU27/euro-area
      // reference lines, which must stay visually distinct from up to 8
      // real country series that already use every --bp-chart-1..8 slot.
      // `s.dash` (a canvas setLineDash pattern) is how those same
      // reference lines read as "not a country" at a glance without a
      // ninth palette colour; both default to the existing plain-solid,
      // palette-cycled behaviour for every caller that does not set them.
      var colour = s.colour || chartColour(s.colourIndex != null ? s.colourIndex : si);
      var rows = aligned[si];
      // A reference-line series is never area-filled even when it happens
      // to be last in the array (it is appended after the real series) --
      // the fill is a "this is the subject" cue that a EU/euro-area
      // average must not borrow.
      var isLastSeries = si === series.length - 1 && !s.isReference;
      var runs = numericRuns(rows);

      if (isLastSeries) {
        runs.forEach(function (run) {
          if (run.length < 2) return;
          ctx.beginPath();
          run.forEach(function (i, k) {
            var x = xOf(i),
              y = yOf(rows[i].value);
            k === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
          });
          ctx.lineTo(xOf(run[run.length - 1]), H - pB);
          ctx.lineTo(xOf(run[0]), H - pB);
          ctx.closePath();
          ctx.fillStyle = colour + '22';
          ctx.fill();
        });
      }

      runs.forEach(function (run) {
        if (run.length < 2) return;
        ctx.beginPath();
        run.forEach(function (i, k) {
          var x = xOf(i),
            y = yOf(rows[i].value);
          k === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
        });
        ctx.strokeStyle = colour;
        ctx.lineWidth = 2.2;
        ctx.lineJoin = 'round';
        ctx.lineCap = 'round';
        if (s.dash) ctx.setLineDash(s.dash);
        ctx.stroke();
        if (s.dash) ctx.setLineDash([]);
      });

      // A reference-line series draws no point markers -- the dashed line
      // itself is the cue, and a dot at every period would read as an
      // extra country among the real, marker-bearing series. `opts.markers
      // === false` (Europe countries/regions comparison charts,
      // docs/features/europe_countries.md, 2026-09-15 amendment) turns
      // markers off for every series in the chart, not just reference ones
      // -- up to 8 selected geographies each drawing a dot at every period
      // reads as visual noise on a dense multi-country line chart. Default
      // stays `true` (opts.markers left undefined) so every OTHER existing
      // caller (macro.html's other history panels via blocks.js, the
      // single-geography detail chart in europe_map.js's selectRegion/
      // selectCountryDetail) is completely unaffected.
      if (!s.isReference && opts.markers !== false) {
        var lastRealIdx = -1;
        rows.forEach(function (r, i) {
          if (r.value != null) lastRealIdx = i;
        });
        rows.forEach(function (r, i) {
          if (r.value == null) return;
          var isLast = i === lastRealIdx;
          ctx.beginPath();
          ctx.arc(xOf(i), yOf(r.value), isLast ? 4.5 : 2.5, 0, Math.PI * 2);
          ctx.fillStyle = colour;
          ctx.fill();
        });
      }
    });

    return { hits: layout.hits };
  }

  /**
   * Vertical bar chart -- macro.md's international-comparison chart (one bar
   * per country) and any single-series "value per category" case.
   * @param items [{label, value, highlight?, status?}]  highlight marks "this is the
   *   subject" (e.g. Belgium among its neighbours) with --bp-accent instead
   *   of the default palette colour, matching tokens-measured.md's own note.
   *   A null/undefined `value` draws no bar (never a 0-height bar standing
   *   in for a real zero) but still yields a hit with status 'missing'.
   * @returns {hits} -- see the file header.
   */
  function drawBar(canvas, items, opts) {
    opts = opts || {};
    if (!items.length) return { hits: [] };
    /* opts.height, like drawLine: a chart in a page-document block is sized by
       the grid cell it was placed in, and a hardcoded 210 either overflowed
       the card or left a band of white inside it. */
    var dims = sizeCanvas(canvas, opts.height || 210),
      ctx = dims.ctx,
      W = dims.W,
      H = dims.H;

    var vals = items
      .map(function (d) {
        return d.value;
      })
      .filter(function (v) {
        return typeof v === 'number';
      });
    var vMax = Math.max.apply(null, vals.concat([0])),
      vMin = Math.min.apply(null, vals.concat([0]));
    var pad = (vMax - vMin) * 0.1 || Math.abs(vMax || 1) * 0.05;
    vMax += pad;
    var vR = vMax - vMin || 1;
    var pL = 46,
      pR = 10,
      pT = 12,
      pB = 30;
    var n = items.length;
    var cW = W - pL - pR;
    var slotW = cW / n;
    var barW = Math.min(44, slotW * 0.55);
    var yOf = function (v) {
      return pT + ((vMax - v) / vR) * (H - pT - pB);
    };
    var zeroY = yOf(0);

    var gridC = gc('--bp-border'),
      labelC = gc('--bp-text-faint');
    niceSteps(vMin, vMax, 4).forEach(function (v) {
      var y = Math.round(yOf(v)) + 0.5;
      ctx.strokeStyle = gridC;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pL, y);
      ctx.lineTo(W - pR, y);
      ctx.stroke();
      ctx.font = '10px "IBM Plex Mono",monospace';
      ctx.fillStyle = labelC;
      ctx.textAlign = 'right';
      ctx.textBaseline = 'middle';
      ctx.fillText(fmtNum(v, opts.locale), pL - 9, y);
    });

    /* ONE COLOUR WHEN THE BARS ARE ONE SERIES. The palette runs per bar
       because this chart's original case is one bar per COUNTRY, where the
       colour identifies the country. A commune's tax take over nineteen years
       is one series, and colouring each year differently says there are
       nineteen things here rather than one line of history -- it read as a
       rainbow on the profile page. The caller says which case it is. */
    var single = typeof opts.colourIndex === 'number';

    /* Label thinning, measured rather than guessed -- the same rule drawLine
       already applies. Nineteen four-character years in 460px collided into a
       grey smear; the most recent bar keeps its label, being the one a reader
       looks for first. */
    ctx.font = '10px "IBM Plex Mono",monospace';
    var widestLabel = 0;
    items.forEach(function (d) {
      widestLabel = Math.max(widestLabel, ctx.measureText(String(d.label)).width);
    });
    var labelEvery = Math.max(1, Math.ceil((widestLabel + 8) / Math.max(slotW, 1)));

    var hits = [];
    items.forEach(function (d, i) {
      var cx = pL + slotW * i + slotW / 2;
      var hasVal = typeof d.value === 'number';
      var y = hasVal ? yOf(d.value) : zeroY;
      if (hasVal) {
        var top = Math.min(y, zeroY),
          h = Math.abs(zeroY - y);
        ctx.fillStyle = d.highlight
          ? gc('--bp-accent')
          : chartColour(single ? opts.colourIndex : i);
        ctx.fillRect(cx - barW / 2, top, barW, h || 1);
      }

      if ((n - 1 - i) % labelEvery === 0) {
        ctx.font = '10px "IBM Plex Mono",monospace';
        ctx.fillStyle = labelC;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        ctx.fillText(d.label, cx, H - pB + 8);
      }

      hits.push({
        x: cx,
        y: y,
        w: slotW,
        h: H - pT - pB,
        point: {
          series: opts.seriesLabel || null,
          label: d.label,
          period: d.period != null ? d.period : d.label,
          value: hasVal ? d.value : null,
          unit: opts.unit || null,
          status: d.status || (hasVal ? 'final' : 'missing'),
        },
      });
    });
    return { hits: hits };
  }

  /**
   * Donut chart -- Namur's age-band breakdown (municipality-profile.md).
   * @param segments [{label, value, colourIndex?, status?}]  a segment whose
   *   `value` is not a number draws no slice (rule 26: a missing share is
   *   not a zero share) but still yields a hit so a data table/keyboard user
   *   can see it was there and unavailable.
   * @returns {hits} -- see the file header.
   */
  function drawDonut(canvas, segments, opts) {
    opts = opts || {};
    var numeric = segments.filter(function (s) {
      return typeof s.value === 'number';
    });
    var total = numeric.reduce(function (s, d) {
      return s + d.value;
    }, 0);
    var dims = sizeCanvas(canvas, opts.height || 210),
      ctx = dims.ctx,
      W = dims.W,
      H = dims.H;
    var cx = W / 2,
      cy = H / 2,
      r = Math.min(W, H) / 2 - 8,
      inner = r * 0.6;

    var hits = [];
    if (!total) {
      segments.forEach(function (seg) {
        hits.push({
          x: cx,
          y: cy,
          w: 0,
          h: 0,
          point: {
            series: null,
            label: seg.label,
            period: null,
            value: typeof seg.value === 'number' ? seg.value : null,
            unit: opts.unit || null,
            status: seg.status || (typeof seg.value === 'number' ? 'final' : 'missing'),
          },
        });
      });
      return { hits: hits };
    }

    var start = -Math.PI / 2;
    segments.forEach(function (seg, i) {
      var midAngle;
      if (typeof seg.value === 'number') {
        var frac = seg.value / total;
        var end = start + frac * Math.PI * 2;
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.arc(cx, cy, r, start, end);
        ctx.closePath();
        ctx.fillStyle = chartColour(seg.colourIndex != null ? seg.colourIndex : i);
        ctx.fill();
        midAngle = (start + end) / 2;
        start = end;
      } else {
        midAngle = start;
      }
      hits.push({
        x: cx + Math.cos(midAngle) * ((r + inner) / 2),
        y: cy + Math.sin(midAngle) * ((r + inner) / 2),
        w: 8,
        h: 8,
        point: {
          series: null,
          label: seg.label,
          period: null,
          value: typeof seg.value === 'number' ? seg.value : null,
          unit: opts.unit || null,
          status: seg.status || (typeof seg.value === 'number' ? 'final' : 'missing'),
        },
      });
    });

    ctx.globalCompositeOperation = 'destination-out';
    ctx.beginPath();
    ctx.arc(cx, cy, inner, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalCompositeOperation = 'source-over';
    return { hits: hits };
  }

  /**
   * Horizontal ranking bars -- the "N / 581" ranking-list pattern
   * (component-inventory.md), drawn as a bar rather than just text so a
   * position among many communes is visually legible.
   * @param items [{label, value, highlight?, status?}]  pre-sorted by caller
   * @returns {hits} -- see the file header.
   */
  function drawRanking(canvas, items, opts) {
    opts = opts || {};
    if (!items.length) return { hits: [] };
    var rowH = 26,
      dims = sizeCanvas(canvas, items.length * rowH + 10),
      ctx = dims.ctx,
      W = dims.W;
    var vals = items
      .map(function (d) {
        return d.value;
      })
      .filter(function (v) {
        return typeof v === 'number';
      });
    var vMax = vals.length ? Math.max.apply(null, vals) : 0;
    var pL = 120,
      pR = 46;
    var barMaxW = W - pL - pR;
    var labelC = gc('--bp-text-muted'),
      valueC = gc('--bp-text');

    var hits = [];
    items.forEach(function (d, i) {
      var y = i * rowH + 6;
      var hasVal = typeof d.value === 'number';
      var w = hasVal && vMax > 0 ? (d.value / vMax) * barMaxW : 0;
      ctx.font = '11px "Inter","IBM Plex Sans",sans-serif';
      ctx.fillStyle = labelC;
      ctx.textAlign = 'right';
      ctx.textBaseline = 'middle';
      ctx.fillText(d.label, pL - 10, y + rowH / 2 - 3);

      if (hasVal) {
        ctx.fillStyle = d.highlight ? gc('--bp-accent') : chartColour(i);
        ctx.fillRect(pL, y, Math.max(w, 2), rowH - 12);

        ctx.font = '11px "IBM Plex Mono",monospace';
        ctx.fillStyle = valueC;
        ctx.textAlign = 'left';
        ctx.fillText(fmtNum(d.value, opts.locale), pL + w + 6, y + rowH / 2 - 3);
      }

      hits.push({
        x: pL + w / 2,
        y: y + rowH / 2,
        w: barMaxW,
        h: rowH,
        point: {
          series: opts.seriesLabel || null,
          label: d.label,
          period: null,
          value: hasVal ? d.value : null,
          unit: opts.unit || null,
          status: d.status || (hasVal ? 'final' : 'missing'),
        },
      });
    });
    return { hits: hits };
  }

  /* ------------------------------------------------------------------ *
   * Shared tooltip / keyboard / data-table layer.
   * ------------------------------------------------------------------ */

  var sharedTip = null;
  function ensureTip() {
    if (sharedTip && document.body.contains(sharedTip)) return sharedTip;
    sharedTip = document.createElement('div');
    sharedTip.className = 'bp-chart-tip';
    sharedTip.setAttribute('role', 'tooltip');
    sharedTip.hidden = true;
    document.body.appendChild(sharedTip);
    return sharedTip;
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  /** The reader-facing text for one point's value: the unit follows the
   * caller's language (via the caller's own locale-aware formatter, per
   * opts.locale) and a missing/non-final value never prints as 0 -- it prints
   * the caller-supplied word for its status, or an em dash if none was
   * given. */
  /**
   * `opts.formatValue`, when the caller supplies one, is a full
   * `(value, unit, decimals, locale) -> string` formatter -- e.g.
   * `MapUI.formatValue`, which already turns "percent_yy" into "2,2%" and
   * "eur" into "€1 234" the same way every other number on the site is
   * shown. Without one, charts.js falls back to appending the RAW unit
   * string after the number -- correct for a page that never loads MapUI,
   * but a caller that has it (every page on this site does) should always
   * pass it, or a reader sees an internal unit code (rule 7). */
  function fmtTipValue(point, opts) {
    opts = opts || {};
    if (point.value == null) {
      var vocab = opts.statusLabels || {};
      return vocab[point.status] || opts.missingLabel || vocab.missing || '—';
    }
    var decimals = opts.decimals != null ? opts.decimals : 1;
    if (typeof opts.formatValue === 'function') {
      return opts.formatValue(point.value, point.unit, decimals, opts.locale);
    }
    var txt = point.value.toLocaleString(opts.locale || 'en-GB', { maximumFractionDigits: decimals });
    if (point.unit) txt += ' ' + point.unit;
    return txt;
  }

  function fmtStatusWord(status, opts) {
    if (!status || status === 'final') return '';
    var vocab = (opts && opts.statusLabels) || {};
    return vocab[status] || status;
  }

  function tipFields(point, opts) {
    var name = point.series || point.territory || point.label;
    return {
      name: name ? String(name) : '',
      period: point.period != null ? String(point.period) : '',
      // Always ONE string -- fmtTipValue() already combines the number and
      // its unit (via opts.formatValue when the caller gave one), so this
      // never needs to be split across two spans/lines.
      value: fmtTipValue(point, opts),
      status: fmtStatusWord(point.status, opts),
    };
  }

  /** `.bp-chart-tip-value` gets `white-space:nowrap` in components.css --
   * short by construction ("2,2%"), it should never wrap. The series/
   * territory NAME is the one line long enough to legitimately wrap, and
   * components.css gives the tooltip a min-width so it wraps at most once. */
  function renderTipHTML(point, opts) {
    var f = tipFields(point, opts);
    var rows = [];
    if (f.name) rows.push('<span>' + escapeHtml(f.name) + '</span>');
    if (f.period) rows.push('<span>' + escapeHtml(f.period) + '</span>');
    rows.push('<span class="bp-chart-tip-value">' + escapeHtml(f.value) + '</span>');
    if (f.status) rows.push('<span>' + escapeHtml(f.status) + '</span>');
    return rows.join('');
  }

  function renderTipText(point, opts) {
    var f = tipFields(point, opts);
    return [f.name, f.period, f.value, f.status].filter(Boolean).join(', ');
  }

  /**
   * One shared `<div class="bp-chart-tip" role="tooltip">` for the whole
   * page, positioned over the nearest data point. Wires pointer hover,
   * touch (pin on tap), and keyboard (Left/Right steps between points,
   * Escape clears) on `canvas` itself, plus a visually-hidden
   * `aria-live="polite"` region so a screen-reader user hears what a mouse
   * user sees.
   *
   * `model` is any object carrying a live `.hits` array -- the same object
   * `drawLine`/`drawBar`/... return. Update `model.hits` in place after a
   * redraw (e.g. `model.hits = BPCharts.drawLine(canvas, series, o).hits`)
   * and the tooltip picks up the new geometry with no further wiring.
   *
   * @param opts {locale, decimals, unit, statusLabels, missingLabel, ariaLabel}
   */
  function attachTooltip(canvas, model, opts) {
    opts = opts || {};
    canvas.tabIndex = 0;
    canvas.setAttribute('role', 'img');
    if (opts.ariaLabel) canvas.setAttribute('aria-label', opts.ariaLabel);

    var live = document.createElement('span');
    live.className = 'bp-sr-only';
    live.setAttribute('aria-live', 'polite');
    if (canvas.parentNode) canvas.insertAdjacentElement('afterend', live);

    var activeIndex = -1;

    function hitsOf() {
      return (model && model.hits) || [];
    }

    function showAt(hit) {
      if (!hit) return;
      var tip = ensureTip();
      tip.innerHTML = renderTipHTML(hit.point, opts);
      tip.hidden = false;
      var rect = canvas.getBoundingClientRect();
      var pointX = rect.left + hit.x,
        pointY = rect.top + hit.y;
      // Measure the tip's OWN natural size first, at a throwaway position
      // that cannot itself constrain that width (see the note below), then
      // place it from that real size -- centred above the point by default,
      // but flipped/clamped to stay inside the viewport near an edge.
      tip.style.left = '0px';
      tip.style.top = '0px';
      var tw = tip.offsetWidth,
        th = tip.offsetHeight;
      var margin = 8,
        vw = document.documentElement.clientWidth,
        vh = document.documentElement.clientHeight;
      var left = pointX - tw / 2;
      if (left + tw > vw - margin) left = pointX - tw - 10; // flip to the point's LEFT near the right edge
      if (left < margin) left = Math.min(margin, vw - tw - margin);
      var top = pointY - th - 10;
      if (top < margin) top = pointY + 14; // flip below near the viewport's top edge
      if (top + th > vh - margin) top = Math.max(margin, vh - th - margin);
      tip.style.left = left + window.scrollX + 'px';
      tip.style.top = top + window.scrollY + 'px';
      live.textContent = renderTipText(hit.point, opts);
    }

    function hide() {
      if (sharedTip) sharedTip.hidden = true;
    }

    canvas.addEventListener('pointermove', function (ev) {
      if (ev.pointerType === 'touch') return; // touch pins on pointerdown instead
      var rect = canvas.getBoundingClientRect();
      var hits = hitsOf();
      var hit = nearestHit(hits, ev.clientX - rect.left);
      if (hit) {
        activeIndex = hits.indexOf(hit);
        showAt(hit);
      }
    });
    canvas.addEventListener('pointerleave', function (ev) {
      if (ev.pointerType === 'touch') return;
      hide();
    });
    canvas.addEventListener('pointerdown', function (ev) {
      if (ev.pointerType !== 'touch') return;
      var rect = canvas.getBoundingClientRect();
      var hits = hitsOf();
      var hit = nearestHit(hits, ev.clientX - rect.left);
      if (hit) {
        activeIndex = hits.indexOf(hit);
        showAt(hit);
        ev.preventDefault();
      }
    });
    canvas.addEventListener('keydown', function (ev) {
      var hits = hitsOf();
      if (!hits.length) return;
      if (ev.key === 'ArrowRight') {
        activeIndex = activeIndex < 0 ? 0 : Math.min(hits.length - 1, activeIndex + 1);
        showAt(hits[activeIndex]);
        ev.preventDefault();
      } else if (ev.key === 'ArrowLeft') {
        activeIndex = activeIndex < 0 ? hits.length - 1 : Math.max(0, activeIndex - 1);
        showAt(hits[activeIndex]);
        ev.preventDefault();
      } else if (ev.key === 'Escape') {
        activeIndex = -1;
        hide();
        live.textContent = '';
        ev.preventDefault();
      }
    });

    return { hide: hide, showAt: showAt, liveRegion: live };
  }

  /**
   * A `<details class="bp-chart-data"><summary>...</summary><table>` with
   * period, value+unit and status for every point behind `model.hits` --
   * everything the tooltip can show, readable without hovering anything, and
   * the thing a statistical claim can point at instead of depending on a
   * tooltip alone.
   *
   * @param opts {summary, seriesLabel_, periodLabel, valueLabel, statusLabel, locale, decimals, statusLabels}
   */
  function dataTable(container, model, opts) {
    opts = opts || {};
    var hits = (model && model.hits) || [];
    var existing = container.querySelector(':scope > details.bp-chart-data');
    if (existing) existing.remove();

    var details = document.createElement('details');
    details.className = 'bp-chart-data';
    var summary = document.createElement('summary');
    summary.textContent = opts.summary || 'Data table';
    details.appendChild(summary);

    var showSeries =
      opts.seriesColumn !== false &&
      hits.some(function (h) {
        return h.point.series || h.point.territory;
      });

    var table = document.createElement('table');
    var thead = document.createElement('thead');
    var headRow = document.createElement('tr');
    var cols = [];
    if (showSeries) cols.push(opts.seriesLabel_ || 'Series');
    cols.push(opts.periodLabel || 'Period', opts.valueLabel || 'Value', opts.statusLabel || 'Status');
    cols.forEach(function (c) {
      var th = document.createElement('th');
      th.textContent = c;
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    var tbody = document.createElement('tbody');
    hits.forEach(function (h) {
      var p = h.point;
      var tr = document.createElement('tr');
      if (showSeries) {
        var tdS0 = document.createElement('td');
        tdS0.textContent = p.series || p.territory || '';
        tr.appendChild(tdS0);
      }
      var tdP = document.createElement('td');
      tdP.textContent = p.period != null ? p.period : p.label || '';
      tr.appendChild(tdP);
      var tdV = document.createElement('td');
      tdV.textContent = fmtTipValue(p, opts);
      tr.appendChild(tdV);
      var tdSt = document.createElement('td');
      tdSt.textContent = fmtStatusWord(p.status, opts) || opts.finalLabel || '';
      tr.appendChild(tdSt);
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    details.appendChild(table);
    container.appendChild(details);
    return details;
  }

  /* ------------------------------------------------------------------ *
   * Redraw-on-resize/theme/lang registry.
   * ------------------------------------------------------------------ */

  var registry = [];
  var ro = null;
  var rafPending = false;

  function scheduleRedraw() {
    if (rafPending) return;
    rafPending = true;
    var raf = global.requestAnimationFrame || function (fn) { return setTimeout(fn, 16); };
    raf(function () {
      rafPending = false;
      redrawAll();
    });
  }

  /** Registers a canvas for automatic redraw on resize (via one shared,
   * rAF-coalesced ResizeObserver -- never one observer per chart) and on the
   * `bp:theme`/`bp:lang` window events another batch's theme/language picker
   * will dispatch (nothing dispatches them yet; redrawing on an event no one
   * fires yet is harmless). `drawFn` takes no arguments and re-draws (and
   * re-attaches hits to) that one canvas. */
  function register(canvas, drawFn) {
    registry.push({ canvas: canvas, drawFn: drawFn });
    if (!ro && typeof ResizeObserver !== 'undefined') {
      ro = new ResizeObserver(function () {
        scheduleRedraw();
      });
    }
    if (ro) ro.observe(canvas.parentElement || canvas);
    return function unregister() {
      registry = registry.filter(function (e) {
        return e.canvas !== canvas;
      });
      if (ro) ro.unobserve(canvas.parentElement || canvas);
    };
  }

  function redrawAll() {
    registry.forEach(function (e) {
      if (document.body.contains(e.canvas)) e.drawFn();
    });
  }

  if (typeof window !== 'undefined') {
    window.addEventListener('bp:theme', scheduleRedraw);
    window.addEventListener('bp:lang', scheduleRedraw);
  }

  /* ------------------------------------------------------------------ *
   * Non-canvas anchors (the age pyramid, wired in a later batch): the same
   * tooltip display, anchored to an arbitrary element instead of a hit.
   * ------------------------------------------------------------------ */

  function showTip(anchorEl, point, opts) {
    opts = opts || {};
    var tip = ensureTip();
    tip.innerHTML = renderTipHTML(point, opts);
    tip.hidden = false;
    var rect = anchorEl.getBoundingClientRect();
    tip.style.left = rect.left + window.scrollX + rect.width / 2 + 'px';
    tip.style.top = rect.top + window.scrollY + 'px';
  }

  function hideTip() {
    if (sharedTip) sharedTip.hidden = true;
  }

  global.BPCharts = {
    niceSteps: niceSteps,
    drawLine: drawLine,
    drawBar: drawBar,
    drawDonut: drawDonut,
    drawRanking: drawRanking,
    alignPeriods: alignPeriods,
    computeLineLayout: computeLineLayout,
    nearestHit: nearestHit,
    attachTooltip: attachTooltip,
    dataTable: dataTable,
    register: register,
    redrawAll: redrawAll,
    showTip: showTip,
    hideTip: hideTip,
  };
  if (typeof module !== 'undefined') module.exports = global.BPCharts;
})(typeof window !== 'undefined' ? window : global);
