/**
 * BelPulse shared chart rendering -- Batch 2 (docs/features/page_builder.md).
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

  /**
   * Line chart with a filled area under the last series -- the exact shape
   * of local.html's single-indicator chart, generalised to N series so a
   * multi-country comparison (macro.md's international GDP chart) reuses it
   * instead of drawing N separate canvases.
   *
   * @param canvas   <canvas> element
   * @param series   [{label, points:[{period, value}], colourIndex?}]
   * @param opts     {locale}
   */
  function drawLine(canvas, series, opts) {
    opts = opts || {};
    if (!series.length || !series[0].points.length) return;
    var dims = sizeCanvas(canvas, 210),
      ctx = dims.ctx,
      W = dims.W,
      H = dims.H;

    var allVals = [];
    series.forEach(function (s) {
      s.points.forEach(function (p) {
        allVals.push(p.value);
      });
    });
    var vMin = Math.min.apply(null, allVals),
      vMax = Math.max.apply(null, allVals);
    var pad = (vMax - vMin) * 0.15 || Math.abs(vMax || 1) * 0.05;
    vMin -= pad;
    vMax += pad;
    var vR = vMax - vMin || 1;
    var pL = 62,
      pR = 10,
      pT = 12,
      pB = 26;
    var n = series[0].points.length;
    var cW = W - pL - pR,
      slW = n > 1 ? cW / (n - 1) : 0;
    var yOf = function (v) {
      return pT + ((vMax - v) / vR) * (H - pT - pB);
    };
    var xOf = function (i) {
      return pL + slW * i;
    };

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

    ctx.font = '10px "IBM Plex Mono",monospace';
    ctx.fillStyle = labelC;
    ctx.textBaseline = 'top';
    // Skip interval from the MEASURED widest label, not a guessed constant.
    // A guess sized for "2019" collides as soon as the periods are quarterly
    // ("2020-Q3") or monthly, which is most of this pipeline's series.
    var widest = 0;
    series[0].points.forEach(function (p) {
      widest = Math.max(widest, ctx.measureText(String(p.period)).width);
    });
    var xSkip = Math.max(1, Math.ceil((widest + 10) / Math.max(slW, 1)));
    // The last label is right-anchored (see below), so its LEFT edge sits at
    // xOf(n-1) - widest, not at its centre -- a plain i % xSkip test placed
    // an ordinary centred label within that span for a dense series (24
    // quarterly points), and the two ran together. Drop any interior
    // candidate whose centre falls inside the last label's own footprint.
    var lastLabelLeftEdge = xOf(n - 1) - widest;
    series[0].points.forEach(function (p, i) {
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
      ctx.fillText(p.period, xOf(i), H - pB + 8);
    });
    ctx.textAlign = 'center';

    series.forEach(function (s, si) {
      var colour = chartColour(s.colourIndex != null ? s.colourIndex : si);
      var pts = s.points;
      var isLastSeries = si === series.length - 1;

      if (isLastSeries && pts.length > 1) {
        ctx.beginPath();
        ctx.moveTo(xOf(0), yOf(pts[0].value));
        pts.forEach(function (p, i) {
          ctx.lineTo(xOf(i), yOf(p.value));
        });
        ctx.lineTo(xOf(pts.length - 1), H - pB);
        ctx.lineTo(xOf(0), H - pB);
        ctx.closePath();
        ctx.fillStyle = colour + '22';
        ctx.fill();
      }

      ctx.beginPath();
      pts.forEach(function (p, i) {
        i === 0 ? ctx.moveTo(xOf(i), yOf(p.value)) : ctx.lineTo(xOf(i), yOf(p.value));
      });
      ctx.strokeStyle = colour;
      ctx.lineWidth = 2.2;
      ctx.lineJoin = 'round';
      ctx.lineCap = 'round';
      ctx.stroke();

      pts.forEach(function (p, i) {
        var isLast = i === pts.length - 1;
        ctx.beginPath();
        ctx.arc(xOf(i), yOf(p.value), isLast ? 4.5 : 2.5, 0, Math.PI * 2);
        ctx.fillStyle = colour;
        ctx.fill();
      });
    });
  }

  /**
   * Vertical bar chart -- macro.md's international-comparison chart (one bar
   * per country) and any single-series "value per category" case.
   * @param items [{label, value, highlight?}]  highlight marks "this is the
   *   subject" (e.g. Belgium among its neighbours) with --bp-accent instead
   *   of the default palette colour, matching tokens-measured.md's own note.
   */
  function drawBar(canvas, items, opts) {
    opts = opts || {};
    if (!items.length) return;
    var dims = sizeCanvas(canvas, 210),
      ctx = dims.ctx,
      W = dims.W,
      H = dims.H;

    var vals = items.map(function (d) {
      return d.value;
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

    items.forEach(function (d, i) {
      var cx = pL + slotW * i + slotW / 2;
      var y = yOf(d.value);
      var top = Math.min(y, zeroY),
        h = Math.abs(zeroY - y);
      ctx.fillStyle = d.highlight ? gc('--bp-accent') : chartColour(i);
      ctx.fillRect(cx - barW / 2, top, barW, h || 1);

      ctx.font = '10px "IBM Plex Mono",monospace';
      ctx.fillStyle = labelC;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      ctx.fillText(d.label, cx, H - pB + 8);
    });
  }

  /**
   * Donut chart -- Namur's age-band breakdown (municipality-profile.md).
   * @param segments [{label, value, colourIndex?}]
   */
  function drawDonut(canvas, segments, opts) {
    opts = opts || {};
    var total = segments.reduce(function (s, d) {
      return s + d.value;
    }, 0);
    if (!total) return;
    var dims = sizeCanvas(canvas, 210),
      ctx = dims.ctx,
      W = dims.W,
      H = dims.H;
    var cx = W / 2,
      cy = H / 2,
      r = Math.min(W, H) / 2 - 8,
      inner = r * 0.6;

    var start = -Math.PI / 2;
    segments.forEach(function (seg, i) {
      var frac = seg.value / total;
      var end = start + frac * Math.PI * 2;
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.arc(cx, cy, r, start, end);
      ctx.closePath();
      ctx.fillStyle = chartColour(seg.colourIndex != null ? seg.colourIndex : i);
      ctx.fill();
      start = end;
    });

    ctx.globalCompositeOperation = 'destination-out';
    ctx.beginPath();
    ctx.arc(cx, cy, inner, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalCompositeOperation = 'source-over';
  }

  /**
   * Horizontal ranking bars -- the "N / 581" ranking-list pattern
   * (component-inventory.md), drawn as a bar rather than just text so a
   * position among many communes is visually legible.
   * @param items [{label, value, highlight?}]  pre-sorted by caller
   */
  function drawRanking(canvas, items, opts) {
    opts = opts || {};
    if (!items.length) return;
    var rowH = 26,
      dims = sizeCanvas(canvas, items.length * rowH + 10),
      ctx = dims.ctx,
      W = dims.W;
    var vMax = Math.max.apply(
      null,
      items.map(function (d) {
        return d.value;
      })
    );
    var pL = 120,
      pR = 46;
    var barMaxW = W - pL - pR;
    var labelC = gc('--bp-text-muted'),
      valueC = gc('--bp-text');

    items.forEach(function (d, i) {
      var y = i * rowH + 6;
      var w = vMax > 0 ? (d.value / vMax) * barMaxW : 0;
      ctx.font = '11px "Inter","IBM Plex Sans",sans-serif';
      ctx.fillStyle = labelC;
      ctx.textAlign = 'right';
      ctx.textBaseline = 'middle';
      ctx.fillText(d.label, pL - 10, y + rowH / 2 - 3);

      ctx.fillStyle = d.highlight ? gc('--bp-accent') : chartColour(i);
      ctx.fillRect(pL, y, Math.max(w, 2), rowH - 12);

      ctx.font = '11px "IBM Plex Mono",monospace';
      ctx.fillStyle = valueC;
      ctx.textAlign = 'left';
      ctx.fillText(fmtNum(d.value, opts.locale), pL + w + 6, y + rowH / 2 - 3);
    });
  }

  global.BPCharts = {
    niceSteps: niceSteps,
    drawLine: drawLine,
    drawBar: drawBar,
    drawDonut: drawDonut,
    drawRanking: drawRanking,
  };
  if (typeof module !== 'undefined') module.exports = global.BPCharts;
})(typeof window !== 'undefined' ? window : global);
