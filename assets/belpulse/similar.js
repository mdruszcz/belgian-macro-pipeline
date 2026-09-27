/**
 * BelPulse comparable-communes layer -- pure logic (PR C,
 * docs/features/peer_model.md, docs/features/commune_portrait.md).
 *
 * Attached as `BPSimilar` the way charts.js attaches `BPCharts`. Every
 * function here is pure: no DOM, no fetch, and no indicator id (rule 24) --
 * commune.html's own script passes in already-resolved data and reads
 * config-driven metadata flags (meta.additive, meta.peer_deviation,
 * entry.selection_variable) rather than this module knowing what any
 * indicator IS.
 *
 * NAMING. "peer" already means province mates elsewhere in commune.html
 * (peerGeographies, buildPeerStrip, loadLeadPeerChart, .peerstrip,
 * .leadpeer, v4PeerChartTitle) -- left untouched. Everything in this module,
 * and every DOM class/id it implies, uses `sim` / `.sim-` / `simXxx` only.
 * No user-facing string contains "peer" (checked by
 * tests/test_similar_logic.py's static scan of assets/i18n.js).
 *
 * The page computes no statistic (rule 4/27): every number here is read
 * straight off public/data/peers/<nis>.json (median, position, deviation),
 * never recomputed from the raw series.
 */
(function (global) {
  'use strict';

  /* ---- list selection --------------------------------------------------
     'off' | 'region' | 'national'. Persisted to localStorage
     'belpulse-similar' by the caller; this function only decides what a
     (possibly absent, possibly corrupted) stored value should resolve to. */
  var VALID_LISTS = ['off', 'region', 'national'];
  function resolveList(stored, saveData) {
    if (VALID_LISTS.indexOf(stored) >= 0) return stored;
    if (saveData === true) return 'off';
    return 'region';
  }

  /* ---- benchmark state ---------------------------------------------------
     benchmarkState(sim, list, code, latest, meta) -> {state, entry, withheld, other}

     sim: the parsed peers/<nis>.json payload, or null/undefined.
     list: 'off' | 'region' | 'national'.
     code: the indicator id (used only to index into sim.lists/sim.withheld,
       never inspected for its own meaning -- rule 24).
     latest: {period, value} -- the commune's own latest numeric point, as
       already resolved by the caller from state.profile.indicators[code].
     meta: {additive, peer_deviation} from metadata/indicators.json.
  */
  var CONSISTENCY_EPS_FLOOR = 1;
  function _entryConsistent(entry, list, sim, latest) {
    if (!entry || !latest) return false;
    if (entry.period !== latest.period) return false;
    var tol = 1e-6 * Math.max(1, Math.abs(latest.value));
    if (Math.abs(entry.value - latest.value) > tol) return false;
    if (entry.of !== entry.peers_with_value + 1) return false;
    var members = (sim.peers && sim.peers[list]) || [];
    if (
      sim.min_peers_with_value > entry.peers_with_value ||
      entry.peers_with_value > members.length
    ) {
      return false;
    }
    if (entry.position < 1 || entry.position > entry.of) return false;
    return true;
  }

  function _withheldState(withheld) {
    if (!withheld || !withheld.reason) return 'generic';
    if (withheld.reason === 'excluded') return 'excluded';
    if (withheld.reason === 'few_peers') return 'few_peers';
    if (withheld.reason === 'no_current_value') {
      return withheld.own_period ? 'stale' : 'generic';
    }
    // PR #287 (peer model, not yet merged as of this writing): two more
    // specific reasons for the commune's OWN row existing at the current
    // period but not being usable -- distinct from 'stale' (no_current_value
    // with an own_period, i.e. an older period is available) and from
    // 'generic' (nothing else fits). Both carry {reason, period, own_period}
    // the same shape as no_current_value. An unrecognised reason still
    // falls back to 'generic' below, so this module works whether or not
    // #287 has merged (rule 26: suppressed/na/missing/zero never collapse).
    if (withheld.reason === 'suppressed') return 'suppressed';
    if (withheld.reason === 'na') return 'na';
    return 'generic';
  }

  function _otherListOk(sim, otherList, code, meta) {
    if (!sim || !sim.lists) return false;
    var entry = sim.lists[otherList] && sim.lists[otherList][code];
    if (!entry) return false;
    if (meta.peer_deviation === 'none') return true;
    return true; // an entry exists at all -> 'ok' or one of the no_pct_* states
  }

  function benchmarkState(sim, list, code, latest, meta) {
    meta = meta || {};
    if (meta.additive !== false || list === 'off' || !sim) {
      return { state: 'none', entry: null, withheld: null, other: false };
    }

    var listEntries = (sim.lists && sim.lists[list]) || {};
    var listWithheld = (sim.withheld && sim.withheld[list]) || {};
    var entry = listEntries[code] || null;
    var withheld = listWithheld[code] || null;
    var otherList = list === 'region' ? 'national' : 'region';
    var other = _otherListOk(sim, otherList, code, meta);

    if (entry) {
      if (!_entryConsistent(entry, list, sim, latest)) {
        return { state: 'mismatch', entry: entry, withheld: withheld, other: other };
      }
      if (meta.peer_deviation === 'none') {
        return { state: 'no_pct_config', entry: entry, withheld: withheld, other: other };
      }
      if (entry.deviation_pct === null || entry.deviation_pct === undefined) {
        var reason = entry.deviation_withheld;
        var state =
          reason === 'median_zero'
            ? 'no_pct_zero'
            : reason === 'median_negative'
            ? 'no_pct_negative'
            : 'no_pct_other';
        return { state: state, entry: entry, withheld: withheld, other: other };
      }
      return { state: 'ok', entry: entry, withheld: withheld, other: other };
    }

    return { state: _withheldState(withheld), entry: null, withheld: withheld, other: other };
  }

  /* ---- % deviation formatting -------------------------------------------
     deviationParts(d) -> {direction: 'above'|'below'|'equal', tiny: bool,
       magnitude: number, decimals: 0|1}
     d is the PUBLISHED deviation_pct (never recomputed -- rule 27). */
  function deviationParts(d) {
    if (d === 0) return { direction: 'equal', tiny: false, magnitude: 0, decimals: 1 };
    var direction = d > 0 ? 'above' : 'below';
    var abs = Math.abs(d);
    if (abs < 0.05) {
      return { direction: direction, tiny: true, magnitude: 0.1, decimals: 1 };
    }
    var r = Math.round(abs * 10) / 10;
    if (r >= 100) {
      return { direction: direction, tiny: false, magnitude: Math.round(abs), decimals: 0 };
    }
    return { direction: direction, tiny: false, magnitude: r, decimals: 1 };
  }

  /* ---- ordinal words ------------------------------------------------- */
  function ordinal(n, lang) {
    if (lang === 'fr' || lang === 'nl') return n + 'e';
    var mod100 = n % 100;
    if (mod100 >= 11 && mod100 <= 13) return n + 'th';
    switch (n % 10) {
      case 1:
        return n + 'st';
      case 2:
        return n + 'nd';
      case 3:
        return n + 'rd';
      default:
        return n + 'th';
    }
  }

  /* ---- position wording --------------------------------------------- */
  function positionParts(e, lang) {
    if (e.position === 1) return { key: 'simPosHighest', ord: null };
    if (e.position === e.of) return { key: 'simPosLowest', ord: null };
    return { key: 'simPosNth', ord: ordinal(e.position, lang) };
  }

  /* ---- runs of drawable peer points --------------------------------- */
  function peerRuns(points, xOfTime, medianStep) {
    var runs = [];
    var cur = [];
    var lastT = null;
    points.forEach(function (p) {
      var t = xOfTime(p.period);
      if (t === null || t === undefined) {
        if (cur.length) runs.push(cur);
        cur = [];
        lastT = null;
        return;
      }
      if (typeof p.value !== 'number') {
        if (cur.length) runs.push(cur);
        cur = [];
        lastT = null;
        return;
      }
      if (lastT !== null && medianStep > 0 && t - lastT > medianStep * 1.9) {
        if (cur.length) runs.push(cur);
        cur = [];
      }
      cur.push(p);
      lastT = t;
    });
    if (cur.length) runs.push(cur);
    return runs;
  }

  /* ---- nearest peer for hover/keyboard -------------------------------- */
  function nearestPeer(cands, pointerY, tol, communeY) {
    var communeDist = Math.abs(communeY - pointerY);
    var best = null;
    var bestDist = Infinity;
    (cands || []).forEach(function (c) {
      var d = Math.abs(c.y - pointerY);
      if (d > tol) return;
      if (d > communeDist) return;
      if (d < bestDist || (d === bestDist && best && c.rank < best.rank)) {
        best = c;
        bestDist = d;
      }
    });
    return best;
  }

  /* ---- model provisional flag ------------------------------------------ */
  function isProvisional(version) {
    return /-/.test(String(version || ''));
  }

  global.BPSimilar = {
    resolveList: resolveList,
    benchmarkState: benchmarkState,
    deviationParts: deviationParts,
    ordinal: ordinal,
    positionParts: positionParts,
    peerRuns: peerRuns,
    nearestPeer: nearestPeer,
    isProvisional: isProvisional,
  };
  if (typeof module !== 'undefined') module.exports = global.BPSimilar;
})(typeof window !== 'undefined' ? window : global);
