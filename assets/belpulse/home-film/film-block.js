/* assets/belpulse/home-film/film-block.js -- issue #312 batch 2.
 *
 * Click-to-play wiring for the `film` block type (src/pages/render.py's
 * _render_film, assets/belpulse/blocks/registry.json). Loaded only on a
 * page that carries at least one `film` block (src/pages/shell.py's
 * _has_block_type() check), so a page without one links none of this.
 *
 * WHAT THIS DOES NOT DO. It never sets `autoplay` and never calls
 * video.load() or video.play() before a click: the <video> element the
 * renderer emits already carries `preload="none"`, so nothing is fetched
 * until the reader asks for it (claude.md rule 30). This script's only two
 * jobs are (1) turn on native controls and start playback on the play
 * button's click, and (2) once the file's own real metadata exists (it
 * does not today, with preload="none" -- see below), show its run time in
 * the button's own label rather than a typed-in number (rule 36).
 *
 * Queries by class, not id, so a page can carry more than one film block
 * without this script needing to know how many or guess at ids.
 */
(function () {
  'use strict';

  function wire(frame) {
    var video = frame.querySelector('.film-video');
    var btn = frame.querySelector('.film-play');
    var label = frame.querySelector('.film-play-label');
    if (!video || !btn) return;

    function applyDuration() {
      if (!label) return;
      var tpl = label.getAttribute('data-with-duration');
      if (!tpl || !isFinite(video.duration) || video.duration <= 0) return;
      label.textContent = tpl.replace('{s}', Math.round(video.duration));
    }
    // Fires only once the browser has actually read the file's metadata --
    // with preload="none" that never happens before the click below, so in
    // today's build the label keeps its plain text. Wired anyway, so
    // nothing here needs revisiting if preload is ever loosened.
    video.addEventListener('loadedmetadata', applyDuration);
    applyDuration();

    btn.addEventListener('click', function () {
      video.setAttribute('controls', 'controls');
      video.controls = true;
      var p = video.play();
      if (p && typeof p.catch === 'function') p.catch(function () {});
      btn.hidden = true;
    });
  }

  var frames = document.querySelectorAll('.bp-block--film .film-frame');
  for (var i = 0; i < frames.length; i++) wire(frames[i]);
})();
